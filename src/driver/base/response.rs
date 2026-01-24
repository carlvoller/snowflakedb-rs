use std::{
    collections::{HashMap, VecDeque},
    io::Read,
};

use async_stream::try_stream;
use flate2::read::GzDecoder;
use futures_util::stream::BoxStream;
use serde::Deserialize;

use crate::{
    SnowflakeError,
    connection::Connection,
    driver::{base::bindings, primitives::column::Column},
    error,
    http::{self, GenericResponse, client::SnowflakeHttpClient},
};

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct QueryResponseChunk {
    pub url: String,
    pub row_count: i64,
    pub uncompressed_size: i64,
    pub compressed_size: i64,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AsynchronousResponseData {
    pub get_result_url: String,
    pub query_aborts_after_secs: i32,
    pub query_id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FileTransferResponseData {
    pub parallel: i64,
    pub threshold: i64,

    pub presigned_urls: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Fixed,
    Real,
    Text,
    Date,
    Variant,
    TimestampLtz,
    TimestampNtz,
    TimestampTz,
    Object,
    Binary,
    Time,
    Boolean,
    Array,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct RawQueryResponse {
    // pub(crate) parameters: Vec<super::NameValueParameter>,
    pub(crate) rowtype: Vec<Column>,

    #[serde(default)]
    pub(crate) rowset: Option<Vec<serde_json::Value>>,

    /// Only exists for Arrow responses
    pub(crate) rowsetbase64: Option<String>,

    pub(crate) total: i64,
    pub(crate) returned: i64,
    pub(crate) query_id: String,
    pub(crate) sql_state: Option<String>,

    pub(crate) number_of_binds: i32,
    pub(crate) meta_data_of_binds: Option<Vec<bindings::BindMetadata>>,

    pub(crate) statement_type_id: i64,
    pub(crate) version: i64,

    #[serde(default)]
    pub(crate) chunks: VecDeque<QueryResponseChunk>,
    #[serde(default)]
    pub(crate) chunk_headers: HashMap<String, String>,

    // no idea what this is but looks important
    pub(crate) qrmk: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
#[allow(dead_code)]
pub(crate) enum ExecResponseData {
    Query(RawQueryResponse),
    AsyncQuery(AsynchronousResponseData),
    PutGet(FileTransferResponseData),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ExecResponseErrorData {
    pub age: i64,
    pub error_code: String,
    pub query_id: String,
    pub sql_state: String,

    pub line: Option<i64>,
    pub pos: Option<i64>,

    #[serde(default)]
    pub internal_error: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ExecResponse {
    Success(GenericResponse<ExecResponseData>),
    Failure(GenericResponse<ExecResponseErrorData>),
}

impl RawQueryResponse {
    pub fn stream_chunks<C: SnowflakeHttpClient>(
        mut self,
        conn: Connection<C>,
    ) -> BoxStream<'static, Result<(i64, Vec<u8>), SnowflakeError>> {
        let stream = try_stream! {

            // Reserialize JSON array. This is not ideal, but using RawValue fails with the enum response match.
            if let Some(data) = self.rowset.take() && data.len() > 0 {
                let data_vec = serde_json::to_vec(&data).map_err(|e| error!("failed to serialise rowset json into Vec<u8>", e))?;
                drop(data);
                yield (0i64, data_vec);
            }

            while let Some(chunk) = self.chunks.pop_front() {
                let request = http::RequestBuilder::default()
                    .full_url(chunk.url)
                    .headers(self.chunk_headers.clone())
                    .connection(conn.clone())
                    .build()
                    .map_err(|e| error!("failed to build get chunk request", e))?;

                let resp = request.get_as_bytes().await?;

                if resp[0] == 0x1F && resp[1] == 0x8B {
                    let mut gz = GzDecoder::new(resp.as_slice());
                    let mut output = Vec::new();
                    gz.read_to_end(&mut output)
                        .map_err(|e| error!("failed to decompress chunk", e))?;
                    yield (chunk.row_count, output);
                } else {
                    yield (chunk.row_count, resp.to_vec());
                }
            }
        };

        Box::pin(stream)
    }
}
