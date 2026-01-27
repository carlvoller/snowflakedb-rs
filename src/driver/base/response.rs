use std::{
    collections::{HashMap, VecDeque},
    io::Read,
};

use async_stream::try_stream;
#[cfg(feature = "arrow")]
use base64::Engine;
use flate2::read::GzDecoder;
use futures_util::stream::{BoxStream, FuturesOrdered, FuturesUnordered, StreamExt};
use serde::Deserialize;

static STATEMENT_TYPE_ID_SELECT: i64 = 0x1000;
static STATEMENT_TYPE_ID_DML: i64 = 0x3000;
static STATEMENT_TYPE_ID_MULTI_TABLE_INSERT: i64 = STATEMENT_TYPE_ID_DML + 0x500;
#[allow(dead_code)]
static STATEMENT_TYPE_ID_MULTI_STATEMENT: i64 = 0xA000;

use crate::{
    SnowflakeError,
    connection::Connection,
    driver::{base::bindings, primitives::column::Column},
    error,
    http::{self, GenericResponse, client::SnowflakeHttpClient},
    this_errors,
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
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct QueryStats {
    pub num_dml_duplicates: i64,
    pub num_rows_inserted: i64,
    pub num_rows_updated: i64,
    pub num_rows_deleted: i64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub(crate) struct RawQueryResponse {
    // pub(crate) parameters: Vec<super::NameValueParameter>,
    pub(crate) rowtype: Vec<Column>,

    #[serde(default)]
    pub(crate) rowset: Option<Vec<serde_json::Value>>,

    /// Only exists for Binary responses. (So only Arrow?)
    pub(crate) rowset_base64: Option<String>,

    pub(crate) total: i64,
    pub(crate) returned: i64,
    pub(crate) query_id: String,
    pub(crate) query_result_format: Option<String>,
    pub(crate) sql_state: Option<String>,

    pub(crate) number_of_binds: i32,
    pub(crate) meta_data_of_binds: Option<Vec<bindings::BindMetadata>>,

    pub(crate) statement_type_id: i64,
    pub(crate) version: i64,

    pub(crate) stats: Option<QueryStats>,

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
    pub fn is_dml(&self) -> bool {
        STATEMENT_TYPE_ID_DML <= self.statement_type_id
            && self.statement_type_id <= STATEMENT_TYPE_ID_MULTI_TABLE_INSERT
    }

    pub fn is_dql(&self) -> bool {
        self.statement_type_id == STATEMENT_TYPE_ID_SELECT
    }

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

            #[cfg(feature = "arrow")]
            if let Some(binary_data) = self.rowset_base64.take() && binary_data.len() > 0 {
                let batch = decode_base64(&binary_data)?;
                drop(binary_data);
                yield (0i64, batch);
            }

            let spawn_next = |chunks: &mut VecDeque<QueryResponseChunk>, conn: &Connection<C>, headers: &HashMap<String, String>| {
                if let Some(chunk) = chunks.pop_front() {
                    let conn = conn.clone();
                    let headers = headers.clone();
                    let url = chunk.url.clone();
                    let row_count = chunk.row_count;

                    Some(async move {
                        let request = http::RequestBuilder::default()
                            .full_url(url)
                            .headers(headers)
                            .connection(conn)
                            .build()
                            .map_err(|e| error!("failed to build get chunk request", e))?;

                        let resp = request.get_as_bytes().await?;

                        let output = if resp.len() > 2 && resp[0] == 0x1F && resp[1] == 0x8B {
                            let mut gz = GzDecoder::new(resp.as_slice());
                            let mut decoded = Vec::new();
                            gz.read_to_end(&mut decoded)
                                .map_err(|e| error!("failed to decompress chunk", e))?;
                            decoded
                        } else {
                            resp
                        };

                        Ok::<(i64, Vec<u8>), SnowflakeError>((row_count, output))
                    })
                } else {
                    None
                }
            };

            if conn.get_opts().download_chunks_in_order {
                let mut chunk_downloads = FuturesOrdered::new();
                for _ in 0..conn.get_opts().download_chunks_in_parallel {
                    if let Some(stream) = spawn_next(&mut self.chunks, &conn, &self.chunk_headers) {
                        chunk_downloads.push_back(stream);
                    }
                }

                while let Some(chunk) = chunk_downloads.next().await {
                    yield chunk?;

                    if let Some(stream) = spawn_next(&mut self.chunks, &conn, &self.chunk_headers) {
                        chunk_downloads.push_back(stream);
                    }
                }
            } else {
                let mut chunk_downloads = FuturesUnordered::new();
                    for _ in 0..conn.get_opts().download_chunks_in_parallel {
                    if let Some(stream) = spawn_next(&mut self.chunks, &conn, &self.chunk_headers) {
                        chunk_downloads.push(stream);
                    }
                }

                while let Some(chunk) = chunk_downloads.next().await {
                    yield chunk?;

                    if let Some(stream) = spawn_next(&mut self.chunks, &conn, &self.chunk_headers) {
                        chunk_downloads.push(stream);
                    }
                }
            }

        };

        Box::pin(stream)
    }
}

#[cfg(feature = "arrow")]
fn decode_base64(input: &str) -> Result<Vec<u8>, SnowflakeError> {
    let engine = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::general_purpose::PAD,
    );

    Ok(this_errors!(
        "failed to deserialise rowset_base64 into Vec<u8>",
        engine.decode(input)
    ))
}
