use std::{
    collections::VecDeque,
    io::{Cursor, Read},
    sync::Arc,
};

use async_stream::try_stream;

use crate::{
    SnowflakeError,
    auth::session::Session,
    connection::Connection,
    driver::{
        Protocol,
        base::{BinaryQueryBuilder, bindings::Bindings, response::RawQueryResponse},
        primitives::{column::Column, row::Row},
        query::{Query, QueryDescribeResult, QueryResult},
    },
    error,
    http::client::SnowflakeHttpClient,
    this_errors,
};

use futures_util::TryStreamExt;

#[derive(Clone)]
pub struct JsonProtocol {}

impl Protocol for JsonProtocol {
    type Query<'a, C>
        = JsonQuery<'a, C>
    where
        C: SnowflakeHttpClient + 'a;
}

impl Default for JsonProtocol {
    fn default() -> Self {
        Self {}
    }
}

pub struct JsonQuery<'a, C: SnowflakeHttpClient> {
    session: &'a mut Session<C>,
    bindings: Bindings,
    query: String,
}

impl<'a, C: SnowflakeHttpClient> Query<'a, C> for JsonQuery<'a, C> {
    type Result = JsonQueryResult<C>;

    fn bind_row(&mut self, params: Vec<impl crate::driver::primitives::cell::ToCellValue>) {
        self.bindings.bind_row(params);
    }

    fn bind_row_named(
        &mut self,
        params: Vec<(
            impl ToString,
            impl crate::driver::primitives::cell::ToCellValue,
        )>,
    ) {
        self.bindings.bind_row_named(params);
    }

    fn new(query: impl ToString, session: &'a mut Session<C>) -> Self {
        Self {
            session,
            bindings: Bindings::new(),
            query: query.to_string(),
        }
    }

    async fn describe(
        mut self,
    ) -> Result<crate::driver::query::QueryDescribeResult, SnowflakeError> {
        let query = this_errors!(
            "failed to build underlying binary query",
            BinaryQueryBuilder::default()
                .accept_header("application/json")
                .sql_text(self.query)
                .is_describe_only(true)
                .bindings(self.bindings)
                .build()
        );

        let raw = query.run(&mut self.session).await?;

        let cols = raw
            .rowtype
            .clone()
            .into_iter()
            .map(|x| Arc::new(x))
            .collect::<Vec<Arc<Column>>>();

        Ok(QueryDescribeResult {
            num_of_binds: raw.number_of_binds,
            bind_metadata: raw.meta_data_of_binds,
            columns: cols,
        })
    }

    async fn execute(mut self) -> Result<Self::Result, SnowflakeError> {
        let query = this_errors!(
            "failed to build underlying binary query",
            BinaryQueryBuilder::default()
                .accept_header("application/json")
                .sql_text(self.query)
                .is_describe_only(false)
                .bindings(self.bindings)
                .build()
        );

        let raw = query.run(&mut self.session).await?;

        let cols = raw
            .rowtype
            .clone()
            .into_iter()
            .map(|x| Arc::new(x))
            .collect::<Vec<Arc<Column>>>();

        Ok(JsonQueryResult {
            conn: self.session.get_conn(),
            raw,
            cols,
        })
    }
}

pub struct JsonQueryResult<C: SnowflakeHttpClient + Clone> {
    conn: Connection<C>,
    raw: RawQueryResponse,

    cols: Vec<Arc<Column>>,
}

impl<C: SnowflakeHttpClient + Clone> QueryResult for JsonQueryResult<C> {
    fn expected_result_length(&self) -> i64 {
        self.raw.total
    }

    fn columns(&self) -> Vec<Arc<Column>> {
        self.cols.clone()
    }

    fn rows(
        self,
    ) -> futures_util::stream::BoxStream<
        'static,
        Result<crate::driver::primitives::row::Row, SnowflakeError>,
    > {
        let mut retrieved_first_chunk = false;
        let (_total, mut _retrieved, mut cursor) = (self.raw.total, self.raw.returned, 0i64);
        let cols = self.columns();
        let mut raw_stream = self.raw.stream_chunks(self.conn);

        let stream = try_stream! {
            while let Some((_row_count, chunk)) = raw_stream.try_next().await? {

                // println!("received new chunk. total_rows={:?} retrieved_rows={:?} cursor={:?}", total, retrieved, cursor);

                // _retrieved += row_count;

                let mut chunk_as_json_slice: VecDeque<Vec<Option<String>>> = if retrieved_first_chunk {
                    let prefix = Cursor::new(b"[");
                    let chunk_data = Cursor::new(chunk);
                    let suffix = Cursor::new(b"]");

                    serde_json::from_reader(prefix.chain(chunk_data.chain(suffix))).map_err(|e| error!("failed to parse chunk data as json", e))?
                } else {
                    retrieved_first_chunk = true;
                    serde_json::from_reader(chunk.as_slice()).map_err(|e| error!("failed to parse chunk data as json", e))?
                };

                while let Some(row) = chunk_as_json_slice.pop_front() {
                    yield Row::new(cols.clone(), row, cursor);
                    cursor += 1;
                }
            }
        };

        Box::pin(stream)
    }
}
