use std::sync::Arc;

use futures_util::stream::BoxStream;

use crate::{
    SnowflakeError,
    auth::session::Session,
    driver::{
        base::bindings::BindMetadata,
        primitives::{cell::ToCellValue, column::Column, row},
    },
    http::client::SnowflakeHttpClient,
};

pub trait Query<'a, C: SnowflakeHttpClient> {
    type Result: QueryResult;

    fn new(query: impl ToString, session: &'a mut Session<C>) -> Self;

    fn bind_row(&mut self, params: Vec<impl ToCellValue>);
    fn bind_row_named(&mut self, params: Vec<(impl ToString, impl ToCellValue)>);

    fn execute(self) -> impl Future<Output = Result<Self::Result, SnowflakeError>>;
    fn describe(self) -> impl Future<Output = Result<QueryDescribeResult, SnowflakeError>>;
}

pub trait QueryResult {
    fn expected_result_length(&self) -> i64;
    fn columns(&self) -> Vec<Arc<Column>>;

    fn rows(self) -> BoxStream<'static, Result<row::Row, SnowflakeError>>;
}

#[derive(Debug)]
/// Metadata for a [`Query`](`crate::driver::query::Query`)
pub struct QueryDescribeResult {
    pub num_of_binds: i32,
    pub bind_metadata: Option<Vec<BindMetadata>>,
    pub columns: Vec<Arc<Column>>,
}
