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
    type Describe: DescribeResult;

    fn new(query: impl ToString, session: &'a mut Session<C>) -> Self;

    fn bind_row(&mut self, params: Vec<impl ToCellValue>);
    fn bind_row_named(&mut self, params: Vec<(impl ToString, impl ToCellValue)>);

    fn execute(self) -> impl Future<Output = Result<Self::Result, SnowflakeError>>;
    fn describe(self) -> impl Future<Output = Result<Self::Describe, SnowflakeError>>;
}

pub trait QueryResult {
    /// Returns Column Metadata. Useful for knowing what kind of data will be returned.
    fn columns(&self) -> Vec<Arc<Column>>;

    /// Returns `true` if the query was a DML Query. For example, an `UPDATE`, `INSERT` or `DELETE`.
    fn is_dml(&self) -> bool;

    /// Returns `true` if the query was a DQL Query. For example, a `SELECT`.
    fn is_dql(&self) -> bool;

    /// Returns how many rows were affected by any DML Query. Returns `0` if none were affected, or query was not a DML Query.
    fn rows_affected(&self) -> i64;

    /// Returns how many rows were affected by an `UPDATE`. Returns `0` if none were affected, or query was not an `UPDATE`.
    fn rows_updated(&self) -> i64;

    /// Returns how many rows were deleted. Returns `0` if none were deleted, or query was not a `DELETE`.
    fn rows_deleted(&self) -> i64;

    /// Returns how many rows were inserted. Returns `0` if none was inserted, or query was not an `INSERT`.
    fn rows_inserted(&self) -> i64;

    /// Returns how many rows are expected from the query. If the query was a DML Query, this returns 1.
    fn expected_result_length(&self) -> i64;

    fn rows(self) -> BoxStream<'static, Result<row::Row, SnowflakeError>>;
}

pub trait DescribeResult {
    /// Returns Column Metadata. Useful for knowing what kind of data will be returned.
    fn columns(&self) -> Vec<Arc<Column>>;

    /// Returns Bind Metadata. Useful for knowing what parameters this query expects when actually executed.
    fn bind_metadata(&self) -> Option<Vec<BindMetadata>>;

    /// Returns how many parameters this query expects.
    fn bind_count(&self) -> i32;
}

// #[derive(Debug)]
// /// Metadata for a [`Query`](`crate::driver::query::Query`)
// pub struct QueryDescribeResult {
//     pub num_of_binds: i32,
//     pub bind_metadata: Option<Vec<BindMetadata>>,
//     pub columns: Vec<Arc<Column>>,
// }
