use crate::{
    SnowflakeError,
    driver::{
        Protocol,
        primitives::row::Row,
        query::{Query, QueryDescribeResult},
    },
    http::client::SnowflakeHttpClient,
};

pub trait Executor<'a, C: SnowflakeHttpClient + 'a, T: Protocol>
where
    T::Query<'a, C>: Query<'a, C>,
{
    /// Returns a Query. You can bind parameters to the Query, or execute it to get a Stream.
    fn fetch<'b>(
        &'b mut self,
        query: impl ToString,
    ) -> impl Future<Output = Result<T::Query<'b, C>, SnowflakeError>>
    where
        'a: 'b;

    /// Returns a QueryResult. This buffers the entire result before returning.
    fn fetch_all<'b>(
        &'b mut self,
        query: impl ToString,
    ) -> impl Future<Output = Result<Vec<Row>, SnowflakeError>>
    where
        'a: 'b;

    /// Describe the `query`. Useful for getting information on the columns returned, or required bind parameters.
    fn describe<'b>(
        &'b mut self,
        query: impl ToString,
    ) -> impl Future<Output = Result<QueryDescribeResult, SnowflakeError>>
    where
        'a: 'b;

    /// Pings the Snowflake Database. Useful for checking if credentials are valid and server is up.
    fn ping<'b>(&'b mut self) -> impl Future<Output = Result<(), SnowflakeError>>
    where
        'a: 'b;
}
