use crate::{
    SnowflakeError,
    driver::{Protocol, primitives::row::Row, query::Query},
    http::client::SnowflakeHttpClient,
};

pub trait Executor<C: SnowflakeHttpClient, T: Protocol>
where
    T::Query<C>: Query<C>,
{
    /// Returns a Query. You can bind parameters to the Query, or execute it to get a Stream.
    fn query(
        &mut self,
        query: impl ToString,
    ) -> impl Future<Output = Result<T::Query<C>, SnowflakeError>>;

    /// Returns a QueryResult. This buffers the entire result before returning.
    fn fetch_all(
        &mut self,
        query: impl ToString,
    ) -> impl Future<Output = Result<Vec<Row>, SnowflakeError>>;

    /// Pings the Snowflake Database. Useful for checking if credentials are valid and server is up.
    fn ping(&mut self) -> impl Future<Output = Result<(), SnowflakeError>>;
}
