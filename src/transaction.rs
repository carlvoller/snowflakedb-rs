use crate::{
    SnowflakeError,
    auth::session::Session,
    driver::{
        Protocol,
        primitives::row::Row,
        query::{Query, QueryResult},
    },
    executor::Executor,
    http::client::SnowflakeHttpClient,
};

use futures_util::StreamExt;

#[cfg(test)]
#[path = "./transaction_test.rs"]
mod transaction_test;

/// Holds a snowflake transaction. Can be committed or rollbacked.
pub struct Transaction<C: SnowflakeHttpClient, T: Protocol> {
    _protocol: T,
    session: Session<C>,
}

impl<C: SnowflakeHttpClient, T: Protocol> Transaction<C, T> {
    pub(crate) fn new(p: T, session: Session<C>) -> Self {
        Self {
            _protocol: p,
            session,
        }
    }

    pub async fn commit(mut self) -> Result<(), SnowflakeError> {
        let q = T::Query::new("COMMIT;", &mut self.session);
        q.execute().await?;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), SnowflakeError> {
        let q = T::Query::new("ROLLBACK;", &mut self.session);
        q.execute().await?;
        Ok(())
    }
}

impl<'a, C: SnowflakeHttpClient, T: Protocol> Executor<'a, C, T> for Transaction<C, T> {
    async fn query<'b>(
        &'b mut self,
        query: impl ToString,
    ) -> Result<T::Query<'b, C>, crate::SnowflakeError>
    where
        'a: 'b,
    {
        let query = T::Query::new(query, &mut self.session);
        Ok(query)
    }

    async fn fetch_all<'b>(
        &'b mut self,
        query: impl ToString,
    ) -> Result<Vec<Row>, crate::SnowflakeError>
    where
        'a: 'b,
    {
        let query = T::Query::new(query, &mut self.session);

        let results = query.execute().await?;
        let expected_result_len = results.expected_result_length();

        let mut rows = Vec::with_capacity(expected_result_len as usize);
        let mut row_stream = results.rows();

        while let Some(row) = row_stream.next().await {
            let row = row?;
            rows.push(row);
        }

        Ok(rows)
    }

    async fn ping<'b>(&'b mut self) -> Result<(), crate::SnowflakeError>
    where
        'a: 'b,
    {
        self.fetch_all("SELECT 1;").await?;
        Ok(())
    }
}
