use std::sync::Arc;

use crate::{
    SnowflakeError,
    auth::session::Session,
    driver::{
        Protocol,
        primitives::row::Row,
        query::{Query, QueryResult},
    },
    error,
    executor::Executor,
    http::client::SnowflakeHttpClient,
};

use futures_util::{StreamExt, lock::Mutex};

#[cfg(test)]
#[path = "./transaction_test.rs"]
mod transaction_test;

/// Holds a snowflake transaction. Can be committed or rollbacked.
pub struct SnowflakeTransaction<C: SnowflakeHttpClient, T: Protocol> {
    _protocol: T,

    // I use a futures_util::lock::Mutex for Session is expected to be locked over an await point
    pub(crate) session: Option<Arc<futures_util::lock::Mutex<Session<C>>>>,
}

impl<C: SnowflakeHttpClient, T: Protocol> SnowflakeTransaction<C, T> {
    pub(crate) fn new(p: T, session: Session<C>) -> Self {
        Self {
            _protocol: p,
            session: Some(Arc::new(Mutex::new(session))),
        }
    }

    pub async fn commit(self) -> Result<(), SnowflakeError> {
        if let Some(existing) = self.session.as_ref() {
            let weak = Arc::downgrade(existing);
            let q = T::Query::new("COMMIT;", weak);
            q.execute().await?;
            Ok(())
        } else {
            Err(error!(
                "The underlying session for this transaction is dead."
            ))
        }
    }

    pub async fn rollback(self) -> Result<(), SnowflakeError> {
        if let Some(existing) = self.session.as_ref() {
            let weak = Arc::downgrade(existing);
            let q = T::Query::new("ROLLBACK;", weak);
            q.execute().await?;
            Ok(())
        } else {
            Err(error!(
                "The underlying session for this transaction is dead."
            ))
        }
    }
}

impl<C: SnowflakeHttpClient, T: Protocol> Executor<C, T> for SnowflakeTransaction<C, T> {
    async fn query(&mut self, query: impl ToString) -> Result<T::Query<C>, crate::SnowflakeError> {
        if let Some(existing) = self.session.as_ref() {
            let weak = Arc::downgrade(existing);
            let query = T::Query::new(query, weak);
            Ok(query)
        } else {
            Err(error!(
                "The underlying session for this transaction is dead."
            ))
        }
    }

    async fn fetch_all(&mut self, query: impl ToString) -> Result<Vec<Row>, crate::SnowflakeError> {
        if let Some(existing) = self.session.as_ref() {
            let weak = Arc::downgrade(existing);
            let query = T::Query::new(query, weak);

            let results = query.execute().await?;
            let expected_result_len = results.expected_result_length();

            let mut rows = Vec::with_capacity(expected_result_len as usize);
            let mut row_stream = results.rows();

            while let Some(row) = row_stream.next().await {
                let row = row?;
                rows.push(row);
            }

            Ok(rows)
        } else {
            Err(error!(
                "The underlying session for this transaction is dead."
            ))
        }
    }

    async fn ping(&mut self) -> Result<(), crate::SnowflakeError> {
        self.fetch_all("SELECT 1;").await?;
        Ok(())
    }
}
