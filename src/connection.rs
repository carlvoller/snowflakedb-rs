use std::{collections::VecDeque, sync::Arc};

use derive_builder::Builder;
use futures_util::StreamExt;

#[cfg(feature = "arrow")]
use crate::driver::protocols::ArrowProtocol;
use crate::{
    SnowflakeError,
    auth::{self, AuthStrategy, session::Session},
    driver::{
        Protocol,
        primitives::row::Row,
        protocols::JsonProtocol,
        query::{Query, QueryResult},
    },
    error,
    executor::Executor,
    http::client::SnowflakeHttpClient,
    transaction::SnowflakeTransaction,
};

use futures_util::lock::Mutex;

#[derive(Builder, Debug, Clone)]
pub struct SnowflakeConnectionOpts {
    pub(crate) pool_size: usize,

    pub(crate) strategy: AuthStrategy,

    #[builder(setter(into))]
    pub(crate) account_id: String,

    #[builder(setter(into))]
    pub(crate) username: String,

    #[builder(setter(into, strip_option))]
    pub(crate) database: Option<String>,

    #[builder(setter(into, strip_option))]
    pub(crate) schema: Option<String>,

    #[builder(setter(into, strip_option))]
    pub(crate) role: Option<String>,

    #[builder(setter(into, strip_option))]
    pub(crate) warehouse: Option<String>,

    /// Override the Snowflake API endpoint used. Useful for region specific or private snowflake instances.
    ///
    /// If unset, this will default to `https://{account_id}.snowflakecomputing.com`
    ///
    /// 如果你是在中国境内连接，请将此项设置为 `https://{account_id}.snowflakecomputing.cn`
    #[builder(setter(into, strip_option), default = None)]
    pub(crate) host: Option<String>,

    /// Enable downloading multiple chunks at once
    #[builder(setter(into), default = 1)]
    pub(crate) download_chunks_in_parallel: usize,

    /// Should parallel chunks be streamed in order? Set this to false for better performance
    #[builder(setter(into), default = true)]
    pub(crate) download_chunks_in_order: bool,
}

impl SnowflakeConnectionOpts {
    #[cfg(feature = "arrow")]
    pub async fn connect_arrow_with_client<C: SnowflakeHttpClient>(
        self,
    ) -> Result<SnowflakePool<C, ArrowProtocol>, SnowflakeError> {
        let client = C::new();

        let pool_size = self.pool_size;

        let connection = Connection {
            client: client.clone(),
            opts: Arc::new(self),
        };

        let mut sessions = VecDeque::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push_back(session);
        }

        Ok(SnowflakePool {
            _protocol: ArrowProtocol::default(),
            conn: connection,
            pool: Arc::new(std::sync::Mutex::new(sessions)),
        })
    }

    pub async fn connect_json_with_client<C: SnowflakeHttpClient>(
        self,
    ) -> Result<SnowflakePool<C, JsonProtocol>, SnowflakeError> {
        let client = C::new();

        let pool_size = self.pool_size;

        let connection = Connection {
            client: client.clone(),
            opts: Arc::new(self),
        };

        let mut sessions = VecDeque::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push_back(session);
        }

        Ok(SnowflakePool {
            _protocol: JsonProtocol::default(),
            conn: connection,
            pool: Arc::new(std::sync::Mutex::new(sessions)),
        })
    }

    #[cfg(feature = "reqwest")]
    pub async fn connect_json(
        self,
    ) -> Result<SnowflakePool<reqwest::Client, JsonProtocol>, SnowflakeError> {
        let client = reqwest::Client::new();

        let pool_size = self.pool_size;

        let connection = Connection {
            client: client.clone(),
            opts: Arc::new(self),
        };

        let mut sessions = VecDeque::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push_back(session);
        }

        Ok(SnowflakePool {
            _protocol: JsonProtocol::default(),
            conn: connection,
            pool: Arc::new(std::sync::Mutex::new(sessions)),
        })
    }

    #[cfg(all(feature = "reqwest", feature = "arrow"))]
    pub async fn connect_arrow(
        self,
    ) -> Result<SnowflakePool<reqwest::Client, ArrowProtocol>, SnowflakeError> {
        let client = reqwest::Client::new();

        let pool_size = self.pool_size;

        let connection = Connection {
            client: client.clone(),
            opts: Arc::new(self),
        };

        let mut sessions = VecDeque::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push_back(session);
        }

        Ok(SnowflakePool {
            _protocol: ArrowProtocol::default(),
            conn: connection,
            pool: Arc::new(std::sync::Mutex::new(sessions)),
        })
    }
}

#[derive(Clone)]
pub(crate) struct Connection<C>
where
    C: Clone,
{
    client: C,
    opts: Arc<SnowflakeConnectionOpts>,
}

impl<C> Connection<C>
where
    C: SnowflakeHttpClient + Clone,
{
    pub(crate) fn get_opts(&self) -> Arc<SnowflakeConnectionOpts> {
        self.opts.clone()
    }

    pub(crate) fn get_client(&self) -> C {
        self.client.clone()
    }
}

#[derive(Clone)]
pub struct SnowflakePool<C: SnowflakeHttpClient + Clone, T: Protocol> {
    _protocol: T,
    conn: Connection<C>,

    // Use a std::sync::Mutex here because I won't be holding this lock over an await
    // and i need to use this in a impl Drop
    pub(crate) pool: Arc<std::sync::Mutex<VecDeque<Session<C>>>>,
}

impl<C: SnowflakeHttpClient + Clone, T: Protocol> SnowflakePool<C, T> {
    pub async fn get<'a>(&'a self) -> Result<SnowflakeConnection<C, T>, SnowflakeError> {
        let conn = self.find_session().await?;
        Ok(conn)
    }

    async fn find_session(&self) -> Result<SnowflakeConnection<C, T>, SnowflakeError> {
        // Is this generally safe to do??
        // If the mutex is poisoned i should want to crash the program right?
        let session = {
            let mut pool = self.pool.lock().unwrap();
            pool.pop_front()
        };

        if let Some(session) = session {
            let is_session_dirty = session.is_dirty;
            let session_wrapped = Arc::new(Mutex::new(session));
            let weak = Arc::downgrade(&session_wrapped);

            // If a previous connection ran BEGIN, this ensures the next consumer isn't using an existing transaction
            if is_session_dirty {
                let q = T::Query::new("ROLLBACK;", weak);
                q.execute().await?;
            }

            return Ok(SnowflakeConnection {
                _protocol: self._protocol.clone(),
                session: Some(session_wrapped),
                pool: self.pool.clone(),
            });
        }

        Err(error!("no available sessions"))
    }

    pub async fn begin(&self) -> Result<SnowflakeTransaction<C, T>, SnowflakeError> {
        let session = auth::session::Session::new(self.conn.clone()).await?;
        let mut transaction = SnowflakeTransaction::new(self._protocol.clone(), session);
        transaction.query("BEGIN;").await?.execute().await?;
        Ok(transaction)
    }
}

pub struct SnowflakeConnection<C: SnowflakeHttpClient + Clone, T: Protocol> {
    _protocol: T,

    // I use a futures_util::lock::Mutex for Session is expected to be locked over an await point
    pub(crate) session: Option<Arc<futures_util::lock::Mutex<Session<C>>>>,

    // I use a std::sync::Mutex here because the pool lock is only expected to be held for a very
    // short amount of time. It is not expected to be held over an await point
    pub(crate) pool: Arc<std::sync::Mutex<VecDeque<Session<C>>>>,
}

impl<'a, C: SnowflakeHttpClient, T: Protocol> Drop for SnowflakeConnection<C, T> {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let mut pool = self.pool.lock().unwrap();
            if let Some(session) = Arc::into_inner(session) {
                let mut session = session.into_inner();
                session.is_dirty = true;
                pool.push_back(session);
            }
        }
    }
}

impl<C: SnowflakeHttpClient, T: Protocol> Executor<C, T> for SnowflakeConnection<C, T> {
    async fn query(&mut self, query: impl ToString) -> Result<T::Query<C>, crate::SnowflakeError> {
        if let Some(existing) = self.session.as_ref() {
            let weak = Arc::downgrade(existing);
            let query = T::Query::new(query, weak);
            Ok(query)
        } else {
            Err(error!(
                "The underlying session for this connection is dead."
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
                "The underlying session for this connection is dead."
            ))
        }
    }

    async fn ping(&mut self) -> Result<(), crate::SnowflakeError> {
        self.fetch_all("SELECT 1;").await?;
        Ok(())
    }
}
