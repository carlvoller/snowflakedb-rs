use std::{
    sync::Arc,
    sync::{Mutex, MutexGuard},
};

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
    transaction::Transaction,
};

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
    pub(crate) download_chunks_in_order: bool
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

        let mut sessions = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push(Arc::new(Mutex::new(session)));
        }

        Ok(SnowflakePool {
            _protocol: ArrowProtocol::default(),
            conn: connection,
            pool: sessions,
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

        let mut sessions = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push(Arc::new(Mutex::new(session)));
        }

        Ok(SnowflakePool {
            _protocol: JsonProtocol::default(),
            conn: connection,
            pool: sessions,
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

        let mut sessions = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push(Arc::new(Mutex::new(session)));
        }

        Ok(SnowflakePool {
            _protocol: JsonProtocol::default(),
            conn: connection,
            pool: sessions,
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

        let mut sessions = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let session = auth::session::Session::new(connection.clone()).await?;
            sessions.push(Arc::new(Mutex::new(session)));
        }

        Ok(SnowflakePool {
            _protocol: ArrowProtocol::default(),
            conn: connection,
            pool: sessions,
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

pub struct SnowflakePool<C: SnowflakeHttpClient + Clone, T: Protocol> {
    _protocol: T,
    conn: Connection<C>,
    pub(crate) pool: Vec<Arc<Mutex<Session<C>>>>,
}

impl<C: SnowflakeHttpClient + Clone, T: Protocol> SnowflakePool<C, T> {
    pub async fn get<'a>(&'a self) -> Result<SnowflakeConnection<'a, C, T>, SnowflakeError> {
        let mut conn = self.find_session()?;

        // If a previous connection ran BEGIN, this ensures the next consumer isn't using an existing transaction
        if conn.session.is_dirty {
            let q = T::Query::new("ROLLBACK;", &mut conn.session);
            q.execute().await?;
        }

        Ok(conn)
    }

    fn find_session<'a>(&'a self) -> Result<SnowflakeConnection<'a, C, T>, SnowflakeError> {
        for session_lock in &self.pool {
            if let Ok(guard) = session_lock.try_lock() {
                return Ok(SnowflakeConnection {
                    _protocol: self._protocol.clone(),
                    session: guard,
                });
            }
        }

        Err(error!("no available sessions"))
    }

    pub async fn begin(&self) -> Result<Transaction<C, T>, SnowflakeError> {
        let mut session = auth::session::Session::new(self.conn.clone()).await?;
        let q = T::Query::new("BEGIN;", &mut session);
        q.execute().await?;
        Ok(Transaction::new(self._protocol.clone(), session))
    }
}

pub struct SnowflakeConnection<'a, C: SnowflakeHttpClient + Clone, T: Protocol> {
    _protocol: T,
    pub(crate) session: MutexGuard<'a, Session<C>>,
}

impl<'a, C: SnowflakeHttpClient, T: Protocol> Drop for SnowflakeConnection<'a, C, T> {
    fn drop(&mut self) {
        self.session.is_dirty = true;
    }
}

impl<'a, C: SnowflakeHttpClient, T: Protocol> Executor<'a, C, T> for SnowflakeConnection<'a, C, T> {
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
