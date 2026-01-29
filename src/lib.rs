pub mod auth;
pub(crate) mod connection;
pub(crate) mod driver;
pub(crate) mod errors;
pub(crate) mod executor;
pub(crate) mod http;
pub(crate) mod transaction;

pub(crate) use errors::{SnowflakeError, error, this_errors};

pub use connection::{
    SnowflakeConnection, SnowflakeConnectionOpts, SnowflakeConnectionOptsBuilder, SnowflakePool,
};
pub use driver::{
    primitives::{
        cell::{Cell, CellValue, ToCellValue},
        column::{Column, ColumnType},
        row::Row,
    },
    protocols::{JsonDescribeResult, JsonProtocol},
    query::{DescribeResult, Query, QueryResult},
};

pub use transaction::SnowflakeTransaction;

pub struct NoClient;

#[cfg(feature = "reqwest")]
pub type DefaultClient = reqwest::Client;

#[cfg(not(feature = "reqwest"))]
pub type DefaultClient = NoClient;

pub type JsonSnowflakeConnection<C = DefaultClient> = SnowflakeConnection<C, JsonProtocol>;
pub type JsonSnowflakeTransaction<C = DefaultClient> = SnowflakeTransaction<C, JsonProtocol>;
pub type JsonSnowflakePool<C = DefaultClient> = SnowflakePool<C, JsonProtocol>;
pub type JsonQuery<C = DefaultClient> = JQ<C>;
pub type JsonQueryResult<C = DefaultClient> = JQR<C>;

use crate::driver::protocols::{JsonQuery as JQ, JsonQueryResult as JQR};

#[cfg(feature = "arrow")]
pub type ArrowSnowflakeConnection<C = DefaultClient> = SnowflakeConnection<C, ArrowProtocol>;
#[cfg(feature = "arrow")]
pub type ArrowSnowflakeTransaction<C = DefaultClient> = SnowflakeTransaction<C, ArrowProtocol>;
#[cfg(feature = "arrow")]
pub type ArrowSnowflakePool<C = DefaultClient> = SnowflakePool<C, ArrowProtocol>;
#[cfg(feature = "arrow")]
pub type ArrowQuery<C = DefaultClient> = AQ<C>;
#[cfg(feature = "arrow")]
pub type ArrowQueryResult<C = DefaultClient> = AQR<C>;

pub use http::client::SnowflakeHttpClient;

pub use executor::Executor;

#[cfg(feature = "arrow")]
use crate::driver::protocols::{ArrowQuery as AQ, ArrowQueryResult as AQR};

#[cfg(feature = "arrow")]
pub use crate::driver::protocols::{ArrowDescribeResult, ArrowProtocol};

#[cfg(test)]
mod tests {}
