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
    protocols::{JsonProtocol, JsonQuery, JsonQueryResult},
    query::{DescribeResult, Query, QueryResult},
};

pub use transaction::SnowflakeTransaction;

#[cfg(feature = "reqwest")]
pub type JsonSnowflakeConnection<'a, C = reqwest::Client> =
    SnowflakeConnection<'a, C, JsonProtocol>;

#[cfg(not(feature = "reqwest"))]
pub type JsonSnowflakeConnection<'a, C> = SnowflakeConnection<'a, C, JsonProtocol>;

#[cfg(feature = "reqwest")]
pub type JsonSnowflakeTransaction<C = reqwest::Client> = SnowflakeTransaction<C, JsonProtocol>;

#[cfg(not(feature = "reqwest"))]
pub type JsonSnowflakeTransaction<C> = SnowflakeTransaction<C, JsonProtocol>;

#[cfg(feature = "reqwest")]
pub type JsonSnowflakePool<C = reqwest::Client> = SnowflakePool<C, JsonProtocol>;

#[cfg(not(feature = "reqwest"))]
pub type JsonSnowflakePool<C> = SnowflakePool<C, JsonProtocol>;

#[cfg(all(feature = "reqwest", feature = "arrow"))]
pub type ArrowSnowflakeConnection<'a, C = reqwest::Client> =
    SnowflakeConnection<'a, C, ArrowProtocol>;

#[cfg(all(not(feature = "reqwest"), feature = "arrow"))]
pub type ArrowSnowflakeConnection<'a, C> = SnowflakeConnection<'a, C, ArrowProtocol>;

#[cfg(all(feature = "reqwest", feature = "arrow"))]
pub type ArrowSnowflakeTransaction<C = reqwest::Client> = SnowflakeTransaction<C, ArrowProtocol>;

#[cfg(all(not(feature = "reqwest"), feature = "arrow"))]
pub type ArrowSnowflakeTransaction<C> = SnowflakeTransaction<C, ArrowProtocol>;

#[cfg(all(feature = "reqwest", feature = "arrow"))]
pub type ArrowSnowflakePool<C = reqwest::Client> = SnowflakePool<C, ArrowProtocol>;

#[cfg(all(not(feature = "reqwest"), feature = "arrow"))]
pub type ArrowSnowflakePool<C> = SnowflakePool<C, ArrowProtocol>;

#[cfg(feature = "arrow")]
pub use driver::protocols::{ArrowProtocol, ArrowQuery, ArrowQueryResult};

pub use http::client::SnowflakeHttpClient;

pub use executor::Executor;

#[cfg(test)]
mod tests {}
