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
    protocols::{JsonQuery, JsonQueryResult},
    query::{DescribeResult, Query, QueryResult},
};

#[cfg(feature = "arrow")]
pub use driver::protocols::{ArrowQuery, ArrowQueryResult};

pub use http::client::SnowflakeHttpClient;

pub use executor::Executor;

pub use transaction::Transaction;

#[cfg(test)]
mod tests {}
