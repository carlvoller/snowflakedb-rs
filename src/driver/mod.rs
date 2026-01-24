use crate::{driver::query::Query, http::client::SnowflakeHttpClient};

pub(crate) mod base;
pub mod primitives;
pub mod protocols;
pub mod query;

pub trait Protocol: Clone {
    type Query<'a, C>: Query<'a, C>
    where
        C: SnowflakeHttpClient + 'a;
}
