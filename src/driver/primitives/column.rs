use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
/// An enum for which snowflake type is stored in this column
pub enum ColumnType {
    Fixed,
    Real,
    Decfloat,
    Text,
    Date,
    Variant,
    TimestampLtz,
    TimestampNtz,
    TimestampTz,
    Object,
    Array,
    Map,
    Binary,
    Time,
    Boolean,
    Null,
    Slice,
    ChangeType,
    NotSupported,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// Describes a column in a [`QueryResult`](`crate::driver::query::QueryResult`)
pub struct Column {
    #[serde(rename = "type")]
    pub col_type: ColumnType,
    pub name: String,

    pub precision: Option<i64>,
    pub scale: Option<i64>,
    pub nullable: bool,
}
