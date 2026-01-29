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

impl ColumnType {
    pub fn name(&self) -> String {
        match self {
            ColumnType::Fixed => "FIXED",
            ColumnType::Real => "REAL",
            ColumnType::Decfloat => "DECFLOAT",
            ColumnType::Text => "TEXT",
            ColumnType::Date => "DATE",
            ColumnType::Variant => "VARIANT",
            ColumnType::TimestampLtz => "TIMESTAMP_LTZ",
            ColumnType::TimestampNtz => "TIMESTAMP_NTZ",
            ColumnType::TimestampTz => "TIMESTAMP_TZ",
            ColumnType::Object => "OBJECT",
            ColumnType::Array => "ARRAY",
            ColumnType::Map => "MAP",
            ColumnType::Binary => "BINARY",
            ColumnType::Time => "TIME",
            ColumnType::Boolean => "BOOLEAN",
            ColumnType::Null => "NULL",
            ColumnType::Slice => "SLICE",
            ColumnType::ChangeType => "CHANGE_TYPE",
            ColumnType::NotSupported => "NOT_SUPPORTED",
        }
        .to_string()
    }
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
