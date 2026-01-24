use std::{fmt::Display, sync::Arc};

#[cfg(feature = "chrono")]
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
/// An enum describing which data_type and value is stored in this [`Cell`](`crate::driver::primitives::cell::Cell`)
pub enum CellValue {
    /// Fixed Precision and Scale units are casted to Decimal
    #[cfg(feature = "decimal")]
    Fixed(Option<Decimal>),
    #[cfg(feature = "decimal")]
    Decfloat(Option<Decimal>),

    #[cfg(not(feature = "decimal"))]
    Fixed(Option<String>),
    #[cfg(not(feature = "decimal"))]
    Decfloat(Option<String>),

    Real(Option<f64>),
    Text(Option<String>),
    Boolean(Option<bool>),

    /// Semi-structured data types.
    Variant(Option<serde_json::Value>),
    Object(Option<serde_json::Value>),
    Array(Option<serde_json::Value>),
    Map(Option<serde_json::Value>),

    /// Timestamps.
    #[cfg(feature = "chrono")]
    TimestampLtz(Option<DateTime<Utc>>),
    #[cfg(feature = "chrono")]
    TimestampNtz(Option<NaiveDateTime>),
    #[cfg(feature = "chrono")]
    TimestampTz(Option<DateTime<FixedOffset>>),
    #[cfg(feature = "chrono")]
    Time(Option<NaiveTime>),
    #[cfg(feature = "chrono")]
    Date(Option<NaiveDate>),

    #[cfg(not(feature = "chrono"))]
    TimestampLtz(Option<String>),
    #[cfg(not(feature = "chrono"))]
    TimestampNtz(Option<String>),
    #[cfg(not(feature = "chrono"))]
    TimestampTz(Option<String>),
    #[cfg(not(feature = "chrono"))]
    Time(Option<String>),
    #[cfg(not(feature = "chrono"))]
    Date(Option<String>),

    Binary(Option<Vec<u8>>),
    Null,

    /// Internal/Helper types.
    Slice(Option<serde_json::Value>),
    ChangeType(Option<String>),
    NotSupported(Option<String>),
}

#[derive(Debug, Clone)]
/// A single cell in a [`Row`](`crate::driver::primitives::row::Row`)
pub struct Cell {
    pub col: Arc<super::column::Column>,
    pub value: CellValue,
}

pub trait ToCellValue {
    fn to_cell_value(self) -> CellValue;
}

macro_rules! impl_to_cell_value {
    ($($t:ty),*) => {
        $(
            impl ToCellValue for $t {
                fn to_cell_value(self) -> CellValue {
                    CellValue::Text(Some(self.to_string()))
                }
            }

            impl ToCellValue for &$t {
                fn to_cell_value(self) -> CellValue {
                    CellValue::Text(Some(self.to_string()))
                }
            }

            impl ToCellValue for Option<$t> {
                fn to_cell_value(self) -> CellValue {
                    CellValue::Text(self.map(|x| x.to_string()))
                }
            }
        )*
    };
}

impl ToCellValue for &str {
    fn to_cell_value(self) -> CellValue {
        CellValue::Text(Some(self.to_string()))
    }
}

impl ToCellValue for Option<&str> {
    fn to_cell_value(self) -> CellValue {
        CellValue::Text(self.map(|x| x.to_string()))
    }
}

impl_to_cell_value!(
    i8,
    i16,
    i32,
    i64,
    i128,
    f32,
    f64,
    bool,
    String,
    serde_json::Value
);

#[cfg(feature = "decimal")]
impl_to_cell_value!(Decimal);

#[cfg(feature = "chrono")]
impl_to_cell_value!(
    DateTime<chrono::Utc>,
    DateTime<chrono::Local>,
    DateTime<chrono::FixedOffset>,
    NaiveDate,
    NaiveDateTime,
    NaiveTime
);

impl ToCellValue for CellValue {
    fn to_cell_value(self) -> CellValue {
        self
    }
}

pub fn value_to_name(val: &CellValue) -> &'static str {
    match val {
        CellValue::Fixed(_) => "FIXED",
        CellValue::Decfloat(_) => "DECFLOAT",
        CellValue::Real(_) => "REAL",
        CellValue::Text(_) => "TEXT",
        CellValue::Boolean(_) => "BOOLEAN",
        CellValue::Variant(_) => "VARIANT",
        CellValue::Object(_) => "OBJECT",
        CellValue::Array(_) => "ARRAY",
        CellValue::Map(_) => "MAP",
        CellValue::TimestampLtz(_) => "TIMESTAMP_LTZ",
        CellValue::TimestampNtz(_) => "TIMESTAMP_NTZ",
        CellValue::TimestampTz(_) => "TIMESTAMP_TZ",
        CellValue::Time(_) => "TIME",
        CellValue::Date(_) => "DATE",
        CellValue::Binary(_) => "BINARY",
        CellValue::Null => "NULL",
        CellValue::Slice(_) => "SLICE",
        CellValue::ChangeType(_) => "CHANGE_TYPE",
        CellValue::NotSupported(_) => "NOT_SUPPORTED",
    }
}

pub fn get_null_from_column(col: &super::column::Column) -> CellValue {
    match col.col_type {
        super::column::ColumnType::Fixed => CellValue::Fixed(None),
        super::column::ColumnType::Real => CellValue::Real(None),
        super::column::ColumnType::Decfloat => CellValue::Decfloat(None),
        super::column::ColumnType::Text => CellValue::Text(None),
        super::column::ColumnType::Date => CellValue::Date(None),
        super::column::ColumnType::Variant => CellValue::Variant(None),
        super::column::ColumnType::TimestampLtz => CellValue::TimestampLtz(None),
        super::column::ColumnType::TimestampNtz => CellValue::TimestampNtz(None),
        super::column::ColumnType::TimestampTz => CellValue::TimestampTz(None),
        super::column::ColumnType::Object => CellValue::Object(None),
        super::column::ColumnType::Array => CellValue::Array(None),
        super::column::ColumnType::Map => CellValue::Map(None),
        super::column::ColumnType::Binary => CellValue::Binary(None),
        super::column::ColumnType::Time => CellValue::Time(None),
        super::column::ColumnType::Boolean => CellValue::Boolean(None),
        super::column::ColumnType::Null => CellValue::Null,
        super::column::ColumnType::Slice => CellValue::Slice(None),
        super::column::ColumnType::ChangeType => CellValue::ChangeType(None),
        super::column::ColumnType::NotSupported => CellValue::NotSupported(None),
    }
}

impl Into<Option<String>> for CellValue {
    fn into(self) -> Option<String> {
        match self {
            CellValue::Text(data) => data.map(|x| x),
            CellValue::Boolean(data) => data.map(|x| x.to_string()),
            CellValue::Real(data) => data.map(|x| x.to_string()),
            CellValue::Fixed(data) | CellValue::Decfloat(data) => data.map(|x| x.to_string()),
            CellValue::Variant(data)
            | CellValue::Array(data)
            | CellValue::Map(data)
            | CellValue::Object(data)
            | CellValue::Slice(data) => data.map(|x| x.to_string()),
            CellValue::Null => None,

            #[cfg(feature = "chrono")]
            CellValue::TimestampLtz(date_time) => date_time.map(|ts| {
                let seconds = (ts.timestamp() as i128) * 1_000_000_000i128;
                let nanoseconds = ts.timestamp_subsec_nanos() as i128;

                let final_ts = seconds + nanoseconds;
                final_ts.to_string()
            }),
            #[cfg(feature = "chrono")]
            CellValue::TimestampNtz(naive_date_time) => naive_date_time.map(|ts| {
                let ts = ts.and_utc();
                let seconds = (ts.timestamp() as i128) * 1_000_000_000i128;
                let nanoseconds = ts.timestamp_subsec_nanos() as i128;

                let final_ts = seconds + nanoseconds;
                final_ts.to_string()
            }),
            #[cfg(feature = "chrono")]
            CellValue::TimestampTz(date_time) => date_time.map(|ts| {
                let offset_in_seconds = ts.offset().local_minus_utc() / 60 + 1440;

                let seconds = ts.timestamp() as i128;
                let nanoseconds = ts.timestamp_subsec_nanos() as i128;

                let final_ts = ((seconds * 1_000_000_000) + nanoseconds).to_string();
                format!("{final_ts} {offset_in_seconds}")
            }),
            #[cfg(feature = "chrono")]
            CellValue::Time(naive_time) => naive_time.map(|time| {
                let seconds = time.num_seconds_from_midnight() as i128;
                let nanoseconds = time.nanosecond() as i128;

                let total_nanos = (seconds * 1_000_000_000) + nanoseconds;
                total_nanos.to_string()
            }),
            #[cfg(feature = "chrono")]
            CellValue::Date(naive_date) => naive_date
                .map(|date| {
                    date.and_hms_opt(0, 0, 0)
                        .map(|x| x.and_utc().timestamp_millis())
                        .map(|x| format!("{x}"))
                })
                .flatten(),

            #[cfg(not(feature = "chrono"))]
            CellValue::Date(x)
            | CellValue::Time(x)
            | CellValue::TimestampTz(x)
            | CellValue::TimestampLtz(x)
            | CellValue::TimestampNtz(x) => x.map(|x| x.to_string()),

            CellValue::Binary(data) => data.map(|x| hex::encode(x)),
            CellValue::ChangeType(data) => data.map(|x| x),
            CellValue::NotSupported(data) => data.map(|x| x),
        }
    }
}

impl Display for CellValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value_as_string = match self {
            #[cfg(feature = "decimal")]
            CellValue::Fixed(decimal) => decimal.map(|x| x.to_string()),
            #[cfg(feature = "decimal")]
            CellValue::Decfloat(decimal) => decimal.map(|x| x.to_string()),

            #[cfg(not(feature = "decimal"))]
            CellValue::Fixed(x) => x.as_deref().map(|x| x.to_string()),
            #[cfg(not(feature = "decimal"))]
            CellValue::Decfloat(x) => x.as_deref().map(|x| x.to_string()),

            #[cfg(feature = "chrono")]
            CellValue::TimestampLtz(date_time) => date_time.map(|x| x.to_string()),
            #[cfg(feature = "chrono")]
            CellValue::TimestampNtz(naive_date_time) => naive_date_time.map(|x| x.to_string()),
            #[cfg(feature = "chrono")]
            CellValue::TimestampTz(date_time) => date_time.map(|x| x.to_string()),
            #[cfg(feature = "chrono")]
            CellValue::Time(naive_time) => naive_time.map(|x| x.to_string()),
            #[cfg(feature = "chrono")]
            CellValue::Date(naive_date) => naive_date.map(|x| x.to_string()),

            #[cfg(not(feature = "chrono"))]
            CellValue::TimestampLtz(x) => x.as_deref().map(|x| x.to_string()),
            #[cfg(not(feature = "chrono"))]
            CellValue::TimestampNtz(x) => x.as_deref().map(|x| x.to_string()),
            #[cfg(not(feature = "chrono"))]
            CellValue::TimestampTz(x) => x.as_deref().map(|x| x.to_string()),
            #[cfg(not(feature = "chrono"))]
            CellValue::Time(x) => x.as_deref().map(|x| x.to_string()),
            #[cfg(not(feature = "chrono"))]
            CellValue::Date(x) => x.as_deref().map(|x| x.to_string()),

            CellValue::Real(x) => x.map(|x| x.to_string()),
            CellValue::Text(x) => x.as_deref().map(|x| x.to_string()),
            CellValue::Boolean(x) => x.map(|x| x.to_string()),
            CellValue::Variant(value) => value.as_ref().map(|x| x.to_string()),
            CellValue::Object(value) => value.as_ref().map(|x| x.to_string()),
            CellValue::Array(value) => value.as_ref().map(|x| x.to_string()),
            CellValue::Map(value) => value.as_ref().map(|x| x.to_string()),

            CellValue::Binary(items) => items
                .as_ref()
                .map(|x| format!("BINARY(length={:?})", x.len())),
            CellValue::Null => Some("None".into()),
            CellValue::Slice(value) => value.as_ref().map(|x| x.to_string()),
            CellValue::ChangeType(x) => x.as_deref().map(|x| x.to_string()),
            CellValue::NotSupported(x) => x.as_deref().map(|x| x.to_string()),
        };

        write!(f, "{}", value_as_string.unwrap_or("None".into()))
    }
}
