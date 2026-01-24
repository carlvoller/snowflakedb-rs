use std::{fmt::Debug, str::FromStr, sync::Arc};

#[cfg(feature = "chrono")]
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;

use crate::{SnowflakeError, error, this_errors};

use super::column::{Column, ColumnType};

#[macro_export]
macro_rules! row {
    ($($val:expr),* $(,)?) => {
        vec![
            $(
                $crate::driver::primitives::cell::ToCellValue::to_cell_value($val)
            ),*
        ]
    };
}

/// A single row in a [`QueryResult`](`crate::driver::query::QueryResult`)
pub struct Row {
    /// Snowflake returns all values as strings. Cast it only when requested
    values: Vec<Option<String>>,
    columns: Vec<Arc<Column>>,
    pub idx: i64,
}

impl Row {
    pub fn new(columns: Vec<Arc<Column>>, values: Vec<Option<String>>, idx: i64) -> Self {
        Self {
            values,
            columns,
            idx,
        }
    }

    pub fn get(&self, idx: usize) -> Result<super::cell::Cell, SnowflakeError> {
        let value = if let Some(val) = self.values[idx].as_deref() {
            cast_snowflake_to_rust_type(&self.columns[idx], val)?
        } else {
            super::cell::CellValue::Null
        };

        Ok(super::cell::Cell {
            col: self.columns[idx].clone(),
            value,
        })
    }
}

impl IntoIterator for Row {
    type Item = Result<super::cell::Cell, SnowflakeError>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        let cells: Vec<Result<super::cell::Cell, SnowflakeError>> = self
            .columns
            .into_iter()
            .zip(self.values.into_iter())
            .map(|(col, val_str)| {
                let value = if let Some(val) = val_str.as_deref() {
                    cast_snowflake_to_rust_type(&col, val)?
                } else {
                    super::cell::get_null_from_column(&col)
                };
                Ok(super::cell::Cell { col, value })
            })
            .collect();

        cells.into_iter()
    }
}

fn cast_snowflake_to_rust_type(
    col: &Column,
    value: &str,
) -> Result<super::cell::CellValue, SnowflakeError> {
    match col.col_type {
        #[cfg(feature = "decimal")]
        ColumnType::Fixed | ColumnType::Decfloat => {
            // Snowflake has a default precision and scale of (38, 0)
            // https://docs.snowflake.com/en/sql-reference/intro-summary-data-types
            // let precision = col.precision.unwrap_or(38);
            // let scale = col.scale.unwrap_or(0);

            let decimal = this_errors!(
                "failed to convert FIXED/DECFLOAT to Decimal",
                Decimal::from_str(value)
            );

            if let ColumnType::Fixed = col.col_type {
                Ok(super::cell::CellValue::Fixed(Some(decimal)))
            } else {
                Ok(super::cell::CellValue::Decfloat(Some(decimal)))
            }
        }

        #[cfg(not(feature = "decimal"))]
        ColumnType::Fixed | ColumnType::Decfloat => {
            Ok(super::cell::CellValue::Text(Some(value.to_string())))
        }

        #[cfg(feature = "chrono")]
        ColumnType::Date => Ok(super::cell::CellValue::Date(NaiveDate::from_epoch_days(
            this_errors!("failed to decode DATE to i32", i32::from_str(value)),
        ))),

        #[cfg(feature = "chrono")]
        ColumnType::Time => {
            let time_parts = value
                .split('.')
                .map(|x| {
                    Ok(this_errors!(
                        "failed to convert TIME to u32",
                        u32::from_str(x)
                    ))
                })
                .collect::<Result<Vec<u32>, SnowflakeError>>()?;

            let seconds = time_parts[0];
            let nanoseconds = if time_parts.len() > 1
                && let Some(scale) = col.scale
            {
                let mut fraction = time_parts[1];
                if scale < 9 {
                    fraction *= 10u32.pow((9 - scale) as u32);
                }
                fraction
            } else {
                0
            };

            Ok(super::cell::CellValue::Time(
                NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds),
            ))
        }

        #[cfg(feature = "chrono")]
        ColumnType::TimestampLtz => {
            let time_parts = value
                .split('.')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            let seconds = this_errors!(
                "failed to convert TIMESTAMP_LTZ seconds to i64",
                i64::from_str(time_parts[0].as_str())
            );

            let nanoseconds = if time_parts.len() > 1
                && let Some(scale) = col.scale
            {
                let mut fraction = this_errors!(
                    "failed to convert TIMESTAMP_LTZ nanoseconds to u32",
                    u32::from_str(time_parts[0].as_str())
                );

                if scale < 9 {
                    fraction *= 10u32.pow((9 - scale) as u32);
                }
                fraction
            } else {
                0
            };

            let ts = Utc.timestamp_opt(seconds, nanoseconds).single();

            Ok(super::cell::CellValue::TimestampLtz(ts))
        }

        #[cfg(feature = "chrono")]
        ColumnType::TimestampTz => {
            let (time, offset) = value
                .split_once(" ")
                .ok_or(error!("malformed TIMESTAMP_TZ"))?;

            let time_parts = time
                .split('.')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            let seconds = this_errors!(
                "failed to convert TIMESTAMP_TZ seconds to i64",
                i64::from_str(time_parts[0].as_str())
            );

            let nanoseconds = if time_parts.len() > 1
                && let Some(scale) = col.scale
            {
                let mut fraction = this_errors!(
                    "failed to convert TIMESTAMP_TZ nanoseconds to u32",
                    u32::from_str(time_parts[0].as_str())
                );

                if scale < 9 {
                    fraction *= 10u32.pow((9 - scale) as u32);
                }
                fraction
            } else {
                0
            };

            let offset = this_errors!(
                "failed to convert TIMESTAMP_TZ offset to i32",
                i32::from_str(offset)
            );

            let tz_offset = FixedOffset::east_opt(offset * 60)
                .ok_or(error!(format!("invalid offset in TIMESTAMP_TZ {offset}")))?;

            let ts = tz_offset.timestamp_opt(seconds, nanoseconds).single();

            Ok(super::cell::CellValue::TimestampTz(ts))
        }

        #[cfg(feature = "chrono")]
        ColumnType::TimestampNtz => {
            let time_parts = value
                .split('.')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();

            let seconds = this_errors!(
                "failed to convert TIMESTAMP_NTZ seconds to i64",
                i64::from_str(time_parts[0].as_str())
            );

            let nanoseconds = if time_parts.len() > 1
                && let Some(scale) = col.scale
            {
                let mut fraction = this_errors!(
                    "failed to convert TIMESTAMP_NTZ nanoseconds to u32",
                    u32::from_str(time_parts[0].as_str())
                );

                if scale < 9 {
                    fraction *= 10u32.pow((9 - scale) as u32);
                }
                fraction
            } else {
                0
            };

            let ts = DateTime::from_timestamp(seconds, nanoseconds).map(|dt| dt.naive_utc());

            Ok(super::cell::CellValue::TimestampNtz(ts))
        }

        #[cfg(not(feature = "chrono"))]
        ColumnType::Date
        | ColumnType::Time
        | ColumnType::TimestampLtz
        | ColumnType::TimestampNtz
        | ColumnType::TimestampTz => Ok(super::cell::CellValue::Text(Some(value.to_string()))),

        ColumnType::Boolean => Ok(super::cell::CellValue::Boolean(Some(value == "true"))),
        ColumnType::Real => Ok(this_errors!(
            "failed to convert from REAL to f64",
            f64::from_str(value)
                .map(Some)
                .map(super::cell::CellValue::Real)
        )),
        ColumnType::Object => Ok(this_errors!(
            "failed to convert from OBJECT to serde_json::Value",
            serde_json::from_str(value).map(super::cell::CellValue::Object)
        )),
        ColumnType::Array => Ok(this_errors!(
            "failed to convert from ARRAY to serde_json::Value",
            serde_json::from_str(value).map(super::cell::CellValue::Array)
        )),
        ColumnType::Map => Ok(this_errors!(
            "failed to convert from MAP to serde_json::Value",
            serde_json::from_str(value).map(super::cell::CellValue::Map)
        )),
        ColumnType::Variant => Ok(this_errors!(
            "failed to convert from VARIANT to serde_json::Value",
            serde_json::from_str(value).map(super::cell::CellValue::Variant)
        )),
        ColumnType::Binary => Ok(this_errors!(
            "failed to decode BINARY to Vec<u8>",
            hex::decode(&value)
                .map(Some)
                .map(super::cell::CellValue::Binary)
        )),

        ColumnType::Null => Ok(super::cell::CellValue::Null),
        ColumnType::Text => Ok(super::cell::CellValue::Text(Some(value.to_string()))),
        ColumnType::ChangeType => Ok(super::cell::CellValue::ChangeType(Some(value.to_string()))),
        ColumnType::NotSupported => Ok(super::cell::CellValue::NotSupported(Some(
            value.to_string(),
        ))),
        ColumnType::Slice => Err(error!("Encountered a SLICE type, unsure how to handle.")),
    }
}

impl Debug for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Row {{\n")?;
        for (idx, item) in self.values.iter().enumerate() {
            let col = self.columns.get(idx).unwrap();
            let item = item.as_deref().unwrap_or("None");
            let cell_value =
                cast_snowflake_to_rust_type(col, item).unwrap_or(super::cell::CellValue::Null);
            write!(f, "  {}: {}\n", col.name, cell_value,)?;
        }
        write!(f, "}}")
    }
}
