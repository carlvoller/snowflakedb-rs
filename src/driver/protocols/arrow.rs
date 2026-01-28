use std::{
    str::FromStr,
    sync::{Arc, Weak},
};

use arrow_array::{Array, timezone::Tz};
use arrow_ipc::reader::StreamReader;
use arrow_schema::Field;
use async_stream::try_stream;
#[cfg(feature = "decimal")]
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::TimeZone;
use futures_util::{TryStreamExt, lock::Mutex};

use crate::{
    CellValue, Column, ColumnType, Query, QueryResult, Row, SnowflakeError, SnowflakeHttpClient,
    auth::session::Session,
    connection::Connection,
    driver::{
        Protocol,
        base::{
            BinaryQueryBuilder,
            bindings::{BindMetadata, Bindings},
            response::RawQueryResponse,
        },
        query::DescribeResult,
    },
    error, this_errors,
};

#[derive(Clone)]
pub struct ArrowProtocol {}

impl Protocol for ArrowProtocol {
    type Query<C>
        = ArrowQuery<C>
    where
        C: SnowflakeHttpClient;
}

impl Default for ArrowProtocol {
    fn default() -> Self {
        Self {}
    }
}

pub struct ArrowQuery<C: SnowflakeHttpClient> {
    session: Weak<Mutex<Session<C>>>,
    bindings: Bindings,
    query: String,
}

impl<C: SnowflakeHttpClient> Query<C> for ArrowQuery<C> {
    type Result = ArrowQueryResult<C>;
    type Describe = ArrowDescribeResult;

    fn new(query: impl ToString, session: Weak<Mutex<Session<C>>>) -> Self {
        Self {
            session,
            bindings: Bindings::new(),
            query: query.to_string(),
        }
    }

    fn bind_row(&mut self, params: Vec<impl crate::ToCellValue>) {
        self.bindings.bind_row(params);
    }

    fn bind_row_named(&mut self, params: Vec<(impl ToString, impl crate::ToCellValue)>) {
        self.bindings.bind_row_named(params);
    }

    async fn execute(self) -> Result<Self::Result, crate::SnowflakeError> {
        let query = this_errors!(
            "failed to build underlying binary query",
            BinaryQueryBuilder::default()
                .accept_header("application/snowflake")
                .sql_text(self.query)
                .is_describe_only(false)
                .bindings(self.bindings)
                .build()
        );

        let session = self
            .session
            .upgrade()
            .ok_or(error!("The surrounding connection for this query is dead."))?;

        let mut session = session.lock().await;

        let raw = query.run(&mut session).await?;

        let cols = raw
            .rowtype
            .clone()
            .into_iter()
            .map(|x| Arc::new(x))
            .collect::<Vec<Arc<Column>>>();

        Ok(ArrowQueryResult {
            conn: session.get_conn(),
            raw,
            cols,
        })
    }

    async fn describe(self) -> Result<ArrowDescribeResult, crate::SnowflakeError> {
        let query = this_errors!(
            "failed to build underlying binary query",
            BinaryQueryBuilder::default()
                .accept_header("application/snowflake")
                .sql_text(self.query)
                .is_describe_only(true)
                .bindings(self.bindings)
                .build()
        );

        let session = self
            .session
            .upgrade()
            .ok_or(error!("The surrounding connection for this query is dead."))?;

        let mut session = session.lock().await;

        let raw = query.run(&mut session).await?;

        let cols = raw
            .rowtype
            .clone()
            .into_iter()
            .map(|x| Arc::new(x))
            .collect::<Vec<Arc<Column>>>();

        Ok(ArrowDescribeResult { columns: cols, raw })
    }
}

pub struct ArrowQueryResult<C: SnowflakeHttpClient + Clone> {
    conn: Connection<C>,
    raw: RawQueryResponse,

    cols: Vec<Arc<Column>>,
}

impl<C: SnowflakeHttpClient> ArrowQueryResult<C> {
    pub fn record_batches(
        self,
    ) -> futures_util::stream::BoxStream<
        'static,
        Result<arrow_array::RecordBatch, crate::SnowflakeError>,
    > {
        let is_dml = self.is_dml();
        let mut raw_stream = self.raw.stream_chunks(self.conn);
        let cols = self.cols.clone();

        let stream = try_stream! {

            if !is_dml {
                while let Some((_row_count, chunk)) = raw_stream.try_next().await? {
                    let batches = StreamReader::try_new(chunk.as_slice(), None)
                        .map_err(|err| error!("failed to read arrow record batch", err))?;

                    for batch in batches {
                        let batch = batch.map_err(|err| error!("failed to read arrow record batch", err))?;
                        let batch = transform_record_batch(batch, cols.as_slice())?;
                        yield batch;
                    }
                }
            } else {
                Err(error!("there are no rows to retrieve"))?;
                return
            }
        };

        Box::pin(stream)
    }

    pub fn schema(&self) -> arrow_schema::Schema {
        if self.is_dml() {
            arrow_schema::Schema::empty()
        } else {
            row_types_to_arrow_schema(&self.cols)
        }
    }
}

impl<C: SnowflakeHttpClient + Clone> QueryResult for ArrowQueryResult<C> {
    fn expected_result_length(&self) -> i64 {
        self.raw.total
    }

    fn columns(&self) -> Vec<Arc<Column>> {
        self.cols.clone()
    }

    fn rows(
        self,
    ) -> futures_util::stream::BoxStream<
        'static,
        Result<crate::driver::primitives::row::Row, crate::SnowflakeError>,
    > {
        let (_total, mut _retrieved, mut cursor) = (self.raw.total, self.raw.returned, 0i64);
        let cols = self.columns();
        let is_dml = self.raw.is_dml();
        let mut raw_stream = self.raw.stream_chunks(self.conn);

        let stream = try_stream! {
            if !is_dml {
                while let Some((_row_count, chunk)) = raw_stream.try_next().await? {
                    let batches = StreamReader::try_new(chunk.as_slice(), None)
                        .map_err(|err| error!("failed to read arrow record batch", err))?;

                    for batch in batches {
                        let batch = batch.map_err(|err| error!("failed to read arrow record batch", err))?;
                        let batch = transform_record_batch(batch, cols.as_slice())?;
                        let row_count = batch.num_rows();
                        for idx in 0..row_count {
                            let row = batch
                                .columns()
                                .iter()
                                .map(|col| arrow_to_cell_value(col, idx).map(|x| x.unwrap_or(CellValue::Null)))
                                .collect::<Result<Vec<CellValue>, crate::SnowflakeError>>()?;
                            yield Row::new_from_cell_values(cols.clone(), row, cursor);
                            cursor += 1;
                        }
                    }
                }
            } else {
                Err(error!("there are no rows to retrieve"))?;
                return
            }

        };

        Box::pin(stream)
    }

    fn is_dml(&self) -> bool {
        self.raw.is_dml()
    }

    fn is_dql(&self) -> bool {
        self.raw.is_dql()
    }

    fn rows_updated(&self) -> i64 {
        self.raw
            .stats
            .as_ref()
            .map(|x| x.num_rows_updated)
            .unwrap_or(0)
    }

    fn rows_affected(&self) -> i64 {
        self.raw
            .stats
            .as_ref()
            .map(|x| {
                x.num_rows_updated + x.num_dml_duplicates + x.num_rows_deleted + x.num_rows_inserted
            })
            .unwrap_or(0)
    }

    fn rows_deleted(&self) -> i64 {
        self.raw
            .stats
            .as_ref()
            .map(|x| x.num_rows_deleted)
            .unwrap_or(0)
    }

    fn rows_inserted(&self) -> i64 {
        self.raw
            .stats
            .as_ref()
            .map(|x| x.num_rows_inserted)
            .unwrap_or(0)
    }
}

#[derive(Debug)]
pub struct ArrowDescribeResult {
    columns: Vec<Arc<Column>>,
    raw: RawQueryResponse,
}

impl DescribeResult for ArrowDescribeResult {
    fn columns(&self) -> Vec<Arc<Column>> {
        self.columns.clone()
    }

    fn bind_metadata(&self) -> Option<Vec<BindMetadata>> {
        self.raw.meta_data_of_binds.clone()
    }

    fn bind_count(&self) -> i32 {
        self.raw.number_of_binds
    }

    fn is_dml(&self) -> bool {
        self.raw.is_dml()
    }

    fn is_dql(&self) -> bool {
        self.raw.is_dql()
    }
}

impl ArrowDescribeResult {
    pub fn schema(&self) -> arrow_schema::Schema {
        if self.is_dml() {
            arrow_schema::Schema::empty()
        } else {
            row_types_to_arrow_schema(&self.columns)
        }
    }

    pub fn params_schema(&self) -> arrow_schema::Schema {
        if let Some(binds) = self.bind_metadata() {
            let binds_as_cols: Vec<Arc<Column>> = binds
                .iter()
                .map(|x| Arc::new(x.to_owned().into()))
                .collect();

            row_types_to_arrow_schema(binds_as_cols.as_slice())
        } else {
            let fields = (0..self.bind_count())
                .map(|x| Field::new(format!("{x}"), arrow_schema::DataType::Utf8, false))
                .collect::<Vec<Field>>();

            arrow_schema::Schema::new(fields)
        }
    }
}

fn transform_record_batch(
    batch: arrow_array::RecordBatch,
    cols: &[Arc<Column>],
) -> Result<arrow_array::RecordBatch, SnowflakeError> {
    let mut fields = Vec::with_capacity(cols.len());
    let mut arrays = Vec::with_capacity(cols.len());

    for (idx, arr) in batch.columns().iter().enumerate() {
        let col_ref = &cols[idx];
        let (dt, new_array) = arrow_response_to_arrow_snowflake(col_ref, arr)?;
        fields.push(Field::new(&col_ref.name, dt, col_ref.nullable));
        arrays.push(new_array);
    }

    let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));

    Ok(this_errors!(
        "failed to construct RecordBatch",
        arrow_array::RecordBatch::try_new(schema, arrays)
    ))
}

// Taken from https://github.com/apache/arrow-adbc/blob/main/go/adbc/driver/snowflake/record_reader.go
fn row_types_to_arrow_schema(row_types: &[Arc<Column>]) -> arrow_schema::Schema {
    let mut fields = Vec::with_capacity(row_types.len());

    for col in row_types {
        let data_type = match col.col_type {
            crate::ColumnType::Fixed | crate::ColumnType::Decfloat => {
                if col.scale.unwrap() == 0 {
                    arrow_schema::DataType::Int64
                } else {
                    arrow_schema::DataType::Decimal128(
                        col.precision.unwrap() as u8,
                        col.scale.unwrap() as i8,
                    )
                }
            }
            crate::ColumnType::Real => arrow_schema::DataType::Float64,
            crate::ColumnType::Date => arrow_schema::DataType::Date32,
            crate::ColumnType::TimestampLtz => {
                let offset_in_secs = chrono::Local
                    .timestamp_opt(0, 0)
                    .single()
                    .unwrap()
                    .offset()
                    .local_minus_utc();
                let offset_in_hours = offset_in_secs / 60 / 60;
                let offset_string = format!(
                    "{:?}{:?}:00",
                    if offset_in_hours >= 0 { "+" } else { "" },
                    offset_in_hours
                );

                let offset_string_with_arc = Arc::from(offset_string.as_str());
                arrow_schema::DataType::Timestamp(
                    arrow_schema::TimeUnit::Nanosecond,
                    Some(offset_string_with_arc),
                )
            }
            crate::ColumnType::TimestampNtz => {
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
            }
            crate::ColumnType::TimestampTz => {
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
            }
            crate::ColumnType::Binary => arrow_schema::DataType::Binary,
            crate::ColumnType::Time => {
                arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond)
            }
            crate::ColumnType::Boolean => arrow_schema::DataType::Boolean,
            _ => arrow_schema::DataType::Utf8,
        };
        fields.push(Field::new(&col.name, data_type, col.nullable));
    }

    arrow_schema::Schema::new(fields)
}

// Maps the Arrow from Snowflake, into the Arrow Schema used in gosnowflake
fn arrow_response_to_arrow_snowflake(
    snowflake_column: &Column,
    array: impl arrow_array::Array,
) -> Result<(arrow_schema::DataType, arrow_array::ArrayRef), crate::SnowflakeError> {
    match snowflake_column.col_type {
        ColumnType::Fixed | ColumnType::Decfloat => {
            let target_type = arrow_schema::DataType::Decimal128(
                snowflake_column.precision.unwrap() as u8,
                snowflake_column.scale.unwrap() as i8,
            );
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::Real => {
            let target_type = arrow_schema::DataType::Float64;
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::Date => {
            let target_type = arrow_schema::DataType::Date32;
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::TimestampLtz => {
            let offset_in_secs = chrono::Local
                .timestamp_opt(0, 0)
                .single()
                .unwrap()
                .offset()
                .local_minus_utc();
            let offset_in_hours = offset_in_secs / 60 / 60;
            let offset_string = format!(
                "{:?}{:?}:00",
                if offset_in_hours >= 0 { "+" } else { "" },
                offset_in_hours
            );

            let offset_string_with_arc = Arc::from(offset_string.as_str());
            let target_type = arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Nanosecond,
                Some(offset_string_with_arc),
            );
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::TimestampNtz | ColumnType::TimestampTz => {
            let target_type =
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None);
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::Binary => {
            let target_type = arrow_schema::DataType::Binary;
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::Time => {
            let target_type = arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond);
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        ColumnType::Boolean => {
            let target_type = arrow_schema::DataType::Boolean;
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
        _ => {
            let target_type = arrow_schema::DataType::Utf8;
            let casted_array = this_errors!(
                "failed to cast arrow array",
                arrow_cast::cast(&array, &target_type)
            );
            Ok((target_type, casted_array))
        }
    }
}

fn arrow_to_cell_value(
    array: &dyn Array,
    row_idx: usize,
) -> Result<Option<CellValue>, crate::SnowflakeError> {
    Ok(match array.data_type() {
        arrow_schema::DataType::Null => Some(CellValue::Null),
        arrow_schema::DataType::Boolean => Some(CellValue::Boolean(
            array
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .map(|x| x.value(row_idx)),
        )),
        arrow_schema::DataType::Int8 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_i8(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::Int16 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int16Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_i16(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::Int32 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_i32(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::Int64 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_i64(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::UInt8 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::UInt8Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_u8(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::UInt16 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::UInt16Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_u16(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::UInt32 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::UInt32Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_u32(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::UInt64 => Some(CellValue::Fixed(
            array
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .map(|x| {
                    #[cfg(not(feature = "decimal"))]
                    return Some(x.value(row_idx) as f64);
                    #[cfg(feature = "decimal")]
                    return BigDecimal::from_u64(x.value(row_idx));
                })
                .flatten(),
        )),
        arrow_schema::DataType::Float16 => Some(CellValue::Real(
            array
                .as_any()
                .downcast_ref::<arrow_array::Float16Array>()
                .map(|x| x.value(row_idx).to_f64()),
        )),
        arrow_schema::DataType::Float32 => Some(CellValue::Real(
            array
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .map(|x| x.value(row_idx) as f64),
        )),
        arrow_schema::DataType::Float64 => Some(CellValue::Real(
            array
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .map(|x| x.value(row_idx) as f64),
        )),
        arrow_schema::DataType::Timestamp(unit, zone) => {
            use arrow_schema::TimeUnit;

            let naive_ts = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampSecondArray>()
                    .map(|x| x.value_as_datetime(row_idx))
                    .flatten(),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                    .map(|x| x.value_as_datetime(row_idx))
                    .flatten(),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                    .map(|x| x.value_as_datetime(row_idx))
                    .flatten(),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                    .map(|x| x.value_as_datetime(row_idx))
                    .flatten(),
            };

            if let Some(zone) = zone {
                let zone_parsed = this_errors!("failed to parse timezone", Tz::from_str(zone));
                let ts = naive_ts
                    .map(|x| {
                        x.and_local_timezone(zone_parsed)
                            .single()
                            .map(|x| x.fixed_offset())
                    })
                    .flatten();
                Some(CellValue::TimestampTz(ts))
            } else {
                Some(CellValue::TimestampNtz(naive_ts))
            }
        }
        arrow_schema::DataType::Date32 => array
            .as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .map(|x| CellValue::Date(x.value_as_date(row_idx))),
        arrow_schema::DataType::Date64 => array
            .as_any()
            .downcast_ref::<arrow_array::Date64Array>()
            .map(|x| CellValue::TimestampNtz(x.value_as_datetime(row_idx))),
        arrow_schema::DataType::Time32(time_unit) => {
            use arrow_schema::TimeUnit;
            match time_unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<arrow_array::Time32SecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::Time32MillisecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                _ => Some(CellValue::Time(None)),
            }
        }
        arrow_schema::DataType::Time64(time_unit) => {
            use arrow_schema::TimeUnit;
            match time_unit {
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::Time64MicrosecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::Time64NanosecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                _ => Some(CellValue::Time(None)),
            }
        }
        arrow_schema::DataType::Duration(time_unit) => {
            use arrow_schema::TimeUnit;
            match time_unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<arrow_array::DurationSecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::DurationMillisecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::DurationMicrosecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<arrow_array::DurationNanosecondArray>()
                    .map(|x| CellValue::Time(x.value_as_time(row_idx))),
            }
        }
        arrow_schema::DataType::Interval(_) => {
            // FIXME: Figure this out. what even is this arrow type supposed to be used for?
            return Err(error!("no idea how to represent this in Rust"));
        }
        arrow_schema::DataType::Binary => array
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            // FIXME: Is there a better way to return this without cloning the data?
            .map(|x| CellValue::Binary(Some(x.value_data().to_vec()))),
        arrow_schema::DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            // FIXME: Is there a better way to return this without cloning the data?
            .map(|x| CellValue::Binary(Some(x.value_data().to_vec()))),
        arrow_schema::DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            // FIXME: Is there a better way to return this without cloning the data?
            .map(|x| CellValue::Binary(Some(x.value_data().to_vec()))),
        arrow_schema::DataType::BinaryView => array
            .as_any()
            .downcast_ref::<arrow_array::BinaryViewArray>()
            // FIXME: Is there a better way to return this without cloning the data?
            // Maybe supporting std::io::Chain and std::io::Cursor might be a good idea
            .map(|x| CellValue::Binary(Some(x.value(row_idx).to_vec()))),
        arrow_schema::DataType::Utf8 => array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .map(|x| CellValue::Text(Some(x.value(row_idx).to_string()))),
        arrow_schema::DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .map(|x| CellValue::Text(Some(x.value(row_idx).to_string()))),
        arrow_schema::DataType::Utf8View => array
            .as_any()
            .downcast_ref::<arrow_array::StringViewArray>()
            .map(|x| CellValue::Text(Some(x.value(row_idx).to_string()))),

        #[cfg(feature = "decimal")]
        arrow_schema::DataType::Decimal32(_, scale) => {
            use bigdecimal::num_bigint::BigInt;

            let value_as_primitive = array
                .as_any()
                .downcast_ref::<arrow_array::Decimal32Array>()
                .map(|x| x.value(row_idx));

            value_as_primitive
                .map(|x| {
                    BigInt::from_i32(x)
                        .map(|big| CellValue::Decfloat(Some(BigDecimal::new(big, *scale as i64))))
                })
                .flatten()
        }

        #[cfg(feature = "decimal")]
        arrow_schema::DataType::Decimal64(_, scale) => {
            use bigdecimal::num_bigint::BigInt;

            let value_as_primitive = array
                .as_any()
                .downcast_ref::<arrow_array::Decimal64Array>()
                .map(|x| x.value(row_idx));

            value_as_primitive
                .map(|x| {
                    BigInt::from_i64(x)
                        .map(|big| CellValue::Decfloat(Some(BigDecimal::new(big, *scale as i64))))
                })
                .flatten()
        }

        #[cfg(feature = "decimal")]
        arrow_schema::DataType::Decimal128(_, scale) => {
            use bigdecimal::num_bigint::BigInt;

            let value_as_primitive = array
                .as_any()
                .downcast_ref::<arrow_array::Decimal128Array>()
                .map(|x| x.value(row_idx));

            value_as_primitive
                .map(|x| {
                    BigInt::from_i128(x)
                        .map(|big| CellValue::Decfloat(Some(BigDecimal::new(big, *scale as i64))))
                })
                .flatten()
        }

        #[cfg(feature = "decimal")]
        arrow_schema::DataType::Decimal256(_, scale) => {
            use bigdecimal::num_bigint::BigInt;

            let value_as_primitive = array
                .as_any()
                .downcast_ref::<arrow_array::Decimal256Array>()
                .map(|x| x.value(row_idx));

            value_as_primitive.map(|x| {
                CellValue::Decfloat(Some(BigDecimal::new(
                    BigInt::from_signed_bytes_le(&x.to_le_bytes()),
                    *scale as i64,
                )))
            })
        }

        #[cfg(not(feature = "decimal"))]
        arrow_schema::DataType::Decimal32(_, scale) => array
            .as_any()
            .downcast_ref::<arrow_array::Decimal32Array>()
            .map(|x| {
                CellValue::Decfloat(Some((x.value(row_idx) as f64) * 10f64.powf(*scale as f64)))
            }),

        #[cfg(not(feature = "decimal"))]
        arrow_schema::DataType::Decimal64(_, scale) => array
            .as_any()
            .downcast_ref::<arrow_array::Decimal64Array>()
            .map(|x| {
                CellValue::Decfloat(Some((x.value(row_idx) as f64) * 10f64.powf(*scale as f64)))
            }),

        #[cfg(not(feature = "decimal"))]
        arrow_schema::DataType::Decimal128(_, scale) => array
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .map(|x| {
                CellValue::Decfloat(Some((x.value(row_idx) as f64) * 10f64.powf(*scale as f64)))
            }),

        #[cfg(not(feature = "decimal"))]
        // Not very precise, use `decimal` crate for better precision
        arrow_schema::DataType::Decimal256(_, scale) => array
            .as_any()
            .downcast_ref::<arrow_array::Decimal256Array>()
            .map(|x| {
                CellValue::Decfloat(Some(
                    (x.value(row_idx).as_i128() as f64) * 10f64.powf(*scale as f64),
                ))
            }),

        arrow_schema::DataType::List(_) => {
            // FIXME: How can I represent a List into a CellValue? Recursively parse it into a serde_json::Value maybe?
            return Err(error!("ListArray not supported"));
        }
        arrow_schema::DataType::ListView(_) => {
            // FIXME: How can I represent a List into a CellValue? Recursively parse it into a serde_json::Value maybe?
            return Err(error!("ListView not supported"));
        }
        arrow_schema::DataType::FixedSizeList(_, _) => {
            // FIXME: How can I represent a List into a CellValue? Recursively parse it into a serde_json::Value maybe?
            return Err(error!("FixedSizeList not supported"));
        }
        arrow_schema::DataType::LargeList(_) => {
            // FIXME: How can I represent a List into a CellValue? Recursively parse it into a serde_json::Value maybe?
            return Err(error!("LargeList not supported"));
        }
        arrow_schema::DataType::LargeListView(_) => {
            // FIXME: How can I represent a List into a CellValue? Recursively parse it into a serde_json::Value maybe?
            return Err(error!("LargeListView not supported"));
        }
        arrow_schema::DataType::Struct(_) => {
            // FIXME: How can I represent a Struct into a CellValue? This seems especially complicated
            return Err(error!("Struct not supported"));
        }
        arrow_schema::DataType::Dictionary(_, _) => {
            // FIXME: How can I represent a Dictionary into a CellValue?
            return Err(error!("Dictionary not supported"));
        }

        // FIXME: Not familiar with any of these types below
        arrow_schema::DataType::Union(_, _) => {
            return Err(error!("Union not supported"));
        }
        arrow_schema::DataType::Map(_, _) => {
            return Err(error!("Map not supported"));
        }
        arrow_schema::DataType::RunEndEncoded(_, _) => {
            return Err(error!("RunEndEncoded not supported"));
        }
    })
}
