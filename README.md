# snowflakedb-rs
A lightweight, comprehensive and familiar database driver for the [Snowflake](https://www.snowflake.com/en/) SQL Database written **natively** in Rust.

## Features
- [x] Query results in JSON or Arrow
- [x] Stream results in JSON or Arrow RecordSets
- [x] Parse results into `Column`, `Row`, `Cell` primatives for easy Rust usage
- [x] SQLx inspired API
- [x] Password auth
- [x] Certification auth 
- [x] Automatic token renewal
- [x] Managed transactions
- [x] Query bindings (named and anonymous parameters, even batched bindings)
- [x] Describe queries without actual execution
- [x] Custom API hostname for users in Mainland China, or custom enterprise users.
- [x] Async agnostic, supports `tokio`, `smol`, etc
- [x] Provide your own HTTP Client, or use `reqwest` feature [(See here)](#using-a-custom-http-client)
- [x] `chrono` and `rust_decimal` parsing of cell values
- [x] Lightweight by design with minimal dependencies
- [ ] GET/PUT support (WIP)

## Motivation
snowflakedb-rs was built to provide all the functionality required to write an [Arrow Database Connection Protocol (ADBC)](https://arrow.apache.org/adbc/current/index.html) adapter for Snowflake natively in Rust.

At the moment, the only existing library that provides all the features required for an ADBC adapter in Rust is the [adbc_snowflake](https://crates.io/crates/adbc_snowflake) crate. However, adbc_snowflake currently wraps [gosnowflake](https://github.com/snowflakedb/gosnowflake), requiring a Go compiler and bundling the Go runtime with your Rust binary in order to use. Furthermore, adbc_snowflake does not provide any streaming capabilities, which was not ideal.

As such, snowflakedb-rs aims to provide as much of the existing functionality gosnowflake currently provides, without needing to bundle a Go runtime, or call any other external dependencies for that matter. This library directly calls the exact same undocumented API used in gosnowflake.

Additionally, snowflakedb-rs provides many useful primitives that allows you to work with Snowflake directly in idiomatic Rust. The snowflakedb-rs API is partly inspired by [SQLx](https://github.com/launchbadge/sqlx), which should provide a familiar interface for Rust developers.

Whether you plan to develop your own ADBC adapter, or just use Snowflake in idiomatic and familiar Rust, snowflakedb-rs will provide something useful for you.

## Installation
You can install snowflakedb-rs using:
```bash
$ cargo add snowflakedb-rs
```

> You don't need a Golang compiler, just Cargo!

### Cargo Feature Flags
```toml
# Cargo.toml
snowflakedb-rs = {
    version = "1.0.0",
    features = ["auth-cert", "arrow", "chrono", "decimal", "reqwest"]
}
```

- `arrow`: Use arrow as the data exchange medium between Snowflake and Rust. By default, JSON is used.

- `auth-cert`: Use certificate authentication with Snowflake

- `chrono`: Deserialise `DATE`, `TIME`, `TIMESTAMP_LTZ`, `TIMESTAMP_NTZ`, `TIMESTAMP_TZ` into chrono types.

- `decimal`: Deserialise `DECFLOAT` and `FIXED` into a `rust_decimal::Decimal`.

- `reqwest`: Use `reqwest` as the underlying HTTP client. Disable if you want to use a custom HTTP client. [(See here)](#using-a-custom-http-client)

> Warning: Its highly recommended to enable the `chrono` feature for most people. Snowflake returns Date/Time types in difficult to read ints and floats, and snowflakedb-rs will return these types as a  `String` of raw numbers if `chrono` is disabled.

> If `decimal` if not enabled, `DECFLOAT` and `FIXED` will be returned as a `f64` in when using a JSON Connection.

> Enabling `arrow` will also enable `chrono`.

## Usage

### Creating a Snowflake Connection

To get a `SnowflakeConnection`, create a `SnowflakeConnectionOptsBuilder` and build it with your desired options and authentication strategy (`AuthStrategy`).

By default, `snowflakedb-rs` comes with only `AuthStrategy::Password`. If you enable the `auth-cert` feature, you will also have `AuthStrategy::Certificate`.

Here's how you create a `SnowflakePool` with the `Password` AuthStrategy:
```rust
use snowflakedb_rs::{
    SnowflakeConnectionOptsBuilder,
    SnowflakeConnectionOpts,
};


async fn main() {
    let opts: SnowflakeConnectionOpts = SnowflakeConnectionOptsBuilder::default()
        .account_id("ACCOUNT_ID")
        .username("USERNAME")
        .warehouse("WAREHOUSE") // Optional
        .role("ROLE") // Optional
        .database("DATABASE") // Optional
        .schema("SCHEMA") // Optional
        .strategy(AuthStrategy::Password("PASSWORD".into()))
        .pool_size(5)
        .build()
        .unwrap();

    // Requires the `reqwest` feature enabled
    let pool = opts
        .connect_json()
        .await
        .unwrap();

    let conn = pool
        .get()
        .await
        .unwrap();

    // ...
}
```

To get a `SnowflakePool`, use the `SnowflakeConnectionOpts::connect_json()` method. This returns a `SnowflakePool` that will use JSON as the communication protocol between Snowflake and Rust.

To get a `SnowflakeConnection`, use the `SnowflakePool::get()` method. This method returns a `SnowflakeConnection` if one is available, or an `Err(SnowflakeError)` if all connections are in use.

### Queries

Run a `SELECT` query:

```rust

use snowflakedb_rs::CellValue;

async fn main() {
    // ...
    let query = conn
        .fetch("SELECT CURRENT_TIME()")
        .await
        .unwrap();

    let results = query
        .execute()
        .await
        .unwrap();

    // rows is a BoxStream from futures_core.
    let mut rows = results.rows();

    while let Some(row) = rows.try_next().await.unwrap() {
        let cell = row.get(0).unwrap();
        println!("Column Type = {:?}", cell.col.col_type);
        println!("Column Name = {:?}", cell.col.name);
        println!("Value = {:?}", cell.value);

        // You can even match on the value!
        match cell.value {
            // `x` is `Option<String>` by default, or 
            // `Option<chrono::NaiveTime>` with `chrono` feature enabled
            CellValue::Time(x) => println!("Got time: {:?}", x)
            _ => panic!("wrong type!")
        }
    }

    // ...
}
```

RUN an `INSERT` statement:
 ```rust
use snowflakedb_rs::{CellValue, Row, row};
use rust_decimal::Decimal;

async fn main() {
    // ...
    let mut query = conn
        .fetch("INSERT INTO MY_TABLE VALUES (?, ?, ?)")
        .await
        .unwrap();

    // Requires `decimal` feature to be enabled
    let pi = Decimal::new(31415, 4);
    query.bind_row(row![90, "Carl Voller", pi]);
    query.bind_row(row![0.4, "Alice", pi]);
    query.bind_row(row![10, "Bob", pi]);

    let results = query
        .execute()
        .await
        .unwrap();

    if results.is_dml() {
        println!("Rows Affected: {:?}", results.rows_affected());
        assert!(results.rows_affected() == 3);
    }

    // ...
}
```

Create and use a Transaction:
```rust
use snowflakedb_rs::{CellValue, Row, row};

async fn main() {
    // ...
    let mut tx = pool
        .begin()
        .await
        .unwrap();

    let mut query = tx
        .fetch("INSERT INTO MY_TABLE VALUES (?, ?)")
        .await
        .unwrap();

    query.bind_row(row![5, 10]);

    let results = query
        .execute()
        .await
        .unwrap();

    let rows = results.rows()
        .try_collect::<Vec<Row>>()
        .await
        .expect("failed to get rows");

    if results.is_dml() {
        println!("Rows Affected: {:?}", results.rows_affected());
        assert!(results.rows_affected() == 1);
    }

    tx.rollback().await.unwrap();
    // tx.commit().await.unwrap()

    // ...
}
```

Describe a Query to get its return column types and number of expected parameters:

```rust
use snowflakedb_rs::{CellValue, Row, row};

async fn main() {
    // ...
    let query = conn
        .query("SELECT * FROM MY_TABLE WHERE ID > ?")
        .await
        .unwrap();

    let describe = query
        .describe()
        .await
        .unwrap();

    // The query has a single anonymous parameter (or "bind")
    assert!(describe.bind_count() == 1);

    // bind_metadata has more information on what Snowflake expects
    assert!(describe.bind_metadata().unwrap().len() == 1);

    // Assuming MY_TABLE has 3 columns
    assert!(describe.columns.len() == 3);

    // ...
}
```

### Apache Arrow
When you enable the `arrow` feature, you can configure snowflakedb-rs to use the Arrow format for communication with Snowflake's API.

Here's how you configure arrow:
```rust
use snowflakedb_rs::{
    SnowflakeConnectionOptsBuilder,
    SnowflakeConnectionOpts,
};


async fn main() {
    let opts: SnowflakeConnectionOpts = SnowflakeConnectionOptsBuilder::default()
        .account_id("ACCOUNT_ID")
        .username("USERNAME")
        .warehouse("WAREHOUSE") // Optional
        .role("ROLE") // Optional
        .database("DATABASE") // Optional
        .schema("SCHEMA") // Optional
        .strategy(AuthStrategy::Password("PASSWORD".into()))
        .pool_size(5)
        .build()
        .unwrap();

    // Requires the `reqwest` feature enabled
    let pool = opts
        .connect_arrow() // <-- Specify arrow instead of json!
        .await
        .unwrap();

    let conn = pool
        .get()
        .await
        .unwrap();

    // ...
}
```

The Arrow `SnowflakeConnection` supports all the same methods the JSON `SnowflakeConnection`, and a few additional methods too.

Here is how you directly access the underlying Snowflake `RecordBatch`es returned:

```rust
let query = conn
    .fetch("SELECT CURRENT_TIME()")
    .await
    .unwrap();

let results = query
    .execute()
    .await
    .unwrap();

// Streams RecordBatches as they come in
let mut record_batches = results.record_batches();

while let Some(batch) = record_batches.try_next().await.unwrap() {
    println!("Got Arrow Batch: {:?}", batch);
}
```


Here's how you describe an `arrow_schema::Schema`:
```rust
let query = conn
    .fetch("SELECT CURRENT_TIME()")
    .await
    .unwrap();

let describe = query
    .describe()
    .await
    .unwrap();

// Gives you an arrow_schema::Schema
let schema = describe.schema();
```


### Using a Custom HTTP Client
By default, no HTTP Client is provided. However, snowflakedb-rs uses HTTP to communicate with Snowflake's APIs.

For the majority of users, its recommended to enabled the `reqwest` feature and move on with your day. However, if you want to use another HTTP Client, you need to create a Wrapper around you Client and implement the `SnowflakeHttpClient` trait that is exported.

Example implementation of `SnowflakeHttpClient` for `reqwest::Client`:
```rust
use snowflakedb_rs::SnowflakeHttpClient;

struct MyReqwestClient(reqwest::Client);

impl SnowflakeHttpClient for MyReqwestClient {
    fn new() -> Self {
        MyReqwestClient(
            reqwest::Client::builder()
                .gzip(true)
                .referer(false)
                .build()
                .unwrap()
        )
    }

    fn get(
        &self,
        url: &str,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<Vec<u8>, SnowflakeError>> {
        async move {
            use reqwest::header::HeaderMap;

            let url = reqwest::Url::parse(url).unwrap();

            let headers = headers
                    .iter()
                    .map(|(k, v)| {
                        (
                            reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                            reqwest::header::HeaderValue::from_str(v.as_str()).unwrap(),
                        )
                    }).collect::<Vec<(
                        reqwest::header::HeaderName, reqwest::header::HeaderValue
                    )>();

            let resp = self.get(url)
                .headers(HeaderMap::from_iter(headers))
                .send()
                .await
                .unwrap()

            let bytes = resp.bytes().await.unwrap();

            Ok(bytes.to_vec())
        }
    }

    fn post(
        &self,
        url: &str,
        body: Vec<u8>,
        headers: HashMap<String, String>,
    ) -> impl Future<Output = Result<Vec<u8>, SnowflakeError>> {
        async move {
            use reqwest::header::HeaderMap;

            let url = reqwest::Url::parse(url).unwrap();

            let headers = headers
                    .iter()
                    .map(|(k, v)| {
                        (
                            reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                            reqwest::header::HeaderValue::from_str(v.as_str()).unwrap(),
                        )
                    })
                    .collect::<Vec<(
                        reqwest::header::HeaderName, reqwest::header::HeaderValue
                    )>()
            );

            let resp = self.get(url)
                .headers(HeaderMap::from_iter(headers))
                .body(body)
                .send()
                .await
                .unwrap()

            let bytes = resp.bytes().await.unwrap();

            Ok(bytes.to_vec())
        }
    }
}
```

> You don't have to manually do this if you're already planning to use `reqwest`! Use the `reqwest` feature. This is only required for other HTTP clients.

Use your HTTP Client:
```rust
use snowflakedb_rs::{
    SnowflakeConnectionOptsBuilder,
    SnowflakeConnectionOpts,
};


async fn main() {
    let opts: SnowflakeConnectionOpts = SnowflakeConnectionOptsBuilder::default()
        .account_id("ACCOUNT_ID")
        .username("USERNAME")
        .warehouse("WAREHOUSE") // Optional
        .role("ROLE") // Optional
        .database("DATABASE") // Optional
        .schema("SCHEMA") // Optional
        .strategy(AuthStrategy::Password("PASSWORD".into()))
        .pool_size(5)
        .build()
        .unwrap();

    let pool = opts
        .connect_json_with_client::<MyReqwestClient>()
        .await
        .unwrap();

    let conn = pool
        .get()
        .await
        .unwrap();

    // ...
}
```

## Contributing
PRs are welcomed! Any help is appreciated. There are a number of TODOs, FIXMEs, and improvements that can be done around the repo. If any of them are tied to a feature you need, please create an issue.


## License
Copyright © 2026, Carl Ian Voller. Released under the BSD-3-Clause License.