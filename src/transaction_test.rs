use futures_util::TryStreamExt;

#[cfg(test)]
use super::*;

#[tokio::test]
async fn test_tx() {
    use crate::CellValue;
    use crate::auth::AuthStrategy;
    use crate::row;

    let opts = crate::SnowflakeConnectionOptsBuilder::default()
        .account_id(std::env::var("SF_ACCOUNT_ID").unwrap())
        .warehouse(std::env::var("SF_WAREHOUSE").unwrap())
        .username(std::env::var("SF_USERNAME").unwrap())
        .role(std::env::var("SF_ROLE").unwrap())
        .database(std::env::var("SF_DATABASE").unwrap())
        .schema(std::env::var("SF_SCHEMA").unwrap())
        .strategy(AuthStrategy::Password(
            std::env::var("SF_PASSWORD").unwrap(),
        ))
        .pool_size(1)
        .build()
        .expect("failed to build connection options");

    let pool = opts
        .connect_json()
        .await
        .expect("failed to connect with json");

    let mut tx = pool.begin().await.expect("failed to get transaction");

    let mut query = tx
        .fetch("INSERT INTO TEST_TABLE VALUES (?, ?, ?)")
        .await
        .expect("failed to create query");

    query.bind_row(row![999, "Carl Voller", "2004-01-05"]);

    let results = query.execute().await.expect("failed to execute query");
    let rows = results
        .rows()
        .try_collect::<Vec<Row>>()
        .await
        .expect("failed to get rows");

    assert!(rows.len() == 1);

    let cell = rows[0].get(0).expect("failed to get first column in row");

    assert!(cell.col.name == "number of rows inserted");

    if let CellValue::Fixed(num_of_rows_affected) = cell.value {
        assert!(num_of_rows_affected.unwrap() == "1")
    }

    tx.rollback().await.expect("failed to rollback");
}
