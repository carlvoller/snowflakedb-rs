use futures_util::TryStreamExt;

#[cfg(test)]
use super::*;

#[cfg(feature = "arrow")]
#[tokio::test]
async fn test_tx() {
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
        .download_chunks_in_parallel(10 as usize)
        .download_chunks_in_order(false)
        .build()
        .expect("failed to build connection options");

    let pool = opts
        .connect_arrow()
        .await
        .expect("failed to connect with json");

    let mut tx = pool.begin().await.expect("failed to get transaction");

    let describe_results = tx
        .query("SELECT * FROM TEST_TABLE")
        .await
        .expect("failed to describe")
        .describe()
        .await
        .expect("failed to describe");

    println!("{:?}", describe_results);

    let mut query = tx
        .query("INSERT INTO TEST_TABLE VALUES (?, ?, ?)")
        .await
        .expect("failed to create query");

    query.bind_row(row![999, "Carl Voller", "2004-01-05"]);

    let results = query.execute().await.expect("failed to execute query");

    if results.is_dql() {
        let expected = results.expected_result_length();
        let mut rows = results.rows();
        let mut count = 0;
        while let Some(row) = rows.try_next().await.expect("failed to try stream") {
            println!("{:?}", row);
            count += 1;
        }
        assert!(count == expected);
        println!("Retrieved {:?} rows", count);
    } else if results.is_dml() {
        println!("Number of affected rows: {:?}", results.rows_affected());
    }

    let fetch_batch = tx
        .query("SELECT * FROM TEST_TABLE")
        .await
        .expect("failed to describe");

    let results = fetch_batch
        .execute()
        .await
        .expect("failed to execute query");

    println!("{:?}", results.schema());

    let mut batches = results.record_batches();
    while let Some(row) = batches.try_next().await.expect("failed to try stream") {
        println!("{:?}", row);
    }

    tx.rollback().await.expect("failed to rollback");
}
