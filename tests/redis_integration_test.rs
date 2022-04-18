use std::time::Duration;

use tokio;

use avantis_utils::redis;
#[tokio::test]
async fn testx() {
    redis::initialize(
        "redis://avantis-redis-dev-5295a48.3o3tur.clustercfg.apse1.cache.amazonaws.com:6379",
        2,
    )
    .await
    .unwrap();

    let key = "TEST1234";
    let expire_seconds = 5;

    redis::del(key).await.unwrap();

    // Initial Set
    let get_data = || async { Ok("HELO".to_string()) };
    let result = redis::get_or_set_with_expire2(&key, get_data, expire_seconds)
        .await
        .unwrap();
    assert_eq!(result, "HELO");

    let result = redis::hget::<_, _, String>(&key, "value")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, "HELO");

    // First get, return cached value & fetch new one
    tokio::time::sleep(Duration::from_secs(6)).await;
    let get_data = || async { Ok("HELO2".to_string()) };
    let result = redis::get_or_set_with_expire2(&key, get_data, expire_seconds)
        .await
        .unwrap();

    assert_eq!(result, "HELO");

    // Second get, return cached value
    tokio::time::sleep(Duration::from_secs(2)).await;
    let result = redis::get_or_set_with_expire2(&key, get_data, expire_seconds)
        .await
        .unwrap();

    assert_eq!(result, "HELO2");

    let result = redis::get_or_set_with_expire2(&key, get_data, expire_seconds)
        .await
        .unwrap();
    assert_eq!(result, "HELO2");
}
