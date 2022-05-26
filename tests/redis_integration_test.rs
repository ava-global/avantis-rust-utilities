#![cfg(all(feature = "redis-utils", feature = "cfg"))]

use ::redis::AsyncCommands;
use avantis_utils::redis::GetOrFetchExt;
use avantis_utils::redis::GetOrRefreshExt;
use avantis_utils::redis::Result;
use serial_test::serial;
use tokio;

#[tokio::test]
#[serial]
async fn test_get_or_fetch() -> Result<()> {
    let mut connection = connection::get_redis_connection().await.unwrap();

    let key = "TEST_GET_OR_FETCH";

    // Test that caching works

    let get_cached_data_count = 5;
    let expire_seconds = 1000;

    let _: () = connection.del(&key).await.unwrap();

    let start_test_when = unix_time_now_millis();

    let result = connection
        .get_or_fetch(
            &key,
            || async { computation::long(0).await },
            expire_seconds,
        )
        .await
        .unwrap();
    assert_eq!(
        computation::result(0),
        result,
        "Should return computed data: {}. Got {}",
        computation::result(0),
        result,
    );

    let result: Option<String> = connection.get(key).await.unwrap();
    let result = result.unwrap();
    assert_eq!(
        computation::result(0),
        result,
        "Should return cached data: {}. Got {}",
        computation::result(0),
        result,
    );

    for _ in 0..get_cached_data_count {
        let result = connection
            .get_or_fetch(
                &key,
                || async { computation::long(1).await },
                expire_seconds,
            )
            .await
            .unwrap();
        assert_eq!(
            computation::result(0),
            result,
            "Should return cached data: {}. Got {}",
            computation::result(0),
            result,
        );
    }

    let time_used_millis = unix_time_now_millis() - start_test_when;

    assert!(
        time_used_millis
            < computation::LONG_COMPUTATION_TIME_MILLIS as u128 * get_cached_data_count,
        "Computation should be computed once if ttl is long enough. ideal computation time: {} ms. time_used: {} ms",
        computation::LONG_COMPUTATION_TIME_MILLIS,
        time_used_millis
    );

    // Test that expire strategy works

    let expire_seconds = 1;

    let _: () = connection.del(&key).await.unwrap();

    connection
        .get_or_fetch(
            key,
            || async { computation::simple(0).await },
            expire_seconds,
        )
        .await
        .unwrap();

    for n in 1..4 {
        computation::wait_expire(expire_seconds).await;

        let result = connection
            .get_or_fetch(
                &key,
                move || async move { computation::simple(n).await },
                expire_seconds,
            )
            .await
            .unwrap();
        assert_eq!(
            computation::result(n),
            result,
            "Should return new computed data: {}. Got {}",
            computation::result(n),
            result,
        );

        let result: Option<String> = connection.get(&key).await.unwrap();
        let result = result.unwrap();
        assert_eq!(
            computation::result(n),
            result,
            "Should store cached data: {}, Got {}",
            computation::result(n),
            result
        );
    }

    // Test that error are thrown properly

    let expire_seconds = 1;

    let _: () = connection.del(&key).await.unwrap();

    let result: Result<String> = connection
        .get_or_fetch(
            &key,
            || async { computation::fail("unable to load data").await },
            expire_seconds,
        )
        .await;
    let err = result.unwrap_err();

    assert_eq!(format!("{:?}", err), "Data(unable to load data)");

    // Test that error are thrown if refreshing fail

    let expire_seconds = 1;

    let _: () = connection.del(key).await.unwrap();

    connection
        .get_or_fetch(
            key,
            || async { computation::simple(0).await },
            expire_seconds,
        )
        .await
        .unwrap();

    computation::wait_expire(expire_seconds).await;

    let result: Result<String> = connection
        .get_or_fetch(
            &key,
            || async { computation::fail("unable to load data").await },
            expire_seconds,
        )
        .await;
    let err = result.unwrap_err();

    assert_eq!(format!("{:?}", err), "Data(unable to load data)");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_or_refresh() -> Result<()> {
    let mut connection = connection::get_redis_connection().await.unwrap();

    let key = "TEST_GET_OR_REFRESH";

    // Test that it properly return cached data

    let get_cached_data_count = 5;
    let expire_seconds = 1000;

    let _: () = connection.del(key).await.unwrap();

    let start_test_when = unix_time_now_millis();

    let result = connection::get_redis_connection()
        .await
        .unwrap()
        .get_or_refresh(key, || async { computation::long(0).await }, expire_seconds)
        .await
        .unwrap();
    assert_eq!(
        computation::result(0),
        result,
        "Should return computed data: {}. Got {}",
        computation::result(0),
        result,
    );

    let result: Option<String> = connection.hget(key, "value").await.unwrap();
    let result = result.unwrap();
    assert_eq!(
        computation::result(0),
        result,
        "Should return cached data: {}. Got {}",
        computation::result(0),
        result,
    );

    for _ in 0..get_cached_data_count {
        let result = connection::get_redis_connection()
            .await
            .unwrap()
            .get_or_refresh(key, || async { computation::long(1).await }, expire_seconds)
            .await
            .unwrap();
        assert_eq!(
            computation::result(0),
            result,
            "Should return cached data: {}. Got {}",
            computation::result(0),
            result,
        );
    }

    let time_used_millis = unix_time_now_millis() - start_test_when;

    assert!(
        time_used_millis
            < computation::LONG_COMPUTATION_TIME_MILLIS as u128 * get_cached_data_count,
        "Computation should be computed once if ttl is long enough. ideal computation time: {} ms. time_used: {} ms",
        computation::LONG_COMPUTATION_TIME_MILLIS,
        time_used_millis
    );

    // Test that refresh works if data is expired

    let expire_seconds = 1;

    let _: () = connection.del(key).await.unwrap();

    connection::get_redis_connection()
        .await
        .unwrap()
        .get_or_refresh(
            key,
            || async { computation::simple(0).await },
            expire_seconds,
        )
        .await
        .unwrap();

    for n in 1..4 {
        computation::wait_expire(expire_seconds).await;

        let result = connection::get_redis_connection()
            .await
            .unwrap()
            .get_or_refresh(
                key,
                move || async move { computation::simple(n).await },
                expire_seconds as usize,
            )
            .await
            .unwrap();

        assert_eq!(
            computation::result(n - 1),
            result,
            "Should return expired cached data: {}. Got {}",
            computation::result(n - 1),
            result,
        );

        // This assertion might be flaky depends on if redis successfully cached data within
        // wait_simple timeframe. If this happens, kindly increase BUFFER_COMPUTATION_TIME_MILLIS
        computation::wait_simple().await;
        let result: Option<String> = connection.hget(key, "value").await.unwrap();
        let result = result.unwrap();
        assert_eq!(
            computation::result(n),
            result,
            "Should store cached data: {}, Got {}",
            computation::result(n),
            result
        );
    }

    // Test that error are thrown properly

    let expire_seconds = 1;

    let _: () = connection.del(key).await.unwrap();

    let result: Result<String> = connection::get_redis_connection()
        .await
        .unwrap()
        .get_or_refresh(
            key,
            || async { computation::fail("unable to load data").await },
            expire_seconds as usize,
        )
        .await;
    let err = result.unwrap_err();

    assert_eq!(format!("{:?}", err), "Data(unable to load data)");

    // Test that error are *NOT* thrown if refreshing fail

    let expire_seconds = 1;

    let _: () = connection.del(key).await.unwrap();

    connection::get_redis_connection()
        .await
        .unwrap()
        .get_or_refresh(
            key,
            || async { computation::simple(0).await },
            expire_seconds as usize,
        )
        .await
        .unwrap();

    computation::wait_expire(expire_seconds).await;

    let result = connection::get_redis_connection()
        .await
        .unwrap()
        .get_or_refresh(
            key,
            || async { computation::fail("unable to load data").await },
            expire_seconds as usize,
        )
        .await
        .unwrap();

    assert_eq!(
        computation::result(0),
        result,
        "Should return expired cached data: {}. Got {}",
        computation::result(0),
        result,
    );

    Ok(())
}

mod computation {
    use std::time::Duration;

    use anyhow::bail;
    use anyhow::Ok;
    use anyhow::Result;
    use tokio::time::sleep;

    pub(super) static LONG_COMPUTATION_TIME_MILLIS: u64 = 3000;
    pub(super) static BUFFER_COMPUTATION_TIME_MILLIS: u64 = 1000;

    pub(super) async fn simple(input: i32) -> Result<String> {
        Ok(result(input))
    }
    pub(super) async fn long(input: i32) -> Result<String> {
        sleep(Duration::from_millis(LONG_COMPUTATION_TIME_MILLIS)).await;
        Ok(result(input))
    }
    pub(super) async fn fail(message: &'static str) -> Result<String> {
        bail!(message)
    }

    pub(super) fn result(input: i32) -> String {
        format!("HELLO WORLD {input}")
    }

    pub(super) async fn wait_expire(cache_ttl: usize) {
        sleep(Duration::from_secs(cache_ttl as u64)).await;
        sleep(Duration::from_millis(BUFFER_COMPUTATION_TIME_MILLIS)).await;
    }

    pub(super) async fn wait_simple() {
        tokio::time::sleep(Duration::from_millis(BUFFER_COMPUTATION_TIME_MILLIS)).await;
    }
}

mod connection {
    use once_cell::sync::Lazy;
    use tokio::sync::OnceCell;

    use avantis_utils::config::load_config;
    use avantis_utils::config::Environment;
    use avantis_utils::redis::Connection;
    use avantis_utils::redis::Pool;
    use avantis_utils::redis::RedisConfig;

    #[derive(Clone, Debug, PartialEq, serde::Deserialize)]
    struct ExampleConfig {
        redis: RedisConfig,
    }

    impl ExampleConfig {
        fn load(environment: Environment) -> anyhow::Result<Self> {
            load_config(environment)
        }
    }

    static CONFIG: Lazy<ExampleConfig> =
        Lazy::new(|| ExampleConfig::load(Environment::Test).unwrap());

    static REDIS_POOL: OnceCell<Pool> = OnceCell::const_new();
    pub(super) async fn get_redis_connection() -> anyhow::Result<Connection> {
        REDIS_POOL
            .get_or_init(|| async { CONFIG.redis.init_pool().await.unwrap() })
            .await
            .get()
            .await
            .map_err(|err| err.into())
    }
}

fn unix_time_now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}
