use std::cmp::max;
use std::time::Duration;

use ::redis::AsyncCommands;
use avantis_utils::config::load_config;
use avantis_utils::config::Environment;
use avantis_utils::redis::connection::RedisConfig;
use avantis_utils::redis::AsyncCommandsExt;
use avantis_utils::redis::Result;
use serial_test::serial;
use tokio;

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
struct ExampleConfig {
    redis: RedisConfig,
}

#[tokio::test]
#[serial]
async fn test_get_or_fetch() -> Result<()> {
    let pool = load_config::<ExampleConfig>(Environment::Test)?
        .redis
        .init_pool()
        .await?;

    let mut conn = pool.get().await?;

    let key = "TEST_GET_OR_FETCH";
    let expire_seconds = 1;

    fn hello_world(time: i32) -> String {
        format!("HELLO WORLD {time}")
    }

    let wait_cache_expire = || async {
        tokio::time::sleep(Duration::from_secs(expire_seconds + 1)).await;
    };

    // Test that caching works

    let _: () = conn.del(&key).await?;
    let get_data = move || async move { anyhow::Ok(hello_world(0)) };
    conn.get_or_fetch(&key, get_data, expire_seconds as usize)
        .await?;
    for n in 0..5 {
        let get_data = move || async move { anyhow::Ok(hello_world(n)) };
        let result = conn
            .get_or_fetch(&key, get_data, expire_seconds as usize)
            .await?;
        assert_eq!(result, hello_world(0));
    }

    // Test that expire strategy works

    let _: () = conn.del(&key).await?;

    for n in 0..3 {
        let get_data = move || async move { anyhow::Ok(hello_world(n)) };
        let result = conn
            .get_or_fetch(&key, get_data, expire_seconds as usize)
            .await?;
        assert_eq!(result, hello_world(n));

        let result: Option<String> = conn.get(&key).await?;
        assert_eq!(result.unwrap(), hello_world(n));

        wait_cache_expire().await;
    }

    // Test that error are thrown properly

    let _: () = conn.del(&key).await?;

    let get_data = move || async move { anyhow::bail!("unable to load data") };
    let result: Result<String> = conn
        .get_or_fetch(&key, get_data, expire_seconds as usize)
        .await;

    assert_eq!(
        format!("{:?}", result.unwrap_err()),
        "Data(unable to load data)"
    );

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_get_or_refresh() -> Result<()> {
    let pool = load_config::<ExampleConfig>(Environment::Test)?
        .redis
        .init_pool()
        .await?;

    let mut conn = pool.get().await?;

    let key = "TEST_GET_OR_REFRESH";
    let expire_seconds = 1;

    fn hello_world(time: i32) -> String {
        format!("HELLO WORLD {time}")
    }

    let wait_cache_expire = || async {
        tokio::time::sleep(Duration::from_secs(expire_seconds + 1)).await;
    };

    // Test that caching works

    let _: () = conn.del(&key).await?;

    let get_data = move || async move { anyhow::Ok(hello_world(0)) };
    conn.get_or_refresh(&key, get_data, 1000).await?;
    for n in 0..5 {
        let get_data = move || async move { anyhow::Ok(hello_world(n)) };
        let result = conn
            .get_or_refresh(&key, get_data, expire_seconds as usize)
            .await?;
        assert_eq!(result, hello_world(0));
    }

    // Test that expire strategy works

    let _: () = conn.del(&key).await?;

    for n in 0..3 {
        let get_data = move || async move { anyhow::Ok(hello_world(n)) };
        let result = conn
            .get_or_refresh(&key, get_data, expire_seconds as usize)
            .await?;
        assert_eq!(result, hello_world(max(n - 1, 0)));

        let result: Option<String> = conn.hget(&key, "value").await?;
        assert_eq!(result.unwrap(), hello_world(n));

        wait_cache_expire().await;
    }

    // Test that error are thrown properly

    let _: () = conn.del(&key).await?;

    let get_data = move || async move { anyhow::bail!("unable to load data") };
    let result: Result<String> = conn
        .get_or_refresh(&key, get_data, expire_seconds as usize)
        .await;

    assert_eq!(
        format!("{:?}", result.unwrap_err()),
        "Data(unable to load data)"
    );

    Ok(())
}
