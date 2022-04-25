use std::time::Duration;

use ::redis::AsyncCommands;
use anyhow::Result;
use avantis_utils::config::load_config;
use avantis_utils::config::Environment;
use avantis_utils::redis;
use avantis_utils::redis::Connection;
use avantis_utils::redis::GetOrFetchExt;
use avantis_utils::redis::Pool;
use avantis_utils::redis::RedisConfig;
use once_cell::sync::Lazy;
use tokio;
use tokio::sync::OnceCell;

#[tokio::main]
async fn main() -> Result<()> {
    let mut redis = get_redis_connection().await?;

    let _: () = redis.set("name", "John").await?;
    let name: String = redis.get("name").await?;
    let _: () = redis.del("name").await?;

    println!("name: {name}");

    redis
        .get_or_refresh("text", || async { hello_world(0).await }, 1)
        .await?;
    let text: String = redis
        .get_or_refresh("text", || async { hello_world(1).await }, 1)
        .await?;

    println!("text: {text}");

    tokio::time::sleep(Duration::from_secs(1)).await;

    redis
        .get_or_refresh("text", || async { hello_world(2).await }, 1)
        .await?;
    let text: String = redis
        .get_or_refresh("text", || async { hello_world(3).await }, 1)
        .await?;

    println!("text: {text}");

    Ok(())
}

async fn hello_world(time: usize) -> anyhow::Result<String> {
    Ok(format!("Hello World: {time}"))
}

static CONFIG: Lazy<ExampleConfig> =
    Lazy::new(|| ExampleConfig::load(Environment::Develop).unwrap());

static REDIS_POOL: OnceCell<Pool> = OnceCell::const_new();
pub(crate) async fn get_redis_connection() -> anyhow::Result<Connection> {
    REDIS_POOL
        .get_or_init(|| async { CONFIG.redis.init_pool().await.unwrap() })
        .await
        .get()
        .await
        .map_err(|err| err.into())
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
struct ExampleConfig {
    redis: RedisConfig,
}

impl ExampleConfig {
    fn load(environment: Environment) -> anyhow::Result<Self> {
        load_config(environment)
    }
}
