use async_trait::async_trait;
use bb8_redis::bb8::RunError;
use bb8_redis::redis::RedisError;
use redis::{AsyncCommands, FromRedisValue, ToRedisArgs};
use std::{
    future::Future,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tracing::error;

pub use connection::Connection;
pub use connection::Pool;
pub use connection::RedisConfig;

#[async_trait]
pub trait AsyncCommandsExt: AsyncCommands {
    async fn get_or_fetch<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: FromRedisValue + ToRedisArgs + Send + Sync,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = anyhow::Result<V>> + Send;

    async fn get_or_refresh<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: FromRedisValue + ToRedisArgs + Send + Sync,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = anyhow::Result<V>> + Send;
}

#[async_trait]
impl AsyncCommandsExt for redis_cluster_async::Connection {
    async fn get_or_fetch<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: FromRedisValue + ToRedisArgs + Send + Sync,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = anyhow::Result<V>> + Send,
    {
        match self.get(&key).await {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => {
                let result = data_loader().await?;
                self.set_ex(&key, &result, expire_seconds).await?;
                Ok(result)
            }
            Err(err) => {
                error!("redis error: {:?}", err);
                Ok(data_loader().await?)
            }
        }
    }

    async fn get_or_refresh<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Sync,
        V: FromRedisValue + ToRedisArgs + Send + Sync,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = anyhow::Result<V>> + Send,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let is_expired = |expired_when: u64| expired_when > now;

        macro_rules! awaiting_get_and_set {
            () => {{
                let new_expired_when = now + expire_seconds as u64;

                let new_value = data_loader().await?;

                let _: () = self.hset(&key, "expired_when", new_expired_when).await?;
                let _: () = self.hset(&key, "value", &new_value).await?;

                Ok(new_value)
            }};
        }

        match (
            self.hget::<_, _, Option<u64>>(&key, "expired_when").await,
            self.hget::<_, _, Option<V>>(&key, "value").await,
        ) {
            (Ok(Some(expired_when)), Ok(Some(value))) if is_expired(expired_when) => Ok(value),
            (Ok(Some(_)), Ok(Some(value))) => {
                // TODO: This should spawn task to get and set in background instead
                let _: Result<V> = awaiting_get_and_set!();

                Ok(value)
            }
            (Ok(None), _) | (_, Ok(None)) => awaiting_get_and_set!(),
            (Err(err), _) | (_, Err(err)) => {
                error!("redis error: {:?}", err);

                awaiting_get_and_set!()
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("data error")]
    Data(#[from] anyhow::Error),
    #[error("redis error")]
    Redis(#[from] RedisError),
    #[error("cluster connection error")]
    Cluster(#[from] RunError<RedisError>),
}

pub type Result<T> = std::result::Result<T, Error>;

mod connection {
    use async_trait::async_trait;
    use bb8_redis::bb8;
    use redis::IntoConnectionInfo;
    use redis::RedisError;
    use redis::RedisResult;
    use serde::Deserialize;

    use super::Result;

    pub type Pool = bb8::Pool<RedisClusterConnectionManager>;
    pub type Connection = bb8::PooledConnection<'static, RedisClusterConnectionManager>;

    #[derive(Clone, Debug, PartialEq, Deserialize)]
    pub struct RedisConfig {
        pub hosts: Vec<String>,
        pub expire_seconds: usize,
        pub max_connections: u32,
    }

    impl RedisConfig {
        fn hosts_str(&self) -> Vec<&str> {
            self.hosts.iter().map(AsRef::as_ref).collect()
        }

        pub async fn init_pool(&self) -> Result<Pool> {
            Ok(bb8::Pool::builder()
                .max_size(self.max_connections)
                .build(RedisClusterConnectionManager::new(self.hosts_str())?)
                .await?)
        }
    }

    pub struct RedisClusterConnectionManager {
        client: redis_cluster_async::Client,
    }

    impl RedisClusterConnectionManager {
        pub fn new<T: IntoConnectionInfo>(info: Vec<T>) -> Result<Self> {
            Ok(RedisClusterConnectionManager {
                client: redis_cluster_async::Client::open(info)?,
            })
        }
    }

    #[async_trait]
    impl bb8::ManageConnection for RedisClusterConnectionManager {
        type Connection = redis_cluster_async::Connection;
        type Error = RedisError;

        async fn connect(&self) -> RedisResult<Self::Connection> {
            self.client.get_connection().await
        }

        async fn is_valid(&self, _: &mut Self::Connection) -> RedisResult<()> {
            Ok(())
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }
}
