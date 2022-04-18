use async_trait::async_trait;
use bb8_redis::bb8::RunError;
use bb8_redis::redis::RedisError;
use redis::{AsyncCommands, FromRedisValue, ToRedisArgs};
use redis_cluster_async::Connection;
use std::{
    future::Future,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tracing::error;

#[async_trait]
pub trait AsyncCommandsExt: AsyncCommands {
    async fn get_or_fetch<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Clone + Sync,
        V: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<V>> + Send;

    async fn get_or_refresh<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Clone + Sync + Copy,
        V: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<V>> + Send;
}

#[async_trait]
impl AsyncCommandsExt for Connection {
    async fn get_or_fetch<K, V, F, Fut>(
        &mut self,
        key: K,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        K: ToRedisArgs + Send + Clone + Sync,
        V: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
        F: FnOnce() -> Fut + Send + 'static,
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
        K: ToRedisArgs + Send + Sync + Clone + Copy,
        V: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<V>> + Send,
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let is_expired = |expired_when: u64| expired_when > now;

        match (
            self.hget::<_, _, Option<u64>>(key.clone(), "expired_when")
                .await,
            self.hget::<_, _, Option<V>>(key.clone(), "value").await,
        ) {
            (Ok(Some(expired_when)), Ok(Some(value))) if is_expired(expired_when) => Ok(value),
            (Ok(Some(_)), Ok(Some(value))) => {
                let new_expired_when = now + expire_seconds as u64;

                let new_value = data_loader().await?;

                let _: () = self
                    .hset(key.clone(), "expired_when", new_expired_when)
                    .await?;
                let _: () = self.hset(key.clone(), "value", new_value.clone()).await?;

                Ok(value)
            }
            (Ok(None), _) | (_, Ok(None)) => {
                let new_expired_when = now + expire_seconds as u64;

                let new_value = data_loader().await?;

                let _: () = self
                    .hset(key.clone(), "expired_when", new_expired_when)
                    .await?;
                let _: () = self.hset(key.clone(), "value", new_value.clone()).await?;

                Ok(new_value)
            }
            (Err(err), _) | (_, Err(err)) => {
                error!("redis error: {:?}", err);
                let new_expired_when = now + expire_seconds as u64;

                let new_value = data_loader().await?;

                let _: () = self
                    .hset(key.clone(), "expired_when", new_expired_when)
                    .await?;
                let _: () = self.hset(key.clone(), "value", new_value.clone()).await?;

                Ok(new_value)
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

pub mod connection {
    use async_trait::async_trait;
    use bb8_redis::bb8;
    use bb8_redis::bb8::Pool;
    use bb8_redis::bb8::PooledConnection;
    use redis::IntoConnectionInfo;
    use redis::RedisError;
    use redis::RedisResult;
    use redis_cluster_async::Client;
    use redis_cluster_async::Connection;
    use serde::Deserialize;

    use super::Result;

    pub type PooledClusterConnection = PooledConnection<'static, RedisClusterConnectionManager>;

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

        pub async fn init_pool(&self) -> Result<Pool<RedisClusterConnectionManager>> {
            Ok(bb8::Pool::builder()
                .max_size(self.max_connections)
                .build(RedisClusterConnectionManager::new(self.hosts_str())?)
                .await?)
        }
    }

    pub struct RedisClusterConnectionManager {
        client: Client,
    }

    impl RedisClusterConnectionManager {
        pub fn new<T: IntoConnectionInfo>(info: Vec<T>) -> Result<Self> {
            Ok(RedisClusterConnectionManager {
                client: Client::open(info)?,
            })
        }
    }

    #[async_trait]
    impl bb8::ManageConnection for RedisClusterConnectionManager {
        type Connection = Connection;
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
