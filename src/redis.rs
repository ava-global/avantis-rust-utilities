use async_trait::async_trait;
use bb8_redis::bb8::RunError;
use redis_rs::{AsyncCommands, ErrorKind, FromRedisValue, RedisError, ToRedisArgs};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::{
    future::Future,
    str::from_utf8,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

// TODO: tracing error wont works. find a new way to communicate to user that it works or not
use tracing::error;

pub use connection::Connection;
pub use connection::Pool;
pub use connection::RedisConfig;

// TODO: add tests for VecRedisValue

#[async_trait]
pub trait GetOrFetchExt: AsyncCommands {
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
}

#[async_trait]
impl GetOrFetchExt for redis_cluster_async::Connection {
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
        if cfg!(test) {
            return Ok(data_loader().await?);
        }

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
}

#[async_trait]
pub trait GetOrRefreshExt {
    async fn get_or_refresh<'a, V, F, Fut>(
        mut self,
        key: &str, // Would be nice if key is K: ToRedisArgs + Send + Sync instead.
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        V: FromRedisValue + ToRedisArgs + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<V>> + Send;
}

#[async_trait]
impl GetOrRefreshExt for connection::Connection {
    async fn get_or_refresh<'a, V, F, Fut>(
        mut self,
        key: &str,
        data_loader: F,
        expire_seconds: usize,
    ) -> Result<V>
    where
        V: FromRedisValue + ToRedisArgs + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<V>> + Send,
    {
        if cfg!(test) {
            return Ok(data_loader().await?);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let is_expired = |expired_when: u64| now > expired_when;

        let owned_key = key.to_owned();
        macro_rules! awaiting_get_and_set {
            () => {{
                let new_expired_when = now + expire_seconds as u64;

                let new_value = data_loader().await?;

                let _: () = self
                    .hset(&owned_key, "expired_when", new_expired_when)
                    .await?;
                let _: () = self.hset(&owned_key, "value", &new_value).await?;

                let result: Result<V> = Ok(new_value);

                result
            }};
        }

        let expired_when: Result<Option<u64>> = Ok(self.hget(key, "expired_when").await?);
        let value: Result<Option<V>> = Ok(self.hget(key, "value").await?);

        match (expired_when, value) {
            (Ok(Some(expired_when)), Ok(Some(value))) if !is_expired(expired_when) => Ok(value),
            (Ok(Some(_)), Ok(Some(value))) => {
                tokio::spawn(async move {
                    if let Err(e) = async { awaiting_get_and_set!() }.await {
                        error!("Failed to load and set in background: {}", e);
                    }
                });

                Ok(value)
            }
            (Ok(None), _) | (_, Ok(None)) => {
                awaiting_get_and_set!()
            }
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

macro_rules! invalid_type_error {
    ($v:expr, $det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response was of incompatible type",
            format!("{:?} (response was {:?})", $det, $v),
        ))
    };
}

pub struct VecRedisValue<T: Serialize + DeserializeOwned>(pub Vec<T>);

impl<T: Serialize + DeserializeOwned> std::ops::Deref for VecRedisValue<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Serialize + DeserializeOwned> From<Vec<T>> for VecRedisValue<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T: Serialize + DeserializeOwned> ToRedisArgs for VecRedisValue<T> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis_rs::RedisWrite,
    {
        let string = json!(self.0).to_string();
        out.write_arg(string.as_bytes())
    }
}

impl<T: Serialize + DeserializeOwned> FromRedisValue for VecRedisValue<T> {
    fn from_redis_value(v: &redis_rs::Value) -> redis_rs::RedisResult<Self> {
        match *v {
            redis_rs::Value::Data(ref bytes) => {
                let json = from_utf8(bytes)?.to_string();
                let result = serde_json::from_str::<Vec<T>>(&json).map_err(|err| {
                    invalid_type_error!(
                        v,
                        format!(
                            "Could not deserialize into {} struct with err {}.",
                            stringify!($t),
                            err
                        )
                    )
                })?;
                Ok(VecRedisValue(result))
            }
            _ => Err(invalid_type_error!(
                v,
                format!("Could not deserialize into {} struct.", stringify!($t))
            )),
        }
    }
}

mod connection {
    use async_trait::async_trait;
    use bb8_redis::bb8;
    use redis_rs::aio::ConnectionLike;
    use redis_rs::IntoConnectionInfo;
    use redis_rs::RedisError;
    use redis_rs::RedisResult;
    use serde::Deserialize;

    use super::Result;

    pub type Pool = bb8::Pool<RedisClusterConnectionManager>;
    pub type Connection = bb8::PooledConnection<'static, RedisClusterConnectionManager>;

    #[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
    pub struct RedisConfig {
        pub hosts_csv: String,
        pub expire_seconds: usize,
        pub max_connections: u32,
    }

    impl RedisConfig {
        fn hosts(&self) -> Vec<&str> {
            self.hosts_csv.split(',').collect()
        }

        pub async fn init_pool(&self) -> Result<Pool> {
            Ok(bb8::Pool::builder()
                .max_size(self.max_connections)
                .build(RedisClusterConnectionManager::new(self.hosts())?)
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

        async fn is_valid(&self, connection: &mut Self::Connection) -> RedisResult<()> {
            connection
                .req_packed_command(&redis_rs::cmd("PING"))
                .await
                .and_then(check_is_pong)
        }

        fn has_broken(&self, _: &mut Self::Connection) -> bool {
            false
        }
    }

    fn check_is_pong(value: redis_rs::Value) -> RedisResult<()> {
        match value {
            redis_rs::Value::Status(string) if &string == "PONG" => RedisResult::Ok(()),
            _ => RedisResult::Err(RedisError::from((
                redis_rs::ErrorKind::ResponseError,
                "ping request",
            ))),
        }
    }
}
