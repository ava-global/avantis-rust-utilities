use async_trait::async_trait;
use bb8_redis::bb8;
use bb8_redis::redis::cluster::ClusterConnection;
use bb8_redis::redis::{ErrorKind, IntoConnectionInfo, RedisError};
use redis::cluster::ClusterClient;

#[cfg(test)]
use mocktopus::macros::mockable;

use std::{
    future::Future,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::anyhow;
use anyhow::Result;
use bb8_redis::bb8::{self, Pool, PooledConnection};

use tokio::sync::OnceCell;
use tracing::error;

use redis::{Commands, FromRedisValue, ToRedisArgs};

use crate::redis::RedisClusterConnectionManager;

pub struct RedisClusterConnectionManager {
    client: ClusterClient,
}

impl RedisClusterConnectionManager {
    pub fn new<T: IntoConnectionInfo>(
        info: Vec<T>,
    ) -> Result<RedisClusterConnectionManager, RedisError> {
        let connection_info = info
            .into_iter()
            .map(|x| x.into_connection_info().unwrap())
            .collect::<_>();
        Ok(RedisClusterConnectionManager {
            client: ClusterClient::open(connection_info)?,
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisClusterConnectionManager {
    type Connection = ClusterConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_connection()
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn.check_connection() {
            true => Ok(()),
            false => Err((ErrorKind::ResponseError, "connection fail").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

static CONNECTION_POOL: OnceCell<Pool<RedisClusterConnectionManager>> = OnceCell::const_new();

// [redis_endpoint, [key]]
type RedisKeysResponse = Vec<(String, Vec<String>)>;

#[tracing::instrument(name = "redis::set_with_expire_seconds", skip_all)]
#[cfg_attr(test, mockable)]
pub async fn set_with_expire_seconds<K, V>(
    key: &K,
    value: &V,
    expire_seconds: usize,
) -> anyhow::Result<()>
where
    V: ToRedisArgs + Send + Sync,
    K: ToRedisArgs + Send + Sync,
{
    get_connection()
        .await?
        .set_ex(key, value, expire_seconds)
        .map_err(|err| err.into())
}

#[tracing::instrument(name = "redis::get")]
#[cfg_attr(test, mockable)]
pub async fn get<T: FromRedisValue>(key: &str) -> anyhow::Result<Option<T>> {
    get_connection().await?.get(key).map_err(|err| err.into())
}

#[tracing::instrument(name = "redis::set", skip_all)]
#[cfg_attr(test, mockable)]
pub async fn set<V>(key: &str, value: &V) -> anyhow::Result<()>
where
    V: ToRedisArgs + Send + Sync,
{
    get_connection()
        .await?
        .set(key, value)
        .map_err(|err| err.into())
}

#[tracing::instrument(name = "redis::keys")]
#[cfg_attr(test, mockable)]
pub async fn keys(pattern: &str) -> anyhow::Result<Vec<String>> {
    let result: Vec<RedisKeysResponse> = get_connection()
        .await?
        .keys(pattern)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;

    Ok(result
        .into_iter()
        .flat_map(|res| res.into_iter().flat_map(|r| r.1).collect::<Vec<String>>())
        .collect::<Vec<String>>())
}

#[tracing::instrument(name = "redis::m_get")]
#[cfg_attr(test, mockable)]
pub async fn m_get(keys: &[&str]) -> anyhow::Result<Vec<Option<String>>> {
    get_connection().await?.get(keys).map_err(|err| err.into())
}

pub async fn initialize(redis_url: &str, max_size: u32) -> anyhow::Result<()> {
    CONNECTION_POOL
        .set(create_connection_pool(redis_url, max_size).await?)
        .map_err(|_error| return anyhow!("redis connection pool is already initialized"))
}

#[cfg_attr(test, mockable)]
async fn get_connection() -> anyhow::Result<PooledConnection<'static, RedisClusterConnectionManager>>
{
    CONNECTION_POOL
        .get()
        .expect("redis connection pool is not initialized")
        .get()
        .await
        .map_err(|error| error.into())
}

#[cfg_attr(test, mockable)]
async fn create_connection_pool(
    redis_url: &str,
    max_size: u32,
) -> anyhow::Result<Pool<RedisClusterConnectionManager>> {
    let urls: Vec<&str> = redis_url.split(',').collect();
    let manager = RedisClusterConnectionManager::new(urls)?;
    Ok(bb8::Pool::builder()
        .max_size(max_size)
        .build(manager)
        .await?)
}

#[tracing::instrument(name = "redis::get_or_set_with_expire")]
#[cfg_attr(test, mockable)]
pub async fn get_or_set_with_expire<F, Fut, T>(
    cache_key: &str,
    data_loader: F,
    expire_seconds: usize,
) -> anyhow::Result<T>
where
    T: FromRedisValue + ToRedisArgs + Send + Sync,
    Fut: Future<Output = anyhow::Result<T>> + Send,
    F: FnOnce() -> Fut + Send,
{
    match get(cache_key).await {
        Ok(Some(bytes)) => Ok(bytes),
        Ok(None) => {
            let result = data_loader().await?;
            set_with_expire_seconds(&cache_key, &result, expire_seconds).await?;
            Ok(result)
        }
        Err(err) => {
            error!("Failed to connect to redis: {}", err);
            Ok(data_loader().await?)
        }
    }
}

#[tracing::instrument(name = "redis::get_or_set_in_background_with_expire")]
#[cfg_attr(test, mockable)]
pub async fn get_or_set_in_background_with_expire<F, Fut>(
    cache_key: &str,
    data_loader: F,
    expire_seconds: u64,
) -> anyhow::Result<Vec<u8>>
where
    Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send,
    F: FnOnce() -> Fut + Send + 'static,
{
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    let get_result: Result<Option<Vec<u8>>> = get(cache_key).await;
    match get_result {
        Ok(Some(bytes)) => {
            let (expire_time_bytes, rest) = bytes.split_at(std::mem::size_of::<u64>());
            let expire_time: u64 = u64::from_be_bytes(expire_time_bytes.try_into().unwrap());
            if expire_time > now {
                let new_expires_time = now + expire_seconds;
                let cache_key = cache_key.to_owned();
                tokio::spawn(async move {
                    if let Err(e) =
                        load_and_set_in_background(cache_key, data_loader, new_expires_time).await
                    {
                        error!("Failed to load and set in background: {}", e);
                    }
                });
            }
            Ok(rest.to_vec())
        }
        Ok(None) => {
            let result = data_loader().await?;
            let expire_time = now + expire_seconds;
            set_and_append_timestamp(cache_key, &result, expire_time).await?;
            Ok(result)
        }
        Err(err) => {
            error!("Failed to connect to redis: {}", err);
            Ok(data_loader().await?)
        }
    }
}

#[cfg_attr(test, mockable)]
async fn set_and_append_timestamp(cache_key: &str, data: &[u8], timestamp: u64) -> Result<()> {
    let mut payload = timestamp.to_be_bytes().to_vec();
    payload.append(data.to_vec().as_mut());
    set(&cache_key, &payload).await
}

#[cfg_attr(test, mockable)]
async fn load_and_set_in_background<F, Fut>(
    cache_key: String,
    data_loader: F,
    timestamp: u64,
) -> Result<()>
where
    Fut: Future<Output = anyhow::Result<Vec<u8>>> + Send,
    F: FnOnce() -> Fut + Send,
{
    let result = data_loader().await?;
    set_and_append_timestamp(&cache_key, &result, timestamp).await
}
