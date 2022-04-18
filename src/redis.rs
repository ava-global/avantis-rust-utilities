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
use bb8_redis::bb8::{Pool, PooledConnection};

use tokio::sync::OnceCell;
use tracing::error;

use redis::{Commands, FromRedisValue, ToRedisArgs};

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

#[tracing::instrument(name = "redis::del")]
#[cfg_attr(test, mockable)]
pub async fn del(key: &str) -> anyhow::Result<()> {
    get_connection().await?.del(key).map_err(|err| err.into())
}

#[tracing::instrument(name = "redis::set", skip_all)]
#[cfg_attr(test, mockable)]
pub async fn set<V>(key: &str, value: V) -> anyhow::Result<()>
where
    V: ToRedisArgs + Send + Sync,
{
    get_connection()
        .await?
        .set(key, value)
        .map_err(|err| err.into())
}

#[cfg_attr(test, mockable)]
pub async fn hget<K, F, V>(key: K, field: F) -> anyhow::Result<Option<V>>
where
    K: ToRedisArgs + Sync + Send,
    F: ToRedisArgs + Sync,
    V: FromRedisValue + Sync,
{
    get_connection()
        .await?
        .hget(key, field)
        .map_err(|err| err.into())
}

#[cfg_attr(test, mockable)]
pub async fn hset_multiple<K, F, V>(key: K, items: &[(F, V)]) -> anyhow::Result<()>
where
    K: ToRedisArgs + Sync + Send,
    F: ToRedisArgs + Sync,
    V: ToRedisArgs + Sync,
{
    get_connection()
        .await?
        .hset_multiple(key, items)
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

#[cfg_attr(test, mockable)]
pub async fn get_or_fetch<F, Fut, T>(
    cache_key: &str,
    data_loader: F,
    expire_seconds: usize,
) -> anyhow::Result<T>
where
    T: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
    Fut: Future<Output = anyhow::Result<T>> + Send,
    F: FnOnce() -> Fut + Send + 'static,
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

#[cfg_attr(test, mockable)]
pub async fn get_or_refresh<F, Fut, T>(
    key: &str,
    data_loader: F,
    expire_seconds: usize,
) -> anyhow::Result<T>
where
    T: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
    Fut: Future<Output = anyhow::Result<T>> + Send,
    F: FnOnce() -> Fut + Send + 'static,
{
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    let is_expired = |expired_when: u64| expired_when > now;

    async fn load_and_set_cache<F, Fut, T>(
        key: String,
        data_loader: F,
        expired_when: u64,
    ) -> anyhow::Result<T>
    where
        T: FromRedisValue + ToRedisArgs + Send + Sync + Clone,
        Fut: Future<Output = anyhow::Result<T>> + Send,
        F: FnOnce() -> Fut + Send + 'static,
    {
        let new_value = data_loader().await?;
        hset_multiple(&key, &vec![("expired_when", expired_when)]).await?;
        hset_multiple(&key, &vec![("value", new_value.clone())]).await?;

        Ok(new_value)
    }

    match (
        hget::<_, _, u64>(key, "expired_when").await,
        hget::<_, _, T>(key, "value").await,
    ) {
        (Ok(Some(expired_when)), Ok(Some(value))) if is_expired(expired_when) => Ok(value),
        (Ok(Some(_)), Ok(Some(value))) => {
            let cloned_key = key.to_owned();
            let new_expired_when = now + expire_seconds as u64;
            tokio::spawn(async move {
                let _ = load_and_set_cache(cloned_key, data_loader, new_expired_when).await;
                //TODO: handle me
            });

            Ok(value)
        }
        (Ok(None), _) | (_, Ok(None)) => {
            let new_expired_when = now + expire_seconds as u64;
            let new_value =
                load_and_set_cache(key.to_owned(), data_loader, new_expired_when).await?;

            Ok(new_value)
        }
        (Err(err), _) | (_, Err(err)) => {
            error!("Failed to connect to redis: {}", err);
            Ok(data_loader().await?)
        }
    }
}
