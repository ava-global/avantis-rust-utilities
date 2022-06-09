use super::*;

use ::diesel::pg::PgConnection;
use ::diesel::r2d2::{ConnectionManager, Pool, PoolError, PooledConnection};
use ::diesel::{Connection, ConnectionError};
use ::thiserror::Error;

pub type PgPool = Pool<ConnectionManager<PgConnection>>;
pub type PgPooledConnection = PooledConnection<ConnectionManager<PgConnection>>;

pub trait DieselDatabaseConfig {
    fn init_pool(&self) -> Result<PgPool, Error>;
}

impl DieselDatabaseConfig for DatabaseConfig {
    fn init_pool(&self) -> Result<PgPool, Error> {
        let database_url = self.postgres_uri();
        PgConnection::establish(&database_url)?;

        let manager = ConnectionManager::<PgConnection>::new(database_url);
        Ok(Pool::builder()
            .max_size(self.max_connections)
            .connection_timeout(self.connection_timeout())
            .build(manager)?)
    }
}

pub fn fetch_connection(pool: &PgPool) -> Result<PgPooledConnection, Error> {
    Ok(pool.get()?)
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("connection error: `{0}`")]
    ConnectionError(#[from] ConnectionError),
    #[error("pool error: `{0}`")]
    PoolError(#[from] PoolError),
}
