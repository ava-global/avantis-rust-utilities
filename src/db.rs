//! Db module. include common [DatabaseConfig](crate::db::DatabaseConfig) designed
//! to be use with Avantis [config](crate::config) module.
//!
//! By default, we use Postgres as our database at Avantis.

use std::time::Duration;

use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::Error;
use sqlx::Pool;
use sqlx::Postgres;

/// Standard database config. Designed to be used in config module,
/// one database per config.
///
/// # Example
///
/// ```
/// # use avantis_utils::db::DatabaseConfig;
/// let config = DatabaseConfig {
///   host: "localhost".to_string(),
///   user: "username".to_string(),
///   password: "REPLACE_ME".to_string(),
///   db_name: "my_db".to_string(),
///   max_connections: 30
/// };
///
/// println!("{:?}", config);
/// // initialize the pool by calling `config.init_pool().await?`
/// ```
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DatabaseConfig {
    pub host: String,
    pub user: String,
    pub password: String,
    pub db_name: String,
    pub max_connections: u32,
}

impl DatabaseConfig {
    pub async fn init_pool(&self) -> Result<Pool<Postgres>, Error> {
        self.pool_options().connect(&self.postgres_uri()).await
    }

    fn connection_timeout(&self) -> Duration {
        if cfg!(test) {
            Duration::from_nanos(1)
        } else {
            Duration::from_secs(30)
        }
    }

    fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect_timeout(self.connection_timeout())
    }

    fn postgres_uri(&self) -> String {
        format!(
            "postgres://{}:{}@{}/{}",
            self.user, self.password, self.host, self.db_name
        )
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;

    #[test]
    fn test_postgres_uri() {
        assert_eq!(
            "postgres://username:supersecurepassword@localhost/my_db",
            CONFIG.postgres_uri(),
        );
    }

    #[test]
    fn test_pool_options() {
        assert_eq!(
            "\
            PoolOptions { \
                max_connections: 30, \
                min_connections: 0, \
                connect_timeout: 1ns, \
                max_lifetime: Some(1800s), \
                idle_timeout: Some(600s), \
                test_before_acquire: true \
            }\
            ",
            format!("{:?}", CONFIG.pool_options()),
        );
    }

    static CONFIG: Lazy<DatabaseConfig> = Lazy::new(|| DatabaseConfig {
        host: "localhost".to_string(),
        user: "username".to_string(),
        password: "supersecurepassword".to_string(),
        db_name: "my_db".to_string(),
        max_connections: 30,
    });
}
