use super::*;

use ::sqlx::postgres::PgPoolOptions;
use ::sqlx::Error;
use ::sqlx::Pool;
use ::sqlx::Postgres;
use async_trait::async_trait;
use tracing::instrument;

#[async_trait]
pub trait SqlxDatabaseConfig {
    async fn init_pool(&self) -> Result<Pool<Postgres>, Error>;
}

#[async_trait]
impl SqlxDatabaseConfig for DatabaseConfig {
    #[instrument(skip_all, name = "db::sqlx::init_pool", fields(host = %self.host, db = %self.db_name))]
    async fn init_pool(&self) -> Result<Pool<Postgres>, Error> {
        self.pool_options().connect(&self.postgres_uri()).await
    }
}

impl DatabaseConfig {
    fn pool_options(&self) -> PgPoolOptions {
        PgPoolOptions::new()
            .max_connections(self.max_connections)
            .acquire_timeout(self.connection_timeout())
    }
}

#[cfg(test)]
mod tests {
    use once_cell::sync::Lazy;

    use super::*;

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
