use avantis_utils::config::load_config;
use avantis_utils::config::Environment;
use avantis_utils::db;
use avantis_utils::redis;
use once_cell::sync::Lazy;

static CONFIG: Lazy<ExampleConfig> =
    Lazy::new(|| ExampleConfig::load(Environment::Development).unwrap());

fn main() {
    // This is an example. DO NOT ACTUALLY SET YOUR PASSWORD IN YOUR CODEBASE.
    // please refer to security best practice for your application deployment.
    std::env::set_var("APP__DB__PASSWORD", "supersecurepassword");

    // This will load config from
    //   1. config/base
    //   2. config/development
    //   3. overriding env variables
    println!("{:#?}", ExampleConfig::load(Environment::Development));

    // Works with different config file format like toml and json as well.
    println!("{:#?}", ExampleConfig::load(Environment::Test));
    println!("{:#?}", ExampleConfig::load(Environment::Production));

    // Works with environment selected from env too.
    std::env::set_var("APP_ENVIRONMENT", "development");
    println!(
        "{:#?}",
        ExampleConfig::load(Environment::from_env().unwrap())
    );

    setup_db();

    std::env::remove_var("APP__DB__PASSWORD");
    std::env::remove_var("APP_ENVIRONMENT");
}

fn setup_db() {
    // Config could then be loaded into [once_cell::sync::OnceCell] or [once_cell::sync::Lazy]
    // for easier access

    println!("{:#?}", *CONFIG);
    println!("{:#?}", CONFIG.db);
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
struct ExampleConfig {
    log_level: String,
    db: db::DatabaseConfig,
    redis: redis::connection::RedisConfig,
}

impl ExampleConfig {
    fn load(environment: Environment) -> anyhow::Result<Self> {
        load_config(environment)
    }
}
