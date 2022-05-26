fn main() {
    inner::main();
}

#[cfg(all(feature = "cfg", feature = "db", feature = "redis-utils"))]
mod inner {
    use avantis_utils::config::load_config;
    use avantis_utils::config::Environment;
    use avantis_utils::db;
    use avantis_utils::redis;
    use once_cell::sync::Lazy;

    static CONFIG: Lazy<ExampleConfig> =
        Lazy::new(|| ExampleConfig::load(Environment::Develop).unwrap());

    pub(super) fn main() {
        // This is an example. DO NOT ACTUALLY SET YOUR PASSWORD IN YOUR CODEBASE.
        // please refer to security best practice for your application deployment.
        std::env::set_var("APP_DB__PASSWORD", "supersecurepassword");

        // This will load config from
        //   1. config/base
        //   2. config/develop
        //   3. overriding env variables
        println!("{:#?}", ExampleConfig::load(Environment::Develop));

        // Works with different config file format like toml and json as well.
        println!("{:#?}", ExampleConfig::load(Environment::Test));
        println!("{:#?}", ExampleConfig::load(Environment::Production));

        // Works with environment selected from env too.
        std::env::set_var("APP_ENVIRONMENT", "develop");
        println!(
            "{:#?}",
            ExampleConfig::load(Environment::from_env().unwrap())
        );

        setup_db();

        std::env::remove_var("APP_DB__PASSWORD");
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
        redis: redis::RedisConfig,
    }

    impl ExampleConfig {
        fn load(environment: Environment) -> anyhow::Result<Self> {
            load_config(environment)
        }
    }
}

#[cfg(not(all(feature = "cfg", feature = "db", feature = "redis-utils")))]
mod inner {
    pub fn main() {
        println!("Please pass --features cfg,db,redis-utils to cargo when trying this example.");
    }
}
