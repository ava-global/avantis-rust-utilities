use anyhow::Result;
use avantis_utils::config::load_default_config;
use avantis_utils::config::Environment;
use once_cell::sync::Lazy;
use serde::Deserialize;

static CONFIG: Lazy<MyConfig> =
    Lazy::new(|| MyConfig::load_default(Environment::Development).unwrap());

fn main() {
    // This is an example. DO NOT ACTUALLY SET YOUR PASSWORD IN YOUR CODEBASE.
    // please refer to security best practice for your application deployment.
    std::env::set_var("APP__DB__PASSWORD", "supersecurepassword");

    // This will load config from
    //   1. config/base
    //   2. config/development
    //   3. overriding env variables
    println!("{:#?}", MyConfig::load_default(Environment::Development));

    // Works with different config file format like toml and json as well.
    println!("{:#?}", MyConfig::load_default(Environment::Test));
    println!("{:#?}", MyConfig::load_default(Environment::Production));

    std::env::remove_var("APP__DB__PASSWORD");

    setup_db();
}

fn setup_db() {
    // Config could then be loaded into [once_cell::sync::OnceCell] or [once_cell::sync::Lazy]
    // for easier access

    println!("{:#?}", *CONFIG);
    println!("{:#?}", CONFIG.db);
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct MyConfig {
    log_level: String,
    db: MyDbConfig,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct MyDbConfig {
    host: String,
    user: String,
    password: String,
}

impl MyConfig {
    fn load_default(environment: Environment) -> Result<MyConfig> {
        load_default_config(environment)
    }
}
