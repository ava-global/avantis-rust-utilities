use anyhow::Result;
use avantis_utils::config::load_default_config;
use avantis_utils::config::Environment;
use serde::Deserialize;

fn main() {
    std::env::set_var("APP__DB__PASSWORD", "supersecurepassword");

    // This will load config from
    //   1. config/base
    //   2. config/development
    println!("{:#?}", MyConfig::load_default(Environment::Development));

    // Works with different config file format like toml and json as well.

    println!("{:#?}", MyConfig::load_default(Environment::Test));
    println!("{:#?}", MyConfig::load_default(Environment::Production));

    std::env::remove_var("APP__DB__PASSWORD");
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
