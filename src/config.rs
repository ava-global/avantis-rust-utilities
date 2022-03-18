//! A simple config module wrapping over [config::Config] module.
//!
//! At Avantis, we support environment config file system for most configurations
//! include settings (ex. `DEBUG=true`) and data access (ie. database or API endpoints).
//! We also support overriding mechanism via environment variables for credentials
//! (ie. usernames, passwords, API keys). This keep credentials safe from accidentially
//! upload to code repository and provide a single access point to credentials for easier
//! rotation.
//!
//! For most projects, we recommend using [load_default_config] which take an [Environment]
//! enum value, and return a config model struct.
//!     
//! 1. Create a base config file like `config/base.toml`[^1] to your project.
//! 2. Create an environment config file like `config/development.toml` to your project.
//! 3. Set env to replace credentials. Use `APP` for prefix and separator `__` for hierarchy.
//!    For example, `APP__STOCK_DB__PASSWORD` will replace config at field `stock_db.password`.
//! 4. In your code, create a config struct which mirror configuration from earlier steps.
//! 5. Call `load_default_config` with selected Environment into the struct from step 4.
//!
//! For example usage, see [here](https://github.com/ava-global/avantis-rust-utilities/blob/main/examples/config/main.rs)
//! and its config files [here](https://github.com/ava-global/avantis-rust-utilities/tree/main/config).
//!
//! If you need to customize load mechanism, see [load_config] or maybe use [config::Config] directly instead.
//!
//! [^1]: Any format listed in [config::FileFormat] can be used.

use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use config::Config;
use config::Environment as EnvironmentVariables;
use config::File;
use config::FileFormat;
use config::FileSourceFile;
use serde::Deserialize;
use strum::EnumString;

/// Load config from selected [Environment].
/// Returns a Result containing config struct.
/// Convenience [load_config].
///
/// # Example
///
/// ```
/// # use serde::Deserialize;
/// # use avantis_utils::config::load_default_config;
/// # use avantis_utils::config::Environment;
/// #[derive(Clone, Debug, Deserialize, PartialEq)]
/// struct MyConfig {
///     log_level: String,
/// }
///
/// fn main() {
///     let config: MyConfig = load_default_config(Environment::Development).unwrap();
///
///     println!("{:?}", config);
/// }
/// ```
pub fn load_default_config<'a, T: Deserialize<'a>>(environment: Environment) -> Result<T> {
    let base_config_file = File::with_name("config/base").required(true);
    let env_config_file =
        File::with_name(&format!("config/{}", environment.to_string())).required(true);

    let custom_env_vars = EnvironmentVariables::with_prefix("app").separator("__");

    load_config(base_config_file, env_config_file, custom_env_vars)
}

/// Load config from custom sources.
/// Returns a Result containing config struct.
///
/// # Example
///
/// ```
/// # use serde::Deserialize;
/// # use avantis_utils::config::load_config;
/// #[derive(Clone, Debug, Deserialize, PartialEq)]
/// struct MyConfig {
///     log_level: String,
/// }
///
/// fn main() {
///     let config: MyConfig = load_config(
///         config::File::with_name("config/base"),
///         config::File::with_name("config/test"),
///         config::Environment::with_prefix("app").separator("__"),
///     ).unwrap();
///
///     println!("{:?}", config);
/// }
/// ```
pub fn load_config<'a, T: Deserialize<'a>>(
    base_config_file: File<FileSourceFile, FileFormat>,
    env_config_file: File<FileSourceFile, FileFormat>,
    custom_env_vars: EnvironmentVariables,
) -> Result<T> {
    Ok(Config::builder()
        .add_source(base_config_file)
        .add_source(env_config_file)
        .add_source(custom_env_vars)
        .build()?
        .try_deserialize()
        .map_err(|err| {
            anyhow!(
                "Unable to deserialize into config with type {} with error: {}",
                std::any::type_name::<T>(),
                err
            )
        })?)
}

/// Application environment. Affect configuration file loaded by [load_default_config].
///
/// Any format listed in [config::FileFormat] can be used.
#[derive(PartialEq, Debug, EnumString, strum::Display)]
pub enum Environment {
    /// Local environment. Will use `config/local.[FORMAT]`.
    #[strum(serialize = "local")]
    Local,

    /// Test environment. Will use `config/test.[FORMAT]`.
    #[strum(serialize = "test")]
    Test,

    /// Development environment. Will use `config/development.[FORMAT]`.
    #[strum(serialize = "development")]
    Development,

    /// Production environment. Will use `config/production.[FORMAT]`.
    #[strum(serialize = "production")]
    Production,
}

impl Environment {
    /// Load environment from default env `APP_ENVIRONMENT`. Return Result of Environment.
    /// If env `APP_ENVIRONMENT` is not set, return `Ok(Environment::Local)`.
    ///
    /// # Example
    ///
    /// ```
    /// # use avantis_utils::config::Environment;
    /// # std::env::set_var("APP_ENVIRONMENT", "development");
    /// let environment = Environment::from_env().unwrap();
    /// ```
    pub fn from_env() -> Result<Self> {
        Self::from_custom_env("APP_ENVIRONMENT")
    }

    /// Load environment from given env. Return Result of Environment.
    /// If env `APP_ENVIRONMENT` is not set, return `Ok(Environment::Local)`.
    ///
    /// # Example
    ///
    /// ```
    /// # use avantis_utils::config::Environment;
    /// # std::env::set_var("CUSTOM_ENVIRONMENT", "development");
    /// let environment = Environment::from_custom_env("CUSTOM_ENVIRONMENT").unwrap();
    /// ```
    pub fn from_custom_env(key: &str) -> Result<Self> {
        let env = std::env::var(key);

        Ok(Environment::from_str(
            env.as_ref().map(String::as_ref).unwrap_or("local"),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_load_config_success() {
        std::env::set_var("APP__DB__PASSWORD", "supersecurepassword");

        let expected = MyConfig {
            log_level: "info".to_string(),
            db: MyDbConfig {
                host: "localhost".to_string(),
                user: "username".to_string(),
                password: "supersecurepassword".to_string(),
            },
        };

        let actual = load_config::<MyConfig>(
            File::with_name("config/base").required(true),
            File::with_name("config/development").required(true),
            EnvironmentVariables::with_prefix("app").separator("__"),
        )
        .unwrap();

        assert_eq!(expected, actual);

        let actual = load_default_config::<MyConfig>(Environment::Development).unwrap();

        assert_eq!(expected, actual);

        let actual = load_default_config::<MyConfig>(Environment::Test).unwrap();

        assert_eq!(expected, actual);

        let actual = load_default_config::<MyConfig>(Environment::Production).unwrap();

        assert_eq!(expected, actual);

        std::env::remove_var("APP__DB__PASSWORD");
    }

    #[test]
    #[should_panic(expected = "configuration file \"config/unknown_env\" not found")]
    fn test_load_config_file_not_found() {
        load_config::<MyConfig>(
            File::with_name("config/base").required(true),
            File::with_name("config/unknown_env").required(true),
            EnvironmentVariables::with_prefix("app").separator("__"),
        )
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Unable to deserialize into config with type avantis_utils::config::tests::MyConfig with error: missing field"
    )]
    fn test_load_config_missing_fields() {
        load_config::<MyConfig>(
            File::with_name("config/base").required(true),
            File::with_name("config/base").required(true),
            EnvironmentVariables::with_prefix("app").separator("__"),
        )
        .unwrap();
    }
}
