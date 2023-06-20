//! A simple config module wrapping over [config::Config] module.
//!
//! At Avantis, we support environment config file system for most configurations
//! include settings (ex. `DEBUG=true`) and data access (ie. database or API endpoints).
//! We also support overriding mechanism via environment variables for credentials
//! (ie. usernames, passwords, API keys). This keep credentials safe from accidentially
//! upload to code repository and provide a single access point to credentials for easier
//! rotation.
//!
//! For most projects, we recommend using [load_config] which take an [Environment]
//! enum value, and return a config model struct.
//!     
//! 1. Create a base config file like `config/base.toml`[^1] to your project.
//! 2. Create an environment config file like `config/develop.toml` to your project.
//! 3. Set env to replace credentials. Use `APP` for prefix and separator `__` for hierarchy.
//!    For example, `APP_STOCK_DB__PASSWORD` will replace config at field `stock_db.password`.
//! 4. In your code, create a config struct which mirror configuration from earlier steps.
//! 5. Call `load_config` with selected Environment into the struct from step 4.
//!
//! For example usage, see [here](https://github.com/ava-global/avantis-rust-utilities/blob/main/examples/config/main.rs)
//! and its config files [here](https://github.com/ava-global/avantis-rust-utilities/tree/main/config).
//!
//! If you need to customize load mechanism, see [load_custom_config] or maybe use [config::Config] directly instead.
//!
//! [^1]: Any format listed in [config::FileFormat] can be used.

use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use config_rs::Config;
use config_rs::Environment as EnvironmentVariables;
use config_rs::File;
use config_rs::FileFormat;
use config_rs::FileSourceFile;
use serde::Deserialize;
use strum::EnumString;

/// Load config from selected [Environment].
/// Returns a Result containing config struct.
/// Convenience [load_custom_config].
///
/// # Example
///
/// ```
/// # use serde::Deserialize;
/// # use avantis_utils::config::load_config;
/// # use avantis_utils::config::Environment;
/// #[derive(Clone, Debug, Deserialize, PartialEq)]
/// struct MyConfig {
///     log_level: String,
/// }
///
/// fn main() {
///     let config: MyConfig = load_config(Environment::Develop).unwrap();
///
///     println!("{:?}", config);
/// }
/// ```
pub fn load_config<'de, T: Deserialize<'de>>(environment: Environment) -> Result<T> {
    let base_config_file = File::with_name("config/base").required(true);
    let env_config_file = File::with_name(&format!("config/{}", environment)).required(true);

    let custom_env_vars = EnvironmentVariables::with_prefix("app")
        .prefix_separator("_")
        .separator("__");

    load_custom_config(base_config_file, env_config_file, custom_env_vars)
}

/// Load config by path from selected [Environment] and [Path].
/// Returns a Result containing config struct.
/// Convenience [load_custom_config].
///
/// # Example
///
/// ```
/// # use serde::Deserialize;
/// # use avantis_utils::config::load_config_by_path;
/// # use avantis_utils::config::Environment;
/// #[derive(Clone, Debug, Deserialize, PartialEq)]
/// struct MyConfig {
///     log_level: String,
/// }
///
/// fn main() {
///     let config: MyConfig = load_config_by_path(Environment::Develop, "config").unwrap();
///
///     println!("{:?}", config);
/// }
/// ```
pub fn load_config_by_path<'de, T: Deserialize<'de>>(
    environment: Environment,
    path: &str,
) -> Result<T> {
    let base_config_file = File::with_name(&format!("{}/base", path)).required(true);
    let env_config_file = File::with_name(&format!("{}/{}", path, environment)).required(true);

    let custom_env_vars = EnvironmentVariables::with_prefix("app")
        .prefix_separator("_")
        .separator("__");

    load_custom_config(base_config_file, env_config_file, custom_env_vars)
}

/// Load config from custom sources.
/// Returns a Result containing config struct.
///
/// # Example
///
/// ```
/// # use serde::Deserialize;
/// # use avantis_utils::config::load_custom_config;
/// #[derive(Clone, Debug, Deserialize, PartialEq)]
/// struct MyConfig {
///     log_level: String,
/// }
///
/// fn main() {
///     let config: MyConfig = load_custom_config(
///         config_rs::File::with_name("config/base"),
///         config_rs::File::with_name("config/test"),
///         config_rs::Environment::with_prefix("app").separator("__"),
///     ).unwrap();
///
///     println!("{:?}", config);
/// }
/// ```
pub fn load_custom_config<'de, T: Deserialize<'de>>(
    base_config_file: File<FileSourceFile, FileFormat>,
    env_config_file: File<FileSourceFile, FileFormat>,
    custom_env_vars: EnvironmentVariables,
) -> Result<T> {
    Config::builder()
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
        })
}

/// Application environment. Affect configuration file loaded by [load_config].
///
/// Any format listed in [config::FileFormat] can be used.
#[derive(PartialEq, Eq, Debug, EnumString, strum::Display)]
pub enum Environment {
    /// Local environment. Will use `config/local.[FORMAT]`.
    #[strum(serialize = "local")]
    Local,

    /// Test environment. Will use `config/test.[FORMAT]`.
    #[strum(serialize = "test")]
    Test,

    /// Develop environment. Will use `config/develop.[FORMAT]`.
    #[strum(serialize = "develop")]
    Develop,

    /// Production environment. Will use `config/production.[FORMAT]`.
    #[strum(serialize = "production")]
    Production,
}

impl Environment {
    /// Load environment from default env `APP_ENVIRONMENT`. Return Result of Environment.
    /// If env `APP_ENVIRONMENT` is not set, return `Ok(Environment::default())`.
    ///
    /// # Example
    ///
    /// ```
    /// # use avantis_utils::config::Environment;
    /// # std::env::set_var("APP_ENVIRONMENT", "develop");
    /// let environment = Environment::from_env().unwrap();
    /// ```
    pub fn from_env() -> Result<Self> {
        Self::from_custom_env("APP_ENVIRONMENT")
    }

    /// Load environment from given env. Return Result of Environment.
    /// If env `APP_ENVIRONMENT` is not set, return `Ok(Environment::default())`.
    ///
    /// # Example
    ///
    /// ```
    /// # use avantis_utils::config::Environment;
    /// # std::env::set_var("CUSTOM_ENVIRONMENT", "develop");
    /// let environment = Environment::from_custom_env("CUSTOM_ENVIRONMENT").unwrap();
    /// ```
    pub fn from_custom_env(key: &str) -> Result<Self> {
        std::env::var(key)
            .map(|environment_string| {
                Environment::from_str(&environment_string)
                    .map_err(|_| anyhow!("Unknown environment: {environment_string}"))
            })
            .unwrap_or_else(|_| Ok(Environment::default()))
    }
}

impl Default for Environment {
    fn default() -> Self {
        if cfg!(test) {
            Environment::Test
        } else {
            Environment::Local
        }
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

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
        db_name: String,
        max_connections: u32,
    }

    #[test]
    #[serial]
    fn test_load_config_success() {
        std::env::set_var("APP_DB__PASSWORD", "supersecurepassword");

        let expected = MyConfig {
            log_level: "info".to_string(),
            db: MyDbConfig {
                host: "localhost".to_string(),
                user: "username".to_string(),
                password: "supersecurepassword".to_string(),
                db_name: "my_db".to_string(),
                max_connections: 30,
            },
        };

        let actual = load_custom_config::<MyConfig>(
            File::with_name("config/base").required(true),
            File::with_name("config/develop").required(true),
            EnvironmentVariables::with_prefix("app")
                .prefix_separator("_")
                .separator("__"),
        )
        .unwrap();

        assert_eq!(expected, actual);

        let actual = load_config::<MyConfig>(Environment::Develop).unwrap();

        assert_eq!(expected, actual);

        let actual = load_config::<MyConfig>(Environment::Test).unwrap();

        assert_eq!(expected, actual);

        let actual = load_config::<MyConfig>(Environment::Production).unwrap();

        assert_eq!(expected, actual);

        std::env::remove_var("APP_DB__PASSWORD");
    }

    #[test]
    #[serial]
    fn test_load_config_by_path_success() {
        std::env::set_var("APP_DB__PASSWORD", "supersecurepassword");

        let expected = MyConfig {
            log_level: "info".to_string(),
            db: MyDbConfig {
                host: "localhost".to_string(),
                user: "username".to_string(),
                password: "supersecurepassword".to_string(),
                db_name: "my_db".to_string(),
                max_connections: 30,
            },
        };

        let actual = load_custom_config::<MyConfig>(
            File::with_name("config-workspace/config/base").required(true),
            File::with_name("config-workspace/config/develop").required(true),
            EnvironmentVariables::with_prefix("app")
                .prefix_separator("_")
                .separator("__"),
        )
        .unwrap();

        assert_eq!(expected, actual);

        let actual =
            load_config_by_path::<MyConfig>(Environment::Develop, "config-workspace/config")
                .unwrap();

        assert_eq!(expected, actual);

        let actual =
            load_config_by_path::<MyConfig>(Environment::Test, "config-workspace/config").unwrap();

        assert_eq!(expected, actual);

        let actual =
            load_config_by_path::<MyConfig>(Environment::Production, "config-workspace/config")
                .unwrap();

        assert_eq!(expected, actual);

        std::env::remove_var("APP_DB__PASSWORD");
    }

    #[test]
    #[serial]
    #[should_panic(expected = "configuration file \"config/staging\" not found")]
    fn test_load_config_file_not_found() {
        load_custom_config::<MyConfig>(
            File::with_name("config/base").required(true),
            File::with_name("config/staging").required(true),
            EnvironmentVariables::with_prefix("app").separator("__"),
        )
        .unwrap();
    }

    #[test]
    #[serial]
    #[should_panic(
        expected = "Unable to deserialize into config with type avantis_utils::config::tests::MyConfig with error: missing field"
    )]
    fn test_load_config_missing_fields() {
        load_custom_config::<MyConfig>(
            File::with_name("config/base").required(true),
            File::with_name("config/base").required(true),
            EnvironmentVariables::with_prefix("app").separator("__"),
        )
        .unwrap();
    }

    #[test]
    #[serial]
    fn test_environment_from_env() {
        assert_eq!(Environment::Test, Environment::from_env().unwrap());

        assert_eq!(
            Environment::Test,
            Environment::from_custom_env("APP_ENVIRONMENT").unwrap()
        );

        std::env::set_var("APP_ENVIRONMENT", "local");

        assert_eq!(Environment::Local, Environment::from_env().unwrap());

        assert_eq!(
            Environment::Local,
            Environment::from_custom_env("APP_ENVIRONMENT").unwrap()
        );

        std::env::remove_var("APP_ENVIRONMENT")
    }

    #[test]
    #[serial]
    #[should_panic(expected = "Unknown environment: staging")]
    fn test_environment_from_unknown_env() {
        std::env::set_var("APP_ENVIRONMENT_INVALID", "staging");

        let result = Environment::from_custom_env("APP_ENVIRONMENT_INVALID");

        std::env::remove_var("APP_ENVIRONMENT_INVALID");

        result.unwrap();
    }
}
