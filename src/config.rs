use anyhow::anyhow;
use anyhow::Result;
use config::Config;
use config::Environment as EnvironmentVariables;
use config::File;
use config::FileFormat;
use config::FileSourceFile;
use serde::Deserialize;
use strum::EnumString;

pub fn load_default_config<'a, T: Deserialize<'a>>(environment: Environment) -> Result<T> {
    let base_config_file = File::with_name("config/base").required(true);
    let env_config_file =
        File::with_name(&format!("config/{}", environment.to_string())).required(true);

    let custom_env_vars = EnvironmentVariables::with_prefix("app").separator("__");

    load_config(base_config_file, env_config_file, custom_env_vars)
}

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

#[derive(PartialEq, Debug, EnumString, strum::Display)]
pub enum Environment {
    #[strum(serialize = "local")]
    Local,

    #[strum(serialize = "production")]
    Production,

    #[strum(serialize = "development")]
    Development,

    #[strum(serialize = "test")]
    Test,
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
