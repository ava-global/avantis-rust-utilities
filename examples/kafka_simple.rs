use anyhow::Result;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    inner::main().await
}

#[cfg(all(feature = "config", feature = "kafka"))]
mod inner {
    use super::*;
    pub const KAFKA_TOPIC: &str = "test.kafka_simple.avantis_rust_utils.topic";
    pub const KAFKA_CONSUMER_GROUP: &str = "test.kafka_simple.avantis_rust_utils.consumer_group";

    use avantis_utils::config::load_config;
    use avantis_utils::config::Environment;
    use avantis_utils::kafka;
    use avantis_utils::kafka::kafka_consumer;
    use avantis_utils::kafka::kafka_producer;
    use avantis_utils::kafka::kafka_producer::KafkaProducer;
    use avantis_utils::kafka::KafkaConfig;
    use once_cell::sync::Lazy;

    static CONFIG: Lazy<ExampleConfig> =
        Lazy::new(|| ExampleConfig::load(Environment::Develop).unwrap());

    pub async fn main() -> Result<()> {
        let kafka_producer1 = kafka_producer::KafkaProducerImpl::new(CONFIG.kafka.clone());
        kafka_producer1.send(
            KAFKA_TOPIC.to_string(),
            kafka::KafkaKeyMessagePair {
                key: "test key".to_string(),
                message: kafka::KafkaMessage {
                    value: "test message".as_bytes().to_vec(),
                },
            },
        );

        let res = kafka_producer1.bulk_send_and_flush(
            KAFKA_TOPIC.to_string(),
            &vec![
                kafka::KafkaKeyMessagePair {
                    key: "test key".to_string(),
                    message: kafka::KafkaMessage {
                        value: "test message bulk 1".as_bytes().to_vec(),
                    },
                },
                kafka::KafkaKeyMessagePair {
                    key: "test key".to_string(),
                    message: kafka::KafkaMessage {
                        value: "test message bulk 2".as_bytes().to_vec(),
                    },
                },
            ],
        );
        println!("bulk publish result {:?}", &res);

        let kafka_consumer1 = kafka_consumer::KafkaConsumer::new(
            CONFIG.kafka.brokers_csv.to_owned(),
            KAFKA_TOPIC.to_owned(),
            KAFKA_CONSUMER_GROUP.to_owned(),
        );
        let _ = kafka_consumer1
            .try_consume(|message| async move {
                println!("consume message {:?}", std::str::from_utf8(&message.value));
                Ok(())
            })
            .await?;
        Ok(())
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize)]
    struct ExampleConfig {
        kafka: KafkaConfig,
    }

    impl ExampleConfig {
        fn load(environment: Environment) -> anyhow::Result<Self> {
            load_config(environment)
        }
    }
}

#[cfg(not(all(feature = "config", feature = "kafka")))]
mod inner {
    use super::*;

    pub async fn main() -> Result<()> {
        println!("Please pass --features config,kafka to cargo when trying this example.");

        Ok(())
    }
}
