use anyhow::anyhow;
use anyhow::Result;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    inner::main().await
}

#[cfg(all(feature = "config", feature = "kafka"))]
mod inner {
    use super::*;

    use avantis_utils::config::load_config;
    use avantis_utils::config::Environment;
    use avantis_utils::kafka::consumer;
    use avantis_utils::kafka::consumer::ConsumerExt;
    use avantis_utils::kafka::KafkaConfig;
    use avantis_utils::kafka::ProtobufKafkaMessage;
    use avantis_utils::kafka::ProtobufKafkaRecord;
    use once_cell::sync::Lazy;
    use rdkafka::consumer::CommitMode;
    use rdkafka::consumer::StreamConsumer;
    use rdkafka::message::OwnedHeaders;
    use rdkafka::producer::FutureProducer;
    use rdkafka::producer::FutureRecord;
    use rdkafka::util::Timeout;
    use futures_lite::{pin, StreamExt};
    use rdkafka::consumer::Consumer;
    use prost::Message;
    use prost;


    #[derive(Clone, PartialEq, Message)]
    pub struct ProtobufMessage {
        #[prost(string, tag="1")]
        pub message: prost::alloc::string::String,
    }

    impl From<ProtobufMessage> for ProtobufKafkaMessage {
        fn from(protobuf_message: ProtobufMessage) -> Self {
            Self {
                key: protobuf_message.message.clone(),
                value: protobuf_message.encode_to_vec().into(),
            }
        }
    }

    static SETTINGS: Lazy<ExampleSettings> =
        Lazy::new(|| ExampleSettings::load(Environment::Develop).unwrap());

    pub async fn main() -> Result<()> {
        producer().await?;
        consumer().await?;

        Ok(())
    }

    async fn check_msg(msg: ProtobufMessage) -> Result<()>{
        println!("Checking messages {}", msg.message);
        Ok(())
    }

    async fn consumer() -> Result<(), anyhow::Error> {
        let kafka_consumer: StreamConsumer = SETTINGS
            .kafka_config
            .consumer_config(&SETTINGS.kafka_consumer_group)?;

        kafka_consumer.subscribe(&[&SETTINGS.kafka_topic])?;
        let stream = kafka_consumer
            .stream()
            .map(|message| {
                let result = kafka_consumer.process_protobuf_and_commit(
                    message,
                    check_msg,
                    CommitMode::Sync,
                );
                result
            })
            .map(|future_result| async move {
                future_result.await.unwrap_or_else(consumer::process_error)
            });
        pin!(stream);
        Ok(while let Some(future) = stream.next().await {
            future.await;
        })
    }

    async fn producer() -> Result<(), anyhow::Error> {
        let record = ProtobufKafkaRecord {
            topic: &SETTINGS.kafka_topic,
            message: ProtobufMessage{ message: "test message".to_string()}.into()
        };
        let record: FutureRecord<String, [u8]> = FutureRecord::from(&record)
            .headers(OwnedHeaders::new().add("header_key_in_rust_util", "header_value"));

        let producer: FutureProducer = SETTINGS.kafka_config.producer_config()?;

        let result = producer
            .send(record, Timeout::Never)
            .await
            .map_err(|e| anyhow!("Error occur while produce kafka message cause: {:?}", e))?;
        println!("result {:?}", result);
        Ok(())
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize)]
    struct ExampleSettings {
        kafka_config: KafkaConfig,
        kafka_topic: String,
        kafka_consumer_group: String,
    }

    impl ExampleSettings {
        fn load(environment: Environment) -> anyhow::Result<Self> {
            load_config(environment)
        }
    }
}

#[cfg(not(all(feature = "config", feature = "kafka")))]
mod inner {
    use super::*;

    pub fn main() -> () {
        println!("Please pass --features config,kafka to cargo when trying this example.");

        // Ok(())
    }
}
