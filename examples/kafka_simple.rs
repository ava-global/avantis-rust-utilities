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
    use avantis_utils::kafka::producer::with_trace_header;
    use avantis_utils::kafka::KafkaConfig;
    use avantis_utils::kafka::ProtobufKafkaMessage;
    use avantis_utils::kafka::ProtobufKafkaRecord;
    use avantis_utils::telemetry::TelemetrySetting;
    use futures_lite::{pin, StreamExt};
    use once_cell::sync::Lazy;
    use opentelemetry::trace::TraceContextExt;
    use prost;
    use prost::Message;
    use rdkafka::consumer::CommitMode;
    use rdkafka::consumer::Consumer;
    use rdkafka::consumer::StreamConsumer;
    use rdkafka::producer::FutureProducer;
    use rdkafka::producer::FutureRecord;
    use rdkafka::util::Timeout;
    use tracing;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    #[derive(Clone, PartialEq, Message)]
    pub struct ProtobufMessage {
        #[prost(string, tag = "1")]
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

    #[tracing::instrument(name = "kafk_simple::main")]
    pub async fn main() -> Result<()> {
        SETTINGS.telemetry.init_telemetry(env!("CARGO_PKG_NAME"))?;
        let kafka_agent: FutureProducer = SETTINGS.kafka.producer_config()?;
        producer(&kafka_agent).await?;
        producer(&kafka_agent).await?;
        producer(&kafka_agent).await?;
        consumer().await?;

        Ok(())
    }

    #[tracing::instrument(name = "kafk_simple::check_msg", fields(span.kind = "consumer"))]
    async fn check_msg(msg: ProtobufMessage) -> Result<()> {
        println!("Checking messages {}", msg.message);
        check_msg_inner();
        Ok(())
    }

    #[tracing::instrument(name = "kafk_simple::check_msg2")]
    fn check_msg_inner() {
        println!("Checking messages 2");
    }

    #[tracing::instrument(skip_all, name = "kafk_simple::consumer")]
    async fn consumer() -> Result<(), anyhow::Error> {
        let kafka_consumer: StreamConsumer = SETTINGS
            .kafka
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

    #[tracing::instrument(skip_all, name = "kafk_simple::producer")]
    async fn producer(future_producer: &FutureProducer) -> Result<(), anyhow::Error> {
        let trace_id = tracing::Span::current()
            .context()
            .span()
            .span_context()
            .trace_id();
        println!("producer trace id : {}", trace_id);
        let record = ProtobufKafkaRecord {
            topic: &SETTINGS.kafka_topic,
            message: ProtobufMessage {
                message: "test message".to_string(),
            }
            .into(),
        };
        let record: FutureRecord<String, [u8]> = FutureRecord::from(&record);
        let result = future_producer
            .send(with_trace_header(record)?, Timeout::Never)
            .await
            .map_err(|e| anyhow!("Error occur while produce kafka message cause: {:?}", e));
        println!("result {:?}", result);
        Ok(())
    }

    #[derive(Clone, Debug, PartialEq, serde::Deserialize)]
    struct ExampleSettings {
        pub telemetry: TelemetrySetting,
        kafka: KafkaConfig,
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

    pub fn main() {
        println!("Please pass --features config,kafka to cargo when trying this example.");
    }
}
