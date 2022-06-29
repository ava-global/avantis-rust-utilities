use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::Error;
use opentelemetry::global;
use rdkafka::config::FromClientConfig;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{OwnedHeaders, OwnedMessage};
use rdkafka::ClientConfig;
use tracing::instrument;
use tracing::warn;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::KafkaAgent;

pub use rdkafka::producer::{FutureProducer, FutureRecord};
pub use rdkafka::util::Timeout;

impl KafkaAgent {
    #[instrument(skip_all, name = "kafka::init_producer", fields(brokers = %self.kafka.brokers_csv))]
    pub fn producer_config<T>(&self) -> KafkaResult<T>
    where
        T: FromClientConfig,
    {
        ClientConfig::new()
            .set("bootstrap.servers", &self.kafka.brokers_csv)
            .set("message.timeout.ms", "30000")
            .set(
                "security.protocol",
                self.kafka
                    .security_protocol
                    .clone()
                    .unwrap_or_else(|| "ssl".to_string()),
            )
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Debug)
            // .set("log.connection.close", "false")
            .create()
    }

    #[instrument(skip_all, name = "kafka::send")]
    pub async fn send(&self, record: FutureRecord<'_, String, [u8]>) -> Result<(i32, i64), Error> {
        let cx = tracing::Span::current().context();
        let mut trace_metadata = HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut trace_metadata)
        });

        let record = record.headers(
            OwnedHeaders::new()
                .add(
                    "traceparent",
                    &trace_metadata
                        .get("traceparent")
                        .ok_or_else(|| anyhow!("trace metadata don't have traceparent"))?,
                )
                .add(
                    "tracestate",
                    &trace_metadata
                        .get("tracestate")
                        .ok_or_else(|| anyhow!("trace metadata don't have tracestate"))?,
                ),
        );

        self.future_producer
            .as_ref()
            .ok_or_else(|| anyhow!("Cannot start future producer"))?
            .send(record, Timeout::Never)
            .await
            .map_err(|e| anyhow!("Error occur while produce kafka message cause: {:?}", e))
    }
}

pub fn process_error((error, message): (KafkaError, OwnedMessage)) -> (i32, i64) {
    warn!(
        "send kafka fail for message: `{:?}` with error `{}`",
        message, error
    );
    (-1, -1)
}
