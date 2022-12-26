use std::collections::HashMap;

use anyhow::Error;
use opentelemetry::global;
use rdkafka::config::FromClientConfig;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{OwnedHeaders, OwnedMessage};
use rdkafka::ClientConfig;
use tracing::instrument;
use tracing::warn;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::KafkaConfig;

pub use rdkafka::producer::{FutureProducer, FutureRecord};
pub use rdkafka::util::Timeout;

pub fn with_trace_header(
    record: FutureRecord<'_, String, [u8]>,
) -> Result<FutureRecord<'_, String, [u8]>, Error> {
    Ok(record.headers(create_tracing_header()))
}

fn create_tracing_header() -> OwnedHeaders {
    let cx = tracing::Span::current().context();
    let mut trace_metadata = HashMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut trace_metadata)
    });

    let mut headers = OwnedHeaders::new();

    if let Some(traceparent) = trace_metadata.get("traceparent") {
        headers = headers.add("traceparent", traceparent);
    } else {
        warn!("trace metadata don't have traceparent");
    }

    if let Some(tracestate) = trace_metadata.get("tracestate") {
        headers = headers.add("tracestate", tracestate);
    } else {
        warn!("trace metadata don't have tracestate");
    }

    headers
}

impl KafkaConfig {
    #[instrument(skip_all, name = "kafka::init_producer", fields(brokers = %self.brokers_csv))]
    pub fn producer_config<T>(&self) -> KafkaResult<T>
    where
        T: FromClientConfig,
    {
        ClientConfig::new()
            .set("bootstrap.servers", &self.brokers_csv)
            .set("message.timeout.ms", "30000")
            .set(
                "security.protocol",
                self.security_protocol
                    .clone()
                    .unwrap_or_else(|| "ssl".to_string()),
            )
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Debug)
            // .set("log.connection.close", "false")
            .create()
    }
}

pub fn process_error((error, message): (KafkaError, OwnedMessage)) -> (i32, i64) {
    warn!(
        "send kafka fail for message: `{:?}` with error `{}`",
        message, error
    );
    (-1, -1)
}
