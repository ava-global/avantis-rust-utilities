use rdkafka::config::FromClientConfig;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::OwnedMessage;
use rdkafka::ClientConfig;
use tracing::instrument;
use tracing::warn;

use super::KafkaConfig;

pub use rdkafka::producer::{FutureProducer, FutureRecord};
pub use rdkafka::util::Timeout;

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
