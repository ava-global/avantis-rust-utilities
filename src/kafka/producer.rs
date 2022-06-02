use rdkafka::config::FromClientConfig;
use rdkafka::ClientConfig;

use super::KafkaConfig;

pub use rdkafka::producer::{FutureProducer, FutureRecord};
pub use rdkafka::util::Timeout;

impl KafkaConfig {
    pub fn producer_config<T: FromClientConfig>(&self) -> T {
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
            .expect("Producer creation error")
    }
}
