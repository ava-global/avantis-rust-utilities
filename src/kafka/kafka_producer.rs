use std::fmt::{Debug, Formatter};

use std::time::Duration;

use super::KafkaConfig;
use itertools::{Either, Itertools};
#[cfg(test)]
use mockall::automock;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::ClientConfig;

use super::{Bytes, KafkaKeyMessagePair};

pub struct KafkaProducerImpl {
    pub producer: BaseProducer,
    pub setting: KafkaConfig,
}

impl Debug for KafkaProducerImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaProducerImpl").finish()
    }
}

#[cfg_attr(test, automock)]
pub trait KafkaProducer {
    fn publish(&self, topic_name: String, bytes_message: Bytes, key: String);
    fn flush(&self);
    fn bulk_publish_deprecated(&self, topic_name: String, bytes_messages: &[(String, Bytes)]);
    fn bulk_publish(
        &self,
        topic_name: String,
        bytes_messages: &[KafkaKeyMessagePair],
    ) -> anyhow::Result<()>;
}

impl KafkaProducer for KafkaProducerImpl {
    fn publish(&self, topic_name: String, bytes_message: Bytes, key: String) {
        let record = BaseRecord::to(topic_name.as_str())
            .payload(&bytes_message)
            .key(&key);
        self.producer
            .send(record)
            .unwrap_or_else(|_| panic!("Cannot produce message to {}", topic_name));

        self.producer
            .poll(Duration::from_millis(self.setting.poll_duration_millis));
    }

    fn flush(&self) {
        self.producer
            .flush(Duration::from_millis(self.setting.flush_duration_millis));
    }

    fn bulk_publish_deprecated(&self, topic_name: String, bytes_messages: &[(String, Bytes)]) {
        bytes_messages.iter().for_each(|data| {
            let (key, message) = data;
            let record = BaseRecord::to(topic_name.as_str())
                .key(&key)
                .payload(&message);
            self.producer
                .send(record)
                .expect("Failed to enqueue message");
        });

        self.producer
            .flush(Duration::from_millis(self.setting.flush_duration_millis))
    }

    fn bulk_publish(
        &self,
        topic_name: String,
        bytes_messages: &[KafkaKeyMessagePair],
    ) -> anyhow::Result<()> {
        let r1 = bytes_messages.iter().map(|data| {
            let record = BaseRecord::to(topic_name.as_str())
                .key(&data.key)
                .payload(&data.message.value);

            self.producer.send(record)
        });

        let (_, failure): (Vec<_>, Vec<_>) = r1.partition_map(|r| match r {
            Ok(a) => Either::Left(a),
            Err(b) => Either::Right(b),
        });

        self.producer
            .flush(Duration::from_millis(self.setting.flush_duration_millis));

        if failure.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(format!("{:?}", failure))
        }
    }
}

impl KafkaProducerImpl {
    pub fn new(kafka_setting: KafkaConfig) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", kafka_setting.brokers().join(","))
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.ms", "5")
            .set(
                "security.protocol",
                kafka_setting
                    .security_protocol
                    .clone()
                    .unwrap_or("ssl".to_string()),
            )
            .set("log.connection.close", "false")
            .create()
            .expect("Producer creation error");

        Self {
            producer,
            setting: kafka_setting,
        }
    }
}
