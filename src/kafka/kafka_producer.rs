use std::fmt::{Debug, Formatter};

use std::time::Duration;

use super::KafkaConfig;
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
        let result: anyhow::Result<()> = bytes_messages
            .iter()
            .map(|data| {
                // let (key, message) = data;
                let key = &data.key;
                let message = &data.message.value;

                let record = BaseRecord::to(topic_name.as_str())
                    .key(&key)
                    .payload(&message);

                self.producer
                    .send(record)
                    .map_err(|error_pair| error_pair.0.into())
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|_| ());

        self.producer
            .flush(Duration::from_millis(self.setting.flush_duration_millis));

        result
    }
}

impl KafkaProducerImpl {
    pub fn new(kafka_setting: KafkaConfig) -> Self {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", kafka_setting.brokers().join(","))
            .set("queue.buffering.max.messages", "1000000")
            .set("queue.buffering.max.ms", "5")
            .set("security.protocol", "ssl")
            .set("log.connection.close", "false")
            .create()
            .expect("Producer creation error");

        Self {
            producer,
            setting: kafka_setting,
        }
    }
}
