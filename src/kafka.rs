use bytes::Bytes;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use std::ops::Deref;

pub mod consumer;
pub mod producer;

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct KafkaConfig {
    pub brokers_csv: String,
    pub flush_duration_millis: u64,
    pub poll_duration_millis: u64,
    pub security_protocol: Option<String>,
}

pub struct ProtobufKafkaRecord<'a> {
    pub topic: &'a str,
    pub message: ProtobufKafkaMessage,
}

pub struct ProtobufKafkaMessage {
    pub key: String,
    pub value: Bytes,
}

impl<'a> From<&'a ProtobufKafkaRecord<'a>> for FutureRecord<'a, String, [u8]> {
    fn from(record: &'a ProtobufKafkaRecord<'a>) -> FutureRecord<'a, String, [u8]> {
        FutureRecord::to(record.topic)
            .key(&record.message.key)
            .payload(record.message.value.deref())
    }
}

pub struct KafkaAgent {
    pub kafka: KafkaConfig,
    pub future_producer: Option<FutureProducer>,
}

impl KafkaAgent {
    pub fn new(kafka: KafkaConfig) -> Self {
        Self {
            kafka,
            future_producer: None,
        }
    }

    pub fn with_future_producer(mut self) -> KafkaAgent {
        self.future_producer = Some(self.producer_config::<FutureProducer>().unwrap());
        self
    }
}
