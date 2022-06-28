use bytes::Bytes;
use rdkafka::producer::{FutureRecord};
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
            .payload(&record.message.value.deref())
    }
}
