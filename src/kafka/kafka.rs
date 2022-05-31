use std::convert::TryFrom;

use anyhow::{anyhow, Result};
use rdkafka::error::KafkaError;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

pub mod kafka_consumer;
pub mod kafka_producer;

pub type Bytes = Vec<u8>;
pub struct KafkaMessage {
    pub value: Bytes,
}
pub struct KafkaMessages {
    pub messages: Vec<KafkaMessage>,
}

impl From<Vec<u8>> for KafkaMessage {
    fn from(byte_vector: Vec<u8>) -> Self {
        Self { value: byte_vector }
    }
}

impl<'a> TryFrom<BorrowedMessage<'a>> for KafkaMessage {
    type Error = anyhow::Error;

    fn try_from(value: BorrowedMessage<'a>) -> Result<Self> {
        value
            .payload()
            .ok_or_else(|| anyhow!("Unable to deserialize to byte arrays"))
            .map(|x| x.to_vec())
            .map(KafkaMessage::from)
    }
}

impl<'a> TryFrom<core::result::Result<BorrowedMessage<'a>, KafkaError>> for KafkaMessage {
    type Error = anyhow::Error;

    fn try_from(value: core::result::Result<BorrowedMessage<'a>, KafkaError>) -> Result<Self> {
        value
            .map_err::<anyhow::Error, _>(|err| err.into())
            .and_then(KafkaMessage::try_from)
    }
}

pub struct KafkaKeyMessagePair {
    pub key: String,
    pub message: KafkaMessage,
}
