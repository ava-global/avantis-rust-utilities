use std::fmt::Debug;
use std::fmt::Display;
use std::future::Future;

use anyhow::Result;
use async_trait::async_trait;
use prost::DecodeError;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::DefaultConsumerContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use thiserror::Error;
use tracing::{debug, error, info};

use super::KafkaConfig;

pub use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};

impl KafkaConfig {
    pub fn consumer_config<T: FromClientConfig>(&self, group_id: &str) -> T {
        ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", &self.brokers_csv)
            .set("enable.partition.eof", "false")
            .set(
                "security.protocol",
                self.security_protocol
                    .clone()
                    .unwrap_or_else(|| "ssl".to_string()),
            )
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Consumer creation failed")
    }
}

#[async_trait]
pub trait ConsumerExt<C = DefaultConsumerContext>: Consumer<C>
where
    C: ConsumerContext,
{
    async fn process_protobuf_and_commit<F, T, Fut, E>(
        &self,
        message: Result<BorrowedMessage<'_>, KafkaError>,
        process_fn: F,
        mode: CommitMode,
    ) -> Result<(), Error>
    where
        T: prost::Message + Default,
        F: Fn(T) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), E>> + Send + Sync,
        E: Display,
    {
        let message = message?;

        let decoded_message = decode_protobuf::<T>(&message)?;

        process_fn(decoded_message)
            .await
            .map_err(|err| Error::ProcessError(err.to_string()))?;

        self.commit_message(&message, mode)?;

        Ok(())
    }
}

impl ConsumerExt for StreamConsumer {}

fn decode_protobuf<T>(message: &BorrowedMessage<'_>) -> Result<T, Error>
where
    T: prost::Message + Default,
{
    let payload = message.payload().ok_or_else(|| Error::EmptyPayload)?;

    Ok(T::decode(payload)?)
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("kafka error: {0}")]
    KafkaError(#[from] KafkaError),
    #[error("decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("No messages available right now")]
    EmptyPayload,
    #[error("any error: {0}")]
    ProcessError(String),
}

pub struct ConsumerCallbackLogger;

impl ClientContext for ConsumerCallbackLogger {}

impl ConsumerContext for ConsumerCallbackLogger {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("pre rebalance: {:?}", tpl)
            }
            Rebalance::Revoke(tpl) => {
                info!("pre rebalance all partitions are revoke: {:?}", tpl)
            }
            Rebalance::Error(e) => {
                info!("pre rebalance error: {:?}", e)
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("post rebalance: {:?}", tpl)
            }
            Rebalance::Revoke(tpl) => {
                info!("post rebalance all partitions are revoke: {:?}", tpl)
            }
            Rebalance::Error(e) => {
                info!("post rebalance error: {:?}", e)
            }
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(_) => debug!("committed: {:?}", offsets),
            Err(e) => info!("committed error: {:?}", e),
        }
    }
}