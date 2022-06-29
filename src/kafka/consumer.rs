use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::future::Future;
use std::str::Utf8Error;

use anyhow::Result;
use async_trait::async_trait;
use opentelemetry::global;
use prost::DecodeError;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::message::Headers;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use thiserror::Error;
use tracing::instrument;
use tracing::{debug, error, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::KafkaConfig;

pub use rdkafka::consumer::{
    CommitMode, Consumer, DefaultConsumerContext, MessageStream, StreamConsumer,
};

impl KafkaConfig {
    #[instrument(skip_all, name = "kafka::init_consumer", fields(brokers = %self.brokers_csv, group = group_id))]
    pub fn consumer_config<T>(&self, group_id: &str) -> KafkaResult<T>
    where
        T: FromClientConfig,
    {
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
    }
}

pub fn set_trace(message: &BorrowedMessage) -> Result<(), KakfaProcessError> {
    if let Some(header) = message.headers() {
        let traceparent = std::str::from_utf8(
            header
                .get(0)
                .ok_or_else(|| {
                    KakfaProcessError::ParseHeaderError("header 0 not found".to_string())
                })?
                .1,
        )?;
        let tracestate = std::str::from_utf8(
            header
                .get(1)
                .ok_or_else(|| {
                    KakfaProcessError::ParseHeaderError("header 1 not found".to_string())
                })?
                .1,
        )?;

        let mut trace_metadata = HashMap::<String, String>::new();
        trace_metadata.insert("traceparent".to_string(), traceparent.to_owned());
        trace_metadata.insert("tracestate".to_string(), tracestate.to_owned());

        let parent_cx = global::get_text_map_propagator(|prop| prop.extract(&trace_metadata));
        tracing::Span::current().set_parent(parent_cx);
    }
    Ok(())
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
    ) -> Result<(), KakfaProcessError>
    where
        T: prost::Message + Default,
        F: Fn(T) -> Fut + Send + Sync,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Display,
    {
        let message = message?;
        set_trace(&message)?;

        let decoded_message = decode_protobuf::<T>(&message)?;

        process_fn(decoded_message)
            .await
            .map_err(|err| KakfaProcessError::ProcessError(err.to_string()))?;

        self.commit_message(&message, mode)?;

        Ok(())
    }
}

impl<C: ConsumerContext, R> ConsumerExt<C> for StreamConsumer<C, R> {}

pub async fn process_protobuf<F, T, Fut, E>(
    message: Result<BorrowedMessage<'_>, KafkaError>,
    process_fn: F,
) -> Result<(), KakfaProcessError>
where
    T: prost::Message + Default,
    F: Fn(T) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), E>> + Send,
    E: Display,
{
    let message = message?;

    let decoded_message = decode_protobuf::<T>(&message)?;

    process_fn(decoded_message)
        .await
        .map_err(|err| KakfaProcessError::ProcessError(err.to_string()))?;

    Ok(())
}

pub fn process_error(error: KakfaProcessError) {
    warn!(
        "consume and process kafka message fail with error `{}`",
        error
    );
}

#[allow(clippy::unnecessary_lazy_evaluations)]
fn decode_protobuf<T>(message: &BorrowedMessage<'_>) -> Result<T, KakfaProcessError>
where
    T: prost::Message + Default,
{
    let payload = message
        .payload()
        .ok_or_else(|| KakfaProcessError::EmptyPayload)?;

    Ok(T::decode(payload)?)
}

#[derive(Error, Debug)]
pub enum KakfaProcessError {
    #[error("kafka error: {0}")]
    KafkaError(#[from] KafkaError),
    #[error("decode error: {0}")]
    DecodeError(#[from] DecodeError),
    #[error("utf 8 error: {0}")]
    Utf8Error(#[from] Utf8Error),
    #[error("No messages available right now")]
    EmptyPayload,
    #[error("parse header error: {0}")]
    ParseHeaderError(String),
    #[error("any error: {0}")]
    ProcessError(String),
}

pub struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
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
