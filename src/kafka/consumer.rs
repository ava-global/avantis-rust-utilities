use std::fmt::Debug;
use std::fmt::Display;
use std::future::Future;

use anyhow::Result;
use async_trait::async_trait;
use opentelemetry::trace::SpanContext;
use opentelemetry::trace::SpanId;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::TraceId;
use opentelemetry::Context;
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
use rand::Rng;

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

pub fn log_tid() {
    println!(
        "current_context: {:?}",
        tracing::Span::current()
            .context()
            .span()
            .span_context()
            .trace_id()
    );
}

pub fn set_trace_id(hex_trace: &String) {
    log_tid();

    let span_ctx = tracing::Span::current()
        .context()
        .span()
        .span_context()
        .clone();
    let mut rng = rand::thread_rng();
    let update_trace_span_ctx = SpanContext::new(
        TraceId::from_hex(&hex_trace).unwrap(),
        // span_ctx.span_id(),
        SpanId::from_bytes(rng.gen::<[u8; 8]>()),
        span_ctx.trace_flags(),
        span_ctx.is_remote(),
        span_ctx.trace_state().to_owned(),
    );
    let ctx_update = Context::current().with_remote_span_context(update_trace_span_ctx);

    tracing::Span::current().set_parent(ctx_update);
    log_tid();
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
        Fut: Future<Output = Result<(), E>> + Send,
        E: Display,
    {
        let message = message?;
        println!(
            "message hearder key {:?}",
            message.headers().unwrap().get(0).unwrap().0
        );
        let value = std::str::from_utf8(message.headers().unwrap().get(0).unwrap().1).unwrap();
        println!("message hearder value {:?}", value);

        set_trace_id(&value.to_owned());

        let decoded_message = decode_protobuf::<T>(&message)?;

        process_fn(decoded_message)
            .await
            .map_err(|err| Error::ProcessError(err.to_string()))?;

        self.commit_message(&message, mode)?;

        Ok(())
    }
}

impl<C: ConsumerContext, R> ConsumerExt<C> for StreamConsumer<C, R> {}

pub async fn process_protobuf<F, T, Fut, E>(
    message: Result<BorrowedMessage<'_>, KafkaError>,
    process_fn: F,
) -> Result<(), Error>
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
        .map_err(|err| Error::ProcessError(err.to_string()))?;

    Ok(())
}

pub fn process_error(error: Error) {
    warn!(
        "consume and process kafka message fail with error `{}`",
        error
    );
    ()
}

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
