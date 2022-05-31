use std::fmt::Debug;
use std::future::Future;

use std::time::Duration;

use anyhow::Result;
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use tracing::{debug, error, info};

use super::KafkaMessage;

pub struct KafkaConsumer {
    pub consumer: BaseConsumer<ConsumerCallbackLogger>,
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

impl KafkaConsumer {
    pub fn new(kafka_brokers_str: String, topic: String, consumer_group: String) -> Self {
        let consumer: BaseConsumer<ConsumerCallbackLogger> = ClientConfig::new()
            .set("group.id", consumer_group)
            .set("bootstrap.servers", kafka_brokers_str)
            .set("enable.partition.eof", "false")
            .set("security.protocol", "ssl")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create_with_context(ConsumerCallbackLogger {})
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[topic.as_str()])
            .expect("Can't subscribe to specified topics");

        Self { consumer }
    }

    pub async fn try_consume<F, Fut>(&self, process_message_callback: F) -> Result<()>
    where
        F: Fn(KafkaMessage) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        loop {
            let msg_opt = self.consumer.poll(Duration::from_millis(10000));

            if msg_opt.is_none() {
                info!("No messages available right now");
                return Ok(());
            }

            let msg = msg_opt.unwrap();

            if msg.is_err() {
                info!("Error consume message: {:?}", msg.as_ref().err());
                return Ok(());
            }

            if msg.as_ref().unwrap().payload().is_none() {
                info!("No messages available right now.");
                return Ok(());
            }

            let kafka_message = KafkaMessage {
                value: (*(msg.as_ref().unwrap().payload().unwrap()).to_vec()).to_owned(),
            };

            process_message_callback(kafka_message).await?;

            let commit_msg = self
                .consumer
                .commit_message(&msg.unwrap(), CommitMode::Sync);

            match commit_msg {
                Ok(_) => debug!("committed message"),
                Err(e) => error!("error commit message: {:?}", e),
            }
        }
    }
}

pub async fn consume<Fut, T>(
    bootstrap_server: &str,
    group_id: &str,
    topics: &[&str],
    handler: impl Fn(T) -> Fut,
) where
    T: prost::Message + Default + Debug,
    Fut: Future<Output = Result<()>>,
{
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "latest")
        .set("session.timeout.ms", "6000")
        .set("heartbeat.interval.ms", "1000")
        .set("enable.auto.commit", "true")
        .set("security.protocol", "ssl")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(error) => error!("Kafka error: {}", error),
            Ok(message) => {
                if message.payload().is_none() {
                    info!("No messages available right now");
                    continue;
                }

                if let Some(payload) = message.payload() {
                    let parsed_result = T::decode(payload);

                    match parsed_result {
                        Ok(parsed_payload) => {
                            match handler(parsed_payload).await {
                                Ok(_) => (),
                                Err(e) => error!("failed to handle message due to: {}", e),
                            };
                        }
                        Err(e) => error!("Error while deserializing message payload: {}", e),
                    }
                }
            }
        };
    }
}
