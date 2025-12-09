mod common;

use anyhow::Result;
use common::{KafkaConsumer, KafkaProducer, TestSetupBuilder};

#[tokio::test]
async fn test_kafka_produce_consume() -> Result<()> {
    let _test_env = TestSetupBuilder::new("kafka-test")
        .with_ttl(60)
        .add_kafka_topic("messages", 1, 1)
        .build()
        .await?;

    let producer = KafkaProducer::new().await?;
    producer
        .send("messages", "test-key", "Hello from Rust!")
        .await?;

    let consumer = KafkaConsumer::new("test-group").await?;
    consumer.subscribe(&["messages"])?;
    let msg = consumer.receive().await?;

    assert_eq!(msg.key(), Some("test-key"));
    assert_eq!(msg.payload(), Some("Hello from Rust!"));

    Ok(())
}
