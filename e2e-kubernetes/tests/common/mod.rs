pub mod kafka;
pub mod test_setup;

pub use kafka::{KafkaConsumer, KafkaProducer};
pub use test_setup::TestSetupBuilder;

pub const KAFKA_BOOTSTRAP_SERVERS: &str = "localhost:30092";
