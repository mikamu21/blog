mod setup;

pub use setup::TestSetupBuilder;

pub const KAFKA_BOOTSTRAP: &str = "localhost:30092";
pub const MEILISEARCH_URL: &str = "http://localhost:30700";
