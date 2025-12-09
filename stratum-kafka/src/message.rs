use rdkafka::Message as RdkafkaMessage;

/// Owned Kafka message wrapper with simple API
pub struct KafkaMessage {
    key: Option<String>,
    payload: Option<String>,
}

impl KafkaMessage {
    pub(crate) fn from_borrowed(msg: rdkafka::message::BorrowedMessage) -> Self {
        let key = msg
            .key_view::<str>()
            .and_then(|k| k.ok())
            .map(|k| k.to_string());

        let payload = msg
            .payload_view::<str>()
            .and_then(|p| p.ok())
            .map(|p| p.to_string());

        Self { key, payload }
    }

    /// Get message key
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    /// Get message payload
    pub fn payload(&self) -> Option<&str> {
        self.payload.as_deref()
    }
}
