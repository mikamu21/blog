pub mod controller;
pub mod crd;
pub mod resources;

pub use crd::{
    Condition, ConnectorSpec, FieldSpec, IndexSpec, KafkaSpec, SearchIndex, SearchIndexSpec,
    SearchIndexStatus,
};
