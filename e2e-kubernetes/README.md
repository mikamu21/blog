# E2E Kubernetes Testing

E2E testing framework with KIND, Terraform, Kafka, and Rust.

See the full writeup: [Integration Testing with Kubernetes](https://mikamu.substack.com/p/integration-testing-with-kubernetes)

## Quick Start

```bash
cd terraform
terraform init
terraform apply -auto-approve

# Run tests
cd ..
cargo test 
```
