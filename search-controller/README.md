# Search Controller

Kubernetes controller for self-service search infrastructure using kube-rs.

See the full writeup: [Building a Kubernetes Controller](https://mikamu.substack.com/p/building-a-kubernetes-controller)

## Quick Start

```bash
cd terraform
terraform init
terraform apply -auto-approve

# Run tests
cd ..
cargo test --test search_e2e
```

## Components

- **Controller** - Watches SearchIndex CRDs, creates Kafka topics, MeiliSearch indexes, and consumer deployments
- **Consumer** - Bridges Kafka to MeiliSearch, batches documents for indexing
- **CRD** - SearchIndex custom resource for declarative search infrastructure

## Example SearchIndex

```yaml
apiVersion: stratum.dev/v1
kind: SearchIndex
metadata:
  name: products
spec:
  kafka:
    partitions: 3
    replicas: 1
  index:
    fields:
      - name: title
        searchable: true
      - name: description
        searchable: true
      - name: price
        filterable: true
        sortable: true
```
