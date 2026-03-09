# OKK

OKK is a testing framework designed to verify the correctness in key-value storage and index engine systems.

## Prerequisites

Before you begin, ensure you have the following installed:

- Kubernetes Cluster (or Kind)
- Helm

## Quick Start

Follow these steps to add the Helm repository and install a release.


1. Add the Helm repository:

```
helm repo add okk https://oxia-db.github.io/okk/
helm repo update
```

2. Install the chart

```
helm install okk okk/okk --namespace okk --create-namespace
```

3. Run `TestCase` by examples

We provide some commonly used test cases you can apply directly from [example](./examples) dir.

## Configuration

You can customize your deployment by using the --set flag or by providing a custom values.yaml file.

To see all available configuration options, refer to the [values.yaml](./charts/okk/values.yaml) file in the chart's repository.