# Regret Adapters for Oxia

Adapters that bridge [Regret](https://github.com/regret-io/regret) correctness testing to [Oxia](https://github.com/oxia-db/oxia) and its client SDKs.

## Adapters

| Adapter | Language | Image | Status |
|---------|----------|-------|--------|
| `java/` | Java 21 | `regretio/adapter-oxia-java` | Active |

## Usage

Each adapter implements the Regret gRPC `AdapterService` interface and translates operations to the target system's client SDK.

### Java Adapter

```bash
docker run -e OXIA_ADDR=oxia:6648 -e OXIA_NAMESPACE=default -p 9090:9090 regretio/adapter-oxia-java:latest
```

### Reusable CI Workflow

The `regret-test.yml` workflow can be called from any repo to run Regret correctness tests:

```yaml
jobs:
  regret:
    uses: oxia-db/regret-adapters/.github/workflows/regret-test.yml@main
    with:
      candidate_image: "oxia/oxia:v0.16.2-rc0"
      stable_image: "oxia/oxia:latest"
      duration: "3h"
    secrets:
      regret_api: ${{ secrets.REGRET_API_URL }}
      regret_password: ${{ secrets.REGRET_PASSWORD }}
      regret_studio: ${{ secrets.REGRET_STUDIO_URL }}
```

## Adding a New Adapter

1. Create a directory (e.g. `go/`) with the adapter implementation
2. Use the `java-sdk/` as reference for the gRPC interface
3. Add a Dockerfile
4. Add a build job to `.github/workflows/build.yml`
