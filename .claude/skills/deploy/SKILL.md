---
name: deploy
description: Build and deploy okk components to the local kind cluster. Use when you need to rebuild and redeploy coordinator, worker, or agent.
allowed-tools: Bash, Read, Glob, Grep
---

# Deploy OKK to Kind

Deploy okk components to the local kind cluster (`okk-test`).

## Arguments

- `$ARGUMENTS` can be: `all`, `coordinator`, `worker`, `agent`, or `oxia`
- Default (no args): deploy all okk components (coordinator + worker)

## Steps

### Coordinator
1. Build: `cd /Users/mattison/projects/claude/oxia-4/okk && docker build -f coordinator/Dockerfile -t mattison/okk-coordinator:local .`
2. Load: `kind load docker-image mattison/okk-coordinator:local --name okk-test`
3. Restart: `kubectl rollout restart deploy/okk-coordinator -n okk-system --context kind-okk-test`

### Worker (JVM)
1. Build: `cd /Users/mattison/projects/claude/oxia-4/okk && make build-worker-jvm-image`
2. Load: `kind load docker-image mattison/okk-jvm-worker:latest --name okk-test`
3. Restart: `kubectl rollout restart deploy/okk-jvm-worker -n okk-system --context kind-okk-test`

### Agent
1. Build: `cd /Users/mattison/projects/claude/oxia-4/okk/agent && docker build -t okk-agent:local .`
2. Load: `kind load docker-image okk-agent:local --name okk-test`
3. Upgrade helm: `helm upgrade okk charts/okk -n okk-system --kube-context kind-okk-test --set agent.enabled=true --reuse-values`

### Oxia Cluster
1. `helm install oxia oxia/oxia-cluster -n okk-system --kube-context kind-okk-test --set oxia.replicas=3 --set oxia.shards=4`

### Full Deploy (all)
1. Build and load coordinator
2. Build and load worker
3. Helm upgrade: `helm upgrade okk charts/okk -n okk-system --kube-context kind-okk-test --set coordinator.image=mattison/okk-coordinator:local --set worker.image=oxia/okk-jvm-worker:main --set worker.oxiaServiceUrl=oxia:6648`

Always wait for rollout to complete and verify pods are ready.
