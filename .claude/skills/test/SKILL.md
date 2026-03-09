---
name: test
description: Create or manage testcases on the coordinator. Use to start tests, check results, or delete tests.
allowed-tools: Bash, Read
---

# Manage Testcases

Create, list, or delete testcases via the coordinator HTTP API.

## Arguments

- `$ARGUMENTS` can be: `create <type>`, `list`, `delete <name>`, `status <name>`
- Types: `basic`, `streamingSequence`, `metadataWithEphemeral`, `metadataWithNotification`

## Coordinator API

Port-forward first: `kubectl port-forward svc/okk-coordinator 18080:8080 -n okk-system --context kind-okk-test &`

### Create
```bash
curl -s -X POST http://localhost:18080/testcases \
  -H "Content-Type: application/json" \
  -d '{
    "name": "<name>",
    "type": "<type>",
    "namespace": "default",
    "workerEndpoint": "okk-jvm-worker:6666",
    "opRate": 50,
    "properties": {"keySpace": "1000"}
  }'
```

### List
```bash
curl -s http://localhost:18080/testcases | python3 -m json.tool
```

### Status
```bash
curl -s http://localhost:18080/testcases/<name> | python3 -m json.tool
```

### Delete
```bash
curl -s -X DELETE http://localhost:18080/testcases/<name>
```

Always kill port-forward when done.
