---
name: status
description: Check the status of okk components and running testcases in the kind cluster.
allowed-tools: Bash, Read
---

# OKK Status

Check the status of all okk components in the local kind cluster.

## Steps

1. Check all pods: `kubectl get pods -n okk-system --context kind-okk-test`
2. Check services: `kubectl get svc -n okk-system --context kind-okk-test`
3. Check testcase status via coordinator API:
   - Port-forward: `kubectl port-forward svc/okk-coordinator 18080:8080 -n okk-system --context kind-okk-test &`
   - List testcases: `curl -s http://localhost:18080/testcases | python3 -m json.tool`
   - Health: `curl -s http://localhost:18080/healthz`
   - Kill port-forward after
4. Check coordinator logs: `kubectl logs -l app=okk-coordinator -n okk-system --context kind-okk-test --tail=20`
5. Check worker logs: `kubectl logs -l app=okk-jvm-worker -n okk-system --context kind-okk-test --tail=20`
6. Check agent logs (if running): `kubectl logs -l app=okk-agent -n okk-system --context kind-okk-test --tail=20`

Summarize: which components are healthy, how many testcases running, total operations, any failures.
