---
name: build
description: Build okk components locally. Use to compile coordinator, generate proto, or build worker.
allowed-tools: Bash, Read, Glob, Grep
---

# Build OKK

Build okk components locally.

## Arguments

- `$ARGUMENTS` can be: `coordinator`, `proto`, `worker`, `all`

## Commands

### Coordinator
```bash
cd /Users/mattison/projects/claude/oxia-4/okk/coordinator && go build ./cmd/main.go && go vet ./...
```

### Proto (regenerate Go protobuf code)
```bash
cd /Users/mattison/projects/claude/oxia-4/okk && make proto
```

### Worker (JVM)
```bash
cd /Users/mattison/projects/claude/oxia-4/okk && make build-worker-jvm
```

### All
Run coordinator + proto + worker builds.

Report build success/failure for each component.
