---
title: Architecture Overview
linkTitle: Architecture
weight: 10
description: High-level architecture of h5m — components, data flow, and extension points.
draft: false
---

The architecture of h5m is based on the DAG Model. Instead of Horreum's six distinct entity types connected through a fixed linear pipeline, h5m uses a single polymorphic entity — the **Node** — arranged in a directed acyclic graph. This makes the system roughly **10× smaller** (~5k lines vs ~50k) while remaining more flexible.

## The Three Things h5m Does

Every part of the architecture exists to serve three functions:

1. **Accept JSON data** — benchmark results, performance outputs, any structured JSON
2. **Transform it** through a user-defined node graph (jq, JavaScript, JSONata)
3. **Detect changes** using statistical algorithms (Fixed Threshold, Relative Difference)

## Core Components

### Folder

The top-level container. Analogous to a "Test" in Horreum. Each folder owns one node graph and all the computed values produced from uploads to that folder. Folders are fully isolated from each other.

### NodeGroup and the Node Graph

Each folder has exactly one **NodeGroup**. A NodeGroup contains the actual DAG of computation nodes. Nodes are a single polymorphic entity (`NodeEntity`) with a `type` discriminator column. The main discriminator values are `jq`, `ecma` (JavaScript), `nata` (JSONata), `fp` (fingerprint), `ft` (fixed threshold), `rd` (relative difference), with additional types `split`, `root`, `user`, and `ed` (E-Divisive).

When data is uploaded, it enters through a **RootNode** and flows through the graph in topological order.

```
Upload JSON
    │
    ▼
[RootNode]  ← entry point for all uploads
    │
    ├──────────────────────┐
    ▼                      ▼
[jq: cpu]            [jq: throughput]
.results.cpu         .results.throughput
    │                      │
    ▼                      ▼
[Fingerprint]        [rd: throughput-check]
hash(platform,cfg)   relative difference: 20%
    │
    ▼
[ft: cpu-ceiling]
fixed threshold: max=90
```

### Value

Every time a node executes against an uploaded file, it writes a **Value** record. Values are the edges of the DAG at runtime — a node reads its upstream nodes' Values as inputs and writes its own Value as output. All intermediate results are stored, making the full computation history traceable.

### Work Queue

Uploads are processed asynchronously via an **in-process, persistence-backed WorkQueue**. When a file is uploaded:

1. A root Value is created for the raw JSON
2. Work items are created for each node in the graph
3. The WorkQueue sorts them topologically using **Kahn's algorithm**
4. Nodes execute in dependency order; each writes its Value
5. Work items persist to the database so in-progress work survives a crash

No external broker is involved. The queue is entirely in-process.

## Processing Flow — Upload to Detection

```
curl POST /api/folder/my-benchmarks/upload
        │
        ▼
  FolderService.upload()
        │
        ├─ creates root ValueEntity (raw JSON)
        └─ enqueues Work items for all nodes
                │
                ▼
         WorkQueue (in-process)
                │
                ├─ KahnDagSort → topological order
                │
                ├─ executes leaf nodes first (jq/js/jsonata)
                │   └─ writes ValueEntity per node
                │
                └─ executes detection nodes last (ft, rd)
                    └─ writes violation ValueEntity if threshold crossed
```

## Scale Comparison with Horreum

| Aspect | Horreum | h5m |
|--------|---------|-----|
| Code size | ~50k lines | ~5k lines |
| Entity types in pipeline | 6 (Schema, Extractor, Label, Transformer, Variable, Fingerprint) | 1 (NodeEntity, polymorphic) |
| Message broker | ActiveMQ Artemis (external) | In-process WorkQueue |
| Auth dependency | Keycloak (external) | None (optional OIDC) |
| Default database | PostgreSQL (external) | PostgreSQL or SQLite |
| Deployment | Multi-service | Single JAR |

## Adding a New Computation Type

In Horreum, adding a new extraction method required changes across Transformer, Dataset, and Label layers. In h5m, it requires exactly one thing: a new `NodeEntity` subclass with a new discriminator value. The graph wiring, topological sort, work queue, and value storage all work unchanged.

## Framework

h5m is built on [Quarkus](https://quarkus.io/) — a modern Java framework optimised for fast startup and low memory. Key extensions used:

| Extension | Purpose |
|-----------|---------|
| Quarkus REST | REST API (`/api/...` endpoints) |
| Hibernate ORM + Panache | Database access |
| Picocli | CLI (`h5m add folder ...`) |
| Quinoa | Frontend (TypeScript UI) bundling |
| Micrometer + Prometheus | Metrics |
| Quarkus OIDC | Optional authentication |
| GraalVM Polyglot | JavaScript node execution |

