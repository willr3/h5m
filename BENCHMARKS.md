# Benchmarks

h5m has three levels of benchmarks, each testing different aspects at different granularity.

## Quick Reference

| Type | What it tests | Run command |
|------|--------------|-------------|
| JMH microbenchmarks | In-memory algorithm performance (ns) | `mvn clean verify -Dbenchmark` |
| DB edge table | Database insertion & query scaling (ms) | `mvn test -Dtest=EdgeTableBenchmarkTest` |
| qDup end-to-end | Full application upload workflow (s) | See [perf_test/README.md](perf_test/README.md) |

All benchmarks are excluded from the normal `mvn test` run.

---

## JMH Microbenchmarks

**Location:** `src/benchmark/java/.../benchmark/`

In-memory benchmarks measuring algorithm performance at nanosecond precision. No database involved -- these operate on entity graphs with fake IDs.

### Benchmarks

| Class | What it measures |
|-------|-----------------|
| `KahnDagSortBenchmark` | Topological sort: flat lists (10-500 nodes), reversed chains (10-500), layered DAGs (3x5 to 8x10), cycle detection |
| `NodeDependsOnBenchmark` | `Node.dependsOn()` traversal: deep chains (10-1000), wide fans (10-1000), diamond DAGs (4x5 to 8x10) |
| `ValueDependsOnBenchmark` | `Value.dependsOn()` traversal: chain depths 10-1000 |

Shared utility: `GraphBuilder.java` constructs synthetic topologies (chains, fans, diamonds, layered DAGs).

### Running

```bash
# All JMH benchmarks (default: 5 warmup, 10 measurement, 1 fork)
mvn clean verify -Dbenchmark

# Single benchmark class
mvn clean verify -Dbenchmark -Dh5m.benchmark=KahnDagSortBenchmark

# Custom iterations
mvn clean verify -Dbenchmark -Dh5m.benchmark.iterations=20 -Dh5m.benchmark.warmup=10 -Dh5m.benchmark.forks=2
```

### Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `h5m.benchmark` | `.*` | Regex to select benchmark classes |
| `h5m.benchmark.forks` | `1` | JVM forks |
| `h5m.benchmark.iterations` | `10` | Measurement iterations |
| `h5m.benchmark.warmup` | `5` | Warmup iterations |
| `h5m.benchmark.resultType` | `json` | Output format (json, csv, text) |
| `h5m.benchmark.params` | (empty) | Additional JMH parameters |

Results are written to `benchmark/target/` as `jmh-result.json` (or the configured format).

---

## DB Edge Table Benchmarks

**Location:** `src/test/java/.../benchmark/`

Database-backed benchmarks measuring edge table (`node_edge`, `value_edge`) performance at various DAG scales. These are adjacency list join tables storing direct parent-child edges; transitive relationships are computed via recursive SQL CTEs. Uses `@QuarkusTest` with manual `System.nanoTime` timing because JMH cannot bootstrap Quarkus CDI.

### Benchmarks

| Test | Topology | Sizes |
|------|----------|-------|
| `insertFlat_*` | Independent nodes (1 edge per node) | 100, 500, 1000, 2000 |
| `insertChain_*` | Linear dependency chain | 100, 500, 1000 |
| `insertDiamond_*` | Layered DAG (quadratic edge growth) | 3x5, 5x10, 8x20 |
| `queryFqdn_*` | FQDN lookup after flat insert | 100, 500, 1000 |
| `queryDescendants_*` | Descendant value traversal | chain 100/500, diamond 5x10 |

Each insertion test also reports edge counts (nodes, node_edges, ratio, value_edges).

Supporting files:
- `DbGraphBuilder.java` -- creates real JPA entities via `NodeService` with transaction batching
- `BenchmarkTimer.java` -- warmup/measurement timing with statistics (avg, min, max, stddev)

### Running

```bash
# SQLite (fast, good for development)
mvn test -Dtest=EdgeTableBenchmarkTest

# PostgreSQL (production database, auto-starts via Dev Services)
mvn test -Dtest=EdgeTableBenchmarkTest -Dquarkus.datasource.db-kind=postgresql
```

Output is printed to stdout as CSV (timing results) and edge count reports.

---

## Upload Pipeline Benchmarks

**Location:** `src/test/java/.../benchmark/`

End-to-end pipeline benchmarks measuring folder setup, upload, and value calculation using real qvss test data. Tests the full work queue path: upload → work creation → dependency-aware scheduling → value computation.

### Benchmarks

| Test | Nodes | Uploads | Description |
|------|-------|---------|-------------|
| `pipeline_2nodes_9uploads` | 2 flat JQ | 9 | Light workload baseline |
| `pipeline_5nodes_9uploads` | 5 flat JQ | 9 | More nodes, same uploads |
| `pipeline_2nodes_30uploads` | 2 flat JQ | 30 | Same nodes, heavier upload volume |
| `pipeline_5nodes_30uploads` | 5 flat JQ | 30 | Heavy workload |
| `pipeline_chained_9uploads` | 3 chained | 9 | Dependent calculation (throughput → combined) |
| `pipeline_chained_30uploads` | 3 chained | 30 | Chained under heavier load |

Each test reports row counts (nodes, values, node_edges, value_edges, pending_work) after completion.

### Running

```bash
mvn test -Dtest=UploadPipelineBenchmarkTest
```

### Topology Guide

- **Flat:** N independent nodes, each depending only on root. Edge count = N. Tests baseline insertion cost.
- **Chain:** Linear chain root -> n0 -> n1 -> ... -> n(N-1). Edge count = N. Tests sequential dependency handling.
- **Diamond (LxW):** L layers of W nodes each, every node depends on all nodes in the previous layer. Edge count ~ L * W^2. Tests edge table stress under quadratic edge growth.

---

## qDup End-to-End Performance Tests

**Location:** `perf_test/`

Full application benchmarks using [qDup](https://github.com/Hyperfoil/qDup) to measure compile time, upload throughput, and value computation for 100 real-world Quarkus vs Spring Boot benchmark files.

See [perf_test/README.md](perf_test/README.md) for setup and execution instructions.

```bash
# Basic run
java -jar qDup.jar -S gzip_secret="..." -b /tmp/ states.yaml quarkus-spring-boot-comparison.yaml

# With async-profiler flamegraph
java -jar qDup.jar -S profiler="asprof" -S gzip_secret="..." -b /tmp/ states.yaml quarkus-spring-boot-comparison.yaml
```

Results are saved to `run.json`. Key metrics:
```bash
jq '.state.upload.real' run.json          # upload time
jq '.state | "\(.uploads) \(.values)"' run.json  # upload/value counts
```

---

## Build Configuration

Benchmarks are excluded from the normal test run via surefire configuration in `pom.xml`:

```xml
<excludes>
    <exclude>**/*IT.java</exclude>
    <exclude>**/benchmark/**</exclude>
</excludes>
```

The JMH profile is activated with `-Dbenchmark` and adds `src/benchmark/java` as a test source directory.
