# h5m 
h5m is H{orreu}m but lighter (thus fewer characters).

This is a proof of concept for a light weight Horreum entity model based on a directed acyclical graph of computations and resulting values.
The existing Entities (Labels, Extractors, Transformers, Variables, ...)  are gone. 
All entities that calculate values from input become nodes with edges connecting the output of one node to the input of another.

Other changes:
* Replace postgres' jsonpath with `jq`
* Tests are Folders on the file system
* Runs are files in the test folder on the file system
* Value calculations are managed by a persistence backed ExecutorService instead of a JMS Queue.



## Getting started
### 1. Install jq
h5m relies on `jq` for jsonpath processing.

https://jqlang.org/

### 2. Build the project
```shell
mvn clean package -Pnative
```
or you can just build a jar 
```shell
mvn clean package
```
and substitute `java -jar /target/h5m.jar` wherever you see `target/h5m` in the following steps

### 3. Create a Folder
```shell
TEMP_DIR=$(mktemp -d)
target/h5m add folder test
```

### 4. create jq nodes for the test
```shell
target/h5m add jq to test foo .foo[]
target/h5m add jq to test name {foo}:.name
target/h5m add jq to test bar {foo}:.bar
target/h5m add jq to test biz '{bar}:.biz[] + "-it"'
```
The `foo` node's operation is a `jq` filter. The `name`,`bar`,`biz` nodes' operations are `jq` filters with a prefix 
to indicate the node gets input from another node. The `{name}:` prefix creates the edges that connect nodes in the 
computation graph.

### 5. List the nodes 
```shell
target/h5m list test nodes
┌──────┬──────┬───────────┬────────────────┬───────────────────────────┐
│ name │ type │   fqdn    │   operation    │         encoding          │
├──────┼──────┼───────────┼────────────────┼───────────────────────────┤
│ foo  │ jq   │ test:foo  │ .foo[]         │ .foo[]                    │
│ name │ jq   │ test:name │ .name          │ {test:foo}:.name          │
│ bar  │ jq   │ test:bar  │ .bar           │ {test:foo}:.bar           │
│ biz  │ jq   │ test:biz  │ .biz[] + "-it" │ {test:bar}:.biz[] + "-it" │
└──────┴──────┴───────────┴────────────────┴───────────────────────────┘ 
```
The `encoding` is similar to the original `operation` we defined in the `h5m add jq` command invocation but it uses the 
fully qualified name of the source node(s). Node names need to be uniquely identifiable but copying nodes from another group
can cause duplicates so we are considering a "fully qualified name" as a way to resolve the ambiguity.

### 6. Create and upload sample run
```shell
echo '{"foo":[{"name":"primero","bar":{"biz":["one","first"]}},{"name":"segundo","bar":{"biz":["two","second"]}}]}' > $TEMP_DIR/first.json
target/h5m upload $TEMP_DIR to test
```
### 7. List the values
```shell
target/h5m list test values
Count: 10
┌────┬───────────────────────────────────────────────────┬─────────┐
│ id │                       data                        │ node.id │
├────┼───────────────────────────────────────────────────┼─────────┤
│  2 │ {"name":"primero","bar":{"biz":["one","first"]}}  │      51 │
│  3 │ {"name":"segundo","bar":{"biz":["two","second"]}} │      51 │
│  4 │ {"biz":["one","first"]}                           │     151 │
│  5 │ {"biz":["two","second"]}                          │     151 │
│  6 │ "primero"                                         │     101 │
│  7 │ "segundo"                                         │     101 │
│  8 │ "one-it"                                          │     201 │
│  9 │ "first-it"                                        │     201 │
│ 10 │ "two-it"                                          │     201 │
│ 11 │ "second-it"                                       │     201 │
└────┴───────────────────────────────────────────────────┴─────────┘

```
There are multiple rows for the same node because the `foo` node uses `jq`'s iterator syntax (`[]`) that creates a separate result from each entry in `.foo`
h5m sees the separate result as a separate values similar to how a schema transform would create separate datasets in Horreum.

The values can also be grouped into json based on a "source node." This acts like getting the label values for datasets.

```shell
target/h5m list test values by foo
┌────────────────────────────────────────────────────────────────────────────────┐
│                                      data                                      │
├────────────────────────────────────────────────────────────────────────────────┤
│ {"name":"primero","bar":{"biz":["one","first"]},"biz":["one-it","first-it"]}   │
│ {"name":"segundo","bar":{"biz":["two","second"]},"biz":["two-it","second-it"]} │
└────────────────────────────────────────────────────────────────────────────────┘
```
or separate each node as a separate column with `as table`
```shell
target/h5m list test values by foo as table
┌──────────────────────────┬────────────────────────┬───────────┐
│           bar            │          biz           │   name    │
├──────────────────────────┼────────────────────────┼───────────┤
│ {"biz":["one","first"]}  │ ["one-it","first-it"]  │ "primero" │
│ {"biz":["two","second"]} │ ["two-it","second-it"] │ "segundo" │
└──────────────────────────┴────────────────────────┴───────────┘
```

That sums up most of what exists in the h5m cli. You can further explore with
```shell
target/h5m help
```
The database will default to ~/h5m.db (plus associated -shm and -wal files) but the location can be controlled with `H5M_PATH` environment variable.

Delete the 3 database files to reset h5m.

## Design

There are significant differences between `h5m` and the current horreum architecture. The main goals with h5m are:
* simplify the conceptual model for users (fewer entities to learn)
* eliminate the code complexity 
* support a "single jar" deployment mode

### jq
Horreum currently relies on the jsonpath implementation in postgres. Changing to `jq` offers several benefits:
1. jq has a turing complete filter language that supports far more processing options for users
2. AI search results for "How do I perform X in jq" offer more accurate answers than "How do I perform X in postgres jsonpath". There are also several situations where X cannot be done in postgres jsonpath
3. Support alternative persistence options (sqllite, db2, ...) and persistence migration without changing all jsonpath

Concerns:
1. jq requires file access to the input json which could lead to higher IO if the jq operation is performed on a different host than the json persistence.
2. jq is an external dependency that is not bundled with h5m

### Node Graph
The existing Horreum entity model (Schemas, Labels, Extractors, Combination Functions, Runs, Datasets, LabelValues, Fingerprints...) are replaced with: 
* Folder - a folder on disk that will contain the source json (runs in current Horreum)
* Node - a data computation / extraction (Extractor, Label, Combination Function, Fingerprint, Change Detection)
* NodeGroup - a group of nodes that work on the same source data.
* Value - The output of a Node applied to it's input. A Node can produce multiple values (e.g. Datasets) which will be treated as separate inputs for other nodes.

### Nodes

There will be different types of nodes for the different types of tasks. The PoC currently supports `jq` and `js` The Getting Started used `jq`

### WorkQueue

Horreum uses AMQ to asynchronously queue work inside Horreum. Requiring an external message queue to function as an 
asynchronous "to do list" that does not communicate with external processes seems like a dependency mismatch.
Removing AMQ allows h5m to support the single jar deployment model and eliminates unnecessary enter-process communication.

Horreum uses AMQ to provide:   
* asynchronous execution
* persistence and delivery guarantee
* message retry
* queue observability

The AMQ instance does not provide:
* task re-ordering
* inter-task dependency
* de-duplication of tasks

We can achieve the desired features of the AMQ instance and the missing features with a persistence backed WorkQueue
and associated ExecutorService.
* Work is persisted to the database then added to the queue to provide persistence.
* The queue reloads all entities from the database on startup.
* The queue sorts the pending Work based on their interdependency.
* Work is removed from the database when it successfully completes.
* Work is added back into the queue if an error occurs while processing it. (retry limit?)
* Observability can be achieved with [quarkus observability](https://quarkus.io/guides/observability)


 
