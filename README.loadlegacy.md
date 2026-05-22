# Load Legacy

This adds two commands `load-legacy-tests` and `load-legacy-runs` that are used to convert the existing Horreum 
model into the h5m model. Run this with a local backup of a Horreum instance. 

*DO NOT* run these commands on a production database.

## Setup

Make sure `db-kind=postgresql` in `src/main/resources/application.properties` because the legacy model requires postgresql jsonpath.

```
quarkus.datasource.db-kind=postgresql
```

Then build h5m
```shell
mvn clean package -Pnative
```

## Command Overview

### load-legacy-tests

This command works by identifying the schemas used across all non-deleted runs for the test and creating a set of nodes that represent all the Transformers, Extractors, Labels, and Variables.
The new nodes de-duplicate reused Extractors and eliminate Variables that do not make changes to existing Labels.
There are no shared Nodes between Folders.

This command works by _creating_ new tables in the legacy schema. The tables represent all the `$schema` paths in the runs and datasets tables.
The new tables are created the first time the command is run against a legacy schema and can take several minutes.
The tables are re-used for subsequent calls to load.

### load-legacy-runs

This will load all non-deleted runs (optionally for a specific testId) into a Folder with the same name. 

> Note: The `Folder` needs to already exist so use load-legacy-tests before load-legacy-runs.

## Setup

Start with a postgresql instance running a copy of the Horreum database listening on port `6000`.
```bash
podman run --name hdb \
-v <backup_path>:/var/lib/postgresql/data:rw,Z \ 
-e POSTGRES_DB=horreum \
-e PGDATABASE=horreum \
-e POSTGRES_USER=<username> \
-e PGUSER=<username> \
-e PGPASSWORD=<password> \
-e POSTGRES_PASSWORD=<password> \ 
-p 6000:<containerPort> \
mirror.gcr.io/library/postgres:16
```
Start a postgres instance to run the h5m database
```bash
podman run --name h5m \
-e POSTGRES_DB=quarkus \
-e POSTGRES_USER=quarkus \
-e POSTGRES_PASSWORD=quarkus \
-p 5432:5432 \
mirror.gcr.io/library/postgres:17
```
Specify the database url in a `.env` file
```shell
quarkus.datasource.jdbc.url=jdbc:postgresql://0.0.0.0:5432/quarkus
```

Load all the legacy tests with
```bash
h5m load-legacy-tests username=<username> password=<password> url=jdbc:postgresql://0.0.0.0:6000/horreum
```

It can take several minutes for the first invocation against the Horreum database as h5m will be scanning all runs and datasets to create reference tables.

Loading all runs at once will overwhelm h5m using the default configuration. It is best to load a single test at a time
```bash
h5m load-legacy-runs testId=391 username=<username> password=<password> url=jdbc:postgresql://0.0.0.0:6000/horreum
```
Loading in this manner will allow the workQueue to empty (before h5m exits) rather than the loader thread flooding the unbounded queue.

## Unit Testing

There are two unit tests in `H5mTest` that are disabled because they rely on a running Horreum database. They are for debugging purposes and are not 
intended to be enabled. 