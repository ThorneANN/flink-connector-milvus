# flink-connector-milvus

Apache Flink connector for [Milvus](https://milvus.io/) vector database, supporting both the DataStream API and the Table/SQL API.

## Features

- **DataStream API** — type-safe `MilvusSink<T>` with a fluent builder
- **Table / SQL API** — DDL-based sink via `'connector' = 'milvus'`
- **Automatic write semantics** — operation type is driven by `RowKind` (no manual mode config needed):
  - `INSERT` / `UPDATE_AFTER` → Milvus **upsert**
  - `UPDATE_BEFORE` → **skipped**
  - `DELETE` → Milvus **delete** by primary key
- Optional **partition** support (for collections without Partition Key enabled)
- Configurable **retry** with sleep interval on flush failure
- Shaded fat jar (`flink-sql-connector-milvus`) for drop-in cluster deployment

## Requirements

| Component | Version |
|---|---|
| Java | 11+ |
| Apache Flink | 1.20.x or 2.0.x |
| Milvus | 2.x |

## Module Structure

```
flink-connector-milvus/                  (parent POM)
├── flink-connector-milvus/              (core connector — thin jar)
│   └── src/main/java/...
└── flink-sql-connector-milvus/          (shaded fat jar for cluster deployment)
```

Use `flink-connector-milvus` as a library dependency in your application.
Use `flink-sql-connector-milvus` when deploying to a Flink cluster (drop into `lib/` or `plugins/`).

## Quick Start

### DataStream API

```java
MilvusSink<MyRecord> sink = MilvusSink.<MyRecord>builder()
    .config(MilvusSinkConfig.builder()
        .uri("http://localhost:19530")
        .token("root:Milvus")
        .collectionName("my_collection")
        .batchSize(1000)
        .flushIntervalMs(1000)
        .maxRetries(3)
        .build())
    .converter(record -> {
        JsonObject json = new JsonObject();
        json.addProperty("id", record.getId());
        json.addProperty("text", record.getText());
        JsonArray vec = new JsonArray();
        for (float v : record.getEmbedding()) vec.add(v);
        json.add("embedding", vec);
        return json;
    })
    .build();

dataStream.sinkTo(sink);
```

> For the DataStream API all records are treated as **upserts**.
> To perform deletes, use the Table/SQL API which propagates `RowKind.DELETE` automatically.

### Table / SQL API

```sql
CREATE TABLE milvus_sink (
    id        BIGINT,
    text      STRING,
    embedding ARRAY<FLOAT>,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector'       = 'milvus',
    'uri'             = 'http://localhost:19530',
    'token'           = 'root:Milvus',
    'collection-name' = 'my_collection'
);

-- Simple append
INSERT INTO milvus_sink SELECT id, text, embedding FROM source_table;

-- CDC upsert/delete (e.g. from a CDC source)
INSERT INTO milvus_sink SELECT * FROM cdc_source;
```

Write operations are determined automatically from the changelog stream:

| RowKind | Milvus operation |
|---|---|
| `INSERT` | upsert |
| `UPDATE_AFTER` | upsert |
| `UPDATE_BEFORE` | skipped |
| `DELETE` | delete by primary key (`id` field) |

## Configuration Options

| Option | Required | Default | Description |
|---|---|---|---|
| `uri` | Yes | — | Milvus endpoint, e.g. `http://localhost:19530` |
| `collection-name` | Yes | — | Target collection name |
| `token` | No | `""` | Auth token, e.g. `root:Milvus` |
| `database-name` | No | `default` | Milvus database name |
| `batch-size` | No | `1000` | Records to buffer before flushing |
| `flush-interval-ms` | No | `1000` | Periodic flush interval in ms (0 = disabled) |
| `consistency-level` | No | `BOUNDED` | `STRONG`, `BOUNDED`, `SESSION`, `EVENTUALLY` |
| `partition-name` | No | `""` | Target partition name — **leave empty if the collection uses a scalar field as Partition Key** (Milvus routes automatically in that case; setting this alongside Partition Key will cause an error) |
| `max-retries` | No | `3` | Max flush retry attempts after failure (0 = no retry) |
| `retry-interval-ms` | No | `1000` | Sleep between retries in ms |

## Partition Support

Milvus supports two partitioning strategies:

1. **Manual partitions** — you create named partitions and route data explicitly.
   Set `partition-name` to target a specific partition.

2. **Partition Key** — a scalar field in the collection schema is designated as the Partition Key,
   and Milvus routes documents automatically.
   **Do not set `partition-name`** in this case; doing so will cause a Milvus error.

## Type Mapping

| Flink SQL Type | Milvus Field Type |
|---|---|
| `BOOLEAN` | Bool |
| `TINYINT` | Int8 |
| `SMALLINT` | Int16 |
| `INT` | Int32 |
| `BIGINT` | Int64 |
| `FLOAT` | Float |
| `DOUBLE` | Double |
| `STRING` / `VARCHAR` | VarChar |
| `ARRAY<FLOAT>` | FloatVector |
| `BYTES` / `VARBINARY` | BinaryVector (encoded as int array) |
| `ROW<...>` | Nested JSON object |

## Building

```bash
# Compile with Flink 1.20 (default)
mvn compile -P flink-1.x

# Compile with Flink 2.0
mvn compile -P flink-2.x

# Run unit tests (40 tests)
mvn test -pl flink-connector-milvus

# Package — produces thin jar and fat jar
mvn package -P flink-1.x
```

Output jars:

| Jar | Size | Use case |
|---|---|---|
| `flink-connector-milvus/target/flink-connector-milvus-*.jar` | ~25 KB | Application dependency |
| `flink-sql-connector-milvus/target/flink-sql-connector-milvus-*.jar` | ~29 MB | Flink cluster `lib/` deployment |

## Dependency (Maven)

```xml
<dependency>
    <groupId>com.thorne.flink</groupId>
    <artifactId>flink-connector-milvus</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
