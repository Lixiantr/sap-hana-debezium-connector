# SAP HANA source connector for Kafka Connect

This open-source project implements a streaming-only SAP HANA source connector using
HANA log backups. It publishes Debezium-compatible change records through Kafka
Connect, with a native redo decoder, transaction ordering, and durable restart
state. The goal is to capture committed changes without triggers or reads from
application tables on the streaming path.

This is an independent connector, **not an official Debezium or SAP product**.
It is not a general-purpose HANA CDC connector: use it only within the supported
[data contract](docs/data-contract.md) and [compatibility boundary](docs/compatibility.md).
There is no initial snapshot. Consumers must arrange their own initial state or
accept a streaming-only data set.

## Measured results

The connector was measured on a 13,440,000-event all-types log-backup corpus.
Rates below describe distinct output paths; they are not HANA ingestion or
network bandwidth figures.

| Path | Throughput | Accounted redo | Result |
|---|---:|---:|---|
| Connector standalone, NoOp output (three-run median) | 71,467 events/s | 172.99 MB/s | Full corpus decoded |
| Kafka Connect with cached JSON converter | 30,855 events/s | 74.61 MB/s | Full corpus published |

The current Linux build runs 226 tests with zero failures, errors, or skips.
In the August 2026 qualification, a worker-crash replay produced 33,178
identical records, consistent with at-least-once delivery. See
[performance and validation](docs/performance.md) for measurement scope.

These are historical measurements from August 2026, not a current deployment
approval. In particular, a time-limited exception for high-severity findings
in the qualified Kafka image expired on 5 November 2026. Re-run security,
compatibility, and end-to-end qualification before a production deployment.

## Supported operation

- One Kafka Connect task (`tasks.max=1`) with parallel row processing inside the
  connector; `snapshot.mode=no_data`.
- Full-row-image `INSERT`, `UPSERT`, and `REPLACE` under the documented producer
  contract. Logical deletion requires a full image with `IS_DELETED=true` before
  the physical `DELETE`.
- Selected tables only. Records for other tables are ignored, while transaction
  boundaries are still tracked.
- Single-container tables and supported keyed partitions on one active
  indexserver. Multi-host scale-out is not qualified for publication.
- At-least-once delivery with stable source offsets and fail-closed handling of
  missing backups, unsupported records, and incompatible topology/schema.

Direct `DELETE`, sparse `UPDATE`, initial snapshots, schema evolution, encrypted
or compressed log backups, and scale-out capture are outside this support
boundary. See [limitations](docs/limitations.md).

## Build and deploy

Requirements: JDK 17 or newer (runtime 21.0.11 was tested), Maven 3.9+, Kafka
Connect 4.3.1, SAP HANA 2.00.088.*, and the operator-supplied SAP JDBC driver
`ngdbc` 2.27.7. The proprietary JDBC driver is intentionally not included.

```sh
mvn -B clean verify
```

Install the connector JAR and its resolved non-platform runtime dependencies in
an isolated Kafka Connect plugin directory. Add the separately licensed `ngdbc`
JAR there. Configure a read-only HANA control-plane user, mounted readable log
backups, durable local state, and a one-task connector. Start from
[`connector.example.properties`](connector.example.properties) and follow the
[deployment guide](docs/deployment.md). Do not commit credentials.

## Repository layout

```text
src/main/java/          connector, decoder, control plane and diagnostics
src/test/java/          unit and contract tests
src/test/resources/     compact native-record regression fixtures
docs/                  architecture, contract, configuration and operations
pom.xml                Maven build
```

The source and test fixtures are included. Raw HANA backups, environment-specific
configuration, proprietary drivers, benchmark output, and deployment credentials
are not included.

## Documentation

- [Architecture](docs/architecture.md)
- [Data and DBA contract](docs/data-contract.md)
- [Configuration](docs/configuration.md)
- [Deployment](docs/deployment.md)
- [Operations and recovery](docs/operations.md)
- [Compatibility and limitations](docs/compatibility.md)
- [Performance and validation](docs/performance.md)

The connector source code and documentation are licensed under the
[Apache License 2.0](LICENSE). The SAP JDBC driver is separately licensed and
is not included. See [NOTICE](NOTICE.md).
