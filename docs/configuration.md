# Configuration

[`connector.example.properties`](../connector.example.properties) is the
complete example. The authoritative validation schema is
`HanaSourceConnector.CONFIG_DEF`. All connector settings are static: stop,
validate, and restart after a change.

| Setting | Production rule |
|---|---|
| `name` | Stable Kafka Connect connector name |
| `connector.class` | `io.hanacdc.connect.HanaSourceConnector` |
| `tasks.max` | `1` |
| `server.name` | Stable logical source identity; changing it creates a new offset partition |
| `topic` | Ordered output topic for the selected table |
| `snapshot.mode` | `no_data` |
| `prototype.mode` | `false` |
| `table.include.list` | Anchored selected `schema.table` expression |
| `log.backup.discovery.mode` | `catalog` |
| `database.connection.url`, `database.user`, `database.password` | Read-only HANA control-plane connection; keep secrets outside Git |
| `log.backup.path.mapping` | Explicit HANA-to-connector mount mapping |
| `source.topology.mode` | `single_container` or supported `single_service` |
| `log.backup.retention.min.seconds` | At least `7200` |
| `state.dir` | Persistent, exclusive, access-controlled state directory |

`first.position` is an explicit starting offset, not a snapshot marker.
`last.position` gives an exclusive upper bound for bounded tests. In normal
streaming it can be set to a large value. Do not reuse a prior offset with a
different source identity or schema fingerprint.

`log.processor.threads=auto` selects parallel row processing, capped by
`log.processor.max.threads` (qualified cap: six). The native backup reader and
ordered transaction publication are not independently split into Connect
tasks. `poll.max.batch.size` counts records; `poll.interval.ms` is milliseconds.

`control.policy.refresh.interval.ms` and
`control.policy.max.staleness.ms` govern the read-only policy refresh. A stale
or failed policy check stops publication. Backup retention, maximum publication
delay, and safety-margin settings are checks against the DBA contract, not
commands that change HANA backup behavior.

Kafka worker settings, including converters, Kafka TLS/ACLs and internal topic
configuration, are separate from connector settings. The provided
[`worker-performance.properties`](../worker-performance.properties) is only a
performance fragment; merge it with a secured worker configuration.
