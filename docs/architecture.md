# Architecture

The connector reads successful, immutable HANA log-backup objects in catalog
order. A bounded reader validates the backup headers and frames. Row processors
decode native records and reconstruct supported row and LOB values. Transaction
state handles commit/rollback and reduces multiple changes to the same row to
the final committed state. The task then publishes ordered Kafka Connect
`SourceRecord` objects and advances durable offsets.

```text
HANA log backup + catalog
  → continuity and policy validation
  → bounded native reader
  → parallel row decode and table filtering
  → transaction reducer / LOB reconstruction
  → ordered SourceRecord publication and durable checkpoint
```

JDBC serves the read-only control plane: source identity, effective backup
settings, catalog entries, table metadata, and topology. Streaming does not
query application rows. Backup path mapping is configured explicitly because a
HANA path does not establish the connector host's mount path.

Only one Connect task owns an ordered source stream. `log.processor.threads`
controls parallel row processing, not additional Kafka Connect tasks. Checkpoint
and publication ordering remain coordinated by the task.

An excluded table yields no row events, but its transaction boundaries are
still processed. A missing backup, changed source epoch, unsupported record,
ambiguous row image, or changed table schema stops publication rather than
silently skipping data.
