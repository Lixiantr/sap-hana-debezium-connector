# Source data and DBA contract

## Producer contract

The connector requires a stable primary key and complete row images for
`INSERT`, `UPSERT`, and `REPLACE`. A sparse `UPDATE` is not a substitute for a
complete image. To delete a row, the producer first emits a complete replacement
image with `IS_DELETED=true`, then may execute a physical `DELETE`. Consumers
interpret the logical marker as a Debezium delete (`op=d`); a physical delete
alone is not a supported source event. Multiple changes to one key in a single
transaction are reduced to the final committed state.

The application must not change the table schema during capture. Generated,
default, and identity values are captured as stored by HANA; their origin is not
part of the CDC contract. Values exceeding the configured event/LOB limits fail
closed. Chunked Kafka events are not part of this interface.

The connector does not create an initial snapshot. Starting at an offset does
not imply that every existing row will eventually be observed. Downstream
completeness and any fallback-to-HANA policy belong to the consuming system.

## DBA and storage contract

- Provide a read-only control-plane account for the backup catalog, effective
  backup settings, source/service identity, and selected-table metadata.
- Keep successful catalog entries and corresponding physical log-backup objects
  readable by the connector for at least **two hours**, with sufficient safety
  margin for outage, restart, and backlog. The connector validates retention but
  does not change HANA backup creation or cleanup policy.
- Expose an immutable, unencrypted, uncompressed filesystem-visible backup chain
  with complete catalog coverage and explicit HANA-to-local path mapping.
- Notify operators before changing backup retention, topology, HANA version,
  selected table schema, or mount paths. Unsupported changes stop capture.
- Protect the JDBC credential, backup mount, state directory, and Kafka topics
  using normal least-privilege and operational controls.

The exact grants depend on the HANA tenant and catalog-view visibility. Validate
them with the selected version and connector preflight; do not grant blanket
application-table read access merely for streaming.
