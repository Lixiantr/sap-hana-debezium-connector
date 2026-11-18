# Operations and recovery

Monitor the age of the latest successful catalog backup, oldest unread backup,
retention headroom, source-to-publication delay, open transactions, queue
pressure, connector errors, and Kafka offset progress. Alert before the
two-hour minimum retention window is consumed. If the physical backup chain is
lost, do not skip forward automatically; stop and choose an explicit recovery
or resynchronization procedure.

At-least-once delivery means a crash can replay records. Consumers and sinks
must be idempotent by source identity, transaction/position, operation, and
primary key as appropriate. Commit Kafka offsets only after durable sink
success. A replayed record must not change the final materialized state.

The connector fails closed on source epoch or topology changes, schema
fingerprint mismatch, missing/ambiguous backups, unsupported record grammar,
incomplete LOB reconstruction, and expired control-plane state. Resolve the
cause and verify the source contract before restarting. Never reset offsets
merely to suppress an error.

Protect log backups, state and checkpoint files, HANA credentials, and Kafka
topics. Keep secrets out of connector properties checked into source control;
use the deployment platform's secret provider. Restrict access to the read-only
backup mount and rotate credentials using a controlled restart.
