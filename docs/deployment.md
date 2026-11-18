# Kafka Connect deployment

1. Build with `mvn -B clean verify` and verify the resulting artifact hash.
2. Create an isolated plugin directory on every Kafka Connect worker. Place
   the connector JAR and required non-platform runtime dependencies there.
   Do not install a shaded benchmark JAR as the production plugin.
3. Obtain `ngdbc` 2.27.7 from SAP under the operator's license and add it to
   that directory. The driver is not in this repository.
4. Mount the qualified HANA log-backup path read-only. Create an exclusive,
   durable `state.dir` and configure the read-only HANA control account.
5. Configure Kafka security, Connect internal topics, a stable connector name,
   and the selected table. Apply the worker converter/performance fragment
   only after merging it with your secured worker configuration.
6. Restart the workers; check `/connector-plugins` for
   `io.hanacdc.connect.HanaSourceConnector`, validate configuration with
   `/config/validate`, and run connector preflight before creation.
7. Confirm backup continuity, topic output, offset progress, and sink
   idempotency before allowing consumers to rely on the stream.

The qualified performance profile used a 4–8 GiB Kafka worker heap and the
cached JSON converter with schemas enabled. This is a measured profile, not an
universal sizing rule. Provision for the expected backlog and event size.

For upgrades, stop the connector, preserve configuration, offsets, and state,
install the new plugin beside the old one, verify state-format compatibility,
then restart. Roll back only to a build that understands the persisted offsets
and reducer state. Never hand-edit these files. Stateful upgrades require a
tested rollback procedure.
