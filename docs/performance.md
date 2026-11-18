# Performance and validation

The final qualification corpus contained 13,440,000 all-types changes in
immutable HANA log backups. A standalone connector with NoOp output processed
71,467.02 events/s at the three-run median. A subsequent run of the same
qualified artifact processed 68,371.14 events/s (−4.33% versus that median),
with 165.32 MB/s accounted redo on the event path, about 219% CPU, and peak
RSS of 2,974,436 KiB. These rates measure decoding from stored backups; they
do not measure live HANA generation capacity.

With Kafka Connect and the cached JSON converter, the measured rate was
30,855.2 events/s (74.61 MB/s accounted redo). This is a connector-to-Kafka
measurement and should not be treated as sink or database throughput.

The current Linux build runs 226 tests with zero failures, errors, or skips.
Native all-types, transaction, table isolation, cross-backup, and LOB checks
pass. In the August 2026 qualification, a worker-crash replay produced 33,178
identical duplicates while retaining 13,440,000 unique events. A broker-outage
test retained the same unique count with zero duplicates in that run.

These observations are dated August 2026. They do not certify a new HANA
revision, worker image, operating system, schema, or workload. A prior
time-bounded Kafka image security exception expired on 5 November 2026; current
production use requires renewed security and end-to-end qualification.
