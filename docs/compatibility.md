# Compatibility

| Component | Qualified boundary |
|---|---|
| SAP HANA | 2.00.088.* native backup format |
| SAP JDBC | `ngdbc` 2.27.7, supplied separately |
| Java | target 17; runtime 21.0.11 tested |
| Kafka Connect | Apache Kafka 4.3.1 |
| Snapshot | `no_data` only |
| Delivery | at least once |
| Topology | single container or supported keyed partitions on one active indexserver |
| Backup | successful, filesystem-visible, unencrypted and uncompressed log backup |

The qualified Kafka image was
`apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`.
Its historical security exception expired on 5 November 2026; this digest is a
reproduction reference, not a current recommendation.

Changing HANA revision, JDBC driver, Kafka Connect, JDK, converter, or topology
requires renewed functional, security, compatibility, and end-to-end testing.
An apparently compatible version number is not evidence of native log-format
compatibility.
