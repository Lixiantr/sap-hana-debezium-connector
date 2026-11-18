# Limitations

This connector does not provide an initial snapshot, arbitrary sparse `UPDATE`
reconstruction, direct physical `DELETE` publication, or automatic schema
evolution. It does not support encrypted/compressed backups, Backint-only or
S3-only backup paths, multi-host scale-out capture, takeover/System Replication,
or exactly-once delivery. A table without a stable business primary key is not
supported. Unsupported source behavior stops capture; it is not treated as a
recoverable skip.

The connector is not a substitute for HANA backup retention. It cannot reserve
or restore backup objects that the DBA or storage system has removed. Its
two-hour retention requirement is a minimum operating contract, not a guarantee
that two hours will cover every outage or backlog.
