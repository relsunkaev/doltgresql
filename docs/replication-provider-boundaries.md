# Replication provider boundaries

Doltgres implements PostgreSQL wire-protocol and source-mode logical
replication behavior directly. Cloud-provider control-plane features are not
claimed as supported unless a test names the exact SQL or protocol surface.

## Aurora and RDS assumptions

The following provider-specific assumptions are explicit boundaries:

- `rds.logical_replication` is not a Doltgres configuration parameter. Clients
  may use `current_setting('rds.logical_replication', true)` as an optional
  probe, but `SHOW rds.logical_replication` is rejected as an unrecognized
  configuration parameter.
- `track_commit_timestamp` exists for PostgreSQL catalog compatibility, but it
  is off and read-only. Doltgres does not expose PostgreSQL commit timestamps.
- `pglogical` is explicitly rejected. Doltgres does not run logical replication
  apply workers or subscriber-side synchronization.
- RDS Proxy is outside the database engine. Doltgres compatibility claims stop
  at the PostgreSQL protocol and authenticated client behavior; AWS-managed
  proxy provisioning, target health, IAM authentication, and proxy-specific
  metadata are not represented by Doltgres.

The executable boundary test is
`testing/go/provider_replication_boundary_test.go`.
