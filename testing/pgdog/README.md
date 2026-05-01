# PgDog Compatibility Smoke

This directory contains the Doltgres compatibility boundary for PgDog.

PgDog support is currently scoped to a primary-only shard smoke path: clients can connect to PgDog, PgDog can connect to two Doltgres primary shards, DDL can be broadcast to the shards, and ordinary sharded `INSERT` / `SELECT` statements can be routed by a supported shard key type.

## Run

From the repository root:

```bash
testing/pgdog/run_pgdog_smoke.sh
```

The script builds a local `doltgres` binary unless `DOLTGRES_BIN` is set, starts two temporary Doltgres shards on the host, starts `ghcr.io/pgdogdev/pgdog:latest` in Docker, runs a shard-routing smoke test through PgDog, then checks that unsupported PgDog feature paths fail explicitly.

On Homebrew-based macOS setups, the script automatically uses `icu4c@78` for the local Go build when `CGO_CPPFLAGS` is not already set.

Useful overrides:

```bash
DOLTGRES_BIN=/path/to/doltgres \
PGDOG_IMAGE=ghcr.io/pgdogdev/pgdog:latest \
PGDOG_PORT=16432 \
DOLTGRES_SHARD0_PORT=15432 \
DOLTGRES_SHARD1_PORT=15433 \
testing/pgdog/run_pgdog_smoke.sh
```

For CI, prefer pinning `PGDOG_IMAGE` to a digest rather than using `latest`.

## Supported Boundary

Use this PgDog configuration shape:

```toml
[general]
two_phase_commit = false
two_phase_commit_auto = false
prepared_statements = "extended"
read_write_split = "include_primary"
load_schema = "off"

[[databases]]
name = "pgdog"
host = "host.docker.internal"
port = 15432
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 0

[[databases]]
name = "pgdog"
host = "host.docker.internal"
port = 15433
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 1

[[sharded_tables]]
database = "pgdog"
name = "pgdog_items"
column = "tenant_id"
data_type = "bigint"
```

PgDog requires `pgdog.toml` and `users.toml`, uses `[[databases]]` entries for backend primaries, and routes configured sharded-table columns such as `bigint`, `varchar` / `text`, and `uuid`. PgDog also documents `vector` shard keys, but Doltgres does not currently provide a native `vector` type, so this smoke path intentionally uses `bigint`.

## Unsupported Paths

Keep these PgDog features disabled or out of scope for Doltgres until the corresponding PostgreSQL surface is implemented:

| PgDog lane | Doltgres status | Required configuration or behavior |
| --- | --- | --- |
| 2PC / prepared transactions | `PREPARE TRANSACTION`, `COMMIT PREPARED`, and `ROLLBACK PREPARED` are unsupported. | Set `two_phase_commit = false` and `two_phase_commit_auto = false`. Cross-shard writes are not atomic. |
| Resharding and cutover | Doltgres consumes upstream logical replication, but it does not expose PostgreSQL logical replication as a server. | Do not run PgDog resharding or cutover against Doltgres shards. |
| Publication and subscription DDL | Publication/subscription commands and local publication catalogs are not implemented enough for PgDog setup. | Treat PgDog logical replication setup as unsupported. |
| Replication slots and replication stats | `pg_replication_slots`, `pg_stat_replication`, and `pg_stat_replication_slots` are placeholders without local producer state. | Do not use PgDog replica or replication-health workflows against Doltgres. |
| COPY movement | Text/CSV `COPY FROM` exists, but `COPY TO STDOUT` and binary `COPY FROM` are unsupported. | Do not use PgDog sharded COPY or resharding COPY against Doltgres. |
| Vector shard keys | PgDog can route `vector` keys, but Doltgres has no native `vector` type. | Use `bigint`, `varchar` / `text`, or `uuid` shard keys. |
| Replica routing | `pg_is_in_recovery()` reports primary mode, `pg_current_wal_lsn()` returns the synthetic primary compatibility LSN `0/0`, and replay/receive LSNs are `NULL`. There is no standby or lag stream. | Configure only primary Doltgres entries. Do not use PgDog replica routing or lag checks. |

SQL-level `PREPARE`, `EXECUTE`, `DEALLOCATE`, and `pg_prepared_statements` are supported for PgDog's full prepared-statement mode smoke coverage.

The smoke config sets `load_schema = "off"` because PgDog's schema loader currently trips a Doltgres catalog ambiguity while introspecting `information_schema`. The harness still configures the sharded table explicitly and verifies routing with live rows on both shards.

Relevant PgDog docs:

- https://docs.pgdog.dev/configuration/
- https://docs.pgdog.dev/configuration/pgdog.toml/databases/
- https://docs.pgdog.dev/configuration/pgdog.toml/sharded_tables/
- https://docs.pgdog.dev/features/sharding/2pc/
- https://docs.pgdog.dev/features/prepared-statements/
- https://docs.pgdog.dev/configuration/pgdog.toml/general/
