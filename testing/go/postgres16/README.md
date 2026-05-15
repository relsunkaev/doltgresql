# PostgreSQL 16 parity tests

This directory is reserved for PostgreSQL 16 oracle-backed parity tests. Tests
that use cached PostgreSQL expectations should live here so top-level
`testing/go` can stay focused on Doltgres-specific behavior that cannot be
oracle-backed.

Refresh PG16 oracle maps from this directory with a PostgreSQL 16 DSN and
generate its manifest with `--canonical-postgres-major 16`.
