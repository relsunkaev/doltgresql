# PostgreSQL 17 parity tests

This directory is reserved for PostgreSQL 17-specific parity tests. The main `testing/go` suite targets PostgreSQL 16 parity, so tests that require PostgreSQL 17 behavior should live here and use a PostgreSQL 17 oracle cache.

Refresh PG17 oracle maps from this directory with a PostgreSQL 17 DSN and generate its manifest with `--canonical-postgres-major 17`.
