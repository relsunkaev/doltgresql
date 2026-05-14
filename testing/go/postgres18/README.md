# PostgreSQL 18 parity tests

This directory contains PostgreSQL 18-specific parity tests. The main `testing/go` suite targets PostgreSQL 16 parity, so tests that require PostgreSQL 18 behavior should live here and use a PostgreSQL 18 oracle cache.

Refresh PG18 oracle maps from this directory with a PostgreSQL 18 DSN and generate its manifest with `--canonical-postgres-major 18`.
