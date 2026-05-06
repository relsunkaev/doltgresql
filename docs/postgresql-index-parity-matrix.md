# PostgreSQL index parity matrix

This matrix is the current regression and benchmark gate for `dg-6pc`. It uses
the support levels from `docs/postgresql-index-architecture.md`:

- parser-only: statement parses, but no behavior is claimed
- import shortcut: warning or no-op behavior exists only for import flow
- metadata support: durable metadata is visible through catalogs
- semantic support: reads and writes match the claimed PostgreSQL behavior
- planner support: `EXPLAIN` proves the claimed indexed access path
- performance support: benchmarks cover the claimed indexed path or maintenance
  cost

A lane is not full parity unless it reaches every support level that PostgreSQL
would expose for that feature. Explicit non-parity rows are intentional gates:
they prevent parser-only or metadata-only support from being counted as complete.

| Lane | Current support level | Evidence | Remaining boundary |
| --- | --- | --- | --- |
| Plain btree equality/range lookups | performance support | `testing/go/index_benchmark_test.go`: `TestBtreeIndexPlannerShape`, `BenchmarkBtreeSQLLookup`; `testing/go/index_test.go`: `TestBasicIndexing` | PostgreSQL opclass/collation-aware range building is still broader than the current GMS range bridge. |
| Composite btree lookup joins | performance support | `testing/go/index_test.go`: indexed join plan regressions; `testing/go/index_benchmark_test.go`: `BenchmarkBtreeSQLJoin` | Benchmark uses the explicit lookup-join hint for the measured indexed case. |
| Btree index build and write maintenance | performance support | `testing/go/index_benchmark_test.go`: `BenchmarkBtreeIndexBuild`, `BenchmarkBtreeDMLMaintenance` | Benchmarks measure current Dolt/GMS btree storage, not PostgreSQL-specific opclass storage. |
| Single-expression btree indexes | planner support | `testing/go/index_benchmark_test.go`: `TestExpressionBtreeIndexPlannerShape`; `testing/go/index_test.go`: functional-index catalog and hidden-column regressions | Only the supported single-expression shape is claimed as indexed. |
| Mixed expression/column btree indexes | explicit non-parity boundary | `testing/go/index_benchmark_test.go`: `TestMixedExpressionBtreeIndexPlannerBoundary`; `testing/go/index_test.go`: mixed-expression metadata regression | Metadata is preserved, but expression predicates intentionally scan until logical-key seeks exist. |
| Partial btree indexes | metadata support | `testing/go/index_test.go`: partial index metadata, `pg_index.indpred`, `pg_get_expr`, and `pg_get_indexdef` regressions | No partial-predicate planner or executor selectivity claim yet. |
| Btree `INCLUDE` indexes | metadata support | `testing/go/index_test.go`: include-column catalog, `indnkeyatts`, `pg_get_indexdef`, and validation regressions | No index-only scan or include-column storage performance claim yet. |
| Btree sort/null options | metadata support | `testing/go/index_test.go`: sort/null option `pg_get_indexdef` and `pg_index.indoption` regressions | Planner does not claim order satisfaction from these options. |
| Btree collations | metadata support | `testing/go/index_test.go`: collation metadata and unsupported-collation boundary; `testing/go/pgcatalog_test.go`: `pg_collation` rows | Text comparison still needs full PostgreSQL collation semantics before planner parity is claimed. |
| Btree opclasses/opfamilies | catalog and metadata support | `testing/go/index_test.go`: opclass round-tripping and type validation; `testing/go/pgcatalog_test.go`: `pg_opclass`, `pg_opfamily`, `pg_amop`, `pg_amproc`, `pg_operator` fixtures | Supported catalog rows do not by themselves prove every opclass-specific operator is planned with PostgreSQL semantics. |
| Btree `NULLS NOT DISTINCT` unique indexes | semantic support | `testing/go/index_test.go`: `PostgreSQL unique nulls not distinct`; `server/node/nulls_not_distinct_unique_table.go` | Expression `NULLS NOT DISTINCT` unique indexes are rejected explicitly. |
| Unique constraints and standalone unique indexes | semantic and catalog support | `testing/go/index_test.go`: generated/explicit names, `DROP INDEX` and `DROP CONSTRAINT` ownership boundaries; `testing/go/pgcatalog_test.go`: constraint/index catalog fixtures | Broader `ON CONFLICT` arbiter parity is bounded separately. |
| `ON CONFLICT` arbiter selection | explicit non-parity boundary | `testing/go/insert_test.go`: supported single-unique conflict target, bad-target rejection, multi-unique target rejection, arbiter-predicate rejection, and `DO UPDATE WHERE` rejection | Full PostgreSQL arbiter inference across multiple unique, expression, partial, opclass, and collation-qualified indexes is not claimed. |
| Basic `REINDEX INDEX` and `REINDEX TABLE` | semantic support | `testing/go/index_test.go`: reindex lifecycle boundary regressions | `REINDEX CONCURRENTLY` and broader PostgreSQL reindex options remain explicit boundaries. |
| Concurrent index lifecycle | explicit non-parity boundary | `testing/go/index_test.go`: exact unsupported-boundary regressions for `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, and `REINDEX ... CONCURRENTLY`, including no failed-CREATE index relation | PostgreSQL concurrent build/drop/reindex lock and progress semantics are not claimed. |
| JSONB GIN `jsonb_ops` lookups | performance support | `testing/go/index_test.go`: JSONB GIN DDL/DML/planner regressions; `testing/go/index_benchmark_test.go`: `BenchmarkJsonbGinSQLLookup`, `BenchmarkJsonbGinIndexBuild`, `BenchmarkJsonbGinDMLMaintenance` | Direct base-row fetch by posting identity and broader statistics/progress parity are still future work. |
| JSONB GIN `jsonb_path_ops` lookups | performance support | `testing/go/index_test.go`: path-opclass catalog and containment lookup regressions; `testing/go/index_benchmark_test.go`: path containment benchmark | Only the supported containment subset is claimed. |
| JSONPath GIN acceleration | explicit non-parity boundary | `testing/go/index_benchmark_test.go`: `TestJsonbGinJsonPathBoundary` | JSONPath operators and functions must not choose GIN until an indexable subset and recheck semantics are implemented. |
| Hash indexes | explicit non-parity boundary | `testing/go/index_test.go`: hash access-method boundary pins `pg_am` visibility, absent hash opclass/operator metadata, exact `CREATE INDEX USING hash` rejection, and no failed-DDL index relation | The `pg_am` row is catalog visibility only; no hash storage, write maintenance, planner, executor, or opclass support is claimed. |
| GiST indexes | explicit non-parity boundary | `testing/go/index_test.go`: GiST access-method boundary pins `pg_am` visibility, absent GiST opclass/operator metadata, exact `CREATE INDEX USING gist` rejection, and no failed-DDL index relation | The `pg_am` row is catalog visibility only; no GiST storage, write maintenance, planner, executor, or opclass support is claimed. |
| SP-GiST indexes | explicit non-parity boundary | `testing/go/index_test.go`: SP-GiST access-method boundary pins `pg_am` visibility, absent SP-GiST opclass/operator metadata, exact `CREATE INDEX USING spgist` rejection, and no failed-DDL index relation | The `pg_am` row is catalog visibility only; no SP-GiST storage, write maintenance, planner, executor, or opclass support is claimed. |
| BRIN indexes | explicit non-parity boundary | `testing/go/index_test.go`: BRIN access-method boundary pins `pg_am` visibility, absent BRIN opclass/operator metadata, exact `CREATE INDEX USING brin` rejection, and no failed-DDL index relation | The `pg_am` row is catalog visibility only; no BRIN storage, write maintenance, planner, executor, or opclass support is claimed. |
| Unsupported access methods and index options | explicit non-parity boundary | `testing/go/index_test.go`: unsupported method, opclass, collation, storage, tablespace, concurrent drop/reindex, and ALTER INDEX boundary regressions | Import-only no-op behavior must not be counted as index parity. |
| Index stats and progress views | metadata/catalog boundary | `testing/go/pgcatalog_test.go`: `pg_stat_*_indexes`, `pg_statio_*_indexes`, and `pg_stat_progress_create_index` catalog fixtures | Runtime statistics/progress semantics are not yet claimed. |

When closing future index beads, update the relevant row or add a new one with
the executable test, benchmark, or explicit boundary that proves the support
level being claimed.
