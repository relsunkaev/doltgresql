# Cooperative Test-Fix Log

Use this file to avoid overlapping work. Add short entries with:

- agent/name
- timestamp
- current lane
- files you expect to touch
- test status and next action

## Entries

### gamma - 2026-05-12 13:14 America/Phoenix

- Lane: establishing current `./testing/go` failure set.
- Baseline: plain `go test ./testing/go -count=1 -timeout=60m` failed at build because `go-icu-regex` could not find `unicode/regex.h`.
- Rerun: started full package with ICU env:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include`
  - `CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib`
  - `PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig`
- Current files: only `coop.md` so far.
- Next action: summarize failing tests from `/tmp/doltgresql-testing-go-20260512.jsonl`, pick one unclaimed failure, write red/fix/green, commit.

### beta - 2026-05-12 13:15 America/Phoenix

- Lane: access-method superuser privilege checks.
- Files touched: `server/node/access_method.go`.
- Red: `TestCreateAccessMethodRequiresSuperuserRepro` and `TestDropAccessMethodRequiresSuperuserRepro` both failed because non-superusers could mutate `pg_am`.
- Green: focused repros now pass, plus adjacent access-method definition checks:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(CreateAccessMethodPersistsPgAmRepro|DropAccessMethodIfExistsMissingNoopsRepro|CreateAccessMethodRequiresSuperuserRepro|DropAccessMethodRequiresSuperuserRepro)' -count=1 -v`
- Result: committed `47e1030f fix: require superuser for access methods`.
- Current action: avoiding full `./testing/go` while gamma owns the baseline run; will pick a non-overlapping lane after re-reading this file.

### alpha - 2026-05-12 13:15 America/Phoenix

- Lane: table/text-search DDL privilege failures.
- Red tests confirmed:
  - `TestCreateTextSearchConfigurationRequiresSchemaCreatePrivilegeRepro`
  - `TestCreatePolicyRequiresTableOwnershipRepro`
- Touching: `server/tablemetadata/table_metadata.go`, `server/node/create_table.go`, `server/analyzer/serial.go`, `server/tables/pgcatalog/pg_class.go`, `server/node/operator.go`, `server/node/text_search.go`, `server/node/row_security.go`, `server/node/table_ownership.go`, `server/auth/auth_handler.go`.
- Avoiding: `server/node/access_method.go` appears modified by another agent.
- Next action: finish owner metadata + schema CREATE checks, focused green, package green, commit.

### beta - 2026-05-12 13:16 America/Phoenix

- Lane: `CREATE RULE` table ownership check from commit `600677f3`.
- Expected files: `server/connection_handler.go` only.
- Avoiding: alpha's claimed files (`server/tablemetadata/table_metadata.go`, `server/node/create_table.go`, `server/tables/pgcatalog/pg_class.go`, `server/node/operator.go`, `server/node/text_search.go`, `server/node/row_security.go`, `server/node/table_ownership.go`, `server/auth/auth_handler.go`) and gamma's full-suite baseline.
- Red: `TestCreateRuleRequiresTableOwnershipRepro` failed because non-owners could create a rewrite rule.
- Green: rule ownership repro and adjacent rule rewrite checks pass:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(CreateRuleRequiresTableOwnershipRepro|CreateRuleDoAlsoExecutesAuditInsertRepro|DropRuleIfExistsMissingRepro|AlterRuleMissingReachesValidationRepro)' -count=1 -v`
- Result: committed `5fd97934 fix: require table owner for create rule`.
- Current action: re-reading `coop.md` and current worktree before selecting another non-overlapping lane.

### beta - 2026-05-12 13:18 America/Phoenix

- Lane: `DROP LANGUAGE` ownership check from commit `ffe986ef`.
- Expected files: `server/node/drop_language.go` only.
- Avoiding: dirty `testing/go/ddl_privilege_repro_test.go` and alpha's claimed table/text-search files.
- Red: `TestDropLanguageRequiresOwnershipRepro` failed because a non-owner could drop `plpgsql`.
- Green: drop-language ownership and adjacent language DDL checks pass:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(DropLanguageRequiresOwnershipRepro|AlterLanguageOwnerToRequiresOwnershipRepro|AlterLanguageOwnerUpdatesPgLanguageRepro|CreateLanguagePopulatesPgLanguageRepro|DropLanguageIfExistsMissingRepro)' -count=1 -v`
- Result: committed `b2f239ae fix: require owner for drop language`.
- Current action: checking whether gamma's baseline is available before selecting another lane.

### delta - 2026-05-12 13:18 America/Phoenix

- Lane: Alembic/SQLAlchemy startup failure on `SELECT pg_catalog.version()`, then `int2vector` `unnest(pg_index.indkey)` element typing.
- Baseline slice from gamma's jsonl: 79 top-level tests passed, 14 failed before the incomplete run stopped; first failure was `TestAlembicAutogenerate`.
- Expected files: `server/functions/framework/provider.go`, `server/functions/framework/compiled_function.go`, and focused catalog/vector probe tests.
- Avoiding: alpha's DDL ownership files, beta's `server/connection_handler.go`, gamma's full-suite baseline, and dirty peer-owned repro files.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestDumpVersionIdentity$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestPgIndexVectorSlices$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAlembicAutogenerate$' -count=1 -v`
- Extra green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestDumpVersionIdentity|TestPgIndexVectorSlices|TestAlembicAutogenerate)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestTypes$/Int2vector_type$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions/framework -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: committed `493c0c3d fix: resolve pg_catalog builtins and vector polymorphism`; leaving `coop.md` unstaged because it includes active peer edits.

### alpha - 2026-05-12 13:18 America/Phoenix

- Lane: `DROP LANGUAGE` ownership check from commit `ffe986ef`.
- Expected files: likely `server/node/drop_language.go`, maybe `server/auth/languages.go`.
- Avoiding: gamma's full-suite baseline, typed-table repro currently running, and beta's latest re-read lane.
- Red: `TestDropLanguageRequiresOwnershipRepro` failed because non-owners could drop `plpgsql`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'TestDropLanguageRequiresOwnershipRepro|TestDropLanguageIfExistsMissingRepro|TestAlterLanguageOwnerToRequiresOwnershipRepro|TestCreateLanguagePopulatesPgLanguageRepro' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./server/node -count=1`
- Result: beta committed the same fix first as `b2f239ae`; alpha dropped this lane without committing duplicate code.

### alpha - 2026-05-12 13:20 America/Phoenix

- Lane: `CREATE TABLE ... OF` schema privilege guard from commit `40874be2`.
- Expected files: `server/node/create_typed_table.go`.
- Avoiding: gamma's `server/connection_handler.go` ALTER SYSTEM lane and delta's version-function lane.
- Result: already green after `1ad1991c`; no code needed.

### alpha - 2026-05-12 13:21 America/Phoenix

- Lane: `pg_catalog.pg_get_keywords()` compatibility helper.
- Expected files: likely `server/functions/*keyword*` and `server/functions/init.go`.
- Avoiding: delta's version-function/probe files and any `connection_handler.go` lanes.
- Red: `TestPgGetKeywordsExposesReservedWordsRepro` failed because qualified `pg_catalog.pg_get_keywords()` was not registered as a table function.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'TestPgGetKeywordsExposesReservedWordsRepro' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./server/functions -count=1`
- Next action: commit `server/functions/pg_get_keywords.go`, `server/functions/init.go`, and `coop.md`.

### gamma - 2026-05-12 13:19 America/Phoenix

- Lane: `ALTER SYSTEM` transaction-block behavior.
- Red: `TestAlterSystemInsideTransactionRejectedRepro` fails because `ALTER SYSTEM SET/RESET` parse as syntax errors at `system`, not as PostgreSQL transaction-block errors.
- Expected files: `server/connection_handler.go`.
- Avoiding: alpha's ownership/table metadata files, beta/alpha `DROP LANGUAGE` files, and delta's version-function lane.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAlterSystemInsideTransactionRejectedRepro$' -count=1 -v`
- Result: focused repro passes; committing `server/connection_handler.go` only.

### beta - 2026-05-12 13:21 America/Phoenix

- Lane: `ANALYZE` table ownership / maintenance privilege check from `TestAnalyzeTableRequiresOwnershipRepro`.
- Expected files: `server/analyzer/apply_analyze_all_tables.go`, maybe `server/analyzer/init.go` if a separate validator rule is needed.
- Avoiding: alpha's table/text-search/ownership files, gamma's `server/connection_handler.go`, delta's version-function lane, and dirty test files from other agents.
- Red: `TestAnalyzeTableRequiresOwnershipRepro` failed because `ANALYZE analyze_private` succeeded for `analyze_intruder`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'TestAnalyzeTableRequiresOwnershipRepro|TestStats' -count=1 -v`
- Blocked broader check: `go test ./server/analyzer -count=1` currently fails in another dirty functions-framework lane with `undefined: isPolymorphicArrayInput`.
- Result: committed `05c45375 fix: require analyze table privileges`.
- Current action: re-reading `coop.md` and status before selecting another non-overlapping lane.

### gamma - 2026-05-12 13:22 America/Phoenix

- Lane: `CREATE/DROP INDEX CONCURRENTLY` transaction-block rejection.
- Red:
  - `TestCreateIndexConcurrentlyRejectsTransactionBlockRepro` fails because `CREATE INDEX CONCURRENTLY` succeeds inside `BEGIN` and leaves the index after `ROLLBACK`.
  - `TestDropIndexConcurrentlyRejectsTransactionBlockRepro` fails because `DROP INDEX CONCURRENTLY` succeeds inside `BEGIN`.
- Expected files: `server/connection_handler.go`; possibly a comment-only correction in `server/node/create_index_concurrently.go`.
- Avoiding: alpha's keyword/function lane, beta's analyzer lane, delta's version-function/probe lane, and dirty test files.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateIndexConcurrentlyRejectsTransactionBlockRepro|DropIndexConcurrentlyRejectsTransactionBlockRepro)$' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'TestCreateIndexConcurrently(CrossSessionVisibility|MetadataBackedCrossSessionVisibility)$' -count=1`
- Result: focused transaction-block repros and adjacent CONCURRENTLY visibility tests pass; committing gamma-owned files only.

### alpha - 2026-05-12 13:25 America/Phoenix

- Lane: `pg_catalog.pg_get_keywords()` compatibility helper.
- Result: implementation is ready to commit; `server/functions` package is green.
- Focused repro note: `TestPgGetKeywordsExposesReservedWordsRepro` was green before the current peer-owned `server/connection_handler.go` edit. The rerun now fails at build time because `server/connection_handler.go` references undefined `createIndexConcurrentlyPattern` / `dropIndexConcurrentlyPattern`.
- Current stats note: gamma's JSONL ended with 725/1285 top-level passing (56.4%), but this is not a valid full-suite snapshot because the package build is broken by the in-progress concurrent-index lane.

### beta - 2026-05-12 13:26 America/Phoenix

- Lane: `DROP TABLE` / `DROP VIEW` / `DROP MATERIALIZED VIEW` ownership checks from commits `c2142541`, `3e6cefeb`, and `a0893b67`.
- Expected files: `server/node/drop_table.go`, `server/node/drop_view.go`, `server/analyzer/replace_node.go`.
- Avoiding: gamma's `server/connection_handler.go` and `server/node/create_index_concurrently.go`, alpha/delta functions files, and dirty probe tests.
- Red: focused DROP ownership repros failed because non-owners could drop tables, views, and materialized views.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(DropTableRequiresOwnershipDespiteAllPrivilegesRepro|DropViewRequiresOwnershipRepro|DropViewRequiresOwnershipDespiteAllPrivilegesRepro|DropMaterializedViewRequiresOwnershipDespiteAllPrivilegesRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(DropViewAcceptsRestrict|DropMaterializedViewAcceptsRestrict|DynamicViewRebuild)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./server/node -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./server/analyzer -count=1`
- Result: committed `6eb884c0 fix: require owner for drop relation`.

### gamma - 2026-05-12 13:27 America/Phoenix

- Status: looking for next unclaimed lane.
- Blocker: fresh `go test ./testing/go -run '^TestClusterMarksIndexClusteredRepro$' -count=1 -v` cannot build while beta's `server/node/drop_table.go` edit is in progress:
  - `server/node/drop_table.go:66:16: undefined: err`
  - `server/node/drop_table.go:67:7: undefined: err`
  - `server/node/drop_table.go:68:17: undefined: err`
- Current action: not touching `server/node/drop_table.go`; waiting for beta to finish or selecting only non-build-dependent inspection work.

### alpha - 2026-05-12 13:27 America/Phoenix

- Status: backing off the `DROP TABLE` / `DROP VIEW` / `DROP MATERIALIZED VIEW` ownership lane because beta claimed and edited `server/node/drop_table.go`.
- Next lane: `ALTER FUNCTION` unsupported-feature repros, per user direction.
- Expected files: likely `server/ast/alter_function.go`, `server/node/alter_function.go`, and focused tests only if a new regression is needed.
- Avoiding: beta's `server/node/drop_table.go`, delta's `server/functions/framework/*` and probe tests, gamma's concurrent-index files.
- Blocker: focused `testing/go` runs will not build until beta fixes/commits `server/node/drop_table.go`.

### alpha - 2026-05-12 13:29 America/Phoenix

- Result: focused `ALTER FUNCTION` and `ALTER ROUTINE` repros are already green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'TestAlter(Function|Routine).*Repro|TestAlterFunction.*OptionRepro|TestAlterFunctionOwnerToRequiresOwnershipRepro|TestAlterFunctionOwnerUpdatesCatalogRepro|TestAlter$' -count=1 -v`
- New lane: stale `TestAlterStatements` smoke expectations for implemented ownership features.
- Expected files: `testing/go/alter_test.go` only.
- Red: `TestAlterStatements` now fails because `ALTER TYPE ... OWNER TO` and `ALTER PROCEDURE ... OWNER TO` are implemented and correctly reject missing role `foo`, while the smoke test still expects unsupported-warning stubs.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run '^TestAlterStatements$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'TestAlter(Function|Routine).*Repro|TestAlterFunction.*OptionRepro|TestAlterFunctionOwnerToRequiresOwnershipRepro|TestAlterFunctionOwnerUpdatesCatalogRepro' -count=1`
- Next action: commit only `testing/go/alter_test.go` and `coop.md`.

### gamma - 2026-05-12 13:29 America/Phoenix

- Lane: `CLUSTER index ON table` should mark `pg_index.indisclustered`.
- Red: `TestClusterMarksIndexClusteredRepro` fails because top-level `CLUSTER` parses as a syntax error and the catalog query returns no clustered index.
- Expected files: likely `server/connection_handler.go` for a narrow statement conversion, `server/node/*cluster*` or index metadata node code, `server/indexmetadata/index_metadata.go`, and `server/tables/pgcatalog/pg_index.go`.
- Avoiding: beta's `server/node/drop_table.go`, alpha's `ALTER FUNCTION` files, and delta's `server/functions/framework/*` lane.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestClusterMarksIndexClusteredRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateIndexConcurrentlyRejectsTransactionBlockRepro|DropIndexConcurrentlyRejectsTransactionBlockRepro|ClusterMarksIndexClusteredRepro|PartialUniqueIndexEnforcesPredicateRepro)$' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/indexmetadata ./server/node -count=1`
- Result: focused repro and adjacent index metadata checks pass; committing gamma-owned code files only because alpha currently has `server/node/drop_function.go` dirty.

### beta - 2026-05-12 13:32 America/Phoenix

- Lane: `DROP SEQUENCE` ownership checks from commit `2cf4bbde`.
- Expected files: `server/node/drop_sequence.go` only.
- Avoiding: dirty `server/node/drop_function.go`, gamma's CLUSTER/index files, and alpha's ALTER smoke-test lane.
- Next action: run focused red sequence ownership repros, add owner/superuser validation before `collection.DropSequence`, focused green, commit only beta-owned file.

### alpha - 2026-05-12 13:32 America/Phoenix

- Lane: `DROP FUNCTION` ownership checks from `a25ea4fc`.
- Expected files: `server/node/drop_function.go` only.
- Avoiding: beta's relation-drop files, gamma's CLUSTER files, and dirty `testing/go/ddl_privilege_repro_test.go`.
- Red:
  - `TestDropFunctionRequiresOwnershipRepro` lets non-owner drop the function.
  - `TestDropFunctionRequiresOwnershipDespiteAllPrivilegesRepro` lets grantee with ALL PRIVILEGES drop the function.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'Test(DropFunctionRequiresOwnershipRepro|DropFunctionRequiresOwnershipDespiteAllPrivilegesRepro)' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./server/node -count=1`
- Next action: commit only `server/node/drop_function.go` and `coop.md`.
