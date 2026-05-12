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
- Red: sequence ownership repros failed because non-owners could drop sequences, including after `GRANT ALL PRIVILEGES ON SEQUENCE`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(DropSequenceRequiresOwnershipRepro|DropSequenceRequiresOwnershipDespiteAllPrivilegesRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run '^TestSequences/(Basic_CREATE_SEQUENCE_and_DROP_SEQUENCE|DROP_SEQUENCE_IF_NOT_EXISTS)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./server/node -count=1`
- Note: full `TestSequences` still has an unrelated existing `pg_sequence` subtest failure.
- Result: committed `8a080451 fix: require owner for drop sequence`.

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

### alpha - 2026-05-12 13:37 America/Phoenix

- Lane: `DROP SCHEMA` ownership checks from commit `99458501`.
- Expected files: likely `server/node/drop_schema.go` and `server/analyzer/replace_node.go`; tests only if a focused regression gap appears.
- Avoiding: beta's `DROP DATABASE` files, delta's Alembic/concurrent-index files, and gamma's CLUSTER/index metadata files.
- Result: already green on current HEAD; no code change needed.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'Test(DropSchemaRequiresOwnershipGuard|DropSchemaRequiresOwnershipDespiteAllPrivilegesGuard)' -count=1 -v`

### alpha - 2026-05-12 13:39 America/Phoenix

- Lane: schema ownership metadata for `CREATE SCHEMA`, `CREATE SCHEMA AUTHORIZATION`, and `ALTER SCHEMA ... OWNER TO`.
- Expected files: `server/auth/*schema*`, `server/ast/create_schema.go`, `server/ast/alter_schema.go`, `server/node/alter_schema.go`, `server/tables/pgcatalog/pg_namespace.go`, and focused tests only if a regression gap appears.
- Avoiding: beta's `DROP DATABASE` files, delta's Alembic/concurrent-index files, gamma's CLUSTER/index metadata files, and broad table/view/sequence owner-validation cleanup.
- Red:
  - `TestCreateSchemaOwnerUpdatesCatalogRepro`
  - `TestSchemaOwnerCanUseCreatedSchemaRepro`
  - `TestCreateSchemaAuthorizationUpdatesCatalogRepro`
  - `TestCreateSchemaAuthorizationRequiresExistingRoleRepro`
  - `TestAlterSchemaOwnerUpdatesCatalogRepro`
  - `TestAlterSchemaOwnerCanUseTransferredSchemaRepro`
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run 'Test(CreateSchemaRequiresCreatePrivilegeGuard|CreateSchemaRejectsDoltReservedNamespaceRepro|DropSchemaRequiresOwnershipGuard|DropSchemaRequiresOwnershipDespiteAllPrivilegesGuard|CreateSchemaOwnerUpdatesCatalogRepro|SchemaOwnerCanUseCreatedSchemaRepro|CreateSchemaAuthorizationUpdatesCatalogRepro|CreateSchemaAuthorizationRequiresExistingRoleRepro|CreateSchemaAuthorizationRequiresTargetRoleMembershipRepro|AlterSchemaOwnerUpdatesCatalogRepro|AlterSchemaOwnerCanUseTransferredSchemaRepro)' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./server/auth ./server/node ./server/tables/pgcatalog ./server/ast -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test -vet=off ./testing/go -run '^TestSchemas$' -count=1 -v`
- Note: broad `TestAlterOwnerRequiresExistingRoleRepro` still has table/view/sequence owner-validation failures outside this schema lane.
- Next action: commit alpha-owned schema files only.

### delta - 2026-05-12 13:35 America/Phoenix

- Lane: `TestAlembicConcurrentlyMigration` after concurrent-index transaction-block guard.
- Expected files: `testing/go/alembic_concurrently_test.go` only; the product guard is correct, and the stale fixture needed Alembic's supported autocommit path.
- Avoiding: alpha's `server/node/drop_function.go`, beta's drop ownership files, dirty `testing/go/schema_privilege_guard_test.go`, and gamma's committed CLUSTER/index metadata files unless an adjacent check requires them.
- Red: current HEAD fails because Alembic emits `CREATE INDEX CONCURRENTLY ix_alembic_t_code ON alembic_t (code)` through psycopg and Doltgres reports `CREATE INDEX CONCURRENTLY cannot run inside a transaction block`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAlembicConcurrentlyMigration$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateIndexConcurrentlyRejectsTransactionBlockRepro|DropIndexConcurrentlyRejectsTransactionBlockRepro|CreateIndexConcurrently(CrossSessionVisibility|MetadataBackedCrossSessionVisibility)|AlembicAutogenerate)$' -count=1 -v`
- Result: committed `a6af271a test: use alembic autocommit for concurrent indexes`; left `coop.md` uncommitted due active peer edits.

### beta - 2026-05-12 13:35 America/Phoenix

- Lane: `DROP DATABASE` ownership checks from commit `6a010df4`.
- Expected files: `server/node/drop_database.go`, `server/analyzer/replace_node.go`.
- Avoiding: dirty `testing/go/schema_privilege_guard_test.go`, alpha's `server/node/drop_function.go`, and any CLUSTER/index files.
- Next action: run focused red database ownership repros, wrap `plan.DropDB` with owner/superuser validation, focused green, commit only beta-owned files.

### beta - 2026-05-12 13:36 America/Phoenix

- Status: continuing `DROP DATABASE` ownership lane after rechecking status.
- Current dirty file from another lane: `testing/go/ddl_privilege_repro_test.go`; beta will not touch it.
- Edit scope remains `server/node/drop_database.go` and `server/analyzer/replace_node.go` only.

### beta - 2026-05-12 13:38 America/Phoenix

- Result: committed `fe303356 fix: require owner for drop database`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(DropDatabaseRequiresOwnershipRepro|DropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./testing/go -run 'Test(DropDatabaseWithForceDropsIdleDatabase|DropDatabaseWithForceIfExistsNoopsMissingDatabase|DropDatabase)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./server/node -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib go test ./server/analyzer -count=1`

### delta - 2026-05-12 13:41 America/Phoenix

- Lane: `ALTER TYPE ... RENAME TO` and `ALTER DOMAIN ... RENAME TO` metadata lookup from `TestAlterTypeAndDomainRenameToRepro`.
- Expected files: `server/ast/alter_type.go`, `server/ast/alter_domain.go`, and a narrow `server/node/alter_type_rename.go`.
- Avoiding: alpha's schema ownership files/tests and dirty `testing/go/ddl_privilege_repro_test.go`; not taking enum-label rename because stored enum values need separate read/data handling.
- Red: current HEAD rejects `ALTER TYPE rename_enum_object RENAME TO renamed_enum_object` and `ALTER DOMAIN rename_domain_object RENAME TO renamed_domain_object` as unsupported.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAlterTypeAndDomainRenameToRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestAlterTypeAndDomainRenameToRepro|TestAlterCompositeTypeRenameAttribute|TestAlterCompositeTypeRenameAttributeMultipleFields|TestAlterCompositeTypeRenameAttributeMissingErrors|TestAlterCompositeTypeRenameAttributeCollisionErrors|TestAlterCompositeTypeRenameAttributeNonComposite|TestAlterCompositeTypeRenameAttributeSchemaQualified|TestAlterFunctionOwnerToRequiresOwnershipRepro|TestAlterFunctionOwnerUpdatesCatalogRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/ast ./server/node -count=1`
- Result: committed `d66f4f71 fix: support renaming user types and domains`; left `coop.md` uncommitted due active peer edits.

### beta - 2026-05-12 13:39 America/Phoenix

- Lane: `CLUSTER index ON table` table-ownership guard from commit `a5f5e8cd`.
- Expected files: `server/node/cluster_index.go` only.
- Avoiding: active schema-owner auth/catalog files, dirty `testing/go/ddl_privilege_repro_test.go`, and text-search/operator schema-qualified lanes.
- Next action: run focused red `TestClusterRequiresTableOwnershipRepro`, add table ownership validation before marking the index clustered, focused green, commit only beta-owned file.

### beta - 2026-05-12 13:40 America/Phoenix

- Red: `TestClusterRequiresTableOwnershipRepro` failed as expected: non-owner `CLUSTER cluster_private_label_idx ON cluster_private` returned nil error and flipped `pg_index.indisclustered` to `t`.
- Edit: added table ownership validation in `server/node/cluster_index.go` before index metadata mutation.
- Blocker: current shared worktree cannot build focused tests because alpha's in-progress schema-owner files have compile errors:
  - `server/node/create_schema.go:94:15: sdb.CreateSchema undefined`
  - `server/node/create_schema.go:155:6: currentSchemaDatabase redeclared`
  - `server/node/alter_schema.go` field/method `Schema` conflicts and uses the method where a string is expected.
- Next action: verify beta's cluster edit in an isolated clean worktree; leave alpha's dirty files untouched.

### beta - 2026-05-12 13:45 America/Phoenix

- Result: committed `9a078850 fix: require table owner for cluster`.
- Verification was run in clean worktree `/tmp/doltgresql-beta-cluster-verify.viv9kl` because the shared worktree was blocked by alpha's in-progress schema-owner compile errors.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestClusterRequiresTableOwnershipRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestClusterMarksIndexClusteredRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -count=1`
- Rechecked in shared worktree after alpha's schema-owner dirt cleared:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(ClusterRequiresTableOwnershipRepro|ClusterMarksIndexClusteredRepro)$' -count=1 -v`

### gamma - 2026-05-12 13:41 America/Phoenix

- Lane: `age(timestamp, timestamp)` calendar-month day borrowing.
- Red: `TestAgeUsesCalendarMonthBorrowingRepro` fails because `age('2001-02-28','2001-01-31')` returns `27 days` instead of `28 days`, and `age('2004-03-01','2004-01-31')` returns `1 mon` instead of `1 mon 1 day`.
- Expected files: `server/functions/age.go`.
- Avoiding: alpha's schema ownership files, beta's DROP DATABASE files, delta's Alembic fixture, and broad ownership tests.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAgeUsesCalendarMonthBorrowingRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestFunctions/age$' -count=1 -v` (no direct subtest match, but the existing function smoke passed)
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -run 'Age|age' -count=1`
- Result: focused repro passes; committing `server/functions/age.go` only because alpha currently owns dirty schema metadata files.

### gamma - 2026-05-12 13:43 America/Phoenix

- Lane: interval quarter extraction for `extract` and `date_part`.
- Red: stale full-suite discovery shows `TestExtractIntervalQuarterUsesPostgresMonthBucketRepro` and `TestDatePartIntervalQuarterUsesPostgresMonthBucketRepro` failing because interval `3 months` reports quarter `1` instead of PostgreSQL's `2`.
- Expected files: `server/functions/extract.go`, `server/functions/date_part.go`.
- Avoiding: alpha's schema owner files, delta's alter type/domain rename files, beta's cluster ownership guard, and ownership DDL tests.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(ExtractIntervalQuarterUsesPostgresMonthBucketRepro|DatePartIntervalQuarterUsesPostgresMonthBucketRepro)$' -count=1 -v`
- Result: focused repros pass; committing gamma-owned function files only.

### gamma - 2026-05-12 13:45 America/Phoenix

- Lane: qualified schema routing for `CREATE OPERATOR` and `CREATE TEXT SEARCH CONFIGURATION`.
- Red: `TestCreateOperatorUsesQualifiedSchemaRepro` creates operator name `operator_private.===` in `public`, and `TestCreateTextSearchConfigurationUsesQualifiedSchemaRepro` creates `qualified_ts_config` in `public`; both should check CREATE on the qualified schema and leave no object behind.
- Expected files: `server/connection_handler.go`, `server/node/operator.go`, `server/node/text_search.go`.
- Avoiding: alpha's schema owner files, beta's `server/node/cluster_index.go`, and delta's alter type/domain files.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateOperatorUsesQualifiedSchemaRepro|CreateTextSearchConfigurationUsesQualifiedSchemaRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateOperatorRequiresSchemaCreatePrivilegeRepro|CreateOperatorRequiresFunctionExecutePrivilegeRepro|CreateOperatorUsesQualifiedSchemaRepro|CreateTextSearchConfigurationRequiresSchemaCreatePrivilegeRepro|CreateTextSearchConfigurationUsesQualifiedSchemaRepro)$' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -run 'CreateOperator|TextSearch' -count=1`
- Result: focused and adjacent privilege checks pass; committing gamma-owned code files only.

### alpha - 2026-05-12 13:48 America/Phoenix

- Lane: `ALTER DATABASE` support for rename/catalog options/settings.
- Expected files: likely `server/ast/alter_database.go`, `server/ast/rename_database.go`, `server/ast/create_database.go`, plus narrow auth/catalog support if required by focused repros.
- Avoiding: uncommitted `testing/go/alter_type_rename_repro_test.go` materialized-view repro and any peer-owned alter-type materialized-view work.
- Starting stats from latest completed full-suite snapshot: `1367/2593` passing (`52.7%`), `1226` failing; +59 passing vs prior `1308/2568`.
- Next action: run focused `ALTER DATABASE` repros red, inspect database metadata patterns, implement the smallest catalog-backed behavior, then focused green and commit.

### beta - 2026-05-12 13:48 America/Phoenix

- Lane: `ALTER TYPE ... RENAME TO` view metadata from dirty `TestAlterTypeRenameUpdatesViewMetadataRepro`.
- Expected files: likely `server/node/alter_type_rename.go` and the already-dirty `testing/go/alter_type_rename_repro_test.go`; may inspect view storage helpers before editing.
- Avoiding: no active source dirt besides this lane; will not touch `coop.md` in commits unless explicitly needed.
- Baseline manifest note: `/tmp/doltgresql-testing-go-20260512.jsonl` is stale after many commits and had `8163/10034` pass events, `1871` fail events. Next full manifest refresh is due after this focused lane.
- Next action: run focused red, implement root cause using existing type-rename metadata patterns, focused green, then regenerate manifest.

### gamma - 2026-05-12 13:50 America/Phoenix

- Lane: calendar-month interval arithmetic for date/timestamp/timestamptz plus/minus interval.
- Expected files: `server/functions/binary/plus.go`, `server/functions/binary/minus.go`.
- Avoiding: beta's dirty `testing/go/alter_type_rename_repro_test.go` / alter-type metadata lane and alpha's `ALTER DATABASE` lane.
- Red:
  - `TestDatePlusIntervalUsesCalendarMonthsRepro`: `date '2025-01-31' + interval '1 month'` returned `2025-03-02 00:00:00` instead of `2025-02-28 00:00:00`.
  - `TestIntervalPlusDateUsesCalendarMonthsRepro`: same fixed 30-day result.
  - `TestIntervalPlusTimestampUsesCalendarMonthsRepro`: same fixed 30-day result with timestamp.
  - `TestIntervalPlusTimestamptzUsesCalendarMonthsRepro`: epoch was `1740916800` instead of `1740744000`.
  - `TestDateMinusIntervalUsesCalendarMonthsRepro`: `date '2025-03-31' - interval '1 month'` returned `2025-03-01 00:00:00` instead of `2025-02-28 00:00:00`.
- Next action: use existing `duration.Add` helper for calendar-aware interval application, run focused green, adjacent binary function tests, then commit gamma-owned files only.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(DatePlusIntervalUsesCalendarMonthsRepro|IntervalPlusDateUsesCalendarMonthsRepro|IntervalPlusTimestampUsesCalendarMonthsRepro|IntervalPlusTimestamptzUsesCalendarMonthsRepro|DateMinusIntervalUsesCalendarMonthsRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'CalendarMonthsRepro$' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions/binary -count=1`
- Result: focused calendar arithmetic repros pass; committing gamma-owned binary operator files only.

### beta - 2026-05-12 13:55 America/Phoenix

- Resync: the view and materialized-view alter-type rename repros are now committed (`d282397e`, `3f35d476`), so beta will handle both as one catalog-metadata lane unless a later coop note claims the materialized-view implementation.
- Expected source files: `server/node/alter_type_rename.go`; possibly a small helper in the same package if the view rewrite needs to stay readable.
- Avoiding: alpha's `ALTER DATABASE` lane and gamma's interval arithmetic files.
- Next action: run focused red over column/function/view/materialized-view rename metadata tests, implement the smallest metadata update, then report current pass/fail absolute counts from a refreshed `./testing/go` manifest.

### gamma - 2026-05-12 13:55 America/Phoenix

- Lane: negative interval field normalization for `extract` and `date_part`.
- Expected files: `server/functions/extract.go`, `server/functions/date_part.go`.
- Avoiding: beta's alter-type metadata lane, alpha's `ALTER DATABASE` lane, and any dirty `testing/go/alter_type_rename_repro_test.go` changes.
- Red:
  - `TestExtractNegativeIntervalFieldsUsePostgresNormalizationRepro`: hour returned `-2` instead of `-1`, minute `-5` instead of `-4`, year `-2` instead of `-1`, decade `-1` instead of `0`.
  - `TestDatePartNegativeIntervalFieldsUsePostgresNormalizationRepro`: same floor-vs-truncation failures.
- Next action: replace interval floor division with truncated component extraction while preserving existing positive interval behavior, then focused green and commit gamma-owned files only.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(ExtractNegativeIntervalFieldsUsePostgresNormalizationRepro|DatePartNegativeIntervalFieldsUsePostgresNormalizationRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused negative interval normalization repros pass; committing gamma-owned function files only.

### gamma - 2026-05-12 13:58 America/Phoenix

- Lane: fractional Julian day for timestamp/timestamptz `extract` and `date_part`.
- Expected files: `server/functions/extract.go`.
- Avoiding: alpha's dirty `ALTER DATABASE` files and beta's dirty alter-type metadata files.
- Verification setup: shared worktree is currently build-blocked by alpha's `server/ast/alter_database.go` import of missing `postgres/parser/config`, so focused red/green runs are in detached clean worktree `/tmp/doltgresql-gamma-julian.hzKcpl` with the local generated parser files and local `third_party/dolt` checkout linked/copied for build parity.
- Red:
  - `TestExtractTimestampJulianIncludesFractionalDayRepro`: timestamp and timestamptz returned `2451959` instead of `2451959.86018518518518518519`.
  - `TestDatePartTimestampJulianIncludesFractionalDayRepro`: timestamp and timestamptz returned `2451959` instead of `2451959.860185185`.
- Next action: compute Julian values with fractional time-of-day for non-midnight times, preserve integer midnight/date behavior, run focused green in the clean verifier, then commit only `server/functions/extract.go`.
- Green in `/tmp/doltgresql-gamma-julian.hzKcpl`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(ExtractTimestampJulianIncludesFractionalDayRepro|DatePartTimestampJulianIncludesFractionalDayRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused Julian timestamp/timestamptz repros pass in the clean verifier; committing gamma-owned `server/functions/extract.go` only.

### gamma - 2026-05-12 14:00 America/Phoenix

- Lane: timestamptz timezone fields for `extract` and `date_part` should use the active session time zone.
- Expected files: `server/functions/extract.go`, `server/functions/date_part.go`.
- Avoiding: alpha's dirty `ALTER DATABASE` files and beta's dirty alter-type metadata files.
- Verification setup: same clean verifier `/tmp/doltgresql-gamma-julian.hzKcpl` because the shared worktree still fails `go test ./testing/go -run '^$'` on the in-progress `server/ast/alter_database.go` import.
- Red:
  - `TestExtractTimestamptzTimezoneUsesSessionTimeZoneRepro`: with `SET TIME ZONE 'UTC'`, timezone returned `-28800` and timezone_hour `-8` instead of `0`.
  - `TestDatePartTimestamptzTimezoneUsesSessionTimeZoneRepro`: same hardcoded Pacific offset.
- Next action: derive timezone/timezone_hour/timezone_minute from the converted timestamptz location offset, run focused green and `./server/functions` in the clean verifier, then commit gamma-owned files only.
- Green in `/tmp/doltgresql-gamma-julian.hzKcpl`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(ExtractTimestamptzTimezoneUsesSessionTimeZoneRepro|DatePartTimestamptzTimezoneUsesSessionTimeZoneRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused timezone-field repros pass in the clean verifier; committing gamma-owned function files only.

### gamma - 2026-05-12 14:01 America/Phoenix

- Lane: `timezone(text, timestamptz)` sign and `timezone(text, timestamp)` named-zone wall-time offsets.
- Expected files: `server/functions/timezone.go`.
- Avoiding: alpha's dirty `ALTER DATABASE` files and beta's dirty alter-type metadata files.
- Verification setup: clean verifier `/tmp/doltgresql-gamma-julian.hzKcpl`.
- Red:
  - `TestTimezoneTextTimestamptzUsesTargetOffsetRepro`: New York conversion returned `16:00` instead of `08:00`, and fixed offset `-04:45` returned `20:53:40.12` instead of `06:23:40.12`.
  - `TestTimezoneTextTimestampUsesWallTimeOffsetRepro`: named-zone timestamp wall time chose the wrong DST offset around both spring-forward and fall-back boundaries.
- Next action: fix the timestamptz sign and construct named-zone timestamp values as wall time in that location, then focused green and `./server/functions` in the clean verifier.
- Green in `/tmp/doltgresql-gamma-julian.hzKcpl`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(TimezoneTextTimestamptzUsesTargetOffsetRepro|TimezoneTextTimestampUsesWallTimeOffsetRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused timezone conversion repros pass in the clean verifier; committing gamma-owned `server/functions/timezone.go` only.

### beta - 2026-05-12 13:58 America/Phoenix

- Resync: `server/node/alter_type_rename.go` already had uncommitted column/function dependency handling when beta rechecked. Focused results on current shared dirt:
  - PASS: `TestAlterTypeRenameUpdatesExistingColumnMetadataRepro`
  - PASS: `TestAlterTypeRenameUpdatesFunctionSignatureMetadataRepro`
  - FAIL: `TestAlterTypeRenameUpdatesColumnDefaultMetadataRepro` because the stored default still casts to the old type.
  - FAIL: `TestAlterTypeRenameUpdatesViewMetadataRepro` because stored view SQL still casts to the old type.
  - FAIL: `TestAlterTypeRenameUpdatesMaterializedViewMetadataRepro` refresh because column rewrite drops materialized-view comment metadata and the stored refresh definition still casts to the old type.
- Beta is extending the existing dirty alter-type file rather than replacing it. Expected beta additions: default SQL rewrite, view SQL rewrite, and materialized-view comment preservation/rewrite.

### delta - 2026-05-12 14:12 America/Phoenix

- Lane: finish alter-type rename metadata fallout after beta helper was present in the shared tree.
- Source changes: `server/node/alter_type_rename.go`, `server/node/alter_type_rename_attribute.go`, `server/node/alter_type_rename_sql_rewrite.go`, `server/functions/regprocedure.go`, `postgres/parser/sem/tree/create_function.go`.
- Green in clean verifier `/tmp/doltgresql-delta-type-rename.giG5ar`:
  - `go test ./testing/go -run '^(TestAlterCompositeTypeRenameAttribute|TestAlterTypeAndDomainRenameToRepro|TestAlterTypeRenameUpdates)' -count=1`
  - `go test ./server/node ./server/functions -count=1`
- Note: shared checkout still has unrelated `ALTER DATABASE`/regdatabase dirt, so delta did not use the shared tree as the verifier.

### gamma - 2026-05-12 14:04 America/Phoenix

- Lane: `date_trunc(text, timestamptz, text)` should use the named zone's offset at the truncated wall time.
- Expected files: `server/functions/date_trunc.go`.
- Avoiding: alpha's dirty `ALTER DATABASE` files and beta's dirty alter-type metadata files.
- Verification setup: clean verifier `/tmp/doltgresql-gamma-julian.hzKcpl`.
- Red: `TestDateTruncTimestamptzNamedZoneUsesTruncatedOffsetRepro` returned epochs `1615694400` and `1636261200` instead of `1615698000` and `1636257600`, because the implementation truncates in a fixed offset from the input instant.
- Next action: use the parsed timezone location from `convertTzToOffsetSecs` instead of building a fixed-offset location, then focused green and `./server/functions` in the clean verifier.
- Green in `/tmp/doltgresql-gamma-julian.hzKcpl`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestDateTruncTimestamptzNamedZoneUsesTruncatedOffsetRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused `date_trunc` named-zone repro passes in the clean verifier; committing gamma-owned `server/functions/date_trunc.go` only.

### gamma - 2026-05-12 14:10 America/Phoenix

- Lane: `date_bin` should support timestamps outside Go's Unix nanosecond epoch window.
- Expected files: `server/functions/date_bin.go`.
- Avoiding: alpha's dirty `ALTER DATABASE` files and beta's dirty alter-type metadata files.
- Verification setup: clean verifier `/tmp/doltgresql-gamma-julian.hzKcpl`.
- Red: `TestDateBinUsesPostgresTimestampRangeRepro` binned 1500-era timestamps into 2084 and 2300-era timestamps into 1715 because `UnixNano()` overflowed.
- Next action: compute bin offsets relative to `origin` using `timestamp.Sub(origin)` and `origin.Add(...)`, then focused green and `./server/functions` in the clean verifier.
- Green in `/tmp/doltgresql-gamma-julian.hzKcpl`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestDateBinUsesPostgresTimestampRangeRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused `date_bin` timestamp-range repro passes in the clean verifier; committing gamma-owned `server/functions/date_bin.go` only.

### beta - 2026-05-12 14:03 America/Phoenix

- Resync: current shared worktree has new gamma commits and alpha still has dirty `ALTER DATABASE` files, including `server/ast/create_database.go`, so beta focused verification remains in `/tmp/doltgresql-beta-alter-type-verify`.
- Continuing beta-owned files only: `server/node/alter_type_rename.go` and `server/node/alter_type_rename_sql_rewrite.go`.
- Next fix: correct the type-reference SQL rewrite change detection in `rewriteExprTypeReferences`; focused rerun will cover column default, view, materialized-view, plus already-passing column/function metadata repros.

### beta - 2026-05-12 14:15 America/Phoenix

- Focused verifier status: 7/8 ALTER TYPE rename metadata repros pass in `/tmp/doltgresql-beta-alter-type-verify`.
- Remaining beta failure: `TestAlterTypeRenameUpdatesExpressionIndexMetadataRepro` now rewrites the generated expression to the new type name, but index key maintenance reparses the hidden virtual expression and reaches `ExplicitCast.Eval` with an unresolved `renamed_index_status` cast target.
- Expanding beta-owned source by one root-cause file: `server/expression/explicit_cast.go`, to lazily resolve unresolved persisted cast targets at evaluation time. Still avoiding alpha `ALTER DATABASE` files and gamma date/time files.

### alpha - 2026-05-12 14:16 America/Phoenix

- Lane: ALTER DATABASE / CREATE DATABASE catalog metadata plus regdatabase support.
- Green focused repro group in shared worktree:
  - `go test -vet=off ./testing/go -run 'Test(AlterDatabaseRenameRepro|AlterDatabaseCatalogOptionsRepro|AlterDatabaseSetPopulatesPgDbRoleSettingRepro|AlterDatabaseResetSettingRepro|CreateDatabaseCatalogOptionsRepro|CreateDatabaseOwnerUpdatesCatalogRepro|CreateDatabaseOwnerRequiresExistingRoleRepro|AlterDatabaseOwnerUpdatesCatalogRepro|AlterDatabaseOwnerToRequiresOwnershipRepro|DropDatabaseRequiresOwnershipRepro|DropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro)$' -count=1 -v`
  - `go test -vet=off ./server/auth ./server/node ./server/tables/pgcatalog ./server/ast ./server/types ./server/functions ./server/cast ./server/expression -count=1`
- Nested Dolt provider rename support committed in `third_party/dolt`: `a3cf2c7a38 fix: support logical database rename`.
- Staging note: alpha touched only the `regdatabase` id-backed whitelist hunk in `server/expression/explicit_cast.go`; beta owns the runtime unresolved-cast work in that same file.

### gamma - 2026-05-12 14:16 America/Phoenix

- Lane: `xid` input validation should reject out-of-range and non-numeric values instead of silently wrapping/coercing.
- Expected files: `server/functions/xid.go`.
- Avoiding: alpha's dirty `ALTER DATABASE`/regdatabase files, beta's `explicit_cast.go` alter-type lane, dirty `server/types/type.go`, and dirty cast registry files.
- Red in clean verifier `/tmp/doltgresql-gamma-current.A6d5ZQ`:
  - `TestXidRejectsInvalidInputRepro` accepted `'4294967296'`, `'-1'`, and `'abc'` instead of returning PostgreSQL-style out-of-range / invalid-syntax errors.
- Next action: align `xidin` with the existing `oid`/`xid8` parsing patterns, run the focused repro plus `./server/functions`, then commit only `server/functions/xid.go`.
- Green in `/tmp/doltgresql-gamma-current.A6d5ZQ`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestXidRejectsInvalidInputRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions -count=1`
- Result: focused `xid` invalid-input repro passes; committing gamma-owned `server/functions/xid.go` only.

### gamma - 2026-05-12 14:19 America/Phoenix

- Lane: unsupported `CREATE AGGREGATE` forms should fail with the pinned unsupported-DDL contract instead of incidental transition-function lookup errors.
- Expected files: `server/node/create_aggregate.go`.
- Avoiding: beta's dirty `server/expression/explicit_cast.go`, alpha/regdatabase files, and type/cast registry files.
- Red in committed-HEAD verifier `/tmp/doltgresql-gamma-head.WIb56Y`:
  - `TestUnsupportedDdlProbes/CREATE_AGGREGATE_is_rejected` returned `function "int4pl" does not exist` instead of `CREATE AGGREGATE is not yet supported`.
- Next action: keep SQL transition-function aggregates working, but route missing/non-SQL transition implementations through the stable unsupported-DDL message; then run the unsupported probe plus aggregate correctness coverage.

### beta - 2026-05-12 14:21 America/Phoenix

- Lane: finish `ALTER TYPE ... RENAME TO` expression-index metadata fallout.
- Source touched: `server/expression/explicit_cast.go`.
- Avoiding: gamma's dirty `server/node/create_aggregate.go`; leaving peer-owned `coop.md` edits unstaged.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestAlterTypeAndDomainRenameToRepro|TestAlterCompositeTypeRenameAttribute|TestAlterTypeRenameUpdates)' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/expression -count=1`
- Next action: commit only `server/expression/explicit_cast.go`, then do a short status handoff.
- Green in `/tmp/doltgresql-gamma-head.WIb56Y`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestUnsupportedDdlProbes$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestUnsupportedDdlProbes|TestCreateAggregateSqlTransitionFunctionRepro|TestCreateAggregatePgAggregateCatalogRowRepro|TestDropAggregateRemovesUserAggregateRepro|TestAlterAggregateRenameRepro|TestCreateAggregateSqlTransitionFunctionEdges|TestCommentOnAggregatePersistsDescriptionRepro|TestCreateAggregateRequiresSchemaCreatePrivilegeGuard)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -count=1`
- Result: unsupported `CREATE AGGREGATE` probe passes while SQL-transition aggregate coverage remains green; committing gamma-owned `server/node/create_aggregate.go` only.

### gamma - 2026-05-12 14:21 America/Phoenix

- Lane: aggregate ownership checks for DROP/ALTER/CREATE OR REPLACE.
- Expected files: `server/node/drop_aggregate.go`, `server/node/alter_aggregate.go`, `server/node/create_aggregate.go`.
- Avoiding: beta/alpha shared `server/expression/explicit_cast.go`.
- Red in `/tmp/doltgresql-gamma-head.WIb56Y`: non-owner DROP, ALTER OWNER/RENAME/SET SCHEMA, and CREATE OR REPLACE AGGREGATE were accepted; replacement also changed owner.

### beta - 2026-05-12 14:26 America/Phoenix

- Lane: column DEFAULT expression validation for column references and non-scalar expressions.
- Source touched: `server/analyzer/validate_column_defaults.go`.
- Avoiding: gamma's dirty aggregate ownership lane in `server/node/alter_aggregate.go`.
- Red: `TestCreateTableDefaultRejectsColumnReferencesRepro` and `TestAlterColumnDefaultRejectsColumnReferencesRepro` accepted column references in DEFAULT expressions.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateTableDefaultRejectsColumnReferencesRepro|AlterColumnDefaultRejectsColumnReferencesRepro|DefaultExpressionsRejectNonScalarExpressionsRepro|AddColumnVolatileDefaultBackfillsEachExistingRowGuard|GeneratedColumnsProbe|GeneratedColumnRejects(SelfReference|GeneratedColumnReference|AggregateExpressions|WindowExpressions|SetReturningExpressions)Repro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/analyzer -count=1`
- Progress note: latest valid full baseline remains the 1367/2593 snapshot; this lane fixes at least 2 additional top-level manifest failures, but a fresh complete manifest is still needed.
- Green in `/tmp/doltgresql-gamma-head.WIb56Y`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestDropAggregateRequiresOwnershipRepro|TestCreateOrReplaceAggregateRequiresOwnershipRepro|TestAlterAggregateOwnerToRequiresOwnershipRepro|TestAlterAggregateRenameRequiresOwnershipRepro|TestAlterAggregateSetSchemaRequiresOwnershipRepro|TestCreateOrReplaceAggregatePreservesOwnerRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestCreateAggregateSqlTransitionFunctionRepro|TestCreateAggregatePgAggregateCatalogRowRepro|TestDropAggregateRemovesUserAggregateRepro|TestAlterAggregateRenameRepro|TestCreateAggregateSqlTransitionFunctionEdges|TestCommentOnAggregatePersistsDescriptionRepro|TestCreateAggregateRequiresSchemaCreatePrivilegeGuard|TestDropAggregateRequiresOwnershipRepro|TestCreateOrReplaceAggregateRequiresOwnershipRepro|TestAlterAggregateOwnerToRequiresOwnershipRepro|TestAlterAggregateRenameRequiresOwnershipRepro|TestAlterAggregateSetSchemaRequiresOwnershipRepro|TestCreateOrReplaceAggregatePreservesOwnerRepro)$' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -count=1`
- Result: aggregate mutation now requires owner/superuser and CREATE OR REPLACE preserves the existing owner.

### gamma - 2026-05-12 14:25 America/Phoenix

- Lane: `ORDER BY xid` should reject because xid has no ordering operator.
- Expected files: `server/types/type.go`.
- Avoiding: alpha's role-setting parser/ast/node files.
- Red in `/tmp/doltgresql-gamma-head.WIb56Y`: `TestXidOrderingRequiresOrderingOperatorRepro` sorted xid rows instead of erroring.
- Implementation note: initial `DoltgresType.Compare` guard did not fire because this sort path is planned before that compare function is used; moved the check to an analyzer rule over `plan.Sort`/`plan.TopN` fields.
- Green in `/tmp/doltgresql-gamma-head.WIb56Y`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestXidOrderingRequiresOrderingOperatorRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/analyzer ./server/types -count=1`

### alpha - 2026-05-12 14:22 America/Phoenix

- Lane: `ALTER ROLE ... SET/RESET` and `ALTER ROLE ... IN DATABASE ... SET` should persist/remove rows in `pg_db_role_setting`.
- Expected files: `postgres/parser/sem/tree/alter_role.go`, `server/ast/alter_role.go`, `server/node/alter_role.go`, possibly auth metadata helpers already introduced for `pg_db_role_setting`.
- Avoiding: gamma aggregate files and beta shared `server/expression/explicit_cast.go`.
- Next action: run the three focused role-setting repros red, then route role setting clauses into auth `DbRoleSettings`.

### beta - 2026-05-12 14:31 America/Phoenix

- Lane: index DDL table-ownership checks.
- Source touched: `server/node/index_ddl.go`.
- Avoiding: alpha's dirty role-setting parser/ast/node files and gamma's dirty `server/types/type.go`.
- Verification was run in temp worktree `/tmp/doltgresql-beta-index-verify.BaQNPk` because the shared tree is build-blocked by peer dirt.
- Red: non-owners could `DROP INDEX`, `ALTER INDEX SET (...)`, and `ALTER INDEX RENAME`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(DropIndexRequiresTableOwnershipRepro|AlterIndexRequiresTableOwnershipRepro|RenameIndexRequiresTableOwnershipRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(DropIndexRestrictCascade|DropIndexMultipleTargets|RenameIndex|IndexAlterStorage|IndexAlterStatistics|AlterIndexRequiresTableOwnershipRepro|RenameIndexRequiresTableOwnershipRepro|DropIndexRequiresTableOwnershipRepro)' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -count=1`
- Progress note: latest valid full baseline remains 1367/2593 passing; this lane fixes at least 2 more top-level manifest failures plus the already-pinned DROP INDEX ownership repro.

### alpha - 2026-05-12 14:28 America/Phoenix

- Green focused role-setting lane:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run 'Test(AlterRoleSetPopulatesPgDbRoleSettingRepro|AlterRoleResetSettingRepro|AlterRoleInDatabaseSetPopulatesPgDbRoleSettingRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./server/ast ./server/node ./server/auth ./server/tables/pgcatalog ./server/cast ./server/types ./server/functions -count=1`
- Additional root cause fixed for the focused catalog assertions: `oid -> regrole` casts now work, matching the existing `oid -> regdatabase` path.
- Next action: commit alpha-owned parser/AST/node/cast files only. Leaving shared `coop.md` unstaged.

### alpha - 2026-05-12 14:32 America/Phoenix

- Lane: role option privilege checks and invalid connection limits.
- Green focused role authorization/metadata set:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run 'Test(AlterRoleSelfCreatedbPrivilegeEscalationRepro|AlterRoleSelfCreateRolePrivilegeEscalationRepro|AlterRoleSelfLoginAttributeRepro|AlterRoleSelfInheritAttributeRepro|AlterRoleCreatedbRequiresCreatedbRepro|AlterRoleRejectsInvalidConnectionLimitRepro|CreateRoleRejectsInvalidConnectionLimitRepro|CreateRoleCreatedbRequiresCreatedbRepro|CreateRoleBypassRLSRequiresSuperuserRepro|CreateRoleReplicationRequiresSuperuserRepro|CreateRoleSuperuserRequiresSuperuserGuard|AlterRoleBypassRLSRequiresSuperuserGuard|AlterRoleReplicationRequiresSuperuserGuard|AlterRoleSuperuserRequiresSuperuserGuard|AlterRoleSuperuserRequiresSuperuserGuard)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./server/node ./server/auth -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^TestAlterRoleNoLoginPreventsLoginGuard$' -count=1 -v`
- Note: broad `TestAuthQuick`, `TestAuthDoltProcedures`, and `TestRoleConnectionLimitPreventsLoginRepro` still fail independently in the manifest; they are not part of this slice.
- Next action: commit only `server/node/create_role.go` and `server/node/alter_role.go`.

### alpha - 2026-05-12 14:36 America/Phoenix

- Lane: enforce role login metadata during SCRAM authentication.
- Expected files: `server/authentication_scram.go`.
- Red: `TestExpiredRoleValidUntilPreventsLoginRepro` and `TestRoleConnectionLimitPreventsLoginRepro` authenticated successfully despite stored role metadata.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^(TestExpiredRoleValidUntilPreventsLoginRepro|TestRoleConnectionLimitPreventsLoginRepro|TestNoLoginRolePreventsLoginGuard|TestAlterRoleNoLoginPreventsLoginGuard)$' -count=1 -v`
- `go test -vet=off ./server -count=1` is currently blocked by peer-owned dirty `server/ast/alter_table.go` changes referencing missing `pgnodes.NewAlterTableSetColumn*` constructors. Avoiding that lane.
- Next action: commit only `server/authentication_scram.go`.

### delta - 2026-05-12 14:30 America/Phoenix

- Lane: refresh `./testing/go` manifest from committed HEAD and pick the next unclaimed failure.
- Expected files: none yet; manifest run is in a separate clean worktree.
- Avoiding: alpha's dirty `ALTER ROLE ... SET/RESET` parser/ast/node files and gamma's dirty `server/types/type.go` xid ordering lane.
- Starting stats: latest trusted full snapshot in this log remains `1367/2593` passing (`52.7%`), `1226` still failing; many focused fixes landed after it, so this is stale.
- Next action: create a clean verifier from current HEAD, run `go test -json ./testing/go -count=1 -timeout=60m` with ICU env, summarize pass/fail counts, then claim a non-overlapping failure.

### gamma - 2026-05-12 14:29 America/Phoenix

- Lane: integer expressions should not assignment-cast into `name` columns.
- Expected files: `server/functions/framework/cast.go`.
- Avoiding: alpha's staged alter-role/parser/cast-oid files and delta's manifest run.
- Red in `/tmp/doltgresql-gamma-head.WIb56Y`: `TestNameTypeRejectsIntegerAssignmentRepro` accepted `INSERT ... VALUES (1, 12345)` into a `NAME` column.
- Green in `/tmp/doltgresql-gamma-head.WIb56Y`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestNameTypeRejectsIntegerAssignmentRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/functions/framework ./server/cast -count=1`

### gamma - 2026-05-12 14:32 America/Phoenix

- Lane: `ALTER TYPE ... RENAME ATTRIBUTE` should rewrite dependent view, materialized-view, generated-column, and SQL-function metadata.
- Expected files: `server/node/alter_type_rename_attribute.go`, maybe `server/node/alter_type_rename_sql_rewrite.go`.
- Avoiding: dirty role/index files and delta's manifest run.
- Red in `/tmp/doltgresql-gamma-attr.PYhZgq`: four focused composite-attribute metadata repros still stored `.old_name` and failed with `column "old_name" not found`.

### delta - 2026-05-12 14:36 America/Phoenix

- Lane: user-defined `CREATE CAST ... WITH FUNCTION` should drive explicit casts.
- Expected files: `server/expression/explicit_cast.go`; test already exists in `testing/go/cast_correctness_repro_test.go`.
- Avoiding: no overlap with alpha role-setting or beta index DDL lanes after their commits.
- Starting progress: refreshed manifest is only `47/2593` completed (`1.8%`) and still not a valid full baseline; latest trusted full baseline remains `1367/2593`, `1226` left.

### gamma - 2026-05-12 14:43 America/Phoenix

- Lane complete: `ALTER TYPE ... RENAME ATTRIBUTE` now rewrites stored column types plus dependent view, materialized-view, generated-column, and SQL-function metadata; also added the missing type-owner check on the same node.
- Source touched: `server/node/alter_type_rename_attribute.go`, `server/node/alter_type_rename_sql_rewrite.go`.
- Verified in clean detached worktree `/tmp/doltgresql-gamma-attr2.5KzIqe` because shared tree has peer dirt in `create_database.go`, alter-sequence/database, auth, sequence metadata, etc. that currently breaks local server startup.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^(TestAlterCompositeTypeRenameAttributeUpdates(StoredColumns|ViewMetadata|MaterializedViewMetadata|GeneratedColumnMetadata|FunctionBodyMetadata)Repro|TestAlterTypeRenameAttributeRequiresOwnershipRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAlter(TypeRename|CompositeTypeRenameAttribute)' -count=1`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -count=1`

### beta - 2026-05-12 14:38 America/Phoenix

- Lane: `ALTER TABLE ... ALTER COLUMN ... SET STORAGE/COMPRESSION/STATISTICS` should persist through `pg_attribute`.
- Source touched: `server/ast/alter_table.go`, `server/node/table_storage.go`, `server/tablemetadata/table_metadata.go`, `server/tablemetadata/table_metadata_test.go`, `server/tables/pgcatalog/pg_attribute.go`.
- Avoiding: peer dirty `server/node/alter_type_rename_attribute.go`, peer untracked `server/node/alter_table_using.go`, and the concurrent `ALTER COLUMN TYPE USING` hunk in `server/ast/alter_table.go`.
- Verification was run in temp worktree `/tmp/doltgresql-beta-columnmeta.Ib4PCw` because shared tree is build-blocked by peer dirt.
- Red: storage stayed `p`, compression stayed empty, statistics stayed `-1` after the ALTERs.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'TestAlterColumnSet(Storage|Compression|Statistics)PersistsCatalogRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/tablemetadata ./server/node ./server/tables/pgcatalog ./server/ast -count=1`
- Progress note: latest valid full baseline remains `1367/2593` passing (`52.7%`), `1226` left; this lane fixes 3 known top-level failures pending a fresh complete manifest.
- Red check: `TestCreateCastFunctionIsUsedByExplicitCastRepro` and `TestDropCastIfExistsMissingTypeRepro` are already green on current shared code; no edit made for cast.
- Current lane: `ALTER TABLE ... ALTER COLUMN ... TYPE ... USING` from `TestAlterColumnTypeUsingConvertsExistingRowsRepro`.
- Red: focused repro still fails with `ALTER TABLE with USING is not supported yet`; after removing the AST rejection it fails in the existing row rewrite with `integer: unhandled type: string`.
- WIP files: `server/ast/alter_table.go`, `server/node/alter_table_using.go`.
- Blocked: shared build is currently blocked by gamma-owned dirty composite-attribute rename files in `server/node/alter_type_rename_attribute.go` and `server/node/alter_type_rename_sql_rewrite.go`; delta has not committed this lane.
- Manifest result: `/tmp/doltgresql-testing-go-20260512-delta-1435.jsonl` finished with `8447` pass events and `1644` fail events; top-level summary from the JSON is `802` pass / `472` fail (`63.0%`, `472` top-level still failing). This is not directly comparable to the stale `1367/2593` count because the prior note used a different manifest/counting shape.

### beta - 2026-05-12 14:49 America/Phoenix

- Lane: sequence ownership metadata and ownership checks for `CREATE/ALTER SEQUENCE`.
- Source touched: `core/sequences/collection.go`, `core/sequences/serialization.go`, `core/sequences/root_object.go`, `server/ast/alter_sequence.go`, `server/node/alter_sequence.go`, `server/node/create_sequence.go`, `server/node/drop_sequence.go`, `server/auth/auth_handler.go`, `server/tables/pgcatalog/pg_class.go`.
- Avoiding: delta's staged `ALTER COLUMN TYPE USING` files and gamma/alpha dirty database/composite rewrite files.
- Verification was run in temp worktree `/tmp/doltgresql-beta-seqowner-red.m4ngrt` because the shared tree has peer staged/dirty changes.
- Red:
  - `CREATE SEQUENCE ... OWNED BY` accepted a table the caller did not own.
  - `ALTER SEQUENCE ... OWNED BY/NONE` did not require sequence ownership and could clear dependencies.
  - `ALTER SEQUENCE ... OWNER TO` was treated as a no-op for `pg_class.relowner`, and transferred owners did not implicitly have sequence usage.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(CreateSequenceOwnedByRequiresTableOwnershipRepro|AlterSequenceOwnedByRequiresTableOwnershipRepro|AlterSequenceRequiresSequenceOwnershipRepro|AlterSequenceOwnerToRequiresOwnershipRepro|AlterSequenceOwnerUpdatesCatalogRepro|AlterSequenceOwnerCanUseTransferredSequenceRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./core/sequences ./server/node ./server/ast ./server/auth ./server/tables/pgcatalog -count=1`
- Environmental note: first rerun hit `no space left on device`; `go clean -cache -testcache` freed space but ended with `unlinkat ... directory not empty`. Rerun passed after free space recovered.

### alpha - 2026-05-12 14:49 America/Phoenix

- Lane: database ownership and CREATE DATABASE privilege enforcement.
- Committed:
  - `847da5d5 fix: enforce database ownership checks`
  - `7439c8d0 test: align create database smoke expectations`
- Touched: `server/analyzer/replace_node.go`, `server/node/create_database.go`, `server/node/alter_database.go`, `testing/go/create_database_test.go`.
- Red/green covered in temp worktree `/private/tmp/doltgresql-alpha-createdb-verify.MrC2Ch` with isolated `GOCACHE`.
- Green focused:
  - `go test -vet=off ./testing/go -run '^(TestCreateDatabaseRequiresCreatedbPrivilegeRepro|TestDropDatabaseRequiresOwnershipRepro|TestDropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro|TestAlterDatabaseOwnerToRequiresOwnershipRepro|TestAlterDatabaseRenameToRequiresOwnershipRepro|TestAlterDatabaseSetRequiresOwnershipRepro|TestCreateDatabaseOwnerUpdatesCatalogRepro|TestCreateDatabaseOwnerRequiresExistingRoleRepro|TestAlterDatabaseOwnerUpdatesCatalogRepro)$' -count=1 -v`
  - `go test -vet=off ./testing/go -run '^(TestCreateDatabase|TestCreateDatabaseRejectsInvalidEncodingRepro|TestCreateDatabaseOwnerUpdatesCatalogRepro|TestCreateDatabaseOwnerRequiresExistingRoleRepro|TestCreateDatabaseRequiresCreatedbPrivilegeRepro|TestAlterDatabaseSetRequiresOwnershipRepro)$' -count=1 -v`
  - On fresh HEAD worktree `/tmp/doltgresql-alpha-head-verify.n62ueh`: `go test -vet=off ./testing/go -run '^(TestCreateDatabase|TestCreateDatabaseRejectsInvalidEncodingRepro|TestCreateDatabaseRequiresCreatedbPrivilegeRepro|TestDropDatabaseRequiresOwnershipRepro|TestDropDatabaseRequiresOwnershipDespiteAllPrivilegesRepro|TestAlterDatabaseOwnerToRequiresOwnershipRepro|TestAlterDatabaseRenameToRequiresOwnershipRepro|TestAlterDatabaseSetRequiresOwnershipRepro|TestAlterDatabaseCatalogOptionsRequireOwnershipRepro|TestAlterDatabaseResetRequiresOwnershipRepro)$' -count=1 -v`
  - On newer HEAD worktree `/tmp/doltgresql-alpha-head2-verify.GtEo8r`: `go test -vet=off ./testing/go -run '^(TestAlterDatabaseResetRequiresOwnershipRepro|TestAlterDatabaseSetRequiresOwnershipRepro|TestAlterDatabaseCatalogOptionsRequireOwnershipRepro)$' -count=1 -v`
  - `go test -vet=off ./server/node ./server/analyzer ./server/ast ./server/auth ./server/tables/pgcatalog -count=1`
- Progress note: latest completed alpha manifest remains `1377/2496` top-level passing (`55.2%`), `1119` left; not refreshed after these commits because shared tree still has peer dirty sequence/ALTER TABLE/ALTER TYPE files.

### delta - 2026-05-12 14:42 America/Phoenix

- Lane: finish `ALTER TABLE ... ALTER COLUMN ... TYPE ... USING`.
- Source touched: `server/ast/alter_table.go`, `server/node/alter_table_using.go`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run '^TestAlterColumnTypeUsingConvertsExistingRowsRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node -run '^$'`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/ast -run '^$'`
- Next action: commit only delta-owned `ALTER COLUMN TYPE USING` files; leave peer-owned dirty metadata/rename files unstaged.

### delta - 2026-05-12 14:44 America/Phoenix

- Lane: refresh `./testing/go` manifest from committed HEAD `75ffd827` and pick an unclaimed failure.
- Expected files: none yet; manifest runs in a detached clean worktree.
- Avoiding: current dirty sequence/database/attribute-rename files in shared worktree.
- Manifest result: `/tmp/doltgresql-testing-go-20260512-delta-1444.jsonl`, run in `/tmp/doltgresql-delta-manifest.cALYDm` with generated parser files copied and isolated `GOCACHE`.
- Top-level summary: `1451` pass / `1171` fail out of `2622` (`55.3%`), raw events `17827` pass / `3900` fail.
- Compared with the older trusted `1367/2593` snapshot: `+84` passing tests, `-55` failing tests, denominator `+29`.

### delta - 2026-05-12 15:02 America/Phoenix

- Lane: Python client harness failures from unavailable `psycopg[binary]==3.3.4` pin.
- Expected files: `testing/go/*` client harness tests that install psycopg.
- Red: manifest shows Alembic tests fail during `pip install` because the configured index offers psycopg only through `3.2.13`.
- Avoiding: dirty enum/database auth/catalog files in shared worktree.
- Green in clean verifier `/tmp/doltgresql-delta-manifest.cALYDm`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run 'Test(AlembicAutogenerate|AlembicConcurrentlyMigration|PsycopgClientSmoke|SQLAlchemy(ClientSmoke|SQLStateBranching|NestedTransactions)|DjangoMigrationAndORMSmoke)' -count=1 -v`
- Note: shared worktree focused run is blocked by peer-owned dirty `server/tables/pgcatalog/pg_enum.go`; cleaned Go build cache after a linker `errno=28` disk-full failure.

### delta - 2026-05-12 15:12 America/Phoenix

- Lane: table ACL metadata rollback for GRANT/REVOKE from `TestRollbackRevertsGrantTablePrivilegeRepro` / `TestRollbackRevertsRevokeTablePrivilegeRepro`.
- Expected files: likely `server/auth/table_privileges.go` and transaction state plumbing if needed.
- Avoiding: beta's dirty `server/hook/table_modify_column.go` and gamma's dirty enum metadata test.
- Next action: reset clean verifier to current HEAD, run focused repros red, then implement the narrow rollback path.
- Result: already green on current HEAD `23e7dce2`; no edit made.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run 'TestRollbackReverts(Grant|Revoke)TablePrivilegeRepro$' -count=1 -v`

### delta - 2026-05-12 15:24 America/Phoenix

- Lane: refreshed full `./testing/go` manifest from current HEAD `23e7dce2`.
- Manifest result: `/tmp/doltgresql-testing-go-20260512-delta-1512.jsonl`, clean verifier `/tmp/doltgresql-delta-manifest.cALYDm`.
- Top-level summary: `1497` pass / `1136` fail out of `2633` (`56.9%`), raw events `18009` pass / `3800` fail.
- Compared with prior delta manifest `1451/2622` (`55.3%`): `+46` passing, `-35` failing, denominator `+11`.
- Next action: pick a small unclaimed failure from the refreshed list.

### delta - 2026-05-12 15:25 America/Phoenix

- Lane: `COPY FROM` server file should reject relative paths.
- Expected files: likely `server/connection_handler.go` or COPY node validation; focused test `TestCopyFromServerFileRejectsRelativePathRepro`.
- Avoiding: broader COPY privilege/program/parser failures and peer dirty hook/enum files.
- Red from manifest: relative server file was accepted and loaded one row instead of returning an error.
- Green in clean verifier `/tmp/doltgresql-delta-manifest.cALYDm`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^TestCopyFromServerFileRejectsRelativePathRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./server -run '^$'`

### delta - 2026-05-12 15:31 America/Phoenix

- Lane: date constructors must reject invalid calendar dates.
- Expected files: `server/functions/make_date.go`, `server/functions/make_timestamp.go`.
- Avoiding: `to_timestamp` range/infinity and dirty relation-owner/domain files.
- Red from manifest: `make_timestamp(2021,2,29,...)` and `make_timestamptz(2021,2,29,...)` silently normalized instead of erroring.
- Green in clean verifier `/tmp/doltgresql-delta-manifest.cALYDm`:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^TestMakeDateTimestampRejectInvalidCalendarDateRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./server/functions -run 'Make(Date|Timestamp)|make_(date|timestamp)' -count=1`

### gamma - 2026-05-12 14:47 America/Phoenix

- Lane: `ALTER TYPE enum RENAME VALUE` should rewrite stored enum rows so the old label disappears and the new label is returned.
- Expected files: likely `server/node/alter_type_rename_value.go` or enum value/storage helpers; avoiding dirty sequence/database/alter-table/auth/pg_class files.
- Starting from delta manifest `802/1274` top-level passing (`63.0%`), `472` failing; this manifest predates gamma commit `5a3b2fd7`, so it is a conservative floor.

### beta - 2026-05-12 14:54 America/Phoenix

- Lane: `ALTER TABLE ... ADD COLUMN ... NOT NULL` without a default should reject non-empty tables.
- Source touched: `server/hook/table_add_column.go`.
- Avoiding: gamma's dirty enum ALTER TYPE files (`server/ast/alter_type.go`, `server/node/alter_type_enum_value.go`) and alpha's dirty create-database test.
- Red in shared tree: `TestAddNotNullColumnValidatesExistingRowsRepro` accepted the ALTER and synthesized `required_value=0`.
- Verification was run in temp worktree `/tmp/doltgresql-beta-addcol.bAokRD` because shared tree is build-blocked by peer enum dirt.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AddNotNullColumnValidatesExistingRowsRepro|AddUniqueNotNullColumnValidatesExistingRowsRepro|AddColumnVolatileDefaultBackfillsEachExistingRowGuard|AddPrimaryKeyRejectsExistingNullsGuard|AddPrimaryKeyRejectsExistingDuplicatesRepro)$' -count=1 -v`
  - after tightening the error text: `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AddNotNullColumnValidatesExistingRowsRepro|AddUniqueNotNullColumnValidatesExistingRowsRepro|AddColumnVolatileDefaultBackfillsEachExistingRowGuard)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/hook -count=1`
- Progress note: using delta's latest top-level manifest floor plus beta's sequence lane, this fixes 1 more known failure: provisional `809/1274` passing (`63.5%`), `465` left. Needs fresh full manifest.

### beta - 2026-05-12 15:00 America/Phoenix

- Lane: build-time validation for `UNIQUE NULLS NOT DISTINCT` indexes/constraints.
- Source touched: `server/node/nulls_not_distinct_unique_table.go`, `server/node/create_nulls_not_distinct_unique_index.go`, `server/node/create_index_concurrently.go`, `server/ast/create_index.go`, `server/ast/alter_table.go`.
- Avoiding: gamma's dirty enum ALTER TYPE files.
- Verification was run in temp worktree `/tmp/doltgresql-beta-nulluniq.NYn7Ly` because shared tree is build-blocked by peer enum dirt.
- Red:
  - `TestAddUniqueConstraintNullsNotDistinctRejectsExistingDuplicateNullsRepro` accepted duplicate existing NULLs and left the constraint visible.
  - `TestCreateUniqueIndexNullsNotDistinctRejectsExistingDuplicateNullsRepro` accepted duplicate existing NULLs and left the index visible.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AddUniqueConstraintNullsNotDistinctRejectsExistingDuplicateNullsRepro|CreateUniqueIndexNullsNotDistinctRejectsExistingDuplicateNullsRepro|UniqueConstraintNullsNotDistinctRejectsDuplicateNullsRepro|UniqueIndexNullsNotDistinctRejectsDuplicateNullsRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/node ./server/ast -count=1`
- Progress note: using the provisional `809/1274` floor, this fixes 2 more known failures: provisional `811/1274` passing (`63.7%`), `463` left. Needs fresh full manifest.

### gamma - 2026-05-12 15:05 America/Phoenix

- Lane complete: `ALTER TYPE ... ADD VALUE` / `RENAME VALUE` for enum types now persists `pg_enum` labels, refreshes existing enum-typed table columns, and rewrites stored scalar enum rows on rename.
- Source touched: `server/ast/alter_type.go`, `server/node/alter_type_enum_value.go`, `server/tables/pgcatalog/pg_enum.go`, `testing/go/enum_metadata_repro_test.go`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AlterEnumRenameValueRepro|AlterEnumRenameValueUpdatesStoredRowsRepro|AlterEnumAddValuePersistsUsableLabelRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/ast ./server/node ./server/tables/pgcatalog -count=1`
- Adjacent failures still present outside this slice:
  - `TestEnumOrderingUsesDeclarationOrderRepro` still orders enum values lexically in `ORDER BY`.
  - `TestPgEnum` still disagrees with the quoted `"PG_catalog"` expected-error case.
- Next action: commit only gamma-owned enum files; leave peer-owned auth and Python client harness changes unstaged.

### beta - 2026-05-12 15:04 America/Phoenix

- Lane: same-type `ALTER TABLE ... ALTER COLUMN ... TYPE` must still reject row-type dependents.
- Source touched: `server/hook/table_modify_column.go`.
- Avoiding: peer dirty auth/database/client-test files and gamma's dirty enum ALTER TYPE files.
- Verification was run in temp worktree `/tmp/doltgresql-beta-dropcol.Y3D1G2` because shared tree has peer dirt.
- Red: `TestAlterTableSameTypeRejectsRowTypeDependentsRepro` accepted `ALTER COLUMN a SET DATA TYPE INT` even though `row_type_child.parent_row` used the parent row type.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AlterTableSameTypeRejectsRowTypeDependentsRepro|AlterColumnTypeUsingConvertsExistingRowsRepro|AlterColumnTypeAppliesTypmodsToExistingRowsRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/hook -count=1`
- Progress note: latest comparable manifest remains `/tmp/doltgresql-testing-go-20260512-delta-1444.jsonl` at `1451/2622` passing (`55.3%`), `1171` failing. Beta-known committed fixes after that manifest are at least +10 top-level tests, so conservative provisional floor is `1461/2622` passing (`55.7%`), `1161` left.

### beta - 2026-05-12 15:11 America/Phoenix

- Lane: `ALTER TABLE ... ALTER COLUMN ... TYPE <domain>` must validate existing rows before rewriting the schema.
- Source touched: `server/hook/table_modify_column.go`.
- Avoiding: peer dirty enum and Python client harness files.
- Verification was run in temp worktree `/tmp/doltgresql-beta-domainalter.CpJk2p` with isolated `GOCACHE=/tmp/doltgresql-beta-domainalter-gocache`.
- Red: `TestAlterColumnTypeToDomainValidatesExistingRowsRepro` and `TestAlterColumnTypeToNotNullDomainValidatesExistingRowsRepro` accepted violating existing values.
- Green:
  - `GOCACHE=/tmp/doltgresql-beta-domainalter-gocache CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AlterColumnTypeToDomainPreservesAndEnforcesDomainGuard|AlterColumnTypeToDomainValidatesExistingRowsRepro|AlterColumnTypeToNotNullDomainValidatesExistingRowsRepro)$' -count=1 -v`
  - `GOCACHE=/tmp/doltgresql-beta-domainalter-gocache CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(AlterColumnTypeUsingConvertsExistingRowsRepro|AlterColumnTypeAppliesTypmodsToExistingRowsRepro|AlterTableSameTypeRejectsRowTypeDependentsRepro)$' -count=1 -v`
  - `GOCACHE=/tmp/doltgresql-beta-domainalter-gocache CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/hook -count=1`
- Note: broader `TestNestedDomainEnforcesBaseDomainConstraintsRepro` still fails independently with an existing nested-domain cast panic.
- Progress note: latest comparable manifest remains `1451/2622` passing (`55.3%`), `1171` failing. Beta-known committed fixes after that manifest are at least +12 top-level tests, so conservative provisional floor is `1463/2622` passing (`55.8%`), `1159` left.

### alpha - 2026-05-12 15:05 America/Phoenix

- Lane: transactional rollback for auth metadata writes.
- Source touched and committed: `server/auth/database.go`, `server/auth/serialization.go`, `server/auth/table_privileges.go`, `server/connection_handler.go`, `testing/go/auth_transaction_repro_test.go`.
- Result: committed `fe5cc9fc fix: rollback transactional auth metadata`.
- Green focused:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^(TestRollbackRevertsAlterDatabaseSetRepro|TestCommitKeepsAlterDatabaseSet|TestRollbackRestoresPreviousAlterDatabaseSet|TestRollbackRevertsAlterDatabaseCatalogOptionsRepro|TestRollbackRevertsAlterRoleSetRepro|TestRollbackRevertsAlterRoleOptionsRepro|TestRollbackDropsCreatedRoleRepro|TestRollbackRestoresDroppedRoleRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./server/auth ./server -count=1`
- Next action: start a fresh `./testing/go` JSON manifest from current HEAD.

### alpha - 2026-05-12 15:14 America/Phoenix

- Lane: `ALTER TABLE/VIEW ... OWNER TO missing_role` validation.
- Source touched and committed: `server/ast/alter_table.go`, `server/ast/alter_view.go`.
- Result: committed `40f95273 fix: validate relation owner roles`.
- Red: `TestAlterOwnerRequiresExistingRoleRepro` failed for table and view owner changes because missing target roles were accepted.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^TestAlterOwnerRequiresExistingRoleRepro$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./server/ast -count=1`
- Current manifest in progress from clean `f790369c`: `/tmp/doltgresql-testing-go-alpha-20260512-1506.jsonl`; last partial read was `621/848` passing (`73.2%`), `227` completed failures.

### gamma - 2026-05-12 15:07 America/Phoenix

- Lane complete: enum `ORDER BY` regression expectation was stale for the ambiguous projected-text case.
- Red: `TestEnumOrderingUsesDeclarationOrderRepro` still returns `alpha, beta, gamma` for an enum declared as `beta, alpha, gamma`, while enum comparison operators and min/max already pass.
- PostgreSQL check: local PostgreSQL 18 returned `alpha, beta, gamma` for `SELECT status::text ... ORDER BY status`, because `status` resolves to the projected text output; it returned `beta, alpha, gamma` for `ORDER BY enum_declared_order_items.status`.
- Source touched: `testing/go/enum_metadata_repro_test.go`.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(EnumOrderingUsesDeclarationOrderRepro|EnumMinMaxUseDeclarationOrder)$' -count=1 -v`
- Next action: commit only the enum ordering test correction.

### gamma - 2026-05-12 15:13 America/Phoenix

- Lane: `TestPgEnum` quoted-schema behavior after enabling real `pg_enum` rows.
- Red seen during adjacent enum verification: `SELECT * FROM "PG_catalog"."pg_enum"` returned no error even though the test expects quoted schema case to be respected, while `"pg_catalog"."PG_enum"` still errors.
- Expected files: likely catalog handler or pg_catalog table-resolution tests; avoiding delta's auth rollback and beta's table-modify lanes.
- Next action: run focused red, compare with PostgreSQL, then either fix schema resolution for this handler or align a stale test if PostgreSQL disagrees.

### gamma - 2026-05-12 15:25 America/Phoenix

- Lane complete: quoted schema names now resolve case-sensitively, including `pg_catalog` virtual tables and schema create/drop preflights.
- Source touched: `core/context.go`, `core/rootvalue.go`, `testing/go/schemas_test.go`, submodule `third_party/dolt` commit `178f5c3b58 fix: resolve schemas case-sensitively` (`go/libraries/doltcore/doltdb/root_val.go`, `go/libraries/doltcore/sqle/database.go`).
- Note: shared worktree was build-blocked by peer dirty `server/node/drop_view.go`, so verification ran in clean temp worktree `/tmp/doltgresql-gamma-schema.4OGZQv` with generated parser files copied in and `GOTMPDIR=/tmp/doltgresql-gamma-gotmp`.
- Green:
  - `GOTMPDIR=/tmp/doltgresql-gamma-gotmp CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./testing/go -run '^(TestQuotedSchemaNamesAreCaseSensitiveRepro|TestSchemas|TestPgDescription|TestPgEnum|TestPgEventTrigger)$' -count=1 -v`
  - `GOTMPDIR=/tmp/doltgresql-gamma-gotmp CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test -vet=off ./core ./server/tables -count=1`
  - `GOTMPDIR=/tmp/doltgresql-gamma-gotmp CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./libraries/doltcore/doltdb ./libraries/doltcore/sqle -count=1` from `third_party/dolt/go`.
- Next action: commit only gamma-owned parent files plus the `third_party/dolt` submodule pointer; leave `coop.md` and peer dirty files unstaged.

### beta - 2026-05-12 15:27 America/Phoenix

- Lane complete: nested domains now resolve through user-defined base domains and enforce base-domain checks before outer-domain checks.
- Source touched: `server/node/create_domain.go`, `server/types/type.go`, `server/expression/assignment_cast.go`, `server/expression/implicit_cast.go`, `server/expression/explicit_cast.go`, `server/analyzer/domain_constraints.go`, `server/doltgres_handler.go`.
- Shared worktree was build-blocked by peer dirty `server/tables/pgcatalog/pg_class.go` (`undefined: comment`, `undefined: owner`), so verification ran in clean temp worktree `/tmp/doltgresql-beta-nested-domain.yOu4F5` with generated parser files copied in, `third_party/dolt` symlinked, and isolated `GOTMPDIR=/tmp/go-build-beta-nested-domain` / `GOCACHE=/tmp/doltgresql-beta-nesteddomain-gocache`.
- Red: `TestNestedDomainEnforcesBaseDomainConstraintsRepro` first panicked on unresolved user-defined domain base lookup, then returned text `"5"` because RowDescription only collapsed one domain layer.
- Green:
  - `GOTMPDIR=/tmp/go-build-beta-nested-domain GOCACHE=/tmp/doltgresql-beta-nesteddomain-gocache CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./testing/go -run 'Test(NestedDomainEnforcesBaseDomainConstraintsRepro|AlterColumnTypeToDomainPreservesAndEnforcesDomainGuard|AlterColumnTypeToDomainValidatesExistingRowsRepro|AlterColumnTypeToNotNullDomainValidatesExistingRowsRepro)$' -count=1 -v`
  - `GOTMPDIR=/tmp/go-build-beta-nested-domain GOCACHE=/tmp/doltgresql-beta-nesteddomain-gocache CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig go test ./server/types ./server/expression ./server/analyzer ./server/node ./server -count=1`
- Progress note: latest comparable full manifest remains `/tmp/doltgresql-testing-go-20260512-delta-1444.jsonl` at `1451/2622` passing (`55.3%`), `1171` failing. Beta-known committed fixes after that manifest are at least +13 top-level tests, so conservative provisional floor is `1464/2622` passing (`55.8%`), `1158` left.
- Next action: commit only beta-owned domain files; leave `coop.md` and peer dirty files unstaged.

### alpha - 2026-05-12 15:30 America/Phoenix

- Lane complete: `ALTER TABLE/VIEW ... OWNER TO` now persists relation owner metadata, updates `pg_class.relowner` / `pg_views.viewowner`, lets transferred table/view owners use their owned relation, and rejects non-owner hijacks.
- Source touched and committed: `server/auth/relation_owners.go`, `server/auth/database.go`, `server/auth/serialization.go`, `server/auth/auth_handler.go`, `server/node/alter_relation_owner.go`, `server/node/table_ownership.go`, `server/node/drop_view.go`, `server/tables/pgcatalog/pg_class.go`, `server/tables/pgcatalog/pg_views.go`, `server/ast/alter_table.go`, `server/ast/alter_view.go`, `testing/go/ownership_repro_test.go`.
- Result: committed `b21832f6 fix: support relation owner transfers`.
- Red: table/view owner transfer focused tests showed no-op owner changes and missing ownership checks; transferred view test also needed the PostgreSQL-required base-table `GRANT SELECT`.
- PostgreSQL check: local PostgreSQL 16 rejects selecting through a transferred view if the new view owner lacks SELECT on the base table.
- Green:
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig GOCACHE=/tmp/doltgresql-alpha-owner-one-gocache.lwQCms go test -vet=off ./testing/go -run '^(TestAlterTableOwnerUpdatesCatalogRepro|TestAlterTableOwnerCanUseTransferredTableRepro|TestAlterViewOwnerUpdatesCatalogRepro|TestAlterViewOwnerCanUseTransferredViewRepro|TestAlterTableOwnerToRequiresOwnershipRepro|TestAlterViewOwnerToRequiresOwnershipRepro|TestAlterOwnerRequiresExistingRoleRepro|TestPgViewsViewownerMetadataRepro)$' -count=1 -v`
  - `CGO_CPPFLAGS=-I/opt/homebrew/opt/icu4c@78/include CGO_LDFLAGS=-L/opt/homebrew/opt/icu4c@78/lib PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig GOCACHE=/tmp/doltgresql-alpha-owner-one-gocache.lwQCms go test -vet=off ./server/auth ./server/node ./server/ast ./server/tables/pgcatalog -count=1`
- Progress note: stale in-progress alpha manifest `/tmp/doltgresql-testing-go-alpha-20260512-1514.jsonl` last read `1254/2153` passing (`58.2%`), `899` failed, excluding this commit. Next action is a fresh manifest from `b21832f6`.
