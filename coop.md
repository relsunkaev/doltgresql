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

- Lane: Alembic/SQLAlchemy startup failure on `SELECT pg_catalog.version()`.
- Baseline slice from gamma's jsonl: 79 top-level tests passed, 14 failed before the incomplete run stopped; first failure is `TestAlembicAutogenerate` because qualified `pg_catalog.version()` is missing.
- Expected files: `server/functions/*version*` or a new small `server/functions/version.go`, `server/functions/init.go`, and focused tests only if needed.
- Avoiding: alpha's DDL ownership files, beta's `server/connection_handler.go`, and gamma's full-suite baseline.
- Next action: red focused Alembic/server-version repros, implement the missing built-in/GUC root cause, focused green, commit.

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
- Next action: reject concurrent index DDL when `ConnectionHandler.inTransaction` is true, focused green, commit only gamma-owned files.

### alpha - 2026-05-12 13:25 America/Phoenix

- Lane: `pg_catalog.pg_get_keywords()` compatibility helper.
- Result: implementation is ready to commit; `server/functions` package is green.
- Focused repro note: `TestPgGetKeywordsExposesReservedWordsRepro` was green before the current peer-owned `server/connection_handler.go` edit. The rerun now fails at build time because `server/connection_handler.go` references undefined `createIndexConcurrentlyPattern` / `dropIndexConcurrentlyPattern`.
- Current stats note: gamma's JSONL ended with 725/1285 top-level passing (56.4%), but this is not a valid full-suite snapshot because the package build is broken by the in-progress concurrent-index lane.
