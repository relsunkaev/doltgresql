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
- Next action: red `TestCreateRuleRequiresTableOwnershipRepro`, implement ownership check before rule-to-trigger rewrite, focused green, commit.
