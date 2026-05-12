// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestDoltStashPushPopRestoresTrackedRowRepro reproduces a Dolt persistence
// bug: DOLT_STASH should save tracked table edits, restore the committed row,
// and then reapply the edit on pop.
func TestDoltStashPushPopRestoresTrackedRowRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_STASH push pop restores tracked row edits",
			SetUpScript: []string{
				`CREATE TABLE stash_tracked_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO stash_tracked_items VALUES (1, 'committed');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial stash row');`,
				`UPDATE stash_tracked_items SET label = 'stashed' WHERE id = 1;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT label FROM stash_tracked_items;`,
					Expected: []sql.Row{{"stashed"}},
				},
				{
					Query:    `SELECT DOLT_STASH('push', 'tracked-edit');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT label FROM stash_tracked_items;`,
					Expected: []sql.Row{{"committed"}},
				},
				{
					Query:    `SELECT DOLT_STASH('pop', 'tracked-edit');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT label FROM stash_tracked_items;`,
					Expected: []sql.Row{{"stashed"}},
				},
			},
		},
	})
}

// TestDoltStashPushPopRestoresUntrackedTableRepro reproduces a Dolt
// persistence bug: DOLT_STASH --all should save and restore untracked tables
// with their row data.
func TestDoltStashPushPopRestoresUntrackedTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_STASH push pop restores untracked table",
			SetUpScript: []string{
				`CREATE TABLE stash_untracked_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO stash_untracked_items VALUES (1, 'untracked');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_STASH('push', 'untracked-table', '--all');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT DOLT_STASH('pop', 'untracked-table');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT id, label FROM stash_untracked_items;`,
					Expected: []sql.Row{{1, "untracked"}},
				},
			},
		},
	})
}

// TestDoltCommitDiffWorkingSetFilterRepro reproduces a versioned-data
// correctness bug: dolt_commit_diff_<table> should expose the same working-set
// row diff as dolt_diff when filtered to one from/to pair.
func TestDoltCommitDiffWorkingSetFilterRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "dolt_commit_diff table reports filtered working-set additions",
			SetUpScript: []string{
				`CREATE TABLE commit_diff_working_items (pk INT PRIMARY KEY);`,
				`INSERT INTO commit_diff_working_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT diff_type, from_pk, to_pk
						FROM dolt_diff('HEAD', 'WORKING', 'commit_diff_working_items');`,
					Expected: []sql.Row{{"added", nil, 1}},
				},
				{
					Query: `SELECT diff_type, from_pk, to_pk
						FROM dolt_commit_diff_commit_diff_working_items
						WHERE to_commit = HASHOF('main')
							AND from_commit = 'WORKING';`,
					Expected: []sql.Row{{"added", nil, 1}},
				},
			},
		},
	})
}

// TestDoltQueryDiffSupportsAsOfRevisionRepro reproduces a versioned-query
// correctness bug: dolt_query_diff should compare a historical AS OF query with
// the current working query.
func TestDoltQueryDiffSupportsAsOfRevisionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "dolt_query_diff compares AS OF query to working query",
			SetUpScript: []string{
				`CREATE TABLE query_diff_asof_items (pk INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial query diff table');`,
				`INSERT INTO query_diff_asof_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT *
						FROM dolt_query_diff(
							'SELECT * FROM query_diff_asof_items AS OF main',
							'SELECT * FROM query_diff_asof_items'
						);`,
					Expected: []sql.Row{{"", "query_diff_asof_items", "added", 1, 1}},
				},
			},
		},
	})
}

// TestDoltDiffReportsFunctionDefinitionChangesRepro reproduces a versioned-data
// correctness bug: DOLT_DIFF should expose function-definition changes that are
// already visible to DOLT_DIFF_STAT and DOLT_MERGE.
func TestDoltDiffReportsFunctionDefinitionChangesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_DIFF reports changed function definition",
			SetUpScript: []string{
				`CREATE FUNCTION diff_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial diff function value');`,
				`SELECT DOLT_BRANCH('original');`,
				`CREATE OR REPLACE FUNCTION diff_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main diff function value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name
						FROM DOLT_DIFF_STAT(
							'main',
							'original',
							'diff_function_value()'
						);`,
					Expected: []sql.Row{{"public.diff_function_value()"}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_DIFF('main', 'original', 'diff_function_value()');`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDoltDiffReportsSequenceChangesRepro reproduces a versioned-data
// correctness bug: DOLT_DIFF should expose sequence object changes, not only
// table row changes.
func TestDoltDiffReportsSequenceChangesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_DIFF reports changed sequence state",
			SetUpScript: []string{
				`CREATE TABLE diff_sequence_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO diff_sequence_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial diff sequence value');`,
				`SELECT DOLT_BRANCH('original');`,
				`INSERT INTO diff_sequence_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main diff sequence value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name
						FROM DOLT_DIFF_STAT(
							'main',
							'original',
							'diff_sequence_items_id_seq'
						);`,
					Expected: []sql.Row{{"public.diff_sequence_items_id_seq"}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_DIFF(
							'main',
							'original',
							'diff_sequence_items_id_seq'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDoltDiffReportsTriggerChangesRepro reproduces a versioned-data
// correctness bug: DOLT_DIFF should expose changed trigger definitions.
func TestDoltDiffReportsTriggerChangesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_DIFF reports changed trigger definition",
			SetUpScript: []string{
				`CREATE TABLE diff_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION diff_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER diff_trigger_changed
					BEFORE INSERT ON diff_trigger_items
					FOR EACH ROW EXECUTE FUNCTION diff_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial diff trigger value');`,
				`SELECT DOLT_BRANCH('original');`,
				`DROP TRIGGER diff_trigger_changed ON diff_trigger_items;`,
				`CREATE TRIGGER diff_trigger_changed
					BEFORE UPDATE ON diff_trigger_items
					FOR EACH ROW EXECUTE FUNCTION diff_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main diff trigger value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT table_name
						FROM DOLT_DIFF_STAT(
							'main',
							'original',
							'diff_trigger_items.diff_trigger_changed'
						);`,
					Expected: []sql.Row{{"public.diff_trigger_items.diff_trigger_changed"}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_DIFF(
							'main',
							'original',
							'diff_trigger_items.diff_trigger_changed'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedFunctionRepro reproduces a versioned
// persistence bug: DOLT_RESET --hard should discard uncommitted function
// definitions.
func TestDoltResetHardRemovesUncommittedFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted function",
			SetUpScript: []string{
				`CREATE FUNCTION reset_uncommitted_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `SELECT reset_uncommitted_function_value();`,
					ExpectedErr: `function "reset_uncommitted_function_value" does not exist`,
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedProcedureRepro reproduces a versioned
// persistence bug: DOLT_RESET --hard should discard uncommitted procedure
// definitions.
func TestDoltResetHardRemovesUncommittedProcedureRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted procedure",
			SetUpScript: []string{
				`CREATE TABLE reset_procedure_audit (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial procedure reset audit');`,
				`CREATE PROCEDURE reset_uncommitted_procedure()
					LANGUAGE SQL
					AS $$ INSERT INTO reset_procedure_audit VALUES (1) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CALL reset_uncommitted_procedure();`,
				},
				{
					Query:    `SELECT id FROM reset_procedure_audit;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `TRUNCATE reset_procedure_audit;`,
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `CALL reset_uncommitted_procedure();`,
					ExpectedErr: `procedure "reset_uncommitted_procedure" does not exist`,
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedViewRepro reproduces a versioned
// persistence bug: DOLT_RESET --hard should discard uncommitted view
// definitions.
func TestDoltResetHardRemovesUncommittedViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted view",
			SetUpScript: []string{
				`CREATE TABLE reset_view_source (id INT PRIMARY KEY);`,
				`INSERT INTO reset_view_source VALUES (1);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial view reset source');`,
				`CREATE VIEW reset_uncommitted_view AS
					SELECT id FROM reset_view_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id FROM reset_uncommitted_view;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `SELECT id FROM reset_uncommitted_view;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedMaterializedViewRepro reproduces a
// versioned persistence bug: DOLT_RESET --hard should discard uncommitted
// materialized view definitions and their materialized rows.
func TestDoltResetHardRemovesUncommittedMaterializedViewRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted materialized view",
			SetUpScript: []string{
				`CREATE TABLE reset_matview_source (id INT PRIMARY KEY);`,
				`INSERT INTO reset_matview_source VALUES (1);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial matview reset source');`,
				`CREATE MATERIALIZED VIEW reset_uncommitted_matview AS
					SELECT id FROM reset_matview_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id FROM reset_uncommitted_matview;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `SELECT id FROM reset_uncommitted_matview;`,
					ExpectedErr: `not found`,
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedEnumTypeRepro reproduces a versioned
// persistence bug: DOLT_RESET --hard should discard uncommitted enum type
// metadata.
func TestDoltResetHardRemovesUncommittedEnumTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted enum type",
			SetUpScript: []string{
				`CREATE TYPE reset_uncommitted_enum AS ENUM ('one', 'two');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_type
						WHERE typname = 'reset_uncommitted_enum';`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_type
						WHERE typname = 'reset_uncommitted_enum';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedCompositeTypeRepro reproduces a versioned
// persistence bug: DOLT_RESET --hard should discard uncommitted composite type
// metadata.
func TestDoltResetHardRemovesUncommittedCompositeTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted composite type",
			SetUpScript: []string{
				`CREATE TYPE reset_uncommitted_composite AS (id integer);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_type
						WHERE typname = 'reset_uncommitted_composite';`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_type
						WHERE typname = 'reset_uncommitted_composite';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedDomainRepro reproduces a versioned
// persistence bug: DOLT_RESET --hard should discard uncommitted domain
// metadata.
func TestDoltResetHardRemovesUncommittedDomainRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted domain",
			SetUpScript: []string{
				`CREATE DOMAIN reset_uncommitted_domain AS integer;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_type
						WHERE typname = 'reset_uncommitted_domain';`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_type
						WHERE typname = 'reset_uncommitted_domain';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedSequenceGuard keeps coverage for
// DOLT_RESET --hard discarding uncommitted sequence objects.
func TestDoltResetHardRemovesUncommittedSequenceGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted sequence",
			SetUpScript: []string{
				`CREATE SEQUENCE reset_uncommitted_sequence_value;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `SELECT nextval('reset_uncommitted_sequence_value');`,
					ExpectedErr: `relation "reset_uncommitted_sequence_value" does not exist`,
				},
			},
		},
	})
}

// TestDoltResetHardRemovesUncommittedTriggerGuard keeps coverage for
// DOLT_RESET --hard discarding uncommitted trigger definitions.
func TestDoltResetHardRemovesUncommittedTriggerGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard removes uncommitted trigger",
			SetUpScript: []string{
				`CREATE TABLE reset_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION reset_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset trigger table');`,
				`CREATE TRIGGER reset_uncommitted_trigger
					BEFORE INSERT ON reset_trigger_items
					FOR EACH ROW EXECUTE FUNCTION reset_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'reset_uncommitted_trigger';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestDoltResetHardToRevisionRestoresFunctionDefinitionRepro reproduces a
// versioned-data correctness bug: DOLT_RESET --hard to an older revision should
// restore committed function definitions.
func TestDoltResetHardToRevisionRestoresFunctionDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard to revision restores function definition",
			SetUpScript: []string{
				`CREATE FUNCTION reset_revision_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset revision function value');`,
				`CREATE OR REPLACE FUNCTION reset_revision_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main reset revision function value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT reset_revision_function_value();`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard', 'HEAD~1');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT reset_revision_function_value();`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDoltResetHardToRevisionRestoresSequenceStateGuard keeps coverage for
// DOLT_RESET --hard restoring committed sequence state.
func TestDoltResetHardToRevisionRestoresSequenceStateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard to revision restores sequence state",
			SetUpScript: []string{
				`CREATE TABLE reset_revision_sequence_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO reset_revision_sequence_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset revision sequence value');`,
				`INSERT INTO reset_revision_sequence_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main reset revision sequence value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM reset_revision_sequence_items;`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard', 'HEAD~1');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT COUNT(*) FROM reset_revision_sequence_items;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT nextval('reset_revision_sequence_items_id_seq');`,
					Expected: []sql.Row{{int64(2)}},
				},
			},
		},
	})
}

// TestDoltResetHardToRevisionRestoresTriggerDefinitionRepro reproduces a
// versioned-data correctness bug: DOLT_RESET --hard to an older revision should
// restore committed trigger definitions.
func TestDoltResetHardToRevisionRestoresTriggerDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET hard to revision restores trigger definition",
			SetUpScript: []string{
				`CREATE TABLE reset_revision_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION reset_revision_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset revision trigger table');`,
				`CREATE TRIGGER reset_revision_trigger
					BEFORE INSERT ON reset_revision_trigger_items
					FOR EACH ROW EXECUTE FUNCTION reset_revision_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main reset revision trigger value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'reset_revision_trigger';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('--hard', 'HEAD~1');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'reset_revision_trigger';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `INSERT INTO reset_revision_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM reset_revision_trigger_items;`,
					Expected: []sql.Row{{"plain"}},
				},
			},
		},
	})
}

// TestDoltResetUnstagesFunctionDefinitionRepro reproduces a versioned-data
// correctness bug: DOLT_RESET should unstage function definition changes.
func TestDoltResetUnstagesFunctionDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET unstages function definition",
			SetUpScript: []string{
				`CREATE FUNCTION reset_staged_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_ADD('-A');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 't';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('public.reset_staged_function_value()');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT reset_staged_function_value();`,
					Expected: []sql.Row{{7}},
				},
			},
		},
	})
}

// TestDoltResetUnstagesProcedureDefinitionRepro reproduces a versioned-data
// correctness bug: DOLT_RESET should unstage procedure definition changes.
func TestDoltResetUnstagesProcedureDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET unstages procedure definition",
			SetUpScript: []string{
				`CREATE PROCEDURE reset_staged_procedure_value()
					LANGUAGE SQL
					AS $$ SELECT 1 $$;`,
				`SELECT DOLT_ADD('-A');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 't';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SELECT DOLT_RESET((
							SELECT table_name
							FROM dolt_status
							WHERE staged = 't'
							LIMIT 1
						));`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `CALL reset_staged_procedure_value();`,
				},
			},
		},
	})
}

// TestDoltResetUnstagesViewDefinitionRepro reproduces a versioned-data
// correctness bug: DOLT_RESET should unstage view definition changes.
func TestDoltResetUnstagesViewDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET unstages view definition",
			SetUpScript: []string{
				`CREATE TABLE reset_staged_view_source (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset staged view source');`,
				`CREATE VIEW reset_staged_view AS
					SELECT id FROM reset_staged_view_source;`,
				`SELECT DOLT_ADD('-A');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 't';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SELECT DOLT_RESET((
							SELECT table_name
							FROM dolt_status
							WHERE staged = 't'
							LIMIT 1
						));`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT COUNT(*) FROM reset_staged_view;`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestDoltResetUnstagesMaterializedViewDefinitionRepro reproduces a
// versioned-data correctness bug: DOLT_RESET should unstage materialized view
// definition changes.
func TestDoltResetUnstagesMaterializedViewDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET unstages materialized view definition",
			SetUpScript: []string{
				`CREATE TABLE reset_staged_matview_source (id INT PRIMARY KEY);`,
				`INSERT INTO reset_staged_matview_source VALUES (1);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset staged matview source');`,
				`CREATE MATERIALIZED VIEW reset_staged_matview AS
					SELECT id FROM reset_staged_matview_source;`,
				`SELECT DOLT_ADD('-A');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 't';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query: `SELECT DOLT_RESET((
							SELECT table_name
							FROM dolt_status
							WHERE staged = 't'
							LIMIT 1
						));`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT id FROM reset_staged_matview;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDoltResetUnstagesSequenceDefinitionRepro reproduces a versioned-data
// correctness bug: DOLT_RESET should unstage sequence object changes.
func TestDoltResetUnstagesSequenceDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET unstages sequence definition",
			SetUpScript: []string{
				`CREATE SEQUENCE reset_staged_sequence_value;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_ADD('-A');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 't';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('public.reset_staged_sequence_value');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT nextval('reset_staged_sequence_value');`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestDoltResetUnstagesTriggerDefinitionRepro reproduces a versioned-data
// correctness bug: DOLT_RESET should unstage trigger definition changes.
func TestDoltResetUnstagesTriggerDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RESET unstages trigger definition",
			SetUpScript: []string{
				`CREATE TABLE reset_staged_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION reset_staged_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial reset staged trigger table');`,
				`CREATE TRIGGER reset_staged_trigger
					BEFORE INSERT ON reset_staged_trigger_items
					FOR EACH ROW EXECUTE FUNCTION reset_staged_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_ADD('-A');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 't';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RESET('public.reset_staged_trigger_items.reset_staged_trigger');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status WHERE staged = 'f';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `INSERT INTO reset_staged_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM reset_staged_trigger_items;`,
					Expected: []sql.Row{{"triggered"}},
				},
			},
		},
	})
}

// TestDoltCheckoutRestoresFunctionDefinitionGuard keeps coverage for branch
// checkout restoring function bodies.
func TestDoltCheckoutRestoresFunctionDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CHECKOUT restores function definition",
			SetUpScript: []string{
				`CREATE FUNCTION checkout_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial checkout function value');`,
				`SELECT DOLT_BRANCH('original');`,
				`CREATE OR REPLACE FUNCTION checkout_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main checkout function value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT checkout_function_value();`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT DOLT_CHECKOUT('original');`,
					Expected: []sql.Row{{`{0,"Switched to branch 'original'"}`}},
				},
				{
					Query:    `SELECT checkout_function_value();`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDoltCheckoutRestoresSequenceStateGuard keeps coverage for branch checkout
// restoring sequence state.
func TestDoltCheckoutRestoresSequenceStateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CHECKOUT restores sequence state",
			SetUpScript: []string{
				`CREATE TABLE checkout_sequence_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO checkout_sequence_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial checkout sequence value');`,
				`SELECT DOLT_BRANCH('original');`,
				`INSERT INTO checkout_sequence_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main checkout sequence value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT DOLT_CHECKOUT('original');`,
					Expected: []sql.Row{{`{0,"Switched to branch 'original'"}`}},
				},
				{
					Query:    `SELECT nextval('checkout_sequence_items_id_seq');`,
					Expected: []sql.Row{{int64(2)}},
				},
			},
		},
	})
}

// TestDoltCheckoutRestoresTriggerDefinitionsGuard keeps coverage for branch
// checkout restoring trigger definitions.
func TestDoltCheckoutRestoresTriggerDefinitionsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CHECKOUT restores trigger definitions",
			SetUpScript: []string{
				`CREATE TABLE checkout_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION checkout_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial checkout trigger table');`,
				`SELECT DOLT_BRANCH('original');`,
				`CREATE TRIGGER checkout_trigger_changed
					BEFORE INSERT ON checkout_trigger_items
					FOR EACH ROW EXECUTE FUNCTION checkout_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main checkout trigger value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT DOLT_CHECKOUT('original');`,
					Expected: []sql.Row{{`{0,"Switched to branch 'original'"}`}},
				},
				{
					Query:    `INSERT INTO checkout_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM checkout_trigger_items;`,
					Expected: []sql.Row{{"plain"}},
				},
			},
		},
	})
}

// TestDoltCleanRemovesUncommittedFunctionGuard keeps coverage for DOLT_CLEAN
// removing uncommitted function definitions.
func TestDoltCleanRemovesUncommittedFunctionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CLEAN removes uncommitted function",
			SetUpScript: []string{
				`CREATE FUNCTION clean_uncommitted_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_CLEAN();`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `SELECT clean_uncommitted_function_value();`,
					ExpectedErr: `function: 'clean_uncommitted_function_value' not found`,
				},
			},
		},
	})
}

// TestDoltCleanRemovesUncommittedSequenceGuard keeps coverage for DOLT_CLEAN
// removing uncommitted sequence objects.
func TestDoltCleanRemovesUncommittedSequenceGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CLEAN removes uncommitted sequence",
			SetUpScript: []string{
				`CREATE SEQUENCE clean_uncommitted_sequence_value;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_CLEAN();`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:       `SELECT nextval('clean_uncommitted_sequence_value');`,
					ExpectedErr: `relation "clean_uncommitted_sequence_value" does not exist`,
				},
			},
		},
	})
}

// TestDoltCleanRemovesUncommittedTriggerRepro reproduces a versioned
// persistence bug: DOLT_CLEAN should remove uncommitted trigger definitions
// and leave the working set clean.
func TestDoltCleanRemovesUncommittedTriggerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CLEAN removes uncommitted trigger",
			SetUpScript: []string{
				`CREATE TABLE clean_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION clean_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial clean trigger table');`,
				`CREATE TRIGGER clean_uncommitted_trigger
					BEFORE INSERT ON clean_trigger_items
					FOR EACH ROW EXECUTE FUNCTION clean_trigger_func();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_CLEAN();`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `INSERT INTO clean_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM clean_trigger_items;`,
					Expected: []sql.Row{{"plain"}},
				},
			},
		},
	})
}

// TestDoltCherryPickAppliesFunctionDefinitionRepro reproduces a versioned-data
// correctness bug: cherry-picking a function body change leaves the function in
// a broken callable state.
func TestDoltCherryPickAppliesFunctionDefinitionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CHERRY_PICK applies function definition",
			SetUpScript: []string{
				`CREATE FUNCTION cherry_pick_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial cherry-pick function value');`,
				`SELECT DOLT_BRANCH('original');`,
				`CREATE OR REPLACE FUNCTION cherry_pick_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main cherry-pick function value');`,
				`SELECT DOLT_CHECKOUT('original');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT cherry_pick_function_value();`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT substring(DOLT_CHERRY_PICK('main')::text, 34);`,
					Expected: []sql.Row{{",0,0,0}"}},
				},
				{
					Query:    `SELECT cherry_pick_function_value();`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestDoltCherryPickAdvancesSequenceStateGuard keeps coverage for
// cherry-picking a SERIAL row advancing the sequence root object so the next
// generated id cannot collide.
func TestDoltCherryPickAdvancesSequenceStateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CHERRY_PICK advances sequence state",
			SetUpScript: []string{
				`CREATE TABLE cherry_pick_sequence_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO cherry_pick_sequence_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial cherry-pick sequence value');`,
				`SELECT DOLT_BRANCH('original');`,
				`INSERT INTO cherry_pick_sequence_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main cherry-pick sequence value');`,
				`SELECT DOLT_CHECKOUT('original');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM cherry_pick_sequence_items;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT substring(DOLT_CHERRY_PICK('main')::text, 34);`,
					Expected: []sql.Row{{",0,0,0}"}},
				},
				{
					Query:    `SELECT label FROM cherry_pick_sequence_items WHERE id = 2;`,
					Expected: []sql.Row{{"main"}},
				},
				{
					Query:    `SELECT nextval('cherry_pick_sequence_items_id_seq');`,
					Expected: []sql.Row{{int64(3)}},
				},
			},
		},
	})
}

// TestDoltCherryPickAppliesTriggerDefinitionGuard keeps coverage for
// cherry-picking trigger definition changes.
func TestDoltCherryPickAppliesTriggerDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_CHERRY_PICK applies trigger definition",
			SetUpScript: []string{
				`CREATE TABLE cherry_pick_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION cherry_pick_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial cherry-pick trigger table');`,
				`SELECT DOLT_BRANCH('original');`,
				`CREATE TRIGGER cherry_pick_trigger_changed
					BEFORE INSERT ON cherry_pick_trigger_items
					FOR EACH ROW EXECUTE FUNCTION cherry_pick_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main cherry-pick trigger value');`,
				`SELECT DOLT_CHECKOUT('original');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'cherry_pick_trigger_changed';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT substring(DOLT_CHERRY_PICK('main')::text, 34);`,
					Expected: []sql.Row{{",0,0,0}"}},
				},
				{
					Query:    `INSERT INTO cherry_pick_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM cherry_pick_trigger_items;`,
					Expected: []sql.Row{{"triggered"}},
				},
			},
		},
	})
}

// TestDoltRevertRestoresSequenceStateGuard keeps coverage for reverting a
// commit that inserted a SERIAL row also restoring the sequence root object.
func TestDoltRevertRestoresSequenceStateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_REVERT restores sequence state",
			SetUpScript: []string{
				`CREATE TABLE revert_sequence_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO revert_sequence_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial revert sequence value');`,
				`INSERT INTO revert_sequence_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main revert sequence value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM revert_sequence_items;`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT substring(DOLT_REVERT('HEAD')::text, 34);`,
					Expected: []sql.Row{{",0,0,0}"}},
				},
				{
					Query:    `SELECT COUNT(*) FROM revert_sequence_items;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT nextval('revert_sequence_items_id_seq');`,
					Expected: []sql.Row{{int64(2)}},
				},
			},
		},
	})
}

// TestDoltRevertRemovesTriggerDefinitionGuard keeps coverage for reverting
// trigger definition commits.
func TestDoltRevertRemovesTriggerDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_REVERT removes trigger definition",
			SetUpScript: []string{
				`CREATE TABLE revert_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION revert_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial revert trigger table');`,
				`CREATE TRIGGER revert_trigger_changed
					BEFORE INSERT ON revert_trigger_items
					FOR EACH ROW EXECUTE FUNCTION revert_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main revert trigger value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'revert_trigger_changed';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT substring(DOLT_REVERT('HEAD')::text, 34);`,
					Expected: []sql.Row{{",0,0,0}"}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'revert_trigger_changed';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `INSERT INTO revert_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM revert_trigger_items;`,
					Expected: []sql.Row{{"plain"}},
				},
			},
		},
	})
}

// TestDoltRmRemovesFunctionDefinitionGuard keeps coverage for DOLT_RM removing
// committed function definitions from the working set.
func TestDoltRmRemovesFunctionDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RM removes function definition",
			SetUpScript: []string{
				`CREATE FUNCTION rm_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 7 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial rm function value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT rm_function_value();`,
					Expected: []sql.Row{{7}},
				},
				{
					Query:    `SELECT DOLT_RM('rm_function_value()');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:       `SELECT rm_function_value();`,
					ExpectedErr: `function: 'rm_function_value' not found`,
				},
			},
		},
	})
}

// TestDoltRmRemovesSequenceDefinitionGuard keeps coverage for DOLT_RM removing
// committed sequence objects from the working set.
func TestDoltRmRemovesSequenceDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RM removes sequence definition",
			SetUpScript: []string{
				`CREATE SEQUENCE rm_sequence_value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial rm sequence value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT nextval('rm_sequence_value');`,
					Expected: []sql.Row{{int64(1)}},
				},
				{
					Query:    `SELECT DOLT_RM('rm_sequence_value');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query:       `SELECT nextval('rm_sequence_value');`,
					ExpectedErr: `relation "rm_sequence_value" does not exist`,
				},
			},
		},
	})
}

// TestDoltRmRemovesTriggerDefinitionGuard keeps coverage for DOLT_RM removing
// committed trigger definitions from the working set.
func TestDoltRmRemovesTriggerDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_RM removes trigger definition",
			SetUpScript: []string{
				`CREATE TABLE rm_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION rm_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER rm_trigger_changed
					BEFORE INSERT ON rm_trigger_items
					FOR EACH ROW EXECUTE FUNCTION rm_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial rm trigger value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'rm_trigger_changed';`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT DOLT_RM('rm_trigger_items.rm_trigger_changed');`,
					Expected: []sql.Row{{"{0}"}},
				},
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'rm_trigger_changed';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `INSERT INTO rm_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM rm_trigger_items;`,
					Expected: []sql.Row{{"plain"}},
				},
			},
		},
	})
}

// TestCreateOrReplaceFunctionUpdatesCommittedDefinitionGuard keeps coverage for
// function replacement after the previous definition has been committed.
func TestCreateOrReplaceFunctionUpdatesCommittedDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE FUNCTION updates committed definition",
			SetUpScript: []string{
				`CREATE FUNCTION replace_committed_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial function definition');`,
				`CREATE OR REPLACE FUNCTION replace_committed_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT replace_committed_function_value();`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT COUNT(*) FROM dolt_status;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}
