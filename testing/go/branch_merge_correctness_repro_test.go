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

// TestMergePreservesGeneratedColumnValuesRepro guards branch-merge correctness:
// merging non-conflicting rows in a table with a stored generated column should
// preserve generated values for all rows.
func TestMergePreservesGeneratedColumnValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE preserves generated column values",
			SetUpScript: []string{
				`CREATE TABLE merge_generated_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO merge_generated_items (id, base_value) VALUES (1, 10);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial generated row');`,
				`SELECT DOLT_BRANCH('other');`,
				`INSERT INTO merge_generated_items (id, base_value) VALUES (2, 20);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main generated row');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`INSERT INTO merge_generated_items (id, base_value) VALUES (3, 30);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other generated row');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'merge successful') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `SELECT id, base_value, doubled
						FROM merge_generated_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10, 20},
						{2, 20, 40},
						{3, 30, 60},
					},
				},
			},
		},
	})
}

// TestMergeGeneratedColumnConflictReportsConflictRepro reproduces a branch
// merge correctness bug: generated columns participate in the stored row shape,
// but conflicting edits should still report a normal merge conflict.
func TestMergeGeneratedColumnConflictReportsConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports generated column conflicts",
			SetUpScript: []string{
				`CREATE TABLE merge_generated_conflict_items (
					id INT PRIMARY KEY,
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`INSERT INTO merge_generated_conflict_items (id, base_value) VALUES (1, 10);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial generated conflict row');`,
				`SELECT DOLT_BRANCH('other');`,
				`UPDATE merge_generated_conflict_items SET base_value = 20 WHERE id = 1;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main generated conflict row');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`UPDATE merge_generated_conflict_items SET base_value = 30 WHERE id = 1;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other generated conflict row');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'conflicts found') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT table_name, num_conflicts FROM dolt_conflicts;`,
					Expected: []sql.Row{{"merge_generated_conflict_items", Numeric("1")}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsFunctionConflictRepro reproduces a branch
// merge correctness bug: previewing conflicts should include versioned
// function definitions, not only table rows.
func TestPreviewMergeConflictsReportsFunctionConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports function conflicts",
			SetUpScript: []string{
				`CREATE FUNCTION preview_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial function conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE OR REPLACE FUNCTION preview_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main function conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE OR REPLACE FUNCTION preview_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 3 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other function conflict value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_conflict_value()'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsSummaryReportsFunctionConflictRepro reproduces a
// branch merge correctness bug: preview summaries should include root-object
// conflicts such as changed function definitions.
func TestPreviewMergeConflictsSummaryReportsFunctionConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY reports function conflicts",
			SetUpScript: []string{
				`CREATE FUNCTION preview_summary_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial function summary value');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE OR REPLACE FUNCTION preview_summary_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main function summary value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE OR REPLACE FUNCTION preview_summary_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 3 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other function summary value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY('main', 'other');`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestMergeReportsFunctionDefinitionConflictGuard keeps coverage for branch
// merges that detect incompatible function-definition edits.
func TestMergeReportsFunctionDefinitionConflictGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports function definition conflicts",
			SetUpScript: []string{
				`CREATE FUNCTION merge_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial merge function value');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE OR REPLACE FUNCTION merge_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main merge function value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE OR REPLACE FUNCTION merge_conflict_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 3 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other merge function value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'conflicts found') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestMergeAppliesFunctionDefinitionGuard keeps coverage for non-conflicting
// branch merges that apply function-definition changes.
func TestMergeAppliesFunctionDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE applies function definition",
			SetUpScript: []string{
				`CREATE TABLE merge_function_notes (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION merge_apply_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 1 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial merge function apply');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE OR REPLACE FUNCTION merge_apply_function_value()
					RETURNS INT LANGUAGE SQL AS $$ SELECT 2 $$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main merge function apply');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`INSERT INTO merge_function_notes VALUES (1, 'other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other merge function note');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT merge_apply_function_value();`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'merge successful') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT merge_apply_function_value();`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT label FROM merge_function_notes;`,
					Expected: []sql.Row{{"other"}},
				},
			},
		},
	})
}

// TestMergeAdvancesSequenceStateGuard keeps coverage for non-conflicting
// branch merges that apply sequence state changes.
func TestMergeAdvancesSequenceStateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE advances sequence state",
			SetUpScript: []string{
				`CREATE TABLE merge_sequence_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE merge_sequence_notes (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO merge_sequence_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial merge sequence apply');`,
				`SELECT DOLT_BRANCH('other');`,
				`INSERT INTO merge_sequence_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main merge sequence apply');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`INSERT INTO merge_sequence_notes VALUES (1, 'other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other merge sequence note');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT COUNT(*) FROM merge_sequence_items;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'merge successful') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT label FROM merge_sequence_items WHERE id = 2;`,
					Expected: []sql.Row{{"main"}},
				},
				{
					Query:    `SELECT nextval('merge_sequence_items_id_seq');`,
					Expected: []sql.Row{{int64(3)}},
				},
			},
		},
	})
}

// TestMergeAppliesTriggerDefinitionGuard keeps coverage for non-conflicting
// branch merges that apply trigger-definition changes.
func TestMergeAppliesTriggerDefinitionGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE applies trigger definition",
			SetUpScript: []string{
				`CREATE TABLE merge_trigger_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE merge_trigger_notes (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION merge_apply_trigger_func()
					RETURNS TRIGGER AS $$
					BEGIN
						NEW.label := 'triggered';
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial merge trigger apply');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TRIGGER merge_trigger_changed
					BEFORE INSERT ON merge_trigger_items
					FOR EACH ROW EXECUTE FUNCTION merge_apply_trigger_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main merge trigger apply');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`INSERT INTO merge_trigger_notes VALUES (1, 'other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other merge trigger note');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM information_schema.triggers
						WHERE trigger_name = 'merge_trigger_changed';`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'merge successful') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `INSERT INTO merge_trigger_items VALUES (1, 'plain');`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT label FROM merge_trigger_items;`,
					Expected: []sql.Row{{"triggered"}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsSequenceConflictRepro reproduces a branch
// merge correctness bug: previewing conflicts should include versioned sequence
// definitions.
func TestPreviewMergeConflictsReportsSequenceConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports sequence conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_sequence_conflict_items (
					id SERIAL PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO preview_sequence_conflict_items (label) VALUES ('base');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial sequence conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`INSERT INTO preview_sequence_conflict_items (label) VALUES ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main sequence conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`INSERT INTO preview_sequence_conflict_items (label) VALUES ('other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other sequence conflict value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_sequence_conflict_items_id_seq'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}
