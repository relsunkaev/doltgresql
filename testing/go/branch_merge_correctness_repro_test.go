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
					Query:    `BEGIN;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'conflicts found') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT "table", num_conflicts FROM dolt_conflicts;`,
					Expected: []sql.Row{{"merge_generated_conflict_items", Numeric("1")}},
				},
				{
					Query: `SELECT base_base_value, base_doubled, our_base_value, our_doubled, their_base_value, their_doubled
						FROM dolt_conflicts_merge_generated_conflict_items;`,
					Expected: []sql.Row{{10, 20, 30, 60, 20, 40}},
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

// TestPreviewMergeConflictsReportsProcedureConflictRepro reproduces a branch
// merge correctness bug: previewing conflicts should include versioned
// procedure definitions, not only table rows.
func TestPreviewMergeConflictsReportsProcedureConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports procedure conflicts",
			SetUpScript: []string{
				`CREATE PROCEDURE preview_procedure_conflict_value(INOUT value INT)
					LANGUAGE plpgsql
					AS $$
					BEGIN
						value := value + 1;
					END;
					$$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial procedure conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE OR REPLACE PROCEDURE preview_procedure_conflict_value(INOUT value INT)
					LANGUAGE plpgsql
					AS $$
					BEGIN
						value := value + 2;
					END;
					$$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main procedure conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE OR REPLACE PROCEDURE preview_procedure_conflict_value(INOUT value INT)
					LANGUAGE plpgsql
					AS $$
					BEGIN
						value := value + 3;
					END;
					$$;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other procedure conflict value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_procedure_conflict_value(integer)'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsTriggerConflictRepro reproduces a branch
// merge correctness bug: previewing conflicts should include versioned trigger
// definitions, not only table rows.
func TestPreviewMergeConflictsReportsTriggerConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports trigger conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_trigger_conflict_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION preview_trigger_conflict_func()
					RETURNS TRIGGER AS $$
					BEGIN
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER preview_trigger_conflict_changed
					BEFORE INSERT ON preview_trigger_conflict_items
					FOR EACH ROW EXECUTE FUNCTION preview_trigger_conflict_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial trigger conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`DROP TRIGGER preview_trigger_conflict_changed
					ON preview_trigger_conflict_items;`,
				`CREATE TRIGGER preview_trigger_conflict_changed
					BEFORE UPDATE ON preview_trigger_conflict_items
					FOR EACH ROW EXECUTE FUNCTION preview_trigger_conflict_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main trigger conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`DROP TRIGGER preview_trigger_conflict_changed
					ON preview_trigger_conflict_items;`,
				`CREATE TRIGGER preview_trigger_conflict_changed
					BEFORE DELETE ON preview_trigger_conflict_items
					FOR EACH ROW EXECUTE FUNCTION preview_trigger_conflict_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other trigger conflict value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_trigger_conflict_items.preview_trigger_conflict_changed'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsViewConflictRepro reproduces a branch merge
// correctness bug: previewing conflicts should include versioned view
// definitions, not only base table rows.
func TestPreviewMergeConflictsReportsViewConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports view conflicts",
			SetUpScript: []string{
				`CREATE VIEW preview_view_conflict_reader AS SELECT 1 AS value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial view conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE OR REPLACE VIEW preview_view_conflict_reader AS SELECT 2 AS value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main view conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE OR REPLACE VIEW preview_view_conflict_reader AS SELECT 3 AS value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other view conflict value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_view_conflict_reader'
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

// TestPreviewMergeConflictsSummaryReportsEnumTypeConflictRepro reproduces a
// branch merge correctness bug: preview summaries should include root-object
// conflicts such as changed enum type definitions.
func TestPreviewMergeConflictsSummaryReportsEnumTypeConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY reports enum type conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_summary_enum_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial enum summary conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TYPE preview_summary_enum_conflict_type AS ENUM ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main enum summary conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE TYPE preview_summary_enum_conflict_type AS ENUM ('other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other enum summary conflict type');`,
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

// TestPreviewMergeConflictsSummaryReportsCompositeTypeConflictRepro reproduces
// a branch merge correctness bug: preview summaries should include root-object
// conflicts such as changed composite type definitions.
func TestPreviewMergeConflictsSummaryReportsCompositeTypeConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY reports composite type conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_summary_composite_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial composite summary conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TYPE preview_summary_composite_conflict_type AS (main_value integer);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main composite summary conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE TYPE preview_summary_composite_conflict_type AS (other_value integer);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other composite summary conflict type');`,
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

// TestPreviewMergeConflictsSummaryReportsDomainConflictRepro reproduces a
// branch merge correctness bug: preview summaries should include root-object
// conflicts such as changed domain definitions.
func TestPreviewMergeConflictsSummaryReportsDomainConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY reports domain conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_summary_domain_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial domain summary conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE DOMAIN preview_summary_domain_conflict_type AS integer;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main domain summary conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE DOMAIN preview_summary_domain_conflict_type AS text;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other domain summary conflict type');`,
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

// TestMergeReportsMaterializedViewDefinitionConflictGuard keeps coverage for
// branch merges that detect incompatible materialized-view definition edits.
func TestMergeReportsMaterializedViewDefinitionConflictGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports materialized view definition conflicts",
			SetUpScript: []string{
				`CREATE MATERIALIZED VIEW merge_matview_conflict_reader AS
					SELECT 1 AS value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial merge matview value');`,
				`SELECT DOLT_BRANCH('other');`,
				`DROP MATERIALIZED VIEW merge_matview_conflict_reader;`,
				`CREATE MATERIALIZED VIEW merge_matview_conflict_reader AS
					SELECT 2 AS value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main merge matview value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`DROP MATERIALIZED VIEW merge_matview_conflict_reader;`,
				`CREATE MATERIALIZED VIEW merge_matview_conflict_reader AS
					SELECT 3 AS value;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other merge matview value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'conflicts found') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestMergeReportsSequenceDefinitionConflictGuard keeps coverage for branch
// merges that detect incompatible sequence-definition edits.
func TestMergeReportsSequenceDefinitionConflictGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports sequence definition conflicts",
			SetUpScript: []string{
				`CREATE SEQUENCE merge_sequence_conflict_seq;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial sequence conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`SELECT nextval('merge_sequence_conflict_seq');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main sequence conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`DROP SEQUENCE merge_sequence_conflict_seq;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other sequence conflict drop');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'conflicts found') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestMergeReportsTriggerDefinitionConflictRepro reproduces a branch merge
// correctness bug: merging branches should report conflicts when both sides
// change the same trigger definition differently.
func TestMergeReportsTriggerDefinitionConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports trigger definition conflicts",
			SetUpScript: []string{
				`CREATE TABLE merge_trigger_conflict_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE FUNCTION merge_trigger_conflict_func()
					RETURNS TRIGGER AS $$
					BEGIN
						RETURN NEW;
					END;
					$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER merge_trigger_conflict_changed
					BEFORE INSERT ON merge_trigger_conflict_items
					FOR EACH ROW EXECUTE FUNCTION merge_trigger_conflict_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial merge trigger value');`,
				`SELECT DOLT_BRANCH('other');`,
				`DROP TRIGGER merge_trigger_conflict_changed ON merge_trigger_conflict_items;`,
				`CREATE TRIGGER merge_trigger_conflict_changed
					BEFORE UPDATE ON merge_trigger_conflict_items
					FOR EACH ROW EXECUTE FUNCTION merge_trigger_conflict_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main merge trigger value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`DROP TRIGGER merge_trigger_conflict_changed ON merge_trigger_conflict_items;`,
				`CREATE TRIGGER merge_trigger_conflict_changed
					BEFORE DELETE ON merge_trigger_conflict_items
					FOR EACH ROW EXECUTE FUNCTION merge_trigger_conflict_func();`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other merge trigger value');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `BEGIN;`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT strpos(DOLT_MERGE('main')::text, 'conflicts found') > 1;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestMergeReportsEnumTypeConflictRepro reproduces a branch merge correctness
// bug: merging branches should report conflicts when both sides create the same
// enum type name differently.
func TestMergeReportsEnumTypeConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports enum type conflicts",
			SetUpScript: []string{
				`CREATE TABLE merge_enum_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial enum merge conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TYPE merge_enum_conflict_type AS ENUM ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main enum merge conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE TYPE merge_enum_conflict_type AS ENUM ('other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other enum merge conflict type');`,
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

// TestMergeReportsCompositeTypeConflictRepro reproduces a branch merge
// correctness bug: merging branches should report conflicts when both sides
// create the same composite type name differently.
func TestMergeReportsCompositeTypeConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports composite type conflicts",
			SetUpScript: []string{
				`CREATE TABLE merge_composite_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial composite merge conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TYPE merge_composite_conflict_type AS (main_value integer);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main composite merge conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE TYPE merge_composite_conflict_type AS (other_value integer);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other composite merge conflict type');`,
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

// TestMergeReportsDomainConflictRepro reproduces a branch merge correctness
// bug: merging branches should report conflicts when both sides create the same
// domain name differently.
func TestMergeReportsDomainConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_MERGE reports domain conflicts",
			SetUpScript: []string{
				`CREATE TABLE merge_domain_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial domain merge conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE DOMAIN merge_domain_conflict_type AS integer CHECK (VALUE > 0);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main domain merge conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE DOMAIN merge_domain_conflict_type AS integer CHECK (VALUE > 10);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other domain merge conflict type');`,
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
				`CREATE SEQUENCE preview_sequence_conflict_seq;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial sequence conflict value');`,
				`SELECT DOLT_BRANCH('other');`,
				`SELECT nextval('preview_sequence_conflict_seq');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main sequence conflict value');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`DROP SEQUENCE preview_sequence_conflict_seq;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other sequence conflict drop');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_sequence_conflict_seq'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsEnumTypeConflictRepro reproduces a branch
// merge correctness bug: previewing conflicts should include versioned enum
// type definitions.
func TestPreviewMergeConflictsReportsEnumTypeConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports enum type conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_enum_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial enum conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TYPE preview_enum_conflict_type AS ENUM ('main');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main enum conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE TYPE preview_enum_conflict_type AS ENUM ('other');`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other enum conflict type');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_enum_conflict_type'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsCompositeTypeConflictRepro reproduces a
// branch merge correctness bug: previewing conflicts should include versioned
// composite type definitions.
func TestPreviewMergeConflictsReportsCompositeTypeConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports composite type conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_composite_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial composite conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE TYPE preview_composite_conflict_type AS (main_value integer);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main composite conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE TYPE preview_composite_conflict_type AS (other_value integer);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other composite conflict type');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_composite_conflict_type'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestPreviewMergeConflictsReportsDomainConflictRepro reproduces a branch merge
// correctness bug: previewing conflicts should include versioned domain
// definitions.
func TestPreviewMergeConflictsReportsDomainConflictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DOLT_PREVIEW_MERGE_CONFLICTS reports domain conflicts",
			SetUpScript: []string{
				`CREATE TABLE preview_domain_conflict_anchor (id INT PRIMARY KEY);`,
				`SELECT DOLT_COMMIT('-A', '-m', 'initial domain conflict anchor');`,
				`SELECT DOLT_BRANCH('other');`,
				`CREATE DOMAIN preview_domain_conflict_type AS integer;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'main domain conflict type');`,
				`SELECT DOLT_CHECKOUT('other');`,
				`CREATE DOMAIN preview_domain_conflict_type AS text;`,
				`SELECT DOLT_COMMIT('-A', '-m', 'other domain conflict type');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM DOLT_PREVIEW_MERGE_CONFLICTS(
							'main',
							'other',
							'preview_domain_conflict_type'
						);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}
