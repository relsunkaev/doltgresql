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
)

// TestDropTableAcceptsRestrict guards that PostgreSQL
// accepts explicit RESTRICT on DROP TABLE, where it is the default dependency
// behavior.
func TestDropTableAcceptsRestrict(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE accepts explicit RESTRICT",
			SetUpScript: []string{
				`CREATE TABLE drop_table_restrict_items (id INT PRIMARY KEY);`,
				`INSERT INTO drop_table_restrict_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE drop_table_restrict_items RESTRICT;`,
				},
				{
					Query: `CREATE TABLE drop_table_restrict_items (id INT PRIMARY KEY);`,
				},
			},
		},
	})
}

// TestDropViewAcceptsRestrict guards that PostgreSQL
// accepts explicit RESTRICT on DROP VIEW, where it is the default dependency
// behavior.
func TestDropViewAcceptsRestrict(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW accepts explicit RESTRICT",
			SetUpScript: []string{
				`CREATE VIEW drop_view_restrict_items AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP VIEW drop_view_restrict_items RESTRICT;`,
				},
				{
					Query: `CREATE VIEW drop_view_restrict_items AS SELECT 2 AS id;`,
				},
			},
		},
	})
}

// TestDropMaterializedViewAcceptsRestrict guards that
// PostgreSQL accepts explicit RESTRICT on DROP MATERIALIZED VIEW, where it is
// the default dependency behavior.
func TestDropMaterializedViewAcceptsRestrict(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP MATERIALIZED VIEW accepts explicit RESTRICT",
			SetUpScript: []string{
				`CREATE TABLE drop_matview_restrict_source (id INT PRIMARY KEY);`,
				`INSERT INTO drop_matview_restrict_source VALUES (1);`,
				`CREATE MATERIALIZED VIEW drop_matview_restrict_items AS
					SELECT id FROM drop_matview_restrict_source;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP MATERIALIZED VIEW drop_matview_restrict_items RESTRICT;`,
				},
				{
					Query: `CREATE MATERIALIZED VIEW drop_matview_restrict_items AS
						SELECT 2 AS id;`,
				},
			},
		},
	})
}

// TestDropTriggerAcceptsRestrict guards that PostgreSQL
// accepts explicit RESTRICT on DROP TRIGGER, where it is the default dependency
// behavior.
func TestDropTriggerAcceptsRestrict(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TRIGGER accepts explicit RESTRICT",
			SetUpScript: []string{
				`CREATE TABLE drop_trigger_restrict_items (id INT PRIMARY KEY);`,
				`CREATE TABLE drop_trigger_restrict_audit (id INT PRIMARY KEY);`,
				`CREATE FUNCTION drop_trigger_restrict_log() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO drop_trigger_restrict_audit VALUES (NEW.id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER drop_trigger_restrict_before_insert
					BEFORE INSERT ON drop_trigger_restrict_items
					FOR EACH ROW EXECUTE FUNCTION drop_trigger_restrict_log();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TRIGGER drop_trigger_restrict_before_insert
						ON drop_trigger_restrict_items RESTRICT;`,
				},
				{
					Query: `INSERT INTO drop_trigger_restrict_items VALUES (1);`,
				},
				{
					Query: `SELECT COUNT(*) FROM drop_trigger_restrict_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-restrict-correctness-repro-test-testdroptriggeracceptsrestrict-0001-select-count-*-from-drop_trigger_restrict_audit"},
				},
			},
		},
	})
}

// TestDropViewCascadeRepro reproduces a dependency correctness bug:
// PostgreSQL's CASCADE option drops views that depend on the dropped view.
func TestDropViewCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP VIEW CASCADE drops dependent views",
			SetUpScript: []string{
				`CREATE VIEW drop_view_cascade_base AS SELECT 1 AS id;`,
				`CREATE VIEW drop_view_cascade_reader AS
					SELECT id FROM drop_view_cascade_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP VIEW drop_view_cascade_base CASCADE;`,
				},
				{
					Query: `CREATE VIEW drop_view_cascade_reader AS SELECT 2 AS id;`,
				},
			},
		},
	})
}

// TestDropMaterializedViewCascadeRepro reproduces a dependency correctness bug:
// PostgreSQL's CASCADE option drops views that depend on the dropped
// materialized view.
func TestDropMaterializedViewCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP MATERIALIZED VIEW CASCADE drops dependent views",
			SetUpScript: []string{
				`CREATE TABLE drop_matview_cascade_source (id INT PRIMARY KEY);`,
				`INSERT INTO drop_matview_cascade_source VALUES (1);`,
				`CREATE MATERIALIZED VIEW drop_matview_cascade_base AS
					SELECT id FROM drop_matview_cascade_source;`,
				`CREATE VIEW drop_matview_cascade_reader AS
					SELECT id FROM drop_matview_cascade_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP MATERIALIZED VIEW drop_matview_cascade_base CASCADE;`,
				},
				{
					Query: `CREATE VIEW drop_matview_cascade_reader AS SELECT 2 AS id;`,
				},
			},
		},
	})
}

// TestDropTriggerAcceptsCascade guards that PostgreSQL
// accepts explicit CASCADE on DROP TRIGGER.
func TestDropTriggerAcceptsCascade(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TRIGGER accepts explicit CASCADE",
			SetUpScript: []string{
				`CREATE TABLE drop_trigger_cascade_items (id INT PRIMARY KEY);`,
				`CREATE TABLE drop_trigger_cascade_audit (id INT PRIMARY KEY);`,
				`CREATE FUNCTION drop_trigger_cascade_log() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO drop_trigger_cascade_audit VALUES (NEW.id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER drop_trigger_cascade_before_insert
					BEFORE INSERT ON drop_trigger_cascade_items
					FOR EACH ROW EXECUTE FUNCTION drop_trigger_cascade_log();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TRIGGER drop_trigger_cascade_before_insert
						ON drop_trigger_cascade_items CASCADE;`,
				},
				{
					Query: `INSERT INTO drop_trigger_cascade_items VALUES (1);`,
				},
				{
					Query: `SELECT COUNT(*) FROM drop_trigger_cascade_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-restrict-correctness-repro-test-testdroptriggeracceptscascade-0001-select-count-*-from-drop_trigger_cascade_audit"},
				},
			},
		},
	})
}

// TestDropFunctionCascadeRepro reproduces a dependency correctness bug:
// PostgreSQL's CASCADE option drops objects that depend on the dropped
// function.
func TestDropFunctionCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP FUNCTION CASCADE drops dependent triggers",
			SetUpScript: []string{
				`CREATE TABLE drop_function_cascade_items (id INT PRIMARY KEY);`,
				`CREATE TABLE drop_function_cascade_audit (id INT PRIMARY KEY);`,
				`CREATE FUNCTION drop_function_cascade_log() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO drop_function_cascade_audit VALUES (NEW.id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER drop_function_cascade_before_insert
					BEFORE INSERT ON drop_function_cascade_items
					FOR EACH ROW EXECUTE FUNCTION drop_function_cascade_log();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP FUNCTION drop_function_cascade_log() CASCADE;`,
				},
				{
					Query: `INSERT INTO drop_function_cascade_items VALUES (1);`,
				},
				{
					Query: `SELECT COUNT(*) FROM drop_function_cascade_audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-restrict-correctness-repro-test-testdropfunctioncascaderepro-0001-select-count-*-from-drop_function_cascade_audit"},
				},
			},
		},
	})
}

// TestDropProcedureCascadeRepro reproduces a DDL correctness bug: PostgreSQL
// accepts explicit CASCADE on DROP PROCEDURE.
func TestDropProcedureAcceptsCascade(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP PROCEDURE accepts explicit CASCADE",
			SetUpScript: []string{
				`CREATE TABLE drop_procedure_cascade_audit (id INT PRIMARY KEY);`,
				`CREATE PROCEDURE drop_procedure_cascade_proc()
					LANGUAGE SQL
					AS $$ INSERT INTO drop_procedure_cascade_audit VALUES (1) $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP PROCEDURE drop_procedure_cascade_proc() CASCADE;`,
				},
				{
					Query: `SELECT COUNT(*)
						FROM pg_proc
						WHERE proname = 'drop_procedure_cascade_proc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-restrict-correctness-repro-test-testdropprocedureacceptscascade-0001-select-count-*-from-pg_proc"},
				},
			},
		},
	})
}

// TestTruncateAcceptsRestrict guards that PostgreSQL
// accepts explicit RESTRICT on TRUNCATE, where it is the default dependency
// behavior.
func TestTruncateAcceptsRestrict(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE accepts explicit RESTRICT",
			SetUpScript: []string{
				`CREATE TABLE truncate_restrict_items (id INT PRIMARY KEY);`,
				`INSERT INTO truncate_restrict_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `TRUNCATE truncate_restrict_items RESTRICT;`,
				},
				{
					Query: `SELECT COUNT(*) FROM truncate_restrict_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "drop-restrict-correctness-repro-test-testtruncateacceptsrestrict-0001-select-count-*-from-truncate_restrict_items"},
				},
			},
		},
	})
}
