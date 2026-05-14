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

func TestStatementTriggerChecklistGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "zero-row update and delete statement triggers fire once",
			SetUpScript: []string{
				`CREATE TABLE target (id INT PRIMARY KEY, v INT);`,
				`INSERT INTO target VALUES (1, 10);`,
				`CREATE TABLE audit (seq SERIAL PRIMARY KEY, entry TEXT);`,
				`CREATE FUNCTION audit_statement_fire() RETURNS trigger AS $$
				BEGIN
					INSERT INTO audit (entry) VALUES (TG_WHEN || ':' || TG_LEVEL || ':' || TG_OP);
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER before_update BEFORE UPDATE ON target FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_fire();`,
				`CREATE TRIGGER after_update AFTER UPDATE ON target FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_fire();`,
				`CREATE TRIGGER before_delete BEFORE DELETE ON target FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_fire();`,
				`CREATE TRIGGER after_delete AFTER DELETE ON target FOR EACH STATEMENT EXECUTE FUNCTION audit_statement_fire();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE target SET v = 20 WHERE id = 999;`,
				},
				{
					Query: `DELETE FROM target WHERE id = 999;`,
				},
				{
					Query: `SELECT entry FROM audit ORDER BY seq;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-teststatementtriggerchecklistguard-0001-select-entry-from-audit-order"},
				},
			},
		},
		{
			Name: "transition table shadows and restores existing temporary table",
			SetUpScript: []string{
				`CREATE TABLE target (id INT PRIMARY KEY, v INT);`,
				`CREATE TEMPORARY TABLE new_rows (marker INT);`,
				`INSERT INTO new_rows VALUES (42);`,
				`CREATE TABLE audit (seen_count BIGINT, seen_sum BIGINT);`,
				`CREATE FUNCTION audit_transition_shadow() RETURNS trigger AS $$
				BEGIN
					INSERT INTO audit
					VALUES ((SELECT count(*) FROM new_rows), (SELECT coalesce(sum(v), 0) FROM new_rows));
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_insert_transition
					AFTER INSERT ON target
					REFERENCING NEW TABLE AS new_rows
					FOR EACH STATEMENT EXECUTE FUNCTION audit_transition_shadow();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO target VALUES (1, 10), (2, 20);`,
				},
				{
					Query: `SELECT seen_count, seen_sum FROM audit;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-teststatementtriggerchecklistguard-0002-select-seen_count-seen_sum-from-audit"},
				},
				{
					Query: `SELECT marker FROM new_rows;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-teststatementtriggerchecklistguard-0003-select-marker-from-new_rows"},
				},
			},
		},
		{
			Name: "transition table is dropped after trigger function returns",
			SetUpScript: []string{
				`CREATE TABLE target (id INT PRIMARY KEY, v INT);`,
				`CREATE FUNCTION audit_transition_scope() RETURNS trigger AS $$
				BEGIN
					PERFORM count(*) FROM new_rows;
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER after_insert_transition
					AFTER INSERT ON target
					REFERENCING NEW TABLE AS new_rows
					FOR EACH STATEMENT EXECUTE FUNCTION audit_transition_scope();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO target VALUES (1, 10);`,
				},
				{
					Query: `SELECT count(*) FROM new_rows;`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-teststatementtriggerchecklistguard-0004-select-count-*-from-new_rows"},
				},
			},
		},
	})
}

func TestTransitionTableValidationChecklistGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "transition table trigger validation",
			SetUpScript: []string{
				`CREATE TABLE target (id INT PRIMARY KEY, v INT);`,
				`CREATE FUNCTION audit_fn() RETURNS trigger AS $$
				BEGIN
					RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TRIGGER multi_event_transition
						AFTER INSERT OR UPDATE ON target
						REFERENCING NEW TABLE AS new_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-testtransitiontablevalidationchecklistguard-0001-create-trigger-multi_event_transition-after-insert", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TRIGGER insert_old_transition
						AFTER INSERT ON target
						REFERENCING OLD TABLE AS old_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-testtransitiontablevalidationchecklistguard-0002-create-trigger-insert_old_transition-after-insert", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TRIGGER delete_new_transition
						AFTER DELETE ON target
						REFERENCING NEW TABLE AS new_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-testtransitiontablevalidationchecklistguard-0003-create-trigger-delete_new_transition-after-delete", Compare: "sqlstate"},
				},
				{
					Query: `CREATE TRIGGER duplicate_name_transition
						AFTER UPDATE ON target
						REFERENCING OLD TABLE AS changed_rows NEW TABLE AS changed_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`, PostgresOracle: ScriptTestPostgresOracle{ID: "trigger-checklist-guard-test-testtransitiontablevalidationchecklistguard-0004-create-trigger-duplicate_name_transition-after-update", Compare: "sqlstate"},
				},
			},
		},
	})
}
