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
					Query: `SELECT entry FROM audit ORDER BY seq;`,
					Expected: []sql.Row{
						{"BEFORE:STATEMENT:UPDATE"},
						{"AFTER:STATEMENT:UPDATE"},
						{"BEFORE:STATEMENT:DELETE"},
						{"AFTER:STATEMENT:DELETE"},
					},
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
					Query:    `SELECT seen_count, seen_sum FROM audit;`,
					Expected: []sql.Row{{int64(2), int64(30)}},
				},
				{
					Query:    `SELECT marker FROM new_rows;`,
					Expected: []sql.Row{{42}},
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
					Query:       `SELECT count(*) FROM new_rows;`,
					ExpectedErr: "table not found",
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
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`,
					ExpectedErr: "transition tables cannot be specified for triggers with more than one event",
				},
				{
					Query: `CREATE TRIGGER insert_old_transition
						AFTER INSERT ON target
						REFERENCING OLD TABLE AS old_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`,
					ExpectedErr: "OLD TABLE can only be specified for UPDATE or DELETE triggers",
				},
				{
					Query: `CREATE TRIGGER delete_new_transition
						AFTER DELETE ON target
						REFERENCING NEW TABLE AS new_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`,
					ExpectedErr: "NEW TABLE can only be specified for INSERT or UPDATE triggers",
				},
				{
					Query: `CREATE TRIGGER duplicate_name_transition
						AFTER UPDATE ON target
						REFERENCING OLD TABLE AS changed_rows NEW TABLE AS changed_rows
						FOR EACH STATEMENT EXECUTE FUNCTION audit_fn();`,
					ExpectedErr: "OLD TABLE and NEW TABLE transition names must be distinct",
				},
			},
		},
	})
}
