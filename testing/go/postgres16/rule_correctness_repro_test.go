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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestCreateRuleDoAlsoExecutesAuditInsertRepro reproduces a data consistency
// bug: PostgreSQL rewrite rules can add side-effect writes to a DML statement.
func TestCreateRuleDoAlsoExecutesAuditInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE RULE DO ALSO executes audit insert",
			SetUpScript: []string{
				`CREATE TABLE rule_source_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE rule_audit_items (
					source_id INT,
					label TEXT
				);`,
				`CREATE RULE rule_source_items_audit AS
					ON INSERT TO rule_source_items
					DO ALSO
					INSERT INTO rule_audit_items VALUES (NEW.id, NEW.label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO rule_source_items VALUES (1, 'alpha'), (2, 'beta');`,
				},
				{
					Query: `SELECT source_id, label
						FROM rule_audit_items
						ORDER BY source_id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "rule-correctness-repro-test-testcreateruledoalsoexecutesauditinsertrepro-0001-select-source_id-label-from-rule_audit_items"},
				},
			},
		},
	})
}

// TestDropRuleIfExistsMissingRepro reproduces a rewrite-rule compatibility gap:
// PostgreSQL accepts DROP RULE IF EXISTS for an absent rule on an existing table.
func TestDropRuleIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP RULE IF EXISTS missing rule succeeds",
			SetUpScript: []string{
				`CREATE TABLE drop_missing_rule_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP RULE IF EXISTS missing_rule_repro ON drop_missing_rule_target;`,
				},
			},
		},
	})
}

// TestDropRuleIfExistsRemovesExistingRuleRepro reproduces a data consistency
// bug: CREATE RULE is implemented through a trigger rewrite, but DROP RULE IF
// EXISTS is currently converted to a no-op, so the side-effect trigger remains.
func TestDropRuleIfExistsRemovesExistingRuleRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP RULE IF EXISTS removes existing rule side effects",
			SetUpScript: []string{
				`CREATE TABLE drop_rule_source_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE TABLE drop_rule_audit_items (
					source_id INT,
					label TEXT
				);`,
				`CREATE RULE drop_rule_source_items_audit AS
					ON INSERT TO drop_rule_source_items
					DO ALSO
					INSERT INTO drop_rule_audit_items VALUES (NEW.id, NEW.label);`,
				`DROP RULE IF EXISTS drop_rule_source_items_audit
					ON drop_rule_source_items;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO drop_rule_source_items VALUES (1, 'after drop');`,
				},
				{
					Query: `SELECT COUNT(*) FROM drop_rule_audit_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "rule-correctness-repro-test-testdropruleifexistsremovesexistingrulerepro-0001-select-count-*-from-drop_rule_audit_items"},
				},
			},
		},
	})
}

// TestAlterRuleMissingReachesValidationRepro reproduces a rewrite-rule
// compatibility gap: PostgreSQL parses ALTER RULE and validates that the target
// rule exists on the relation.
func TestAlterRuleMissingReachesValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER RULE missing target reaches validation",
			SetUpScript: []string{
				`CREATE TABLE alter_missing_rule_target (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER RULE missing_rule_repro ON alter_missing_rule_target RENAME TO renamed_rule_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "rule-correctness-repro-test-testalterrulemissingreachesvalidationrepro-0001-alter-rule-missing_rule_repro-on-alter_missing_rule_target", Compare: "sqlstate"},
				},
			},
		},
	})
}
