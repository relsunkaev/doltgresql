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
						ORDER BY source_id;`,
					Expected: []sql.Row{{1, "alpha"}, {2, "beta"}},
				},
			},
		},
	})
}
