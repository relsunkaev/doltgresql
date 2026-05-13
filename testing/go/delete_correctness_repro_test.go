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

// TestDeleteUsingDeletesJoinedRowsRepro reproduces a PostgreSQL compatibility
// correctness bug: DELETE ... USING should delete target rows that match joined
// relations.
func TestDeleteUsingDeletesJoinedRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE USING deletes joined target rows",
			SetUpScript: []string{
				`CREATE TABLE delete_using_items (
					id INT PRIMARY KEY,
					group_id INT,
					label TEXT
				);`,
				`CREATE TABLE delete_using_groups (
					group_id INT PRIMARY KEY,
					name TEXT,
					should_delete BOOL
				);`,
				`INSERT INTO delete_using_items VALUES
					(1, 10, 'keep-a'),
					(2, 20, 'delete-b'),
					(3, 20, 'delete-c');`,
				`INSERT INTO delete_using_groups VALUES
					(10, 'keepers', false),
					(20, 'doomed', true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM delete_using_items AS i
						USING delete_using_groups AS g
						WHERE i.group_id = g.group_id
							AND g.should_delete
						RETURNING i.id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "delete-correctness-repro-test-testdeleteusingdeletesjoinedrowsrepro-0001-delete-from-delete_using_items-as-i"},
				},
			},
		},
	})
}
