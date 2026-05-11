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

// TestCreateTablePartitionByPersistsPartitionMetadataRepro reproduces a
// correctness bug: Doltgres accepts PARTITION BY but creates an ordinary table
// instead of persisting PostgreSQL's partitioned-table catalog metadata.
func TestCreateTablePartitionByPersistsPartitionMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE PARTITION BY persists partition metadata",
			SetUpScript: []string{
				`CREATE TABLE partition_catalog_parent (
					id INT,
					label TEXT
				) PARTITION BY LIST (id);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relkind, pg_get_partkeydef(oid)
						FROM pg_catalog.pg_class
						WHERE oid = 'partition_catalog_parent'::regclass;`,
					Expected: []sql.Row{{"p", "LIST (id)"}},
				},
			},
		},
	})
}

// TestPartitionedTableWithoutPartitionRejectsInsertRepro reproduces a data
// correctness bug: PostgreSQL rejects inserts into a partitioned table with no
// matching partition, but Doltgres stores the row in the ordinary table it
// created after ignoring PARTITION BY.
func TestPartitionedTableWithoutPartitionRejectsInsertRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "partitioned table with no partition rejects insert",
			SetUpScript: []string{
				`CREATE TABLE partition_insert_parent (
					id INT,
					label TEXT
				) PARTITION BY LIST (id);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO partition_insert_parent VALUES (1, 'a');`,
					ExpectedErr: `no partition of relation`,
				},
			},
		},
	})
}
