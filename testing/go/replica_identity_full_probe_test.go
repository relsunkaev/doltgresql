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

// TestReplicaIdentityFullProbe pins how `ALTER TABLE ... REPLICA
// IDENTITY FULL` is handled today. Logical-replication consumers
// (Electric `replica: "full"`, Debezium `REPLICA IDENTITY FULL`)
// require this DDL to land so the WAL stream carries pre-image
// tuples. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestReplicaIdentityFullProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ... REPLICA IDENTITY FULL keyword acceptance",
			SetUpScript: []string{
				`CREATE TABLE shapes (id INT PRIMARY KEY, label TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE shapes REPLICA IDENTITY FULL;`,
				},
			},
		},
		{
			Name: "ALTER TABLE ... REPLICA IDENTITY DEFAULT keyword acceptance",
			SetUpScript: []string{
				`CREATE TABLE other (id INT PRIMARY KEY, v TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE other REPLICA IDENTITY DEFAULT;`,
				},
			},
		},
	})
}
