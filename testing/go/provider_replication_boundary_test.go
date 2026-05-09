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

func TestProviderSpecificReplicationBoundaries(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Aurora and RDS replication assumptions are explicit boundaries",
			Assertions: []ScriptTestAssertion{
				{
					Query:    "SELECT current_setting('rds.logical_replication', true);",
					Expected: []sql.Row{{nil}},
				},
				{
					Query:       "SHOW rds.logical_replication;",
					ExpectedErr: `unrecognized configuration parameter "rds.logical_replication"`,
				},
				{
					Query:    "SHOW track_commit_timestamp;",
					Expected: []sql.Row{{int8(0)}},
				},
				{
					Query:       "SET track_commit_timestamp TO 'on';",
					ExpectedErr: "is a read only variable",
				},
				{
					Query:       "CREATE EXTENSION pglogical;",
					ExpectedErr: `extension "pglogical" is not supported by Doltgres`,
				},
			},
		},
	})
}
