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

func TestProviderSpecificReplicationBoundaries(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Aurora and RDS replication assumptions are explicit boundaries",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT current_setting('rds.logical_replication', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "provider-replication-boundary-test-testproviderspecificreplicationboundaries-0001-select-current_setting-rds.logical_replication-true"},
				},
				{
					Query: "SHOW rds.logical_replication;", PostgresOracle: ScriptTestPostgresOracle{ID: "provider-replication-boundary-test-testproviderspecificreplicationboundaries-0002-show-rds.logical_replication", Compare: "sqlstate"},
				},
				{
					Query: "SHOW track_commit_timestamp;", PostgresOracle: ScriptTestPostgresOracle{ID: "provider-replication-boundary-test-testproviderspecificreplicationboundaries-0003-show-track_commit_timestamp"},
				},
				{
					Query: "SET track_commit_timestamp TO 'on';", PostgresOracle: ScriptTestPostgresOracle{ID: "provider-replication-boundary-test-testproviderspecificreplicationboundaries-0004-set-track_commit_timestamp-to-on", Compare: "sqlstate"},
				},
				{
					Query: "CREATE EXTENSION pglogical;", PostgresOracle: ScriptTestPostgresOracle{ID: "provider-replication-boundary-test-testproviderspecificreplicationboundaries-0005-create-extension-pglogical", Compare: "sqlstate"},
				},
			},
		},
	})
}
