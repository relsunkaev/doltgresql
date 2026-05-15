// Copyright 2024 Dolthub, Inc.
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

func TestStatsPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "ANALYZE statement",
				SetUpScript: []string{
					"CREATE TABLE t (pk int primary key);",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "ANALYZE;", PostgresOracle: ScriptTestPostgresOracle{ID: "stats-test-teststats-0001-analyze"},
					},
					{
						Query: "ANALYZE t;", PostgresOracle: ScriptTestPostgresOracle{ID: "stats-test-teststats-0002-analyze-t"},
					},
				},
			},
			{
				Name: "ANALYZE populates dolt statistics",
				SetUpScript: []string{
					"CREATE TABLE stats_provider_plan (id int primary key, tenant int not null, name varchar(10));",
					"CREATE INDEX stats_provider_plan_tenant_name_idx ON stats_provider_plan (tenant, name);",
					"INSERT INTO stats_provider_plan VALUES (1, 1, 'a'), (2, 1, 'a'), (3, 2, 'b'), (4, 2, 'b'), (5, 3, 'c'), (6, 3, 'c');",
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: "ANALYZE stats_provider_plan;", PostgresOracle: ScriptTestPostgresOracle{ID: "stats-test-teststats-0006-analyze-stats_provider_plan"},
					},
				},
			},
		},
	)
}
