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

package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestStats(t *testing.T) {
	RunScripts(t, StatsTests)
}

var StatsTests = []ScriptTest{
	{
		Name: "ANALYZE statement",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key);",

			"ANALYZE;",

			"ANALYZE t;"},
		Assertions: []ScriptTestAssertion{

			{
				Query:    "ANALYZE public.t;",
				Expected: []sql.Row{},
			},
			{
				Query:    "ANALYZE postgres.public.t;",
				Expected: []sql.Row{},
			},
			{
				Query:       "ANALYZE doesnotexists.public.t;",
				ExpectedErr: "database not found: doesnotexists",
			},
		},
	},
	{
		Name: "ANALYZE populates dolt statistics",
		SetUpScript: []string{
			"CREATE TABLE stats_provider_plan (id int primary key, tenant int not null, name varchar(10));",
			"CREATE INDEX stats_provider_plan_tenant_name_idx ON stats_provider_plan (tenant, name);",
			"INSERT INTO stats_provider_plan VALUES (1, 1, 'a'), (2, 1, 'a'), (3, 2, 'b'), (4, 2, 'b'), (5, 3, 'c'), (6, 3, 'c');",

			"ANALYZE stats_provider_plan;"},
		Assertions: []ScriptTestAssertion{

			{
				Query: `SELECT index_name, columns, row_count, distinct_count, null_count
FROM dolt_statistics
WHERE table_name = 'stats_provider_plan'
ORDER BY index_name;`,
				Expected: []sql.Row{
					{"primary", "id", uint64(6), uint64(6), uint64(0)},
					{"stats_provider_plan_tenant_name_idx", "tenant,name", uint64(6), uint64(3), uint64(0)},
				},
			},
		},
	},
}
