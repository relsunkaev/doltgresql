// Copyright 2025 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
)

func TestParameters(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "default_with_oids",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT default_with_oids;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0001-select-default_with_oids", Compare: "sqlstate"},
				},
				{
					Query: "SET default_with_oids = false;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0002-set-default_with_oids-=-false"},
				},
			},
		},
		{
			Name: "DateStyle",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SHOW DateStyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0003-show-datestyle"},
				},
				{
					Query: "SELECT timestamp '2001/02/04 04:05:06.789';", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0004-select-timestamp-2001/02/04-04:05:06.789"},
				},
				{
					Query: "SET datestyle = 'german';", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0005-set-datestyle-=-german"},
				},
				{
					Query: "SHOW DateStyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0006-show-datestyle"},
				},
				{
					Skip:     true, // TODO: the test passes but pgx cannot parse the result
					Query:    "SELECT timestamp '2001/02/04 04:05:06.789';",
					Expected: []sql.Row{{"04.02.2001 04:05:06.789"}},
				},
				{
					Query: "SET datestyle = 'YMD';", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0008-set-datestyle-=-ymd"},
				},
				{
					Query: "SHOW DateStyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0009-show-datestyle"},
				},
				{
					Query: "SET datestyle = 'sQl';", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0010-set-datestyle-=-sql"},
				},
				{
					Query: "SHOW DateStyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0011-show-datestyle"},
				},
				{
					Skip:     true, // TODO: the test passes but pgx cannot parse the result
					Query:    "SELECT timestamp '2001/02/04 04:05:06.789';",
					Expected: []sql.Row{{"02/04/2001 04:05:06.789"}},
				},
				{
					Query: "SET datestyle = 'postgreS';", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0013-set-datestyle-=-postgres"},
				},
				{
					Query: "SHOW DateStyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0014-show-datestyle"},
				},
				{
					Skip:     true, // TODO: the test passes but pgx cannot parse the result
					Query:    "SELECT timestamp '2001/02/04 04:05:06.789';",
					Expected: []sql.Row{{"Sun Feb 04 04:05:06.789 2001"}},
				},
				{
					Query: "RESET datestyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0016-reset-datestyle"},
				},
				{
					Query: "SHOW DateStyle;", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0017-show-datestyle"},
				},
				{
					Query: "SET datestyle = 'unknown';", PostgresOracle: ScriptTestPostgresOracle{ID: "parameters-test-testparameters-0018-set-datestyle-=-unknown", Compare: "sqlstate"},
				},
			},
		},
	})
}
