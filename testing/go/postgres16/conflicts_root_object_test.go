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
)

func TestConflictsRootObjectPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name:        `Function delete "definition" conflict without modification`,
				SetUpScript: []string{`CREATE FUNCTION interpreted_example(input TEXT) RETURNS TEXT AS $$ BEGIN RETURN '1' || input; END; $$ LANGUAGE plpgsql;`},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT interpreted_example('12');", PostgresOracle: ScriptTestPostgresOracle{ID: "conflicts-root-object-test-testconflictsrootobject-0001-select-interpreted_example-12"},
					},
				},
			},
			{
				Name:        `Function update "definition" with custom body`,
				SetUpScript: []string{`CREATE FUNCTION interpreted_example(input TEXT) RETURNS TEXT AS $$ BEGIN RETURN '1' || input; END; $$ LANGUAGE plpgsql;`},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT interpreted_example('12');", PostgresOracle: ScriptTestPostgresOracle{ID: "conflicts-root-object-test-testconflictsrootobject-0021-select-interpreted_example-12"},
					},
				},
			},
			{
				Name:        `Function update "definition" with "theirs" body`,
				SetUpScript: []string{`CREATE FUNCTION interpreted_example(input TEXT) RETURNS TEXT AS $$ BEGIN RETURN '1' || input; END; $$ LANGUAGE plpgsql;`},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT interpreted_example('12');", PostgresOracle: ScriptTestPostgresOracle{ID: "conflicts-root-object-test-testconflictsrootobject-0042-select-interpreted_example-12"},
					},
				},
			},
			{
				Name:        `Function update "definition" with "ancestor" body`,
				SetUpScript: []string{`CREATE FUNCTION interpreted_example(input TEXT) RETURNS TEXT AS $$ BEGIN RETURN '1' || input; END; $$ LANGUAGE plpgsql;`},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT interpreted_example('12');", PostgresOracle: ScriptTestPostgresOracle{ID: "conflicts-root-object-test-testconflictsrootobject-0063-select-interpreted_example-12"},
					},
				},
			},
			{
				Name:        `Function update "return_type" with custom type`,
				SetUpScript: []string{`CREATE FUNCTION interpreted_example(input TEXT) RETURNS INT4 AS $$ BEGIN RETURN input || ''; END; $$ LANGUAGE plpgsql;`},
				Assertions: []ScriptTestAssertion{
					{
						Query: "SELECT interpreted_example('12');", PostgresOracle: ScriptTestPostgresOracle{ID: "conflicts-root-object-test-testconflictsrootobject-0084-select-interpreted_example-12"},
					},
				},
			},
		},
	)
}
