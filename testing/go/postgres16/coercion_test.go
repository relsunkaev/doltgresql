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

func TestCoercion(t *testing.T) {
	RunScriptsWithoutNormalization(t, []ScriptTest{
		{
			Name: "Raw Literals",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 0`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0001-select-0"},
				},
				{
					Query: `SELECT 0.5`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0002-select-0.5"},
				},
				{
					Query: `SELECT 0.50`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0003-select-0.50"},
				},
				{
					Query: `SELECT -0.5`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0004-select-0.5"},
				},
				{
					Query: `SELECT 12345671297673227365.5123624235623456`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0005-select-12345671297673227365.5123624235623456"},
				},
				{
					Query: `SELECT 1`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0006-select-1"},
				},
				{
					Query: `SELECT -1`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0007-select-1"},
				},
				{
					Query: `SELECT 70000`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0008-select-70000"},
				},
				{
					Query: `SELECT 5000000000`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0009-select-5000000000"},
				},
				{
					Query: `SELECT 9223372036854775808`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0010-select-9223372036854775808"},
				},
				{
					Query: `SELECT ''`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0011-select"},
				},
				{
					Query: `SELECT 'test'`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0012-select-test"},
				},
				{
					Query: `SELECT '0'`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0013-select-0"},
				},
			},
		},
		{
			Name: "Math Functions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT abs(1)`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0014-select-abs-1"},
				},
				{
					Query: `SELECT abs(1.5)`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0015-select-abs-1.5"},
				},
				{
					Query: `SELECT abs(5000000000)`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0016-select-abs-5000000000"},
				},
				{
					Query: `SELECT abs(9223372036854775808)`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0017-select-abs-9223372036854775808"},
				},
				{
					Query: `SELECT abs('1')`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0018-select-abs-1"},
				},
				{
					Query: `SELECT abs('1.5')`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0019-select-abs-1.5"},
				},
				{
					Query: `SELECT abs('12345671297673227365.5123624235623456')`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0020-select-abs-12345671297673227365.5123624235623456"},
				},
				{
					Query: `SELECT factorial('1')`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0021-select-factorial-1"},
				},
				{
					Query: `SELECT factorial('1.5')`, PostgresOracle: ScriptTestPostgresOracle{ID: "coercion-test-testcoercion-0022-select-factorial-1.5", Compare: "sqlstate"},
				},
			},
		},
	})
}
