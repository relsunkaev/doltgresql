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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestIcuCollationProbe pins the PostgreSQL behavior for ICU
// nondeterministic collations. PG 12+ apps that need case-insensitive
// equality on string columns rely on ICU `deterministic = false`
// collations; those that target locale-correct ordering rely on ICU
// locale providers. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestIcuCollationProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "CREATE COLLATION provider = icu follows PostgreSQL",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE COLLATION case_insensitive (
						provider = icu,
						locale = 'und-u-ks-level2',
						deterministic = false
					);`,

					PostgresOracle: ScriptTestPostgresOracle{ID: "icu-collation-probe-test-testicucollationprobe-0001-create-collation-case_insensitive-provider-=", Cleanup: []string{"DROP COLLATION IF EXISTS case_insensitive;"}},
				},
			},
		},
	})
}
