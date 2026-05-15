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

func TestDoltResetHardRemovesUncommittedCompositeTypeReproPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "DOLT_RESET hard removes uncommitted composite type",
				SetUpScript: []string{
					`CREATE TYPE reset_uncommitted_composite AS (id integer);`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT COUNT(*)
											FROM pg_catalog.pg_type
											WHERE typname = 'reset_uncommitted_composite';`, PostgresOracle: ScriptTestPostgresOracle{ID: "dolt-versioning-correctness-repro-test-testdoltresethardremovesuncommittedcompositetyperepro-0001-select-count-*-from-pg_catalog.pg_type"},
					},
				},
			},
		},
	)
}

func TestDoltResetHardRemovesUncommittedDomainReproPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "DOLT_RESET hard removes uncommitted domain",
				SetUpScript: []string{
					`CREATE DOMAIN reset_uncommitted_domain AS integer;`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT COUNT(*)
											FROM pg_catalog.pg_type
											WHERE typname = 'reset_uncommitted_domain';`, PostgresOracle: ScriptTestPostgresOracle{ID: "dolt-versioning-correctness-repro-test-testdoltresethardremovesuncommitteddomainrepro-0001-select-count-*-from-pg_catalog.pg_type"},
					},
				},
			},
		},
	)
}

func TestDoltResetHardRemovesUncommittedEnumTypeReproPostgresOraclePrefix(t *testing.T) {
	RunScripts(
		t,
		[]ScriptTest{
			{
				Name: "DOLT_RESET hard removes uncommitted enum type",
				SetUpScript: []string{
					`CREATE TYPE reset_uncommitted_enum AS ENUM ('one', 'two');`,
				},
				Assertions: []ScriptTestAssertion{
					{
						Query: `SELECT COUNT(*)
											FROM pg_catalog.pg_type
											WHERE typname = 'reset_uncommitted_enum';`, PostgresOracle: ScriptTestPostgresOracle{ID: "dolt-versioning-correctness-repro-test-testdoltresethardremovesuncommittedenumtyperepro-0001-select-count-*-from-pg_catalog.pg_type"},
					},
				},
			},
		},
	)
}
