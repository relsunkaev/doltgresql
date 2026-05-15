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

// TestInetColumnRoundTripsAndUsesNetworkOperatorsRepro reproduces a PostgreSQL
// network-address type compatibility gap: inet values should store host/mask
// data and support the standard network containment operators.
func TestInetColumnRoundTripsAndUsesNetworkOperatorsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inet column round trips and uses containment operators",
			SetUpScript: []string{
				`CREATE TABLE inet_roundtrip_items (
					id INT PRIMARY KEY,
					addr inet
				);`,
				`INSERT INTO inet_roundtrip_items VALUES
					(1, '192.168.1.5/24'),
					(2, '10.0.0.1/8');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, host(addr), masklen(addr),
							addr << inet '192.168.0.0/16'
						FROM inet_roundtrip_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "network-type-correctness-repro-test-testinetcolumnroundtripsandusesnetworkoperatorsrepro-0001-select-id-host-addr-masklen"},
				},
			},
		},
	})
}
