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

// TestPointAdditionOperatorRepro reproduces a PostgreSQL geometric-type
// compatibility gap: point + point should add coordinates and return a point.
func TestPointAdditionOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "point addition adds coordinates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ('(1,2)'::point + '(3,4)'::point)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "geometric-type-correctness-repro-test-testpointadditionoperatorrepro-0001-select-1-2-::point-+"},
				},
			},
		},
	})
}
