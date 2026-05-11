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

	"github.com/dolthub/go-mysql-server/sql"
)

// PostgreSQL treats GREATEST and LEAST as conditional expressions over resolved
// common argument types. Doltgres currently rejects ordinary scalar arguments
// before evaluation.
func TestGreatestLeastEvaluateScalarArgumentsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GREATEST and LEAST evaluate scalar arguments",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT GREATEST(1, 2, 3), LEAST(1, 2, 3);`,
					Expected: []sql.Row{{3, 1}},
				},
				{
					Query:    `SELECT GREATEST(NULL, 1, 2), LEAST(NULL, 1, 2), GREATEST(NULL, NULL);`,
					Expected: []sql.Row{{2, 1, nil}},
				},
			},
		},
	})
}
