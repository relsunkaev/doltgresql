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

// TestCommonExtensionsProbe pins how far PG's most-emitted extension
// DDL (`CREATE EXTENSION IF NOT EXISTS uuid-ossp`, `pgcrypto`,
// `citext`) lands today, plus the runtime function shapes ORMs
// reach for. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestCommonExtensionsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "CREATE EXTENSION uuid-ossp keyword acceptance",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
				},
			},
		},
		{
			// pgcrypto's catalog install file uses OUT parameters in
			// CREATE FUNCTION declarations; the parser rejects the
			// keyword today (`at or near "out": syntax error`). The
			// related `gen_random_uuid` builtin (which pgcrypto used
			// to be the only source of pre-PG-13) is registered
			// natively, so apps that only need UUID generation can
			// drop the extension load. Pin the rejection so the gap
			// is visible.
			Name:        "CREATE EXTENSION pgcrypto is rejected on OUT-parameter parse",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
					ExpectedErr: `at or near "out": syntax error`,
				},
			},
		},
		{
			// gen_random_uuid is a builtin in PG 13+; pgcrypto used
			// to provide it. Real-world apps depend on this being
			// callable for default UUID PKs.
			Name:        "gen_random_uuid runtime call",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// Don't assert the value (it's random), just
					// that the call shape lands and the result
					// type-castable to text has the right length.
					Query:    `SELECT length(gen_random_uuid()::text)::text;`,
					Expected: []sql.Row{{"36"}},
				},
			},
		},
	})
}
