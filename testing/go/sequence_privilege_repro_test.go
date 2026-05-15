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

func TestSequenceOwnerCanUseCreatedSequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "sequence owner can use created sequence",
			SetUpScript: []string{
				authTestCreateSuperUser,
				`CREATE USER sequence_owner_user PASSWORD 'sequence';`,
				`GRANT USAGE ON SCHEMA public TO sequence_owner_user;`,
				`GRANT CREATE ON SCHEMA public TO sequence_owner_user;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `CREATE SEQUENCE owner_created_seq;`,
					Expected: []sql.Row{},
					Username: `sequence_owner_user`,
					Password: `sequence`,
				},
				{
					Query:    `SELECT nextval('owner_created_seq');`,
					Expected: []sql.Row{{1}},
					Username: `sequence_owner_user`,
					Password: `sequence`,
				},
			},
		},
	})
}
