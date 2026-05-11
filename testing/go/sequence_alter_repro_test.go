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

// TestAlterSequenceOptionsAffectNextvalRepro reproduces a sequence correctness
// bug: PostgreSQL applies ALTER SEQUENCE option changes to later nextval calls.
func TestAlterSequenceOptionsAffectNextvalRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE options affect nextval",
			SetUpScript: []string{
				`CREATE SEQUENCE alter_sequence_options_seq;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE alter_sequence_options_seq
						RESTART WITH 24
						INCREMENT BY 4
						MAXVALUE 36
						CYCLE;`,
				},
				{
					Query:    `SELECT nextval('alter_sequence_options_seq');`,
					Expected: []sql.Row{{int64(24)}},
				},
				{
					Query:    `SELECT nextval('alter_sequence_options_seq');`,
					Expected: []sql.Row{{int64(28)}},
				},
				{
					Query:    `SELECT nextval('alter_sequence_options_seq');`,
					Expected: []sql.Row{{int64(32)}},
				},
				{
					Query:    `SELECT nextval('alter_sequence_options_seq');`,
					Expected: []sql.Row{{int64(36)}},
				},
				{
					Query:    `SELECT nextval('alter_sequence_options_seq');`,
					Expected: []sql.Row{{int64(1)}},
				},
			},
		},
	})
}

// TestAlterSequenceIfExistsMissingWithOptionsNoopsRepro reproduces a DDL
// correctness bug: IF EXISTS should no-op for a missing sequence before
// applying or rejecting sequence option changes.
func TestAlterSequenceIfExistsMissingWithOptionsNoopsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SEQUENCE IF EXISTS missing sequence with options no-ops",
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SEQUENCE IF EXISTS missing_alter_sequence_options_seq
						RESTART WITH 5
						INCREMENT BY 2
						CYCLE;`,
				},
			},
		},
	})
}
