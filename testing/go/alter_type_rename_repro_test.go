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

// PostgreSQL can rename enum labels without recreating the type. Doltgres
// currently rejects ALTER TYPE before the label can be persisted.
func TestAlterEnumRenameValueRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME VALUE updates enum label",
			SetUpScript: []string{
				`CREATE TYPE rename_enum_status AS ENUM ('new', 'done');`,
				`ALTER TYPE rename_enum_status RENAME VALUE 'done' TO 'archived';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT enumlabel
						FROM pg_catalog.pg_enum
						WHERE enumtypid = 'rename_enum_status'::regtype
						ORDER BY enumsortorder;`,
					Expected: []sql.Row{{"new"}, {"archived"}},
				},
			},
		},
	})
}

// PostgreSQL can rename composite attributes and exposes the renamed attribute
// through row-field selection. Doltgres currently rejects the ALTER TYPE form.
func TestAlterCompositeTypeRenameAttributeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME ATTRIBUTE updates composite field",
			SetUpScript: []string{
				`CREATE TYPE rename_composite_item AS (old_name INT);`,
				`ALTER TYPE rename_composite_item RENAME ATTRIBUTE old_name TO new_name;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT (ROW(7)::rename_composite_item).new_name;`,
					Expected: []sql.Row{{7}},
				},
			},
		},
	})
}

// PostgreSQL can rename type and domain objects in place. Doltgres should
// update type lookup metadata so the old name disappears and the new name is
// usable.
func TestAlterTypeAndDomainRenameToRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TYPE RENAME TO updates enum type lookup",
			SetUpScript: []string{
				`CREATE TYPE rename_enum_object AS ENUM ('new', 'done');`,
				`ALTER TYPE rename_enum_object RENAME TO renamed_enum_object;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							to_regtype('rename_enum_object')::text,
							to_regtype('renamed_enum_object')::text;`,
					Expected: []sql.Row{{nil, "renamed_enum_object"}},
				},
			},
		},
		{
			Name: "ALTER DOMAIN RENAME TO updates domain type lookup",
			SetUpScript: []string{
				`CREATE DOMAIN rename_domain_object AS INT;`,
				`ALTER DOMAIN rename_domain_object RENAME TO renamed_domain_object;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							to_regtype('rename_domain_object')::text,
							to_regtype('renamed_domain_object')::text;`,
					Expected: []sql.Row{{nil, "renamed_domain_object"}},
				},
			},
		},
	})
}
