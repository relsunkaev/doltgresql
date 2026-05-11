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

// TestDropExtensionRestrictRejectsDependentObjectsRepro reproduces an
// extension dependency bug: PostgreSQL's default RESTRICT behavior prevents
// dropping an extension while user objects depend on extension member objects.
func TestDropExtensionRestrictRejectsDependentObjectsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP EXTENSION rejects dependent objects by default",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE TABLE hstore_extension_dependents (
					id INT PRIMARY KEY,
					payload public.hstore
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP EXTENSION hstore;`,
					ExpectedErr: `depend`,
				},
				{
					Query: `SELECT extname
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{"hstore"}},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NOT NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestDropExtensionCascadeRemovesDependentColumnsRepro reproduces an extension
// dependency bug: DROP EXTENSION ... CASCADE should remove user objects that
// depend on extension member objects, including columns of extension types.
func TestDropExtensionCascadeRemovesDependentColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP EXTENSION CASCADE removes dependent columns",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`CREATE TABLE hstore_extension_cascade_dependents (
					id INT PRIMARY KEY,
					payload public.hstore
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP EXTENSION hstore CASCADE;`,
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'hstore_extension_cascade_dependents'
							AND column_name = 'payload';`,
					Expected: []sql.Row{},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestDropExtensionMemberTypeRequiresDropExtensionRepro reproduces an
// extension dependency bug: extension member objects should not be dropped
// directly while their owning extension is installed.
func TestDropExtensionMemberTypeRequiresDropExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TYPE rejects extension member type",
			SetUpScript: []string{
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TYPE public.hstore;`,
					ExpectedErr: `extension`,
				},
				{
					Query: `SELECT extname
						FROM pg_catalog.pg_extension
						WHERE extname = 'hstore';`,
					Expected: []sql.Row{{"hstore"}},
				},
				{
					Query:    `SELECT to_regtype('public.hstore') IS NOT NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}

// TestCreateExtensionRejectsSchemaForNonRelocatableExtensionRepro reproduces
// an extension correctness bug: non-relocatable extensions with a fixed schema
// should reject an explicit conflicting schema.
func TestCreateExtensionRejectsSchemaForNonRelocatableExtensionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE EXTENSION rejects explicit schema for non-relocatable extension",
			Assertions: []ScriptTestAssertion{
				{
					Query:       `CREATE EXTENSION plpgsql WITH SCHEMA public;`,
					ExpectedErr: `schema`,
				},
				{
					Query: `SELECT n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'plpgsql';`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}
