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
// `citext`, `hstore`) lands today, plus the runtime function shapes ORMs
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
			Name: "CREATE EXTENSION plpgsql dump compatibility shim",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'plpgsql';`,
					Expected: []sql.Row{{"plpgsql", "pg_catalog", "f", "1.0"}},
				},
			},
		},
		{
			// pgcrypto's catalog install file uses `name OUT type`
			// parameters in CREATE FUNCTION declarations. This pins
			// the dump-facing extension load shape, while the
			// `gen_random_uuid` runtime assertion below covers the
			// function most ORM schemas need from pgcrypto-era dumps.
			Name:        "CREATE EXTENSION pgcrypto keyword acceptance",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
				},
				{
					Query:       `CREATE EXTENSION pgcrypto;`,
					ExpectedErr: `extension "pgcrypto" already exists`,
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
		{
			Name: "loaded extension appears in pg_extension",
			SetUpScript: []string{
				`CREATE EXTENSION "uuid-ossp";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion, e.extconfig, e.extcondition
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'uuid-ossp';`,
					Expected: []sql.Row{{"uuid-ossp", "public", "t", "1.1", nil, nil}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION WITH SCHEMA records target namespace",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion, e.extconfig, e.extcondition
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'pgcrypto';`,
					Expected: []sql.Row{{"pgcrypto", "extensions", "t", "1.3", nil, nil}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION vector enables built-in pgvector type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS vector;`,
				`CREATE TABLE embeddings (id integer primary key, embedding vector(3));`,
				`INSERT INTO embeddings VALUES (1, '[1,2,3]');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'vector';`,
					Expected: []sql.Row{{"vector", "public"}},
				},
				{
					Query:    `SELECT embedding FROM embeddings WHERE id = 1;`,
					Expected: []sql.Row{{"[1,2,3]"}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION btree_gist dump compatibility shim",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'btree_gist';`,
					Expected: []sql.Row{{"btree_gist", "public", "t", "1.7"}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION citext installs text-compatible type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;`,
				`CREATE TABLE app_users (id integer primary key, email public.citext);`,
				`INSERT INTO app_users VALUES (1, 'Alice@Example.com');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'citext';`,
					Expected: []sql.Row{{"citext", "public"}},
				},
				{
					Query:    `SELECT email::text FROM app_users WHERE id = 1;`,
					Expected: []sql.Row{{"Alice@Example.com"}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION hstore installs text-compatible type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;`,
				`CREATE TABLE vending_machines (id integer primary key, inventory public.hstore);`,
				`INSERT INTO vending_machines VALUES (1, '"A"=>"2", "B"=>"5"');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'hstore';`,
					Expected: []sql.Row{{"hstore", "public"}},
				},
				{
					Query:    `SELECT inventory::text FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{`"A"=>"2", "B"=>"5"`}},
				},
			},
		},
		{
			Name: "DROP EXTENSION supports dump cleanup prelude",
			SetUpScript: []string{
				`DROP EXTENSION IF EXISTS hstore;`,
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`DROP EXTENSION hstore;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT extname FROM pg_catalog.pg_extension WHERE extname = 'hstore';`,
					Expected: []sql.Row{},
				},
				{
					Query:       `DROP EXTENSION hstore;`,
					ExpectedErr: `extension "hstore" does not exist`,
				},
			},
		},
	})
}
