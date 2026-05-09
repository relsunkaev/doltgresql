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
			Name: "uuid-ossp uuid_generate_v4 runtime call",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT length(uuid_generate_v4()::text)::text;`,
					Expected: []sql.Row{{"36"}},
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
			Name: "CREATE EXTENSION citext installs case-insensitive text type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;`,
				`CREATE TABLE app_users (id integer primary key, email public.citext UNIQUE);`,
				`INSERT INTO app_users VALUES (1, 'Alice@Example.com');`,
				`INSERT INTO app_users VALUES (2, 'bob@example.com');`,
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
				{
					Query:    `SELECT ('Alice@Example.com'::public.citext = 'alice@example.com'::public.citext)::text;`,
					Expected: []sql.Row{{"true"}},
				},
				{
					Query:    `SELECT ('Alice@Example.com'::public.citext <> 'alice@example.com'::public.citext)::text, ('bob@example.com'::public.citext > 'ALICE@example.com'::public.citext)::text;`,
					Expected: []sql.Row{{"false", "true"}},
				},
				{
					Query: `EXPLAIN SELECT id FROM app_users WHERE email = 'alice@example.com'::public.citext;`,
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [app_users.id]"},
						{" └─ Filter"},
						{"     ├─ app_users.email = 'alice@example.com'"},
						{"     └─ Table"},
						{"         ├─ name: app_users"},
						{"         └─ columns: [id email]"},
					},
				},
				{
					Query:    `SELECT id FROM app_users WHERE email = 'alice@example.com'::public.citext;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT id FROM app_users WHERE email > 'alice@example.com'::public.citext ORDER BY id;`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:       `UPDATE app_users SET email = 'BOB@example.com' WHERE id = 1;`,
					ExpectedErr: "duplicate",
				},
				{
					Query:       `INSERT INTO app_users VALUES (3, 'alice@example.com');`,
					ExpectedErr: "duplicate",
				},
			},
		},
		{
			Name: "CREATE EXTENSION hstore installs text-compatible type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;`,
				`CREATE TABLE vending_machines (id integer primary key, inventory public.hstore);`,
				`INSERT INTO vending_machines VALUES (1, '"A"=>"2", "B"=>"5"');`,
				`INSERT INTO vending_machines VALUES (2, '"empty"=>NULL, "quoted key"=>"a,b=>c", "quote\"slash\\"=>"v\"\\x"');`,
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
				{
					Query:    `SELECT inventory -> 'A', fetchval(inventory, 'B') FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"2", "5"}},
				},
				{
					Query:    `SELECT (inventory -> 'missing') IS NULL FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT inventory -> 'empty', inventory -> 'quoted key', inventory -> E'quote"slash\\' FROM vending_machines WHERE id = 2;`,
					Expected: []sql.Row{{nil, "a,b=>c", `v"\x`}},
				},
				{
					Query:    `SELECT inventory ? 'A', inventory ? 'missing' FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"t", "f"}},
				},
				{
					Query:    `SELECT exist(inventory, 'empty'), defined(inventory, 'empty'), isexists(inventory, 'quoted key'), isdefined(inventory, 'quoted key') FROM vending_machines WHERE id = 2;`,
					Expected: []sql.Row{{"t", "f", "t", "t"}},
				},
				{
					Query:    `SELECT inventory ?| ARRAY['missing', 'B'], inventory ?| ARRAY['missing', 'other'], inventory ?& ARRAY['A', 'B'], inventory ?& ARRAY['A', 'missing'] FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"t", "f", "t", "f"}},
				},
				{
					Query:    `SELECT exists_any(inventory, ARRAY['missing', 'quoted key']), exists_all(inventory, ARRAY['empty', 'quoted key']) FROM vending_machines WHERE id = 2;`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query:    `SELECT inventory ?| ARRAY[NULL]::text[], inventory ?& ARRAY[NULL]::text[], inventory ?| ARRAY[]::text[], inventory ?& ARRAY[]::text[] FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"f", "t", "f", "t"}},
				},
				{
					Query:    `SELECT inventory @> '"A"=>"2"'::public.hstore, inventory @> '"A"=>"9"'::public.hstore, inventory @> '"missing"=>"1"'::public.hstore, inventory <@ '"A"=>"2", "B"=>"5", "C"=>"6"'::public.hstore FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"t", "f", "f", "t"}},
				},
				{
					Query:    `SELECT inventory @> '"empty"=>NULL'::public.hstore, inventory @> '"empty"=>"x"'::public.hstore, inventory <@ '"empty"=>NULL, "quoted key"=>"a,b=>c", "quote\"slash\\"=>"v\"\\x", "extra"=>"1"'::public.hstore FROM vending_machines WHERE id = 2;`,
					Expected: []sql.Row{{"t", "f", "t"}},
				},
				{
					Query:    `SELECT hs_contains(inventory, '"A"=>"2"'::public.hstore), hs_contained(inventory, '"A"=>"2", "B"=>"5"'::public.hstore) FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query:    `SELECT (inventory || '"B"=>"9", "C"=>NULL'::public.hstore)::text, hs_concat(inventory, '"A"=>NULL'::public.hstore)::text FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{`"A"=>"2", "B"=>"9", "C"=>NULL`, `"A"=>NULL, "B"=>"5"`}},
				},
				{
					Query:    `SELECT delete(inventory, 'A')::text, (inventory - 'B'::text)::text FROM vending_machines WHERE id = 1;`,
					Expected: []sql.Row{{`"B"=>"5"`, `"A"=>"2"`}},
				},
				{
					Query:    `SELECT delete('"A"=>"2", "B"=>"5", "C"=>"6"'::public.hstore, ARRAY['A', 'C'])::text, ('"A"=>"2", "B"=>"5"'::public.hstore - ARRAY[NULL]::text[])::text;`,
					Expected: []sql.Row{{`"B"=>"5"`, `"A"=>"2", "B"=>"5"`}},
				},
				{
					Query:    `SELECT ('"A"=>"2", "B"=>"5"'::public.hstore - '"A"=>"9", "B"=>"5"'::public.hstore)::text, delete('"empty"=>NULL, "quoted key"=>"a,b=>c"'::public.hstore, '"empty"=>NULL'::public.hstore)::text;`,
					Expected: []sql.Row{{`"A"=>"2"`, `"quoted key"=>"a,b=>c"`}},
				},
				{
					Query:       `SELECT 'not hstore'::public.hstore -> 'missing';`,
					ExpectedErr: `invalid input syntax for type hstore`,
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
