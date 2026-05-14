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

// TestAvailableExtensionsProbe pins the extension catalog surface that dump
// and migration tooling use before deciding whether extension DDL can run.
func TestAvailableExtensionsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "supported extension metadata is visible",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;`,
				`CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;`,
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;`,
				`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT name, default_version, installed_version
						FROM pg_catalog.pg_available_extensions
						WHERE name IN ('btree_gist', 'citext', 'hstore', 'pgcrypto', 'plpgsql', 'uuid-ossp', 'vector')
						ORDER BY name;`,
					Expected: []sql.Row{
						{"btree_gist", "1.7", nil},
						{"citext", "1.6", "1.6"},
						{"hstore", "1.8", "1.8"},
						{"pgcrypto", "1.3", "1.3"},
						{"plpgsql", "1.0", nil},
						{"uuid-ossp", "1.1", "1.1"},
						{"vector", "0.0", nil},
					},
				},
				{
					Query: `SELECT name, version, installed, relocatable, schema
						FROM pg_catalog.pg_available_extension_versions
						WHERE name IN ('btree_gist', 'citext', 'hstore', 'pgcrypto', 'plpgsql', 'uuid-ossp', 'vector')
						ORDER BY name, version;`,
					Expected: []sql.Row{
						{"btree_gist", "1.2", "f", "t", nil},
						{"btree_gist", "1.3", "f", "t", nil},
						{"btree_gist", "1.4", "f", "t", nil},
						{"btree_gist", "1.5", "f", "t", nil},
						{"btree_gist", "1.6", "f", "t", nil},
						{"btree_gist", "1.7", "f", "t", nil},
						{"citext", "1.4", "f", "t", nil},
						{"citext", "1.5", "f", "t", nil},
						{"citext", "1.6", "t", "t", nil},
						{"hstore", "1.4", "f", "t", nil},
						{"hstore", "1.5", "f", "t", nil},
						{"hstore", "1.6", "f", "t", nil},
						{"hstore", "1.7", "f", "t", nil},
						{"hstore", "1.8", "t", "t", nil},
						{"pgcrypto", "1.3", "t", "t", nil},
						{"plpgsql", "1.0", "f", "f", "pg_catalog"},
						{"uuid-ossp", "1.1", "t", "t", nil},
						{"vector", "0.0", "f", "t", nil},
					},
				},
			},
		},
	})
}
