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

// PostgreSQL's built-in text-search functions can parse documents and queries
// using the built-in simple configuration. Doltgres has catalog OIDs for these
// functions but does not currently execute them.
func TestBuiltInTextSearchFunctionsMatchTermsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "built-in text-search functions match terms",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_tsvector('simple'::regconfig, 'jumped cats')::text,
							to_tsquery('simple'::regconfig, 'jumped & cats')::text,
							to_tsvector('simple'::regconfig, 'jumped cats') @@
								to_tsquery('simple'::regconfig, 'cats');`,
					Expected: []sql.Row{{"'cats':2 'jumped':1", "'jumped' & 'cats'", true}},
				},
			},
		},
	})
}

// PostgreSQL allows user-defined text-search configurations and makes them
// available to full-text functions. Doltgres currently rejects the definition
// syntax before it can persist the catalog object.
func TestCreateTextSearchConfigurationCopyIsUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEXT SEARCH CONFIGURATION copy is usable",
			SetUpScript: []string{
				`CREATE TEXT SEARCH CONFIGURATION copied_simple_config (COPY = pg_catalog.simple);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT cfgname
						FROM pg_catalog.pg_ts_config
						WHERE cfgname = 'copied_simple_config';`,
					Expected: []sql.Row{{"copied_simple_config"}},
				},
				{
					Query:    `SELECT to_tsvector('copied_simple_config', 'jumped cats');`,
					Expected: []sql.Row{{"'cats':2 'jumped':1"}},
				},
			},
		},
	})
}
