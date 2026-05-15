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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestCreateViewSecurityBarrierPersistsReloptionRepro reproduces a view
// security metadata bug: PostgreSQL persists security_barrier=true in view
// reloptions.
func TestCreateViewSecurityBarrierPersistsReloptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW security_barrier persists reloption",
			SetUpScript: []string{
				`CREATE VIEW security_barrier_view
					WITH (security_barrier = true)
					AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE relname = 'security_barrier_view';`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-option-correctness-repro-test-testcreateviewsecuritybarrierpersistsreloptionrepro-0001-select-cast-reloptions-as-text"},
				},
			},
		},
	})
}

// TestCreateViewSecurityInvokerPersistsReloptionRepro reproduces a view
// security metadata bug: PostgreSQL persists security_invoker=true in view
// reloptions.
func TestCreateViewSecurityInvokerPersistsReloptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW security_invoker persists reloption",
			SetUpScript: []string{
				`CREATE VIEW security_invoker_metadata_view
					WITH (security_invoker = true)
					AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE relname = 'security_invoker_metadata_view';`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-option-correctness-repro-test-testcreateviewsecurityinvokerpersistsreloptionrepro-0001-select-cast-reloptions-as-text"},
				},
			},
		},
	})
}

// TestCreateViewBareSecurityOptionsPersistTrueReloptionsRepro reproduces a
// view reloption parsing bug: PostgreSQL treats bare boolean view options as
// true.
func TestCreateViewBareSecurityOptionsPersistTrueReloptionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW bare security options persist true reloptions",
			SetUpScript: []string{
				`CREATE VIEW bare_security_barrier_view
					WITH (security_barrier)
					AS SELECT 1 AS id;`,
				`CREATE VIEW bare_security_invoker_view
					WITH (security_invoker)
					AS SELECT 1 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT relname, CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE relname IN (
							'bare_security_barrier_view',
							'bare_security_invoker_view'
						)
						ORDER BY relname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-option-correctness-repro-test-testcreateviewbaresecurityoptionspersisttruereloptionsrepro-0001-select-relname-cast-reloptions-as"},
				},
			},
		},
	})
}

// TestCreateOrReplaceViewSecurityInvokerPersistsReloptionRepro reproduces a
// view security metadata bug: PostgreSQL persists security_invoker=true in view
// reloptions when CREATE OR REPLACE VIEW sets the option.
func TestCreateOrReplaceViewSecurityInvokerPersistsReloptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE OR REPLACE VIEW security_invoker persists reloption",
			SetUpScript: []string{
				`CREATE VIEW replace_invoker_metadata_view AS
					SELECT 1 AS id;`,
				`CREATE OR REPLACE VIEW replace_invoker_metadata_view
					WITH (security_invoker = true) AS
					SELECT 2 AS id;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT CAST(reloptions AS TEXT)
						FROM pg_catalog.pg_class
						WHERE relname = 'replace_invoker_metadata_view';`, PostgresOracle: ScriptTestPostgresOracle{ID: "view-option-correctness-repro-test-testcreateorreplaceviewsecurityinvokerpersistsreloptionrepro-0001-select-cast-reloptions-as-text"},
				},
			},
		},
	})
}

// TestCreateViewCheckOptionPersistsMetadataRepro reproduces a view metadata
// correctness bug: PostgreSQL persists a view's local CHECK OPTION.
func TestCreateViewCheckOptionPersistsMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW WITH LOCAL CHECK OPTION persists metadata",
			SetUpScript: []string{
				`CREATE TABLE check_option_source (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`CREATE VIEW check_option_positive AS
					SELECT id, amount FROM check_option_source WHERE amount > 0
					WITH LOCAL CHECK OPTION;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT check_option
						FROM information_schema.views
						WHERE table_schema = 'public'
							AND table_name = 'check_option_positive';`,
					Expected: []sql.Row{{"LOCAL"}},
				},
			},
		},
	})
}

// TestCreateViewCheckOptionReloptionPersistsMetadataRepro reproduces a view
// metadata correctness bug: PostgreSQL accepts check_option as a view option.
func TestCreateViewCheckOptionReloptionPersistsMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE VIEW WITH check_option reloption persists metadata",
			SetUpScript: []string{
				`CREATE TABLE check_option_reloption_source (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`CREATE VIEW check_option_reloption_positive
					WITH (check_option = 'local') AS
					SELECT id, amount FROM check_option_reloption_source WHERE amount > 0;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT check_option
						FROM information_schema.views
						WHERE table_schema = 'public'
							AND table_name = 'check_option_reloption_positive';`,
					Expected: []sql.Row{{"LOCAL"}},
				},
			},
		},
	})
}
