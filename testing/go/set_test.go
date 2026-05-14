// Copyright 2024 Dolthub, Inc.
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

func TestSetStatements(t *testing.T) {
	RunScripts(t, setStmts)
}

// setStmts test on simple cases on setting and showing the config parameters.
// This includes setting the parameters successfully and
// showing the updated value if they are of context, `user` or `superuser`.
// If the parameters are of any other context (e.g. `sighup` or `postmaster`),
// it returns an error as those parameters can only be updated
// through configuration file and/or SIGHUP signal and/or having appropriate roles.
var setStmts = []ScriptTest{
	{
		Name:        "special case for TIME ZONE",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0001-show-timezone"},
			},
			{
				Query: "SET timezone TO '+00:00';", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0002-set-timezone-to-+00:00"},
			},
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0003-show-timezone"},
			},
			{
				Query: "SET TIME ZONE LOCAL;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0004-set-time-zone-local"},
			},
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0005-show-timezone"},
			},
			{
				Query: "SET TIME ZONE '+00:00';", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0006-set-time-zone-+00:00"},
			},
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0007-show-timezone"},
			},
			{
				Query: "SET TIME ZONE '00:00:00';", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0008-set-time-zone-00:00:00"},
			},
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0009-show-timezone"},
			},
			{
				Query: "SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0010-set-time-zone-interval-+00:00"},
			},
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0011-show-timezone"},
			},
			{
				Query: "SET TIME ZONE DEFAULT;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0012-set-time-zone-default"},
			},
			{
				Query: "SHOW timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0013-show-timezone"},
			},
			{
				Query: "SELECT current_setting('timezone')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0014-select-current_setting-timezone"},
			},
		},
	},
	{
		Name:        "special case for SCHEMA",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0015-show-search_path"},
			},
			{
				Query: "SET SCHEMA 'postgres';", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0016-set-schema-postgres"},
			},
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0017-show-search_path"},
			},
			{
				Query: "SET search_path = public, pg_catalog;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0018-set-search_path-=-public-pg_catalog"},
			},
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0019-show-search_path"},
			},
			{
				Query: "SET search_path = postgres;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0020-set-search_path-=-postgres"},
			},
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0021-show-search_path"},
			},
			{
				Query: "SELECT current_setting('search_path')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0022-select-current_setting-search_path"},
			},
		},
	},
	{
		Name:        "special case for NAMES",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW client_encoding", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0023-show-client_encoding"},
			},
			{
				Query: "SET NAMES 'LATIN1';", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0024-set-names-latin1"},
			},
			{
				Query:    "SHOW client_encoding;",
				Expected: []sql.Row{{"LATIN1"}},
			},
			{
				Query:    "SET client_encoding = DEFAULT;",
				Expected: []sql.Row{},
			},
			{
				Query: "SHOW client_encoding;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0027-show-client_encoding"},
			},
			{
				Query: "SELECT current_setting('client_encoding')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0028-select-current_setting-client_encoding"},
			},
		},
	},
	{
		Name:        "special case SEED",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_seed", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0029-show-geqo_seed"},
			},
			{
				Query: "SET SEED 1;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0030-set-seed-1", Compare: "sqlstate"},
			},
			{
				Query:    "SHOW geqo_seed",
				Expected: []sql.Row{{float64(1)}},
			},
			{
				Query: "SELECT current_setting('geqo_seed')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0032-select-current_setting-geqo_seed"},
			},
		},
	},
	{
		Name:        "set 'allow_in_place_tablespaces' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW allow_in_place_tablespaces", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0033-show-allow_in_place_tablespaces"},
			},
			{
				Query: "SET allow_in_place_tablespaces TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0034-set-allow_in_place_tablespaces-to-on"},
			},
			{
				Query: "SHOW allow_in_place_tablespaces", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0035-show-allow_in_place_tablespaces"},
			},
			{
				Query: "SET allow_in_place_tablespaces TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0036-set-allow_in_place_tablespaces-to-default"},
			},
			{
				Query: "SHOW allow_in_place_tablespaces", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0037-show-allow_in_place_tablespaces"},
			},
			{
				Query: "SELECT current_setting('allow_in_place_tablespaces')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0038-select-current_setting-allow_in_place_tablespaces"},
			},
		},
	},
	{
		Name:        "set 'allow_system_table_mods' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW allow_system_table_mods", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0039-show-allow_system_table_mods"},
			},
			{
				Query: "SET allow_system_table_mods TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0040-set-allow_system_table_mods-to-on"},
			},
			{
				Query: "SHOW allow_system_table_mods", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0041-show-allow_system_table_mods"},
			},
			{
				Query: "SET allow_system_table_mods TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0042-set-allow_system_table_mods-to-default"},
			},
			{
				Query: "SHOW allow_system_table_mods", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0043-show-allow_system_table_mods"},
			},
			{
				Query: "SELECT current_setting('allow_system_table_mods')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0044-select-current_setting-allow_system_table_mods"},
			},
		},
	},
	{
		Name:        "set 'application_name' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW application_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0045-show-application_name"},
			},
			{
				Query: "SET application_name TO 'postgresql'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0046-set-application_name-to-postgresql"},
			},
			{
				Query: "SHOW application_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0047-show-application_name"},
			},
			{
				Query: "SET application_name TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0048-set-application_name-to-default"},
			},
			{
				Query: "SHOW application_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0049-show-application_name"},
			},
			{
				Query: "SELECT current_setting('application_name')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0050-select-current_setting-application_name"},
			},
		},
	},
	{
		Name:        "RESET ALL restores session configuration variables",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SET application_name TO 'changed'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0051-set-application_name-to-changed"},
			},
			{
				Query: "SET search_path TO postgres", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0052-set-search_path-to-postgres"},
			},
			{
				Query: "SET enable_hashjoin TO off", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0053-set-enable_hashjoin-to-off"},
			},
			{
				Query: "RESET ALL", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0054-reset-all"},
			},
			{
				Query: "SHOW application_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0055-show-application_name"},
			},
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0056-show-search_path"},
			},
			{
				Query: "SHOW enable_hashjoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0057-show-enable_hashjoin"},
			},
		},
	},
	{
		Name:        "set 'archive_cleanup_command' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW archive_cleanup_command", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0058-show-archive_cleanup_command"},
			},
			{
				Query: "SET archive_cleanup_command TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0059-set-archive_cleanup_command-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('archive_cleanup_command')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0060-select-current_setting-archive_cleanup_command"},
			},
		},
	},
	{
		Name:        "set 'archive_command' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW archive_command", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0061-show-archive_command"},
			},
			{
				Query: "SET archive_command TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0062-set-archive_command-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('archive_command')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0063-select-current_setting-archive_command"},
			},
		},
	},
	{
		Name:        "set 'archive_library' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW archive_library", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0064-show-archive_library"},
			},
			{
				Query: "SET archive_library TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0065-set-archive_library-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('archive_library')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0066-select-current_setting-archive_library"},
			},
		},
	},
	{
		Name:        "set 'archive_mode' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW archive_mode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0067-show-archive_mode"},
			},
			{
				Query: "SET archive_mode TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0068-set-archive_mode-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('archive_mode')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0069-select-current_setting-archive_mode"},
			},
		},
	},
	{
		Name:        "set 'archive_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW archive_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0070-show-archive_timeout"},
			},
			{
				Query: "SET archive_timeout TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0071-set-archive_timeout-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('archive_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0072-select-current_setting-archive_timeout"},
			},
		},
	},
	{
		Name:        "set 'array_nulls' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW array_nulls", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0073-show-array_nulls"},
			},
			{
				Query: "SET array_nulls TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0074-set-array_nulls-to-off"},
			},
			{
				Query: "SHOW array_nulls", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0075-show-array_nulls"},
			},
			{
				Query: "SET array_nulls TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0076-set-array_nulls-to-default"},
			},
			{
				Query: "SHOW array_nulls", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0077-show-array_nulls"},
			},
			{
				Query: "SELECT current_setting('array_nulls')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0078-select-current_setting-array_nulls"},
			},
		},
	},
	{
		Name:        "set 'authentication_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW authentication_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0079-show-authentication_timeout"},
			},
			{
				Query: "SET authentication_timeout TO '120'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0080-set-authentication_timeout-to-120", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('authentication_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0081-select-current_setting-authentication_timeout"},
			},
		},
	},
	{
		Name:        "set 'autovacuum' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0082-show-autovacuum"},
			},
			{
				Query: "SET autovacuum TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0083-set-autovacuum-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0084-select-current_setting-autovacuum"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_analyze_scale_factor' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_analyze_scale_factor", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0085-show-autovacuum_analyze_scale_factor"},
			},
			{
				Query: "SET autovacuum_analyze_scale_factor TO '0.1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0086-set-autovacuum_analyze_scale_factor-to-0.1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_analyze_scale_factor')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0087-select-current_setting-autovacuum_analyze_scale_factor"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_analyze_threshold' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_analyze_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0088-show-autovacuum_analyze_threshold"},
			},
			{
				Query: "SET autovacuum_analyze_threshold TO '50'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0089-set-autovacuum_analyze_threshold-to-50", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_analyze_threshold')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0090-select-current_setting-autovacuum_analyze_threshold"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_freeze_max_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_freeze_max_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0091-show-autovacuum_freeze_max_age"},
			},
			{
				Query: "SET autovacuum_freeze_max_age TO '200000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0092-set-autovacuum_freeze_max_age-to-200000000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_freeze_max_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0093-select-current_setting-autovacuum_freeze_max_age"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_max_workers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_max_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0094-show-autovacuum_max_workers"},
			},
			{
				Query: "SET autovacuum_max_workers TO '3'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0095-set-autovacuum_max_workers-to-3", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_max_workers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0096-select-current_setting-autovacuum_max_workers"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_multixact_freeze_max_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_multixact_freeze_max_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0097-show-autovacuum_multixact_freeze_max_age"},
			},
			{
				Query: "SET autovacuum_multixact_freeze_max_age TO '400000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0098-set-autovacuum_multixact_freeze_max_age-to-400000000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_multixact_freeze_max_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0099-select-current_setting-autovacuum_multixact_freeze_max_age"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_naptime' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_naptime", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0100-show-autovacuum_naptime"},
			},
			{
				Query: "SET autovacuum_naptime TO '60'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0101-set-autovacuum_naptime-to-60", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_naptime')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0102-select-current_setting-autovacuum_naptime"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_vacuum_cost_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_vacuum_cost_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0103-show-autovacuum_vacuum_cost_delay"},
			},
			{
				Query: "SET autovacuum_vacuum_cost_delay TO '2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0104-set-autovacuum_vacuum_cost_delay-to-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_vacuum_cost_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0105-select-current_setting-autovacuum_vacuum_cost_delay"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_vacuum_cost_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_vacuum_cost_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0106-show-autovacuum_vacuum_cost_limit"},
			},
			{
				Query: "SET autovacuum_vacuum_cost_limit TO '-1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0107-set-autovacuum_vacuum_cost_limit-to-1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_vacuum_cost_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0108-select-current_setting-autovacuum_vacuum_cost_limit"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_vacuum_insert_scale_factor' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_vacuum_insert_scale_factor", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0109-show-autovacuum_vacuum_insert_scale_factor"},
			},
			{
				Query: "SET autovacuum_vacuum_insert_scale_factor TO '0.2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0110-set-autovacuum_vacuum_insert_scale_factor-to-0.2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_vacuum_insert_scale_factor')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0111-select-current_setting-autovacuum_vacuum_insert_scale_factor"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_vacuum_insert_threshold' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_vacuum_insert_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0112-show-autovacuum_vacuum_insert_threshold"},
			},
			{
				Query: "SET autovacuum_vacuum_insert_threshold TO '1000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0113-set-autovacuum_vacuum_insert_threshold-to-1000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_vacuum_insert_threshold')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0114-select-current_setting-autovacuum_vacuum_insert_threshold"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_vacuum_scale_factor' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_vacuum_scale_factor", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0115-show-autovacuum_vacuum_scale_factor"},
			},
			{
				Query: "SET autovacuum_vacuum_scale_factor TO '0.2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0116-set-autovacuum_vacuum_scale_factor-to-0.2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_vacuum_scale_factor')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0117-select-current_setting-autovacuum_vacuum_scale_factor"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_vacuum_threshold' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_vacuum_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0118-show-autovacuum_vacuum_threshold"},
			},
			{
				Query: "SET autovacuum_vacuum_threshold TO '50'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0119-set-autovacuum_vacuum_threshold-to-50", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_vacuum_threshold')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0120-select-current_setting-autovacuum_vacuum_threshold"},
			},
		},
	},
	{
		Name:        "set 'autovacuum_work_mem' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW autovacuum_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0121-show-autovacuum_work_mem"},
			},
			{
				Query: "SET autovacuum_work_mem TO '-1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0122-set-autovacuum_work_mem-to-1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('autovacuum_work_mem')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0123-select-current_setting-autovacuum_work_mem"},
			},
		},
	},
	{
		Name:        "set 'backend_flush_after' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW backend_flush_after", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0124-show-backend_flush_after"},
			},
			{
				Query: "SET backend_flush_after TO '256'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0125-set-backend_flush_after-to-256"},
			},
			{
				Query: "SHOW backend_flush_after", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0126-show-backend_flush_after"},
			},
			{
				Query: "SET backend_flush_after TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0127-set-backend_flush_after-to-default"},
			},
			{
				Query: "SHOW backend_flush_after", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0128-show-backend_flush_after"},
			},
			{
				Query: "SELECT current_setting('backend_flush_after')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0129-select-current_setting-backend_flush_after"},
			},
		},
	},
	{
		Name:        "set 'backslash_quote' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW backslash_quote", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0130-show-backslash_quote"},
			},
			{
				Query: "SET backslash_quote TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0131-set-backslash_quote-to-on"},
			},
			{
				Query: "SHOW backslash_quote", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0132-show-backslash_quote"},
			},
			{
				Query: "SET backslash_quote TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0133-set-backslash_quote-to-default"},
			},
			{
				Query: "SHOW backslash_quote", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0134-show-backslash_quote"},
			},
			{
				Query: "SELECT current_setting('backslash_quote')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0135-select-current_setting-backslash_quote"},
			},
		},
	},
	{
		Name:        "set 'backtrace_functions' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW backtrace_functions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0136-show-backtrace_functions"},
			},
			{
				Query: "SET backtrace_functions TO 'default'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0137-set-backtrace_functions-to-default"},
			},
			{
				Query: "SHOW backtrace_functions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0138-show-backtrace_functions"},
			},
			{
				Query: "SET backtrace_functions TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0139-set-backtrace_functions-to-default"},
			},
			{
				Query: "SHOW backtrace_functions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0140-show-backtrace_functions"},
			},
			{
				Query: "SELECT current_setting('backtrace_functions')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0141-select-current_setting-backtrace_functions"},
			},
		},
	},
	{
		Name:        "set 'bgwriter_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bgwriter_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0142-show-bgwriter_delay"},
			},
			{
				Query: "SET bgwriter_delay TO '200'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0143-set-bgwriter_delay-to-200", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('bgwriter_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0144-select-current_setting-bgwriter_delay"},
			},
		},
	},
	{
		Name:        "set 'bgwriter_flush_after' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bgwriter_flush_after", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0145-show-bgwriter_flush_after"},
			},
			{
				Query: "SET bgwriter_flush_after TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0146-set-bgwriter_flush_after-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('bgwriter_flush_after')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0147-select-current_setting-bgwriter_flush_after"},
			},
		},
	},
	{
		Name:        "set 'bgwriter_lru_maxpages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bgwriter_lru_maxpages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0148-show-bgwriter_lru_maxpages"},
			},
			{
				Query: "SET bgwriter_lru_maxpages TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0149-set-bgwriter_lru_maxpages-to-100", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('bgwriter_lru_maxpages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0150-select-current_setting-bgwriter_lru_maxpages"},
			},
		},
	},
	{
		Name:        "set 'bgwriter_lru_multiplier' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bgwriter_lru_multiplier", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0151-show-bgwriter_lru_multiplier"},
			},
			{
				Query: "SET bgwriter_lru_multiplier TO '2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0152-set-bgwriter_lru_multiplier-to-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('bgwriter_lru_multiplier')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0153-select-current_setting-bgwriter_lru_multiplier"},
			},
		},
	},
	{
		Name:        "set 'block_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW block_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0154-show-block_size"},
			},
			{
				Query: "SET block_size TO '8192'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0155-set-block_size-to-8192", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('block_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0156-select-current_setting-block_size"},
			},
		},
	},
	{
		Name:        "set 'bonjour' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bonjour", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0157-show-bonjour"},
			},
			{
				Query: "SET bonjour TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0158-set-bonjour-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('bonjour')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0159-select-current_setting-bonjour"},
			},
		},
	},
	{
		Name:        "set 'bonjour_name' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bonjour_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0160-show-bonjour_name"},
			},
			{
				Query: "SET bonjour_name TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0161-set-bonjour_name-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('bonjour_name')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0162-select-current_setting-bonjour_name"},
			},
		},
	},
	{
		Name:        "set 'bytea_output' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW bytea_output", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0163-show-bytea_output"},
			},
			{
				Query: "SET bytea_output TO 'escape'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0164-set-bytea_output-to-escape"},
			},
			{
				Query: "SHOW bytea_output", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0165-show-bytea_output"},
			},
			{
				Query: "SET bytea_output TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0166-set-bytea_output-to-default"},
			},
			{
				Query: "SHOW bytea_output", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0167-show-bytea_output"},
			},
			{
				Query: "SELECT current_setting('bytea_output')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0168-select-current_setting-bytea_output"},
			},
		},
	},
	{
		Name:        "set 'check_function_bodies' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW check_function_bodies", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0169-show-check_function_bodies"},
			},
			{
				Query: "SET check_function_bodies TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0170-set-check_function_bodies-to-off"},
			},
			{
				Query: "SHOW check_function_bodies", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0171-show-check_function_bodies"},
			},
			{
				Query: "SET check_function_bodies TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0172-set-check_function_bodies-to-default"},
			},
			{
				Query: "SHOW check_function_bodies", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0173-show-check_function_bodies"},
			},
			{
				Query: "SELECT current_setting('check_function_bodies')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0174-select-current_setting-check_function_bodies"},
			},
		},
	},
	{
		Name:        "set 'checkpoint_completion_target' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW checkpoint_completion_target", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0175-show-checkpoint_completion_target"},
			},
			{
				Query: "SET checkpoint_completion_target TO '0.9'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0176-set-checkpoint_completion_target-to-0.9", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('checkpoint_completion_target')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0177-select-current_setting-checkpoint_completion_target"},
			},
		},
	},
	{
		Name:        "set 'checkpoint_flush_after' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW checkpoint_flush_after", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0178-show-checkpoint_flush_after"},
			},
			{
				Query: "SET checkpoint_flush_after TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0179-set-checkpoint_flush_after-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('checkpoint_flush_after')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0180-select-current_setting-checkpoint_flush_after"},
			},
		},
	},
	{
		Name:        "set 'checkpoint_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW checkpoint_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0181-show-checkpoint_timeout"},
			},
			{
				Query: "SET checkpoint_timeout TO '300'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0182-set-checkpoint_timeout-to-300", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('checkpoint_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0183-select-current_setting-checkpoint_timeout"},
			},
		},
	},
	{
		Name:        "set 'checkpoint_warning' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW checkpoint_warning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0184-show-checkpoint_warning"},
			},
			{
				Query: "SET checkpoint_warning TO '30'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0185-set-checkpoint_warning-to-30", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('checkpoint_warning')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0186-select-current_setting-checkpoint_warning"},
			},
		},
	},
	{
		Name:        "set 'client_connection_check_interval' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW client_connection_check_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0187-show-client_connection_check_interval"},
			},
			{
				Query: "SET client_connection_check_interval TO 10", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0188-set-client_connection_check_interval-to-10"},
			},
			{
				Query: "SHOW client_connection_check_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0189-show-client_connection_check_interval"},
			},
			{
				Query: "SET client_connection_check_interval TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0190-set-client_connection_check_interval-to-default"},
			},
			{
				Query: "SHOW client_connection_check_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0191-show-client_connection_check_interval"},
			},
			{
				Query: "SELECT current_setting('client_connection_check_interval')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0192-select-current_setting-client_connection_check_interval"},
			},
		},
	},
	{
		Name:        "set 'client_encoding' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW client_encoding", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0193-show-client_encoding"},
			},
			{
				Query: "SET client_encoding TO 'LATIN1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0194-set-client_encoding-to-latin1"},
			},
			{
				Query:    "SHOW client_encoding",
				Expected: []sql.Row{{"LATIN1"}},
			},
			{
				Query:    "SET client_encoding TO DEFAULT",
				Expected: []sql.Row{},
			},
			{
				Query: "SHOW client_encoding", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0197-show-client_encoding"},
			},
			{
				Query: "SELECT current_setting('client_encoding')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0198-select-current_setting-client_encoding"},
			},
		},
	},
	{
		Name:        "set 'client_min_messages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW client_min_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0199-show-client_min_messages"},
			},
			{
				Query: "SET client_min_messages TO 'log'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0200-set-client_min_messages-to-log"},
			},
			{
				Query: "SHOW client_min_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0201-show-client_min_messages"},
			},
			{
				Query: "SET client_min_messages TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0202-set-client_min_messages-to-default"},
			},
			{
				Query: "SHOW client_min_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0203-show-client_min_messages"},
			},
			{
				Query: "SELECT current_setting('client_min_messages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0204-select-current_setting-client_min_messages"},
			},
		},
	},
	{
		Name:        "set 'cluster_name' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW cluster_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0205-show-cluster_name"},
			},
			{
				Query: "SET cluster_name TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0206-set-cluster_name-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('cluster_name')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0207-select-current_setting-cluster_name"},
			},
		},
	},
	{
		Name:        "set 'commit_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW commit_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0208-show-commit_delay"},
			},
			{
				Query: "SET commit_delay TO 100000", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0209-set-commit_delay-to-100000"},
			},
			{
				Query: "SHOW commit_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0210-show-commit_delay"},
			},
			{
				Query: "SET commit_delay TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0211-set-commit_delay-to-default"},
			},
			{
				Query: "SHOW commit_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0212-show-commit_delay"},
			},
			{
				Query: "SELECT current_setting('commit_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0213-select-current_setting-commit_delay"},
			},
		},
	},
	{
		Name:        "set 'commit_siblings' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW commit_siblings", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0214-show-commit_siblings"},
			},
			{
				Query: "SET commit_siblings TO '1000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0215-set-commit_siblings-to-1000"},
			},
			{
				Query: "SHOW commit_siblings", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0216-show-commit_siblings"},
			},
			{
				Query: "SET commit_siblings TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0217-set-commit_siblings-to-default"},
			},
			{
				Query: "SHOW commit_siblings", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0218-show-commit_siblings"},
			},
			{
				Query: "SELECT current_setting('commit_siblings')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0219-select-current_setting-commit_siblings"},
			},
		},
	},
	{
		Name:        "set 'compute_query_id' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW compute_query_id", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0220-show-compute_query_id"},
			},
			{
				Query: "SET compute_query_id TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0221-set-compute_query_id-to-on"},
			},
			{
				Query: "SHOW compute_query_id", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0222-show-compute_query_id"},
			},
			{
				Query: "SET compute_query_id TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0223-set-compute_query_id-to-default"},
			},
			{
				Query: "SHOW compute_query_id", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0224-show-compute_query_id"},
			},
			{
				Query: "SELECT current_setting('compute_query_id')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0225-select-current_setting-compute_query_id"},
			},
		},
	},
	{
		Name:        "set 'config_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW config_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0226-show-config_file"},
			},
			{
				Query: "SET config_file TO '/Users/postgres/postgresql.conf'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0227-set-config_file-to-/users/postgres/postgresql.conf", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('config_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0228-select-current_setting-config_file"},
			},
		},
	},
	{
		Name:        "set 'constraint_exclusion' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW constraint_exclusion", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0229-show-constraint_exclusion"},
			},
			{
				Query: "SET constraint_exclusion TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0230-set-constraint_exclusion-to-on"},
			},
			{
				Query: "SHOW constraint_exclusion", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0231-show-constraint_exclusion"},
			},
			{
				Query: "SET constraint_exclusion TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0232-set-constraint_exclusion-to-default"},
			},
			{
				Query: "SHOW constraint_exclusion", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0233-show-constraint_exclusion"},
			},
			{
				Query: "SELECT current_setting('constraint_exclusion')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0234-select-current_setting-constraint_exclusion"},
			},
		},
	},
	{
		Name:        "set 'cpu_index_tuple_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW cpu_index_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0235-show-cpu_index_tuple_cost"},
			},
			{
				Query: "SET cpu_index_tuple_cost TO '0.01'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0236-set-cpu_index_tuple_cost-to-0.01"},
			},
			{
				Query: "SHOW cpu_index_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0237-show-cpu_index_tuple_cost"},
			},
			{
				Query: "SET cpu_index_tuple_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0238-set-cpu_index_tuple_cost-to-default"},
			},
			{
				Query: "SHOW cpu_index_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0239-show-cpu_index_tuple_cost"},
			},
			{
				Query: "SELECT current_setting('cpu_index_tuple_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0240-select-current_setting-cpu_index_tuple_cost"},
			},
		},
	},
	{
		Name:        "set 'cpu_operator_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW cpu_operator_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0241-show-cpu_operator_cost"},
			},
			{
				Query: "SET cpu_operator_cost TO '0.005'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0242-set-cpu_operator_cost-to-0.005"},
			},
			{
				Query: "SHOW cpu_operator_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0243-show-cpu_operator_cost"},
			},
			{
				Query: "SET cpu_operator_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0244-set-cpu_operator_cost-to-default"},
			},
			{
				Query: "SHOW cpu_operator_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0245-show-cpu_operator_cost"},
			},
			{
				Query: "SELECT current_setting('cpu_operator_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0246-select-current_setting-cpu_operator_cost"},
			},
		},
	},
	{
		Name:        "set 'cpu_tuple_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW cpu_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0247-show-cpu_tuple_cost"},
			},
			{
				Query: "SET cpu_tuple_cost TO '0.02'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0248-set-cpu_tuple_cost-to-0.02"},
			},
			{
				Query: "SHOW cpu_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0249-show-cpu_tuple_cost"},
			},
			{
				Query: "SET cpu_tuple_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0250-set-cpu_tuple_cost-to-default"},
			},
			{
				Query: "SHOW cpu_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0251-show-cpu_tuple_cost"},
			},
			{
				Query: "SELECT current_setting('cpu_tuple_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0252-select-current_setting-cpu_tuple_cost"},
			},
		},
	},
	{
		Name:        "set 'createrole_self_grant' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW createrole_self_grant", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0253-show-createrole_self_grant"},
			},
			{
				Query: "SET createrole_self_grant TO 'inherit'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0254-set-createrole_self_grant-to-inherit"},
			},
			{
				Query: "SHOW createrole_self_grant", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0255-show-createrole_self_grant"},
			},
			{
				Query: "SET createrole_self_grant TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0256-set-createrole_self_grant-to-default"},
			},
			{
				Query: "SHOW createrole_self_grant", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0257-show-createrole_self_grant"},
			},
			{
				Query: "SELECT current_setting('createrole_self_grant')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0258-select-current_setting-createrole_self_grant"},
			},
		},
	},
	{
		Name:        "set 'cursor_tuple_fraction' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW cursor_tuple_fraction", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0259-show-cursor_tuple_fraction"},
			},
			{
				Query: "SET cursor_tuple_fraction TO '0.2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0260-set-cursor_tuple_fraction-to-0.2"},
			},
			{
				Query: "SHOW cursor_tuple_fraction", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0261-show-cursor_tuple_fraction"},
			},
			{
				Query: "SET cursor_tuple_fraction TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0262-set-cursor_tuple_fraction-to-default"},
			},
			{
				Query: "SHOW cursor_tuple_fraction", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0263-show-cursor_tuple_fraction"},
			},
			{
				Query: "SELECT current_setting('cursor_tuple_fraction')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0264-select-current_setting-cursor_tuple_fraction"},
			},
		},
	},
	{
		Name:        "set 'data_checksums' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW data_checksums", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0265-show-data_checksums"},
			},
			{
				Query: "SET data_checksums TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0266-set-data_checksums-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('data_checksums')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0267-select-current_setting-data_checksums"},
			},
		},
	},
	{
		Name:        "set 'data_directory' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW data_directory", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0268-show-data_directory"},
			},
			{
				Query: "SET data_directory TO '/Users/postgres'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0269-set-data_directory-to-/users/postgres", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('data_directory')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0270-select-current_setting-data_directory"},
			},
		},
	},
	{
		Name:        "set 'data_directory_mode' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW data_directory_mode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0271-show-data_directory_mode"},
			},
			{
				Query: "SET data_directory_mode TO '448'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0272-set-data_directory_mode-to-448", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('data_directory_mode')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0273-select-current_setting-data_directory_mode"},
			},
		},
	},
	{
		Name:        "set 'data_sync_retry' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW data_sync_retry", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0274-show-data_sync_retry"},
			},
			{
				Query: "SET data_sync_retry TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0275-set-data_sync_retry-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('data_sync_retry')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0276-select-current_setting-data_sync_retry"},
			},
		},
	},
	{
		Name:        "set 'DateStyle' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW DateStyle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0277-show-datestyle"},
			},
			{
				Query: "SET DateStyle TO 'ISO, DMY'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0278-set-datestyle-to-iso-dmy"},
			},
			{
				Query: "SHOW DateStyle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0279-show-datestyle"},
			},
			{
				Query: "SET DateStyle TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0280-set-datestyle-to-default"},
			},
			{
				Query: "SHOW DateStyle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0281-show-datestyle"},
			},
			{
				Query: "SELECT current_setting('DateStyle')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0282-select-current_setting-datestyle"},
			},
		},
	},
	{
		Name:        "set 'db_user_namespace' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW db_user_namespace", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0283-show-db_user_namespace"},
			},
			{
				Query: "SET db_user_namespace TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0284-set-db_user_namespace-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('db_user_namespace')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0285-select-current_setting-db_user_namespace"},
			},
		},
	},
	{
		Name:        "set 'deadlock_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW deadlock_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0286-show-deadlock_timeout"},
			},
			{
				Query: "SET deadlock_timeout TO '2000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0287-set-deadlock_timeout-to-2000"},
			},
			{
				Query: "SHOW deadlock_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0288-show-deadlock_timeout"},
			},
			{
				Query: "SET deadlock_timeout TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0289-set-deadlock_timeout-to-default"},
			},
			{
				Query: "SHOW deadlock_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0290-show-deadlock_timeout"},
			},
			{
				Query: "SELECT current_setting('deadlock_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0291-select-current_setting-deadlock_timeout"},
			},
		},
	},
	{
		Name:        "set 'debug_assertions' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_assertions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0292-show-debug_assertions"},
			},
			{
				Query: "SET debug_assertions TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0293-set-debug_assertions-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('debug_assertions')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0294-select-current_setting-debug_assertions"},
			},
		},
	},
	{
		Name:        "set 'debug_discard_caches' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_discard_caches", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0295-show-debug_discard_caches"},
			},
			{
				Query: "SET debug_discard_caches TO '0'", PostgresOracle: // cannot set it to anything other than 0
				ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0296-set-debug_discard_caches-to-0"},
			},
			{
				Query: "SHOW debug_discard_caches", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0297-show-debug_discard_caches"},
			},
			{
				Query: "SELECT current_setting('debug_discard_caches')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0298-select-current_setting-debug_discard_caches"},
			},
		},
	},
	{
		Name:        "set 'debug_io_direct' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_io_direct", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0299-show-debug_io_direct"},
			},
			{
				Query: "SET debug_io_direct TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0300-set-debug_io_direct-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('debug_io_direct')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0301-select-current_setting-debug_io_direct"},
			},
		},
	},
	{
		Name:        "set 'debug_logical_replication_streaming' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_logical_replication_streaming", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0302-show-debug_logical_replication_streaming"},
			},
			{
				Query: "SET debug_logical_replication_streaming TO 'immediate'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0303-set-debug_logical_replication_streaming-to-immediate"},
			},
			{
				Query: "SHOW debug_logical_replication_streaming", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0304-show-debug_logical_replication_streaming"},
			},
			{
				Query: "SET debug_logical_replication_streaming TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0305-set-debug_logical_replication_streaming-to-default"},
			},
			{
				Query: "SHOW debug_logical_replication_streaming", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0306-show-debug_logical_replication_streaming"},
			},
			{
				Query: "SELECT current_setting('debug_logical_replication_streaming')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0307-select-current_setting-debug_logical_replication_streaming"},
			},
		},
	},
	{
		Name:        "set 'debug_parallel_query' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_parallel_query", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0308-show-debug_parallel_query"},
			},
			{
				Query: "SET debug_parallel_query TO 'regress'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0309-set-debug_parallel_query-to-regress"},
			},
			{
				Query: "SHOW debug_parallel_query", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0310-show-debug_parallel_query"},
			},
			{
				Query: "SET debug_parallel_query TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0311-set-debug_parallel_query-to-default"},
			},
			{
				Query: "SHOW debug_parallel_query", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0312-show-debug_parallel_query"},
			},
			{
				Query: "SELECT current_setting('debug_parallel_query')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0313-select-current_setting-debug_parallel_query"},
			},
		},
	},
	{
		Name:        "set 'debug_pretty_print' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_pretty_print", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0314-show-debug_pretty_print"},
			},
			{
				Query: "SET debug_pretty_print TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0315-set-debug_pretty_print-to-off"},
			},
			{
				Query: "SHOW debug_pretty_print", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0316-show-debug_pretty_print"},
			},
			{
				Query: "SET debug_pretty_print TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0317-set-debug_pretty_print-to-default"},
			},
			{
				Query: "SHOW debug_pretty_print", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0318-show-debug_pretty_print"},
			},
			{
				Query: "SELECT current_setting('debug_pretty_print')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0319-select-current_setting-debug_pretty_print"},
			},
		},
	},
	{
		Name:        "set 'debug_print_parse' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_print_parse", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0320-show-debug_print_parse"},
			},
			{
				Query: "SET debug_print_parse TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0321-set-debug_print_parse-to-on"},
			},
			{
				Query: "SHOW debug_print_parse", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0322-show-debug_print_parse"},
			},
			{
				Query: "SET debug_print_parse TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0323-set-debug_print_parse-to-default"},
			},
			{
				Query: "SHOW debug_print_parse", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0324-show-debug_print_parse"},
			},
			{
				Query: "SELECT current_setting('debug_print_parse')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0325-select-current_setting-debug_print_parse"},
			},
		},
	},
	{
		Name:        "set 'debug_print_plan' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_print_plan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0326-show-debug_print_plan"},
			},
			{
				Query: "SET debug_print_plan TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0327-set-debug_print_plan-to-on"},
			},
			{
				Query: "SHOW debug_print_plan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0328-show-debug_print_plan"},
			},
			{
				Query: "SET debug_print_plan TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0329-set-debug_print_plan-to-default"},
			},
			{
				Query: "SHOW debug_print_plan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0330-show-debug_print_plan"},
			},
			{
				Query: "SELECT current_setting('debug_print_plan')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0331-select-current_setting-debug_print_plan"},
			},
		},
	},
	{
		Name:        "set 'debug_print_rewritten' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW debug_print_rewritten", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0332-show-debug_print_rewritten"},
			},
			{
				Query: "SET debug_print_rewritten TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0333-set-debug_print_rewritten-to-on"},
			},
			{
				Query: "SHOW debug_print_rewritten", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0334-show-debug_print_rewritten"},
			},
			{
				Query: "SET debug_print_rewritten TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0335-set-debug_print_rewritten-to-default"},
			},
			{
				Query: "SHOW debug_print_rewritten", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0336-show-debug_print_rewritten"},
			},
			{
				Query: "SELECT current_setting('debug_print_rewritten')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0337-select-current_setting-debug_print_rewritten"},
			},
		},
	},
	{
		Name:        "set 'default_statistics_target' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_statistics_target", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0338-show-default_statistics_target"},
			},
			{
				Query: "SET default_statistics_target TO '10000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0339-set-default_statistics_target-to-10000"},
			},
			{
				Query: "SHOW default_statistics_target", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0340-show-default_statistics_target"},
			},
			{
				Query: "SET default_statistics_target TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0341-set-default_statistics_target-to-default"},
			},
			{
				Query: "SHOW default_statistics_target", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0342-show-default_statistics_target"},
			},
			{
				Query: "SELECT current_setting('default_statistics_target')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0343-select-current_setting-default_statistics_target"},
			},
		},
	},
	{
		Name:        "set 'default_table_access_method' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_table_access_method", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0344-show-default_table_access_method"},
			},
			{
				Query: "SET default_table_access_method TO 'heap'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0345-set-default_table_access_method-to-heap"},
			},
			{
				Query: "SHOW default_table_access_method", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0346-show-default_table_access_method"},
			},
			{
				Query: "SELECT current_setting('default_table_access_method')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0347-select-current_setting-default_table_access_method"},
			},
		},
	},
	{
		Name:        "set 'default_tablespace' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_tablespace", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0348-show-default_tablespace"},
			},
			{
				Query: "SET default_tablespace TO 'pg_default'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0349-set-default_tablespace-to-pg_default"},
			},
			{
				Query: "SHOW default_tablespace", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0350-show-default_tablespace"},
			},
			{
				Query: "SET default_tablespace TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0351-set-default_tablespace-to-default"},
			},
			{
				Query: "SHOW default_tablespace", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0352-show-default_tablespace"},
			},
			{
				Query: "SELECT current_setting('default_tablespace')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0353-select-current_setting-default_tablespace"},
			},
		},
	},
	{
		Name:        "set 'default_text_search_config' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_text_search_config", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0354-show-default_text_search_config"},
			},
			{
				Query: "SET default_text_search_config TO 'pg_catalog.spanish'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0355-set-default_text_search_config-to-pg_catalog.spanish"},
			},
			{
				Query: "SHOW default_text_search_config", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0356-show-default_text_search_config"},
			},
			{
				Query: "SET default_text_search_config TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0357-set-default_text_search_config-to-default"},
			},
			{
				Query: "SHOW default_text_search_config", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0358-show-default_text_search_config"},
			},
			{
				Query: "SELECT current_setting('default_text_search_config')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0359-select-current_setting-default_text_search_config"},
			},
		},
	},
	{
		Name:        "set 'default_toast_compression' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_toast_compression", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0360-show-default_toast_compression"},
			},
			{
				Query: "SET default_toast_compression TO 'lz4'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0361-set-default_toast_compression-to-lz4"},
			},
			{
				Query: "SHOW default_toast_compression", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0362-show-default_toast_compression"},
			},
			{
				Query: "SET default_toast_compression TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0363-set-default_toast_compression-to-default"},
			},
			{
				Query: "SHOW default_toast_compression", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0364-show-default_toast_compression"},
			},
			{
				Query: "SELECT current_setting('default_toast_compression')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0365-select-current_setting-default_toast_compression"},
			},
		},
	},
	{
		Name:        "set 'default_transaction_deferrable' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_transaction_deferrable", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0366-show-default_transaction_deferrable"},
			},
			{
				Query: "SET default_transaction_deferrable TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0367-set-default_transaction_deferrable-to-on"},
			},
			{
				Query: "SHOW default_transaction_deferrable", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0368-show-default_transaction_deferrable"},
			},
			{
				Query: "SET default_transaction_deferrable TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0369-set-default_transaction_deferrable-to-default"},
			},
			{
				Query: "SHOW default_transaction_deferrable", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0370-show-default_transaction_deferrable"},
			},
			{
				Query: "SELECT current_setting('default_transaction_deferrable')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0371-select-current_setting-default_transaction_deferrable"},
			},
		},
	},
	{
		Name:        "set 'default_transaction_isolation' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_transaction_isolation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0372-show-default_transaction_isolation"},
			},
			{
				Query: "SET default_transaction_isolation TO 'serializable'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0373-set-default_transaction_isolation-to-serializable"},
			},
			{
				Query: "SHOW default_transaction_isolation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0374-show-default_transaction_isolation"},
			},
			{
				Query: "SET default_transaction_isolation TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0375-set-default_transaction_isolation-to-default"},
			},
			{
				Query: "SHOW default_transaction_isolation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0376-show-default_transaction_isolation"},
			},
			{
				Query: "SELECT current_setting('default_transaction_isolation')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0377-select-current_setting-default_transaction_isolation"},
			},
		},
	},
	{
		Name:        "set 'default_transaction_read_only' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW default_transaction_read_only", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0378-show-default_transaction_read_only"},
			},
			{
				Query: "SET default_transaction_read_only TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0379-set-default_transaction_read_only-to-on"},
			},
			{
				Query: "SHOW default_transaction_read_only", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0380-show-default_transaction_read_only"},
			},
			{
				Query: "SET default_transaction_read_only TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0381-set-default_transaction_read_only-to-default"},
			},
			{
				Query: "SHOW default_transaction_read_only", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0382-show-default_transaction_read_only"},
			},
			{
				Query: "SELECT current_setting('default_transaction_read_only')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0383-select-current_setting-default_transaction_read_only"},
			},
		},
	},
	{
		Name:        "set 'dynamic_library_path' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW dynamic_library_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0384-show-dynamic_library_path"},
			},
			{
				Query: "SET dynamic_library_path TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0385-set-dynamic_library_path-to"},
			},
			{
				Query: "SHOW dynamic_library_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0386-show-dynamic_library_path"},
			},
			{
				Query: "SET dynamic_library_path TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0387-set-dynamic_library_path-to-default"},
			},
			{
				Query: "SHOW dynamic_library_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0388-show-dynamic_library_path"},
			},
			{
				Query: "SELECT current_setting('dynamic_library_path')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0389-select-current_setting-dynamic_library_path"},
			},
		},
	},
	{
		Name:        "set 'dynamic_shared_memory_type' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW dynamic_shared_memory_type", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0390-show-dynamic_shared_memory_type"},
			},
			{
				Query: "SET dynamic_shared_memory_type TO 'posix'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0391-set-dynamic_shared_memory_type-to-posix", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('dynamic_shared_memory_type')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0392-select-current_setting-dynamic_shared_memory_type"},
			},
		},
	},
	{
		Name:        "set 'effective_cache_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW effective_cache_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0393-show-effective_cache_size"},
			},
			{
				Query: "SET effective_cache_size TO '400000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0394-set-effective_cache_size-to-400000"},
			},
			{
				Query: "SHOW effective_cache_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0395-show-effective_cache_size"},
			},
			{
				Query: "SET effective_cache_size TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0396-set-effective_cache_size-to-default"},
			},
			{
				Query: "SHOW effective_cache_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0397-show-effective_cache_size"},
			},
			{
				Query: "SELECT current_setting('effective_cache_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0398-select-current_setting-effective_cache_size"},
			},
		},
	},
	{
		Name:        "set 'effective_io_concurrency' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW effective_io_concurrency", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0399-show-effective_io_concurrency"},
			},
			{
				Query: "SET effective_io_concurrency TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0400-set-effective_io_concurrency-to-100", Compare: "sqlstate"},
			},
			{
				Query:    "SHOW effective_io_concurrency",
				Expected: []sql.Row{{int64(100)}},
			},
			{
				Query: "SET effective_io_concurrency TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0402-set-effective_io_concurrency-to-default"},
			},
			{
				Query:    "SHOW effective_io_concurrency",
				Expected: []sql.Row{{int64(0)}},
			},
			{
				Query: "SELECT current_setting('effective_io_concurrency')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0404-select-current_setting-effective_io_concurrency"},
			},
		},
	},
	{
		Name:        "set 'enable_async_append' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_async_append", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0405-show-enable_async_append"},
			},
			{
				Query: "SET enable_async_append TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0406-set-enable_async_append-to-off"},
			},
			{
				Query: "SHOW enable_async_append", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0407-show-enable_async_append"},
			},
			{
				Query: "SET enable_async_append TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0408-set-enable_async_append-to-default"},
			},
			{
				Query: "SHOW enable_async_append", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0409-show-enable_async_append"},
			},
			{
				Query: "SELECT current_setting('enable_async_append')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0410-select-current_setting-enable_async_append"},
			},
		},
	},
	{
		Name:        "set 'enable_bitmapscan' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_bitmapscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0411-show-enable_bitmapscan"},
			},
			{
				Query: "SET enable_bitmapscan TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0412-set-enable_bitmapscan-to-off"},
			},
			{
				Query: "SHOW enable_bitmapscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0413-show-enable_bitmapscan"},
			},
			{
				Query: "SET enable_bitmapscan TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0414-set-enable_bitmapscan-to-default"},
			},
			{
				Query: "SHOW enable_bitmapscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0415-show-enable_bitmapscan"},
			},
			{
				Query: "SELECT current_setting('enable_bitmapscan')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0416-select-current_setting-enable_bitmapscan"},
			},
		},
	},
	{
		Name:        "set 'enable_gathermerge' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_gathermerge", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0417-show-enable_gathermerge"},
			},
			{
				Query: "SET enable_gathermerge TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0418-set-enable_gathermerge-to-off"},
			},
			{
				Query: "SHOW enable_gathermerge", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0419-show-enable_gathermerge"},
			},
			{
				Query: "SET enable_gathermerge TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0420-set-enable_gathermerge-to-default"},
			},
			{
				Query: "SHOW enable_gathermerge", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0421-show-enable_gathermerge"},
			},
			{
				Query: "SELECT current_setting('enable_gathermerge')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0422-select-current_setting-enable_gathermerge"},
			},
		},
	},
	{
		Name:        "set 'enable_hashagg' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_hashagg", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0423-show-enable_hashagg"},
			},
			{
				Query: "SET enable_hashagg TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0424-set-enable_hashagg-to-off"},
			},
			{
				Query: "SHOW enable_hashagg", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0425-show-enable_hashagg"},
			},
			{
				Query: "SET enable_hashagg TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0426-set-enable_hashagg-to-default"},
			},
			{
				Query: "SHOW enable_hashagg", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0427-show-enable_hashagg"},
			},
			{
				Query: "SELECT current_setting('enable_hashagg')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0428-select-current_setting-enable_hashagg"},
			},
		},
	},
	{
		Name:        "set 'enable_hashjoin' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_hashjoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0429-show-enable_hashjoin"},
			},
			{
				Query: "SET enable_hashjoin TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0430-set-enable_hashjoin-to-off"},
			},
			{
				Query: "SHOW enable_hashjoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0431-show-enable_hashjoin"},
			},
			{
				Query: "SET enable_hashjoin TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0432-set-enable_hashjoin-to-default"},
			},
			{
				Query: "SHOW enable_hashjoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0433-show-enable_hashjoin"},
			},
			{
				Query: "SELECT current_setting('enable_hashjoin')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0434-select-current_setting-enable_hashjoin"},
			},
		},
	},
	{
		Name:        "set 'enable_incremental_sort' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_incremental_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0435-show-enable_incremental_sort"},
			},
			{
				Query: "SET enable_incremental_sort TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0436-set-enable_incremental_sort-to-off"},
			},
			{
				Query: "SHOW enable_incremental_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0437-show-enable_incremental_sort"},
			},
			{
				Query: "SET enable_incremental_sort TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0438-set-enable_incremental_sort-to-default"},
			},
			{
				Query: "SHOW enable_incremental_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0439-show-enable_incremental_sort"},
			},
			{
				Query: "SELECT current_setting('enable_incremental_sort')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0440-select-current_setting-enable_incremental_sort"},
			},
		},
	},
	{
		Name:        "set 'enable_indexonlyscan' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_indexonlyscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0441-show-enable_indexonlyscan"},
			},
			{
				Query: "SET enable_indexonlyscan TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0442-set-enable_indexonlyscan-to-off"},
			},
			{
				Query: "SHOW enable_indexonlyscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0443-show-enable_indexonlyscan"},
			},
			{
				Query: "SET enable_indexonlyscan TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0444-set-enable_indexonlyscan-to-default"},
			},
			{
				Query: "SHOW enable_indexonlyscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0445-show-enable_indexonlyscan"},
			},
			{
				Query: "SELECT current_setting('enable_indexonlyscan')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0446-select-current_setting-enable_indexonlyscan"},
			},
		},
	},
	{
		Name:        "set 'enable_indexscan' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_indexscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0447-show-enable_indexscan"},
			},
			{
				Query: "SET enable_indexscan TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0448-set-enable_indexscan-to-off"},
			},
			{
				Query: "SHOW enable_indexscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0449-show-enable_indexscan"},
			},
			{
				Query: "SET enable_indexscan TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0450-set-enable_indexscan-to-default"},
			},
			{
				Query: "SHOW enable_indexscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0451-show-enable_indexscan"},
			},
			{
				Query: "SELECT current_setting('enable_indexscan')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0452-select-current_setting-enable_indexscan"},
			},
		},
	},
	{
		Name:        "set 'enable_material' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_material", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0453-show-enable_material"},
			},
			{
				Query: "SET enable_material TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0454-set-enable_material-to-off"},
			},
			{
				Query: "SHOW enable_material", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0455-show-enable_material"},
			},
			{
				Query: "SET enable_material TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0456-set-enable_material-to-default"},
			},
			{
				Query: "SHOW enable_material", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0457-show-enable_material"},
			},
			{
				Query: "SELECT current_setting('enable_material')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0458-select-current_setting-enable_material"},
			},
		},
	},
	{
		Name:        "set 'enable_memoize' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_memoize", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0459-show-enable_memoize"},
			},
			{
				Query: "SET enable_memoize TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0460-set-enable_memoize-to-off"},
			},
			{
				Query: "SHOW enable_memoize", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0461-show-enable_memoize"},
			},
			{
				Query: "SET enable_memoize TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0462-set-enable_memoize-to-default"},
			},
			{
				Query: "SHOW enable_memoize", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0463-show-enable_memoize"},
			},
			{
				Query: "SELECT current_setting('enable_memoize')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0464-select-current_setting-enable_memoize"},
			},
		},
	},
	{
		Name:        "set 'enable_mergejoin' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_mergejoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0465-show-enable_mergejoin"},
			},
			{
				Query: "SET enable_mergejoin TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0466-set-enable_mergejoin-to-off"},
			},
			{
				Query: "SHOW enable_mergejoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0467-show-enable_mergejoin"},
			},
			{
				Query: "SET enable_mergejoin TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0468-set-enable_mergejoin-to-default"},
			},
			{
				Query: "SHOW enable_mergejoin", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0469-show-enable_mergejoin"},
			},
			{
				Query: "SELECT current_setting('enable_mergejoin')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0470-select-current_setting-enable_mergejoin"},
			},
		},
	},
	{
		Name:        "set 'enable_nestloop' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_nestloop", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0471-show-enable_nestloop"},
			},
			{
				Query: "SET enable_nestloop TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0472-set-enable_nestloop-to-off"},
			},
			{
				Query: "SHOW enable_nestloop", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0473-show-enable_nestloop"},
			},
			{
				Query: "SET enable_nestloop TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0474-set-enable_nestloop-to-default"},
			},
			{
				Query: "SHOW enable_nestloop", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0475-show-enable_nestloop"},
			},
			{
				Query: "SELECT current_setting('enable_nestloop')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0476-select-current_setting-enable_nestloop"},
			},
		},
	},
	{
		Name:        "set 'enable_parallel_append' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_parallel_append", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0477-show-enable_parallel_append"},
			},
			{
				Query: "SET enable_parallel_append TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0478-set-enable_parallel_append-to-off"},
			},
			{
				Query: "SHOW enable_parallel_append", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0479-show-enable_parallel_append"},
			},
			{
				Query: "SET enable_parallel_append TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0480-set-enable_parallel_append-to-default"},
			},
			{
				Query: "SHOW enable_parallel_append", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0481-show-enable_parallel_append"},
			},
			{
				Query: "SELECT current_setting('enable_parallel_append')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0482-select-current_setting-enable_parallel_append"},
			},
		},
	},
	{
		Name:        "set 'enable_parallel_hash' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_parallel_hash", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0483-show-enable_parallel_hash"},
			},
			{
				Query: "SET enable_parallel_hash TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0484-set-enable_parallel_hash-to-off"},
			},
			{
				Query: "SHOW enable_parallel_hash", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0485-show-enable_parallel_hash"},
			},
			{
				Query: "SET enable_parallel_hash TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0486-set-enable_parallel_hash-to-default"},
			},
			{
				Query: "SHOW enable_parallel_hash", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0487-show-enable_parallel_hash"},
			},
			{
				Query: "SELECT current_setting('enable_parallel_hash')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0488-select-current_setting-enable_parallel_hash"},
			},
		},
	},
	{
		Name:        "set 'enable_partition_pruning' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_partition_pruning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0489-show-enable_partition_pruning"},
			},
			{
				Query: "SET enable_partition_pruning TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0490-set-enable_partition_pruning-to-off"},
			},
			{
				Query: "SHOW enable_partition_pruning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0491-show-enable_partition_pruning"},
			},
			{
				Query: "SET enable_partition_pruning TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0492-set-enable_partition_pruning-to-default"},
			},
			{
				Query: "SHOW enable_partition_pruning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0493-show-enable_partition_pruning"},
			},
			{
				Query: "SELECT current_setting('enable_partition_pruning')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0494-select-current_setting-enable_partition_pruning"},
			},
		},
	},
	{
		Name:        "set 'enable_partitionwise_aggregate' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_partitionwise_aggregate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0495-show-enable_partitionwise_aggregate"},
			},
			{
				Query: "SET enable_partitionwise_aggregate TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0496-set-enable_partitionwise_aggregate-to-on"},
			},
			{
				Query: "SHOW enable_partitionwise_aggregate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0497-show-enable_partitionwise_aggregate"},
			},
			{
				Query: "SET enable_partitionwise_aggregate TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0498-set-enable_partitionwise_aggregate-to-default"},
			},
			{
				Query: "SHOW enable_partitionwise_aggregate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0499-show-enable_partitionwise_aggregate"},
			},
			{
				Query: "SELECT current_setting('enable_partitionwise_aggregate')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0500-select-current_setting-enable_partitionwise_aggregate"},
			},
		},
	},
	{
		Name:        "set 'enable_partitionwise_join' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_partitionwise_join", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0501-show-enable_partitionwise_join"},
			},
			{
				Query: "SET enable_partitionwise_join TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0502-set-enable_partitionwise_join-to-on"},
			},
			{
				Query: "SHOW enable_partitionwise_join", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0503-show-enable_partitionwise_join"},
			},
			{
				Query: "SET enable_partitionwise_join TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0504-set-enable_partitionwise_join-to-default"},
			},
			{
				Query: "SHOW enable_partitionwise_join", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0505-show-enable_partitionwise_join"},
			},
			{
				Query: "SELECT current_setting('enable_partitionwise_join')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0506-select-current_setting-enable_partitionwise_join"},
			},
		},
	},
	{
		Name:        "set 'enable_presorted_aggregate' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_presorted_aggregate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0507-show-enable_presorted_aggregate"},
			},
			{
				Query: "SET enable_presorted_aggregate TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0508-set-enable_presorted_aggregate-to-off"},
			},
			{
				Query: "SHOW enable_presorted_aggregate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0509-show-enable_presorted_aggregate"},
			},
			{
				Query: "SET enable_presorted_aggregate TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0510-set-enable_presorted_aggregate-to-default"},
			},
			{
				Query: "SHOW enable_presorted_aggregate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0511-show-enable_presorted_aggregate"},
			},
			{
				Query: "SELECT current_setting('enable_presorted_aggregate')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0512-select-current_setting-enable_presorted_aggregate"},
			},
		},
	},
	{
		Name:        "set 'enable_seqscan' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_seqscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0513-show-enable_seqscan"},
			},
			{
				Query: "SET enable_seqscan TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0514-set-enable_seqscan-to-off"},
			},
			{
				Query: "SHOW enable_seqscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0515-show-enable_seqscan"},
			},
			{
				Query: "SET enable_seqscan TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0516-set-enable_seqscan-to-default"},
			},
			{
				Query: "SHOW enable_seqscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0517-show-enable_seqscan"},
			},
			{
				Query: "SELECT current_setting('enable_seqscan')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0518-select-current_setting-enable_seqscan"},
			},
		},
	},
	{
		Name:        "set 'enable_sort' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0519-show-enable_sort"},
			},
			{
				Query: "SET enable_sort TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0520-set-enable_sort-to-off"},
			},
			{
				Query: "SHOW enable_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0521-show-enable_sort"},
			},
			{
				Query: "SET enable_sort TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0522-set-enable_sort-to-default"},
			},
			{
				Query: "SHOW enable_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0523-show-enable_sort"},
			},
			{
				Query: "SELECT current_setting('enable_sort')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0524-select-current_setting-enable_sort"},
			},
		},
	},
	{
		Name:        "set 'enable_tidscan' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW enable_tidscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0525-show-enable_tidscan"},
			},
			{
				Query: "SET enable_tidscan TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0526-set-enable_tidscan-to-off"},
			},
			{
				Query: "SHOW enable_tidscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0527-show-enable_tidscan"},
			},
			{
				Query: "SET enable_tidscan TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0528-set-enable_tidscan-to-default"},
			},
			{
				Query: "SHOW enable_tidscan", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0529-show-enable_tidscan"},
			},
			{
				Query: "SELECT current_setting('enable_tidscan')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0530-select-current_setting-enable_tidscan"},
			},
		},
	},
	{
		Name:        "set 'escape_string_warning' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW escape_string_warning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0531-show-escape_string_warning"},
			},
			{
				Query: "SET escape_string_warning TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0532-set-escape_string_warning-to-off"},
			},
			{
				Query: "SHOW escape_string_warning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0533-show-escape_string_warning"},
			},
			{
				Query: "SET escape_string_warning TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0534-set-escape_string_warning-to-default"},
			},
			{
				Query: "SHOW escape_string_warning", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0535-show-escape_string_warning"},
			},
			{
				Query: "SELECT current_setting('escape_string_warning')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0536-select-current_setting-escape_string_warning"},
			},
		},
	},
	{
		Name:        "set 'event_source' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW event_source", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0537-show-event_source"},
			},
			{
				Query: "SET event_source TO 'PostgreSQL'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0538-set-event_source-to-postgresql", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('event_source')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0539-select-current_setting-event_source"},
			},
		},
	},
	{
		Name:        "set 'exit_on_error' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW exit_on_error", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0540-show-exit_on_error"},
			},
			{
				Query: "SET exit_on_error TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0541-set-exit_on_error-to-on"},
			},
			{
				Query: "SHOW exit_on_error", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0542-show-exit_on_error"},
			},
			{
				Query: "SET exit_on_error TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0543-set-exit_on_error-to-default"},
			},
			{
				Query: "SHOW exit_on_error", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0544-show-exit_on_error"},
			},
			{
				Query: "SELECT current_setting('exit_on_error')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0545-select-current_setting-exit_on_error"},
			},
		},
	},
	{
		Name:        "set 'external_pid_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW external_pid_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0546-show-external_pid_file"},
			},
			{
				Query: "SET external_pid_file TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0547-set-external_pid_file-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('external_pid_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0548-select-current_setting-external_pid_file"},
			},
		},
	},
	{
		Name:        "set 'extra_float_digits' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW extra_float_digits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0549-show-extra_float_digits"},
			},
			{
				Query: "SET extra_float_digits TO -10", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0550-set-extra_float_digits-to-10"},
			},
			{
				Query: "SHOW extra_float_digits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0551-show-extra_float_digits"},
			},
			{
				Query: "SET extra_float_digits TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0552-set-extra_float_digits-to-default"},
			},
			{
				Query: "SHOW extra_float_digits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0553-show-extra_float_digits"},
			},
			{
				Query: "SELECT current_setting('extra_float_digits')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0554-select-current_setting-extra_float_digits"},
			},
		},
	},
	{
		Name:        "set 'from_collapse_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW from_collapse_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0555-show-from_collapse_limit"},
			},
			{
				Query: "SET from_collapse_limit TO 100", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0556-set-from_collapse_limit-to-100"},
			},
			{
				Query: "SHOW from_collapse_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0557-show-from_collapse_limit"},
			},
			{
				Query: "SET from_collapse_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0558-set-from_collapse_limit-to-default"},
			},
			{
				Query: "SHOW from_collapse_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0559-show-from_collapse_limit"},
			},
			{
				Query: "SELECT current_setting('from_collapse_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0560-select-current_setting-from_collapse_limit"},
			},
		},
	},
	{
		Name:        "set 'fsync' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW fsync", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0561-show-fsync"},
			},
			{
				Query: "SET fsync TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0562-set-fsync-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('fsync')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0563-select-current_setting-fsync"},
			},
		},
	},
	{
		Name:        "set 'full_page_writes' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW full_page_writes", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0564-show-full_page_writes"},
			},
			{
				Query: "SET full_page_writes TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0565-set-full_page_writes-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('full_page_writes')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0566-select-current_setting-full_page_writes"},
			},
		},
	},
	{
		Name:        "set 'geqo' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0567-show-geqo"},
			},
			{
				Query: "SET geqo TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0568-set-geqo-to-off"},
			},
			{
				Query: "SHOW geqo", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0569-show-geqo"},
			},
			{
				Query: "SET geqo TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0570-set-geqo-to-default"},
			},
			{
				Query: "SHOW geqo", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0571-show-geqo"},
			},
			{
				Query: "SELECT current_setting('geqo')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0572-select-current_setting-geqo"},
			},
		},
	},
	{
		Name:        "set 'geqo_effort' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_effort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0573-show-geqo_effort"},
			},
			{
				Query: "SET geqo_effort TO 10", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0574-set-geqo_effort-to-10"},
			},
			{
				Query: "SHOW geqo_effort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0575-show-geqo_effort"},
			},
			{
				Query: "SET geqo_effort TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0576-set-geqo_effort-to-default"},
			},
			{
				Query: "SHOW geqo_effort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0577-show-geqo_effort"},
			},
			{
				Query: "SELECT current_setting('geqo_effort')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0578-select-current_setting-geqo_effort"},
			},
		},
	},
	{
		Name:        "set 'geqo_generations' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_generations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0579-show-geqo_generations"},
			},
			{
				Query: "SET geqo_generations TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0580-set-geqo_generations-to-100"},
			},
			{
				Query: "SHOW geqo_generations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0581-show-geqo_generations"},
			},
			{
				Query: "SET geqo_generations TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0582-set-geqo_generations-to-default"},
			},
			{
				Query: "SHOW geqo_generations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0583-show-geqo_generations"},
			},
			{
				Query: "SELECT current_setting('geqo_generations')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0584-select-current_setting-geqo_generations"},
			},
			{
				Query: "SELECT current_setting('geqo_generations')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0585-select-current_setting-geqo_generations"},
			},
		},
	},
	{
		Name:        "set 'geqo_pool_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_pool_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0586-show-geqo_pool_size"},
			},
			{
				Query: "SET geqo_pool_size TO 1", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0587-set-geqo_pool_size-to-1"},
			},
			{
				Query: "SHOW geqo_pool_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0588-show-geqo_pool_size"},
			},
			{
				Query: "SET geqo_pool_size TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0589-set-geqo_pool_size-to-default"},
			},
			{
				Query: "SHOW geqo_pool_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0590-show-geqo_pool_size"},
			},
			{
				Query: "SELECT current_setting('geqo_pool_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0591-select-current_setting-geqo_pool_size"},
			},
		},
	},
	{
		Name:        "set 'geqo_seed' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_seed", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0592-show-geqo_seed"},
			},
			{
				Query: "SET geqo_seed TO 0.2", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0593-set-geqo_seed-to-0.2"},
			},
			{
				Query: "SHOW geqo_seed", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0594-show-geqo_seed"},
			},
			{
				Query: "SET geqo_seed TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0595-set-geqo_seed-to-default"},
			},
			{
				Query: "SHOW geqo_seed", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0596-show-geqo_seed"},
			},
			{
				Query: "SELECT current_setting('geqo_seed')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0597-select-current_setting-geqo_seed"},
			},
		},
	},
	{
		Name:        "set 'geqo_selection_bias' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_selection_bias", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0598-show-geqo_selection_bias"},
			},
			{
				Query: "SET geqo_selection_bias TO 1.7", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0599-set-geqo_selection_bias-to-1.7"},
			},
			{
				Query: "SHOW geqo_selection_bias", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0600-show-geqo_selection_bias"},
			},
			{
				Query: "SET geqo_selection_bias TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0601-set-geqo_selection_bias-to-default"},
			},
			{
				Query: "SHOW geqo_selection_bias", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0602-show-geqo_selection_bias"},
			},
			{
				Query: "SELECT current_setting('geqo_selection_bias')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0603-select-current_setting-geqo_selection_bias"},
			},
		},
	},
	{
		Name:        "set 'geqo_threshold' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW geqo_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0604-show-geqo_threshold"},
			},
			{
				Query: "SET geqo_threshold TO 22", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0605-set-geqo_threshold-to-22"},
			},
			{
				Query: "SHOW geqo_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0606-show-geqo_threshold"},
			},
			{
				Query: "SET geqo_threshold TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0607-set-geqo_threshold-to-default"},
			},
			{
				Query: "SHOW geqo_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0608-show-geqo_threshold"},
			},
			{
				Query: "SELECT current_setting('geqo_threshold')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0609-select-current_setting-geqo_threshold"},
			},
		},
	},
	{
		Name:        "set 'gin_fuzzy_search_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW gin_fuzzy_search_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0610-show-gin_fuzzy_search_limit"},
			},
			{
				Query: "SET gin_fuzzy_search_limit TO 2", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0611-set-gin_fuzzy_search_limit-to-2"},
			},
			{
				Query: "SHOW gin_fuzzy_search_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0612-show-gin_fuzzy_search_limit"},
			},
			{
				Query: "SET gin_fuzzy_search_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0613-set-gin_fuzzy_search_limit-to-default"},
			},
			{
				Query: "SHOW gin_fuzzy_search_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0614-show-gin_fuzzy_search_limit"},
			},
			{
				Query: "SELECT current_setting('gin_fuzzy_search_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0615-select-current_setting-gin_fuzzy_search_limit"},
			},
		},
	},
	{
		Name:        "set 'gin_pending_list_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW gin_pending_list_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0616-show-gin_pending_list_limit"},
			},
			{
				Query: "SET gin_pending_list_limit TO '4000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0617-set-gin_pending_list_limit-to-4000"},
			},
			{
				Query: "SHOW gin_pending_list_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0618-show-gin_pending_list_limit"},
			},
			{
				Query: "SET gin_pending_list_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0619-set-gin_pending_list_limit-to-default"},
			},
			{
				Query: "SHOW gin_pending_list_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0620-show-gin_pending_list_limit"},
			},
			{
				Query: "SELECT current_setting('gin_pending_list_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0621-select-current_setting-gin_pending_list_limit"},
			},
		},
	},
	{
		Name:        "set 'gss_accept_delegation' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW gss_accept_delegation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0622-show-gss_accept_delegation"},
			},
			{
				Query: "SET gss_accept_delegation TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0623-set-gss_accept_delegation-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('gss_accept_delegation')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0624-select-current_setting-gss_accept_delegation"},
			},
		},
	},
	{
		Name:        "set 'hash_mem_multiplier' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW hash_mem_multiplier", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0625-show-hash_mem_multiplier"},
			},
			{
				Query: "SET hash_mem_multiplier TO 20.1", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0626-set-hash_mem_multiplier-to-20.1"},
			},
			{
				Query: "SHOW hash_mem_multiplier", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0627-show-hash_mem_multiplier"},
			},
			{
				Query: "SET hash_mem_multiplier TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0628-set-hash_mem_multiplier-to-default"},
			},
			{
				Query: "SHOW hash_mem_multiplier", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0629-show-hash_mem_multiplier"},
			},
			{
				Query: "SELECT current_setting('hash_mem_multiplier')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0630-select-current_setting-hash_mem_multiplier"},
			},
		},
	},
	{
		Name:        "set 'hba_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW hba_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0631-show-hba_file"},
			},
			{
				Query: "SET hba_file TO '/Users/postgres/pg_hba.conf'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0632-set-hba_file-to-/users/postgres/pg_hba.conf", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('hba_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0633-select-current_setting-hba_file"},
			},
		},
	},
	{
		Name:        "set 'hot_standby' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW hot_standby", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0634-show-hot_standby"},
			},
			{
				Query: "SET hot_standby TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0635-set-hot_standby-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('hot_standby')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0636-select-current_setting-hot_standby"},
			},
		},
	},
	{
		Name:        "set 'hot_standby_feedback' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW hot_standby_feedback", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0637-show-hot_standby_feedback"},
			},
			{
				Query: "SET hot_standby_feedback TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0638-set-hot_standby_feedback-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('hot_standby_feedback')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0639-select-current_setting-hot_standby_feedback"},
			},
		},
	},
	{
		Name:        "set 'huge_page_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW huge_page_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0640-show-huge_page_size"},
			},
			{
				Query: "SET huge_page_size TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0641-set-huge_page_size-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('huge_page_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0642-select-current_setting-huge_page_size"},
			},
		},
	},
	{
		Name:        "set 'huge_pages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW huge_pages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0643-show-huge_pages"},
			},
			{
				Query: "SET huge_pages TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0644-set-huge_pages-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('huge_pages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0645-select-current_setting-huge_pages"},
			},
		},
	},
	{
		Name:        "set 'icu_validation_level' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW icu_validation_level", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0646-show-icu_validation_level"},
			},
			{
				Query: "SET icu_validation_level TO 'disabled'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0647-set-icu_validation_level-to-disabled"},
			},
			{
				Query: "SHOW icu_validation_level", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0648-show-icu_validation_level"},
			},
			{
				Query: "SET icu_validation_level TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0649-set-icu_validation_level-to-default"},
			},
			{
				Query: "SHOW icu_validation_level", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0650-show-icu_validation_level"},
			},
			{
				Query: "SELECT current_setting('icu_validation_level')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0651-select-current_setting-icu_validation_level"},
			},
		},
	},
	{
		Name:        "set 'ident_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ident_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0652-show-ident_file"},
			},
			{
				Query: "SET ident_file TO '/Users/postgres/pg_ident.conf'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0653-set-ident_file-to-/users/postgres/pg_ident.conf", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ident_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0654-select-current_setting-ident_file"},
			},
		},
	},
	{
		Name:        "set 'idle_in_transaction_session_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW idle_in_transaction_session_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0655-show-idle_in_transaction_session_timeout"},
			},
			{
				Query: "SET idle_in_transaction_session_timeout TO 2", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0656-set-idle_in_transaction_session_timeout-to-2"},
			},
			{
				Query: "SHOW idle_in_transaction_session_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0657-show-idle_in_transaction_session_timeout"},
			},
			{
				Query: "SET idle_in_transaction_session_timeout TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0658-set-idle_in_transaction_session_timeout-to-default"},
			},
			{
				Query: "SHOW idle_in_transaction_session_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0659-show-idle_in_transaction_session_timeout"},
			},
			{
				Query: "SELECT current_setting('idle_in_transaction_session_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0660-select-current_setting-idle_in_transaction_session_timeout"},
			},
		},
	},
	{
		Name:        "set 'idle_session_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW idle_session_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0661-show-idle_session_timeout"},
			},
			{
				Query: "SET idle_session_timeout TO '3'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0662-set-idle_session_timeout-to-3"},
			},
			{
				Query: "SHOW idle_session_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0663-show-idle_session_timeout"},
			},
			{
				Query: "SET idle_session_timeout TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0664-set-idle_session_timeout-to-default"},
			},
			{
				Query: "SHOW idle_session_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0665-show-idle_session_timeout"},
			},
			{
				Query: "SELECT current_setting('idle_session_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0666-select-current_setting-idle_session_timeout"},
			},
		},
	},
	{
		Name:        "set 'ignore_checksum_failure' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ignore_checksum_failure", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0667-show-ignore_checksum_failure"},
			},
			{
				Query: "SET ignore_checksum_failure TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0668-set-ignore_checksum_failure-to-on"},
			},
			{
				Query: "SHOW ignore_checksum_failure", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0669-show-ignore_checksum_failure"},
			},
			{
				Query: "SET ignore_checksum_failure TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0670-set-ignore_checksum_failure-to-default"},
			},
			{
				Query: "SHOW ignore_checksum_failure", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0671-show-ignore_checksum_failure"},
			},
			{
				Query: "SELECT current_setting('ignore_checksum_failure')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0672-select-current_setting-ignore_checksum_failure"},
			},
		},
	},
	{
		Name:        "set 'ignore_invalid_pages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ignore_invalid_pages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0673-show-ignore_invalid_pages"},
			},
			{
				Query: "SET ignore_invalid_pages TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0674-set-ignore_invalid_pages-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ignore_invalid_pages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0675-select-current_setting-ignore_invalid_pages"},
			},
		},
	},
	{
		Name:        "set 'ignore_system_indexes' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ignore_system_indexes", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0676-show-ignore_system_indexes"},
			},
			{
				Query: "SET ignore_system_indexes TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0677-set-ignore_system_indexes-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ignore_system_indexes')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0678-select-current_setting-ignore_system_indexes"},
			},
		},
	},
	{
		Name:        "set 'in_hot_standby' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW in_hot_standby", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0679-show-in_hot_standby"},
			},
			{
				Query: "SET in_hot_standby TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0680-set-in_hot_standby-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('in_hot_standby')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0681-select-current_setting-in_hot_standby"},
			},
		},
	},
	{
		Name:        "set 'integer_datetimes' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW integer_datetimes", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0682-show-integer_datetimes"},
			},
			{
				Query: "SET integer_datetimes TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0683-set-integer_datetimes-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('integer_datetimes')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0684-select-current_setting-integer_datetimes"},
			},
		},
	},
	{
		Name:        "set 'IntervalStyle' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW IntervalStyle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0685-show-intervalstyle"},
			},
			{
				Query: "SET IntervalStyle TO 'sql_standard'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0686-set-intervalstyle-to-sql_standard"},
			},
			{
				Query: "SHOW IntervalStyle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0687-show-intervalstyle"},
			},
			{
				Query: "SET IntervalStyle TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0688-set-intervalstyle-to-default"},
			},
			{
				Query: "SHOW IntervalStyle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0689-show-intervalstyle"},
			},
			{
				Query: "SELECT current_setting('IntervalStyle')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0690-select-current_setting-intervalstyle"},
			},
		},
	},
	{
		Name:        "set 'jit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0691-show-jit"},
			},
			{
				Query: "SET jit TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0692-set-jit-to-off"},
			},
			{
				Query: "SHOW jit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0693-show-jit"},
			},
			{
				Query: "SET jit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0694-set-jit-to-default"},
			},
			{
				Query: "SHOW jit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0695-show-jit"},
			},
			{
				Query: "SELECT current_setting('jit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0696-select-current_setting-jit"},
			},
		},
	},
	{
		Name:        "set 'jit_above_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0697-show-jit_above_cost"},
			},
			{
				Query: "SET jit_above_cost TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0698-set-jit_above_cost-to-100"},
			},
			{
				Query: "SHOW jit_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0699-show-jit_above_cost"},
			},
			{
				Query: "SET jit_above_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0700-set-jit_above_cost-to-default"},
			},
			{
				Query: "SHOW jit_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0701-show-jit_above_cost"},
			},
			{
				Query: "SELECT current_setting('jit_above_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0702-select-current_setting-jit_above_cost"},
			},
		},
	},
	{
		Name:        "set 'jit_debugging_support' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_debugging_support", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0703-show-jit_debugging_support"},
			},
			{
				Query: "SET jit_debugging_support TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0704-set-jit_debugging_support-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('jit_debugging_support')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0705-select-current_setting-jit_debugging_support"},
			},
		},
	},
	{
		Name:        "set 'jit_dump_bitcode' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_dump_bitcode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0706-show-jit_dump_bitcode"},
			},
			{
				Query: "SET jit_dump_bitcode TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0707-set-jit_dump_bitcode-to-on"},
			},
			{
				Query: "SHOW jit_dump_bitcode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0708-show-jit_dump_bitcode"},
			},
			{
				Query: "SET jit_dump_bitcode TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0709-set-jit_dump_bitcode-to-default"},
			},
			{
				Query: "SHOW jit_dump_bitcode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0710-show-jit_dump_bitcode"},
			},
			{
				Query: "SELECT current_setting('jit_dump_bitcode')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0711-select-current_setting-jit_dump_bitcode"},
			},
		},
	},
	{
		Name:        "set 'jit_expressions' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_expressions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0712-show-jit_expressions"},
			},
			{
				Query: "SET jit_expressions TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0713-set-jit_expressions-to-off"},
			},
			{
				Query: "SHOW jit_expressions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0714-show-jit_expressions"},
			},
			{
				Query: "SET jit_expressions TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0715-set-jit_expressions-to-default"},
			},
			{
				Query: "SHOW jit_expressions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0716-show-jit_expressions"},
			},
			{
				Query: "SELECT current_setting('jit_expressions')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0717-select-current_setting-jit_expressions"},
			},
		},
	},
	{
		Name:        "set 'jit_inline_above_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_inline_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0718-show-jit_inline_above_cost"},
			},
			{
				Query: "SET jit_inline_above_cost TO '5000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0719-set-jit_inline_above_cost-to-5000"},
			},
			{
				Query: "SHOW jit_inline_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0720-show-jit_inline_above_cost"},
			},
			{
				Query: "SET jit_inline_above_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0721-set-jit_inline_above_cost-to-default"},
			},
			{
				Query: "SHOW jit_inline_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0722-show-jit_inline_above_cost"},
			},
			{
				Query: "SELECT current_setting('jit_inline_above_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0723-select-current_setting-jit_inline_above_cost"},
			},
		},
	},
	{
		Name:        "set 'jit_optimize_above_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_optimize_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0724-show-jit_optimize_above_cost"},
			},
			{
				Query: "SET jit_optimize_above_cost TO '5000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0725-set-jit_optimize_above_cost-to-5000"},
			},
			{
				Query: "SHOW jit_optimize_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0726-show-jit_optimize_above_cost"},
			},
			{
				Query: "SET jit_optimize_above_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0727-set-jit_optimize_above_cost-to-default"},
			},
			{
				Query: "SHOW jit_optimize_above_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0728-show-jit_optimize_above_cost"},
			},
			{
				Query: "SELECT current_setting('jit_optimize_above_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0729-select-current_setting-jit_optimize_above_cost"},
			},
		},
	},
	{
		Name:        "set 'jit_profiling_support' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_profiling_support", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0730-show-jit_profiling_support"},
			},
			{
				Query: "SET jit_profiling_support TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0731-set-jit_profiling_support-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('jit_profiling_support')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0732-select-current_setting-jit_profiling_support"},
			},
		},
	},
	{
		Name:        "set 'jit_provider' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_provider", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0733-show-jit_provider"},
			},
			{
				Query: "SET jit_provider TO 'llvmjit'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0734-set-jit_provider-to-llvmjit", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('jit_provider')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0735-select-current_setting-jit_provider"},
			},
		},
	},
	{
		Name:        "set 'jit_tuple_deforming' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW jit_tuple_deforming", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0736-show-jit_tuple_deforming"},
			},
			{
				Query: "SET jit_tuple_deforming TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0737-set-jit_tuple_deforming-to-off"},
			},
			{
				Query: "SHOW jit_tuple_deforming", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0738-show-jit_tuple_deforming"},
			},
			{
				Query: "SET jit_tuple_deforming TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0739-set-jit_tuple_deforming-to-default"},
			},
			{
				Query: "SHOW jit_tuple_deforming", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0740-show-jit_tuple_deforming"},
			},
			{
				Query: "SELECT current_setting('jit_tuple_deforming')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0741-select-current_setting-jit_tuple_deforming"},
			},
		},
	},
	{
		Name:        "set 'join_collapse_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW join_collapse_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0742-show-join_collapse_limit"},
			},
			{
				Query: "SET join_collapse_limit TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0743-set-join_collapse_limit-to-100"},
			},
			{
				Query: "SHOW join_collapse_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0744-show-join_collapse_limit"},
			},
			{
				Query: "SET join_collapse_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0745-set-join_collapse_limit-to-default"},
			},
			{
				Query: "SHOW join_collapse_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0746-show-join_collapse_limit"},
			},
			{
				Query: "SELECT current_setting('join_collapse_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0747-select-current_setting-join_collapse_limit"},
			},
		},
	},
	{
		Name:        "set 'krb_caseins_users' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW krb_caseins_users", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0748-show-krb_caseins_users"},
			},
			{
				Query: "SET krb_caseins_users TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0749-set-krb_caseins_users-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('krb_caseins_users')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0750-select-current_setting-krb_caseins_users"},
			},
		},
	},
	{
		Name:        "set 'krb_server_keyfile' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW krb_server_keyfile", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0751-show-krb_server_keyfile"},
			},
			{
				Query: "SET krb_server_keyfile TO 'FILE:'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0752-set-krb_server_keyfile-to-file:", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('krb_server_keyfile')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0753-select-current_setting-krb_server_keyfile"},
			},
		},
	},
	{
		Name:        "set 'lc_messages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW lc_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0754-show-lc_messages"},
			},
			{
				Query: "SET lc_messages TO 'en_US'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0755-set-lc_messages-to-en_us"},
			},
			{
				Query: "SHOW lc_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0756-show-lc_messages"},
			},
			{
				Query: "SET lc_messages TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0757-set-lc_messages-to-default"},
			},
			{
				Query: "SHOW lc_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0758-show-lc_messages"},
			},
			{
				Query: "SELECT current_setting('lc_messages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0759-select-current_setting-lc_messages"},
			},
		},
	},
	{
		Name:        "set 'lc_monetary' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW lc_monetary", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0760-show-lc_monetary"},
			},
			{
				Query: "SET lc_monetary TO 'en_US'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0761-set-lc_monetary-to-en_us"},
			},
			{
				Query: "SHOW lc_monetary", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0762-show-lc_monetary"},
			},
			{
				Query: "SET lc_monetary TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0763-set-lc_monetary-to-default"},
			},
			{
				Query: "SHOW lc_monetary", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0764-show-lc_monetary"},
			},
			{
				Query: "SELECT current_setting('lc_monetary')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0765-select-current_setting-lc_monetary"},
			},
		},
	},
	{
		Name:        "set 'lc_numeric' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW lc_numeric", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0766-show-lc_numeric"},
			},
			{
				Query: "SET lc_numeric TO 'en_US'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0767-set-lc_numeric-to-en_us"},
			},
			{
				Query: "SHOW lc_numeric", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0768-show-lc_numeric"},
			},
			{
				Query: "SET lc_numeric TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0769-set-lc_numeric-to-default"},
			},
			{
				Query: "SHOW lc_numeric", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0770-show-lc_numeric"},
			},
			{
				Query: "SELECT current_setting('lc_numeric')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0771-select-current_setting-lc_numeric"},
			},
		},
	},
	{
		Name:        "set 'lc_time' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW lc_time", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0772-show-lc_time"},
			},
			{
				Query: "SET lc_time TO 'en_US'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0773-set-lc_time-to-en_us"},
			},
			{
				Query: "SHOW lc_time", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0774-show-lc_time"},
			},
			{
				Query: "SET lc_time TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0775-set-lc_time-to-default"},
			},
			{
				Query: "SHOW lc_time", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0776-show-lc_time"},
			},
			{
				Query: "SELECT current_setting('lc_time')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0777-select-current_setting-lc_time"},
			},
		},
	},
	{
		Name:        "set 'listen_addresses' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW listen_addresses", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0778-show-listen_addresses"},
			},
			{
				Query: "SET listen_addresses TO 'localhost'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0779-set-listen_addresses-to-localhost", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('listen_addresses')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0780-select-current_setting-listen_addresses"},
			},
		},
	},
	{
		Name:        "set 'lo_compat_privileges' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW lo_compat_privileges", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0781-show-lo_compat_privileges"},
			},
			{
				Query: "SET lo_compat_privileges TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0782-set-lo_compat_privileges-to-on"},
			},
			{
				Query: "SHOW lo_compat_privileges", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0783-show-lo_compat_privileges"},
			},
			{
				Query: "SET lo_compat_privileges TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0784-set-lo_compat_privileges-to-default"},
			},
			{
				Query: "SHOW lo_compat_privileges", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0785-show-lo_compat_privileges"},
			},
			{
				Query: "SELECT current_setting('lo_compat_privileges')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0786-select-current_setting-lo_compat_privileges"},
			},
		},
	},
	{
		Name:        "set 'local_preload_libraries' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW local_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0787-show-local_preload_libraries"},
			},
			{
				Query: "SET local_preload_libraries TO '/'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0788-set-local_preload_libraries-to-/"},
			},
			{
				Query: "SHOW local_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0789-show-local_preload_libraries"},
			},
			{
				Query: "SET local_preload_libraries TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0790-set-local_preload_libraries-to-default"},
			},
			{
				Query: "SHOW local_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0791-show-local_preload_libraries"},
			},
			{
				Query: "SELECT current_setting('local_preload_libraries')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0792-select-current_setting-local_preload_libraries"},
			},
		},
	},
	{
		Name:        "set 'lock_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW lock_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0793-show-lock_timeout"},
			},
			{
				Query: "SET lock_timeout TO 20", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0794-set-lock_timeout-to-20"},
			},
			{
				Query: "SHOW lock_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0795-show-lock_timeout"},
			},
			{
				Query: "SET lock_timeout TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0796-set-lock_timeout-to-default"},
			},
			{
				Query: "SHOW lock_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0797-show-lock_timeout"},
			},
			{
				Query: "SELECT current_setting('lock_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0798-select-current_setting-lock_timeout"},
			},
		},
	},
	{
		Name:        "set 'log_autovacuum_min_duration' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_autovacuum_min_duration", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0799-show-log_autovacuum_min_duration"},
			},
			{
				Query: "SET log_autovacuum_min_duration TO '600'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0800-set-log_autovacuum_min_duration-to-600", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_autovacuum_min_duration')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0801-select-current_setting-log_autovacuum_min_duration"},
			},
		},
	},
	{
		Name:        "set 'log_checkpoints' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_checkpoints", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0802-show-log_checkpoints"},
			},
			{
				Query: "SET log_checkpoints TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0803-set-log_checkpoints-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_checkpoints')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0804-select-current_setting-log_checkpoints"},
			},
		},
	},
	{
		Name:        "set 'log_connections' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_connections", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0805-show-log_connections"},
			},
			{
				Query: "SET log_connections TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0806-set-log_connections-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_connections')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0807-select-current_setting-log_connections"},
			},
		},
	},
	{
		Name:        "set 'log_destination' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_destination", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0808-show-log_destination"},
			},
			{
				Query: "SET log_destination TO 'jsonlog'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0809-set-log_destination-to-jsonlog", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_destination')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0810-select-current_setting-log_destination"},
			},
		},
	},
	{
		Name:        "set 'log_directory' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_directory", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0811-show-log_directory"},
			},
			{
				Query: "SET log_directory TO 'log'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0812-set-log_directory-to-log", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_directory')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0813-select-current_setting-log_directory"},
			},
		},
	},
	{
		Name:        "set 'log_disconnections' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_disconnections", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0814-show-log_disconnections"},
			},
			{
				Query: "SET log_disconnections TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0815-set-log_disconnections-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_disconnections')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0816-select-current_setting-log_disconnections"},
			},
		},
	},
	{
		Name:        "set 'log_duration' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_duration", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0817-show-log_duration"},
			},
			{
				Query: "SET log_duration TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0818-set-log_duration-to-on"},
			},
			{
				Query: "SHOW log_duration", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0819-show-log_duration"},
			},
			{
				Query: "SET log_duration TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0820-set-log_duration-to-default"},
			},
			{
				Query: "SHOW log_duration", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0821-show-log_duration"},
			},
			{
				Query: "SELECT current_setting('log_duration')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0822-select-current_setting-log_duration"},
			},
		},
	},
	{
		Name:        "set 'log_error_verbosity' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_error_verbosity", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0823-show-log_error_verbosity"},
			},
			{
				Query: "SET log_error_verbosity TO 'verbose'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0824-set-log_error_verbosity-to-verbose"},
			},
			{
				Query: "SHOW log_error_verbosity", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0825-show-log_error_verbosity"},
			},
			{
				Query: "SET log_error_verbosity TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0826-set-log_error_verbosity-to-default"},
			},
			{
				Query: "SHOW log_error_verbosity", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0827-show-log_error_verbosity"},
			},
			{
				Query: "SELECT current_setting('log_error_verbosity')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0828-select-current_setting-log_error_verbosity"},
			},
		},
	},
	{
		Name:        "set 'log_executor_stats' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_executor_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0829-show-log_executor_stats"},
			},
			{
				Query: "SET log_executor_stats TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0830-set-log_executor_stats-to-on"},
			},
			{
				Query: "SHOW log_executor_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0831-show-log_executor_stats"},
			},
			{
				Query: "SET log_executor_stats TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0832-set-log_executor_stats-to-default"},
			},
			{
				Query: "SHOW log_executor_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0833-show-log_executor_stats"},
			},
			{
				Query: "SELECT current_setting('log_executor_stats')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0834-select-current_setting-log_executor_stats"},
			},
		},
	},
	{
		Name:        "set 'log_file_mode' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_file_mode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0835-show-log_file_mode"},
			},
			{
				Query: "SET log_file_mode TO '384'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0836-set-log_file_mode-to-384", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_file_mode')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0837-select-current_setting-log_file_mode"},
			},
		},
	},
	{
		Name:        "set 'log_filename' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_filename", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0838-show-log_filename"},
			},
			{
				Query: "SET log_filename TO 'postgresql-%Y-%m-%d_%H%M%S.log'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0839-set-log_filename-to-postgresql-%y-%m-%d_%h%m%s.log", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_filename')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0840-select-current_setting-log_filename"},
			},
		},
	},
	{
		Name:        "set 'log_hostname' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_hostname", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0841-show-log_hostname"},
			},
			{
				Query: "SET log_hostname TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0842-set-log_hostname-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_hostname')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0843-select-current_setting-log_hostname"},
			},
		},
	},
	{
		Name:        "set 'log_line_prefix' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_line_prefix", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0844-show-log_line_prefix"},
			},
			{
				Query: "SET log_line_prefix TO '%m [%p]'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0845-set-log_line_prefix-to-%m-[%p]", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_line_prefix')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0846-select-current_setting-log_line_prefix"},
			},
		},
	},
	{
		Name:        "set 'log_lock_waits' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_lock_waits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0847-show-log_lock_waits"},
			},
			{
				Query: "SET log_lock_waits TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0848-set-log_lock_waits-to-on"},
			},
			{
				Query: "SHOW log_lock_waits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0849-show-log_lock_waits"},
			},
			{
				Query: "SET log_lock_waits TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0850-set-log_lock_waits-to-default"},
			},
			{
				Query: "SHOW log_lock_waits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0851-show-log_lock_waits"},
			},
			{
				Query: "SELECT current_setting('log_lock_waits')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0852-select-current_setting-log_lock_waits"},
			},
		},
	},
	{
		Name:        "set 'log_min_duration_sample' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_min_duration_sample", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0853-show-log_min_duration_sample"},
			},
			{
				Query: "SET log_min_duration_sample TO 1", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0854-set-log_min_duration_sample-to-1"},
			},
			{
				Query: "SHOW log_min_duration_sample", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0855-show-log_min_duration_sample"},
			},
			{
				Query: "SET log_min_duration_sample TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0856-set-log_min_duration_sample-to-default"},
			},
			{
				Query: "SHOW log_min_duration_sample", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0857-show-log_min_duration_sample"},
			},
			{
				Query: "SELECT current_setting('log_min_duration_sample')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0858-select-current_setting-log_min_duration_sample"},
			},
		},
	},
	{
		Name:        "set 'log_min_duration_statement' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_min_duration_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0859-show-log_min_duration_statement"},
			},
			{
				Query: "SET log_min_duration_statement TO 10", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0860-set-log_min_duration_statement-to-10"},
			},
			{
				Query: "SHOW log_min_duration_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0861-show-log_min_duration_statement"},
			},
			{
				Query: "SET log_min_duration_statement TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0862-set-log_min_duration_statement-to-default"},
			},
			{
				Query: "SHOW log_min_duration_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0863-show-log_min_duration_statement"},
			},
			{
				Query: "SELECT current_setting('log_min_duration_statement')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0864-select-current_setting-log_min_duration_statement"},
			},
		},
	},
	{
		Name:        "set 'log_min_error_statement' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_min_error_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0865-show-log_min_error_statement"},
			},
			{
				Query: "SET log_min_error_statement TO 'debug5'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0866-set-log_min_error_statement-to-debug5"},
			},
			{
				Query: "SHOW log_min_error_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0867-show-log_min_error_statement"},
			},
			{
				Query: "SET log_min_error_statement TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0868-set-log_min_error_statement-to-default"},
			},
			{
				Query: "SHOW log_min_error_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0869-show-log_min_error_statement"},
			},
			{
				Query: "SELECT current_setting('log_min_error_statement')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0870-select-current_setting-log_min_error_statement"},
			},
		},
	},
	{
		Name:        "set 'log_min_messages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_min_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0871-show-log_min_messages"},
			},
			{
				Query: "SET log_min_messages TO 'info'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0872-set-log_min_messages-to-info"},
			},
			{
				Query: "SHOW log_min_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0873-show-log_min_messages"},
			},
			{
				Query: "SET log_min_messages TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0874-set-log_min_messages-to-default"},
			},
			{
				Query: "SHOW log_min_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0875-show-log_min_messages"},
			},
			{
				Query: "SELECT current_setting('log_min_messages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0876-select-current_setting-log_min_messages"},
			},
		},
	},
	{
		Name:        "set 'log_parameter_max_length' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_parameter_max_length", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0877-show-log_parameter_max_length"},
			},
			{
				Query: "SET log_parameter_max_length TO '10'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0878-set-log_parameter_max_length-to-10"},
			},
			{
				Query: "SHOW log_parameter_max_length", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0879-show-log_parameter_max_length"},
			},
			{
				Query: "SELECT current_setting('log_parameter_max_length')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0880-select-current_setting-log_parameter_max_length"},
			},
			{
				Query: "SET log_parameter_max_length TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0881-set-log_parameter_max_length-to-default"},
			},
			{
				Query: "SHOW log_parameter_max_length", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0882-show-log_parameter_max_length"},
			},
			{
				Query: "SELECT current_setting('log_parameter_max_length')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0883-select-current_setting-log_parameter_max_length"},
			},
		},
	},
	{
		Name:        "set 'log_parameter_max_length_on_error' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_parameter_max_length_on_error", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0884-show-log_parameter_max_length_on_error"},
			},
			{
				Query: "SET log_parameter_max_length_on_error TO '1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0885-set-log_parameter_max_length_on_error-to-1"},
			},
			{
				Query: "SHOW log_parameter_max_length_on_error", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0886-show-log_parameter_max_length_on_error"},
			},
			{
				Query: "SET log_parameter_max_length_on_error TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0887-set-log_parameter_max_length_on_error-to-default"},
			},
			{
				Query: "SHOW log_parameter_max_length_on_error", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0888-show-log_parameter_max_length_on_error"},
			},
			{
				Query: "SELECT current_setting('log_parameter_max_length_on_error')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0889-select-current_setting-log_parameter_max_length_on_error"},
			},
		},
	},
	{
		Name:        "set 'log_parser_stats' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_parser_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0890-show-log_parser_stats"},
			},
			{
				Query: "SET log_parser_stats TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0891-set-log_parser_stats-to-on"},
			},
			{
				Query: "SHOW log_parser_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0892-show-log_parser_stats"},
			},
			{
				Query: "SET log_parser_stats TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0893-set-log_parser_stats-to-default"},
			},
			{
				Query: "SHOW log_parser_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0894-show-log_parser_stats"},
			},
			{
				Query: "SELECT current_setting('log_parser_stats')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0895-select-current_setting-log_parser_stats"},
			},
		},
	},
	{
		Name:        "set 'log_planner_stats' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_planner_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0896-show-log_planner_stats"},
			},
			{
				Query: "SET log_planner_stats TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0897-set-log_planner_stats-to-on"},
			},
			{
				Query: "SHOW log_planner_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0898-show-log_planner_stats"},
			},
			{
				Query: "SET log_planner_stats TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0899-set-log_planner_stats-to-default"},
			},
			{
				Query: "SHOW log_planner_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0900-show-log_planner_stats"},
			},
			{
				Query: "SELECT current_setting('log_planner_stats')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0901-select-current_setting-log_planner_stats"},
			},
		},
	},
	{
		Name:        "set 'log_recovery_conflict_waits' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_recovery_conflict_waits", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0902-show-log_recovery_conflict_waits"},
			},
			{
				Query: "SET log_recovery_conflict_waits TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0903-set-log_recovery_conflict_waits-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_recovery_conflict_waits')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0904-select-current_setting-log_recovery_conflict_waits"},
			},
		},
	},
	{
		Name:        "set 'log_replication_commands' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_replication_commands", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0905-show-log_replication_commands"},
			},
			{
				Query: "SET log_replication_commands TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0906-set-log_replication_commands-to-on"},
			},
			{
				Query: "SHOW log_replication_commands", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0907-show-log_replication_commands"},
			},
			{
				Query: "SET log_replication_commands TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0908-set-log_replication_commands-to-default"},
			},
			{
				Query: "SHOW log_replication_commands", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0909-show-log_replication_commands"},
			},
			{
				Query: "SELECT current_setting('log_replication_commands')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0910-select-current_setting-log_replication_commands"},
			},
		},
	},
	{
		Name:        "set 'log_rotation_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_rotation_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0911-show-log_rotation_age"},
			},
			{
				Query: "SET log_rotation_age TO '1440'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0912-set-log_rotation_age-to-1440", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_rotation_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0913-select-current_setting-log_rotation_age"},
			},
		},
	},
	{
		Name:        "set 'log_rotation_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_rotation_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0914-show-log_rotation_size"},
			},
			{
				Query: "SET log_rotation_size TO '10240'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0915-set-log_rotation_size-to-10240", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_rotation_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0916-select-current_setting-log_rotation_size"},
			},
		},
	},
	{
		Name:        "set 'log_startup_progress_interval' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_startup_progress_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0917-show-log_startup_progress_interval"},
			},
			{
				Query: "SET log_startup_progress_interval TO '10'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0918-set-log_startup_progress_interval-to-10", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_startup_progress_interval')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0919-select-current_setting-log_startup_progress_interval"},
			},
		},
	},
	{
		Name:        "set 'log_statement' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0920-show-log_statement"},
			},
			{
				Query: "SET log_statement TO 'ddl'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0921-set-log_statement-to-ddl"},
			},
			{
				Query: "SHOW log_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0922-show-log_statement"},
			},
			{
				Query: "SET log_statement TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0923-set-log_statement-to-default"},
			},
			{
				Query: "SHOW log_statement", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0924-show-log_statement"},
			},
			{
				Query: "SELECT current_setting('log_statement')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0925-select-current_setting-log_statement"},
			},
		},
	},
	{
		Name:        "set 'log_statement_sample_rate' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_statement_sample_rate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0926-show-log_statement_sample_rate"},
			},
			{
				Query: "SET log_statement_sample_rate TO 0.5", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0927-set-log_statement_sample_rate-to-0.5"},
			},
			{
				Query: "SHOW log_statement_sample_rate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0928-show-log_statement_sample_rate"},
			},
			{
				Query: "SET log_statement_sample_rate TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0929-set-log_statement_sample_rate-to-default"},
			},
			{
				Query: "SHOW log_statement_sample_rate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0930-show-log_statement_sample_rate"},
			},
			{
				Query: "SELECT current_setting('log_statement_sample_rate')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0931-select-current_setting-log_statement_sample_rate"},
			},
		},
	},
	{
		Name:        "set 'log_statement_stats' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_statement_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0932-show-log_statement_stats"},
			},
			{
				Query: "SET log_statement_stats TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0933-set-log_statement_stats-to-on"},
			},
			{
				Query: "SHOW log_statement_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0934-show-log_statement_stats"},
			},
			{
				Query: "SET log_statement_stats TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0935-set-log_statement_stats-to-default"},
			},
			{
				Query: "SHOW log_statement_stats", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0936-show-log_statement_stats"},
			},
			{
				Query: "SELECT current_setting('log_statement_stats')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0937-select-current_setting-log_statement_stats"},
			},
		},
	},
	{
		Name:        "set 'log_temp_files' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_temp_files", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0938-show-log_temp_files"},
			},
			{
				Query: "SET log_temp_files TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0939-set-log_temp_files-to-100"},
			},
			{
				Query: "SHOW log_temp_files", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0940-show-log_temp_files"},
			},
			{
				Query: "SET log_temp_files TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0941-set-log_temp_files-to-default"},
			},
			{
				Query: "SHOW log_temp_files", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0942-show-log_temp_files"},
			},
			{
				Query: "SELECT current_setting('log_temp_files')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0943-select-current_setting-log_temp_files"},
			},
		},
	},
	{
		Name:        "set 'log_timezone' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_timezone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0944-show-log_timezone"},
			},
			{
				Query: "SET log_timezone TO 'America/Los_Angeles'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0945-set-log_timezone-to-america/los_angeles", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_timezone')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0946-select-current_setting-log_timezone"},
			},
		},
	},
	{
		Name:        "set 'log_transaction_sample_rate' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_transaction_sample_rate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0947-show-log_transaction_sample_rate"},
			},
			{
				Query: "SET log_transaction_sample_rate TO '0.5'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0948-set-log_transaction_sample_rate-to-0.5"},
			},
			{
				Query: "SHOW log_transaction_sample_rate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0949-show-log_transaction_sample_rate"},
			},
			{
				Query: "SET log_transaction_sample_rate TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0950-set-log_transaction_sample_rate-to-default"},
			},
			{
				Query: "SHOW log_transaction_sample_rate", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0951-show-log_transaction_sample_rate"},
			},
			{
				Query: "SELECT current_setting('log_transaction_sample_rate')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0952-select-current_setting-log_transaction_sample_rate"},
			},
		},
	},
	{
		Name:        "set 'log_truncate_on_rotation' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW log_truncate_on_rotation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0953-show-log_truncate_on_rotation"},
			},
			{
				Query: "SET log_truncate_on_rotation TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0954-set-log_truncate_on_rotation-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('log_truncate_on_rotation')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0955-select-current_setting-log_truncate_on_rotation"},
			},
		},
	},
	{
		Name:        "set 'logging_collector' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW logging_collector", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0956-show-logging_collector"},
			},
			{
				Query: "SET logging_collector TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0957-set-logging_collector-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('logging_collector')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0958-select-current_setting-logging_collector"},
			},
		},
	},
	{
		Name:        "set 'logical_decoding_work_mem' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW logical_decoding_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0959-show-logical_decoding_work_mem"},
			},
			{
				Query: "SET logical_decoding_work_mem TO '64000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0960-set-logical_decoding_work_mem-to-64000"},
			},
			{
				Query: "SHOW logical_decoding_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0961-show-logical_decoding_work_mem"},
			},
			{
				Query: "SET logical_decoding_work_mem TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0962-set-logical_decoding_work_mem-to-default"},
			},
			{
				Query: "SHOW logical_decoding_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0963-show-logical_decoding_work_mem"},
			},
			{
				Query: "SELECT current_setting('logical_decoding_work_mem')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0964-select-current_setting-logical_decoding_work_mem"},
			},
		},
	},
	{
		Name:        "set 'maintenance_io_concurrency' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW maintenance_io_concurrency", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0965-show-maintenance_io_concurrency"},
			},
			{
				Query: "SET maintenance_io_concurrency TO '1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0966-set-maintenance_io_concurrency-to-1", Compare: "sqlstate"},
			},
			{
				Query:    "SHOW maintenance_io_concurrency",
				Expected: []sql.Row{{int64(1)}},
			},
			{
				Query: "SET maintenance_io_concurrency TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0968-set-maintenance_io_concurrency-to-default"},
			},
			{
				Query:    "SHOW maintenance_io_concurrency",
				Expected: []sql.Row{{int64(0)}},
			},
			{
				Query: "SELECT current_setting('maintenance_io_concurrency')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0970-select-current_setting-maintenance_io_concurrency"},
			},
		},
	},
	{
		Name:        "set 'maintenance_work_mem' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW maintenance_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0971-show-maintenance_work_mem"},
			},
			{
				Query: "SET maintenance_work_mem TO '64000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0972-set-maintenance_work_mem-to-64000"},
			},
			{
				Query: "SHOW maintenance_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0973-show-maintenance_work_mem"},
			},
			{
				Query: "SET maintenance_work_mem TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0974-set-maintenance_work_mem-to-default"},
			},
			{
				Query: "SHOW maintenance_work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0975-show-maintenance_work_mem"},
			},
			{
				Query: "SELECT current_setting('maintenance_work_mem')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0976-select-current_setting-maintenance_work_mem"},
			},
		},
	},
	{
		Name:        "set 'max_connections' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_connections", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0977-show-max_connections"},
			},
			{
				Query: "SET max_connections TO '150'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0978-set-max_connections-to-150", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_connections')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0979-select-current_setting-max_connections"},
			},
		},
	},
	{
		Name:        "set 'max_files_per_process' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_files_per_process", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0980-show-max_files_per_process"},
			},
			{
				Query: "SET max_files_per_process TO '1000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0981-set-max_files_per_process-to-1000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_files_per_process')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0982-select-current_setting-max_files_per_process"},
			},
		},
	},
	{
		Name:        "set 'max_function_args' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_function_args", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0983-show-max_function_args"},
			},
			{
				Query: "SET max_function_args TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0984-set-max_function_args-to-100", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_function_args')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0985-select-current_setting-max_function_args"},
			},
		},
	},
	{
		Name:        "set 'max_identifier_length' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_identifier_length", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0986-show-max_identifier_length"},
			},
			{
				Query: "SET max_identifier_length TO '63'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0987-set-max_identifier_length-to-63", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_identifier_length')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0988-select-current_setting-max_identifier_length"},
			},
		},
	},
	{
		Name:        "set 'max_index_keys' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_index_keys", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0989-show-max_index_keys"},
			},
			{
				Query: "SET max_index_keys TO '32'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0990-set-max_index_keys-to-32", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_index_keys')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0991-select-current_setting-max_index_keys"},
			},
		},
	},
	{
		Name:        "set 'max_locks_per_transaction' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_locks_per_transaction", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0992-show-max_locks_per_transaction"},
			},
			{
				Query: "SET max_locks_per_transaction TO '64'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0993-set-max_locks_per_transaction-to-64", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_locks_per_transaction')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0994-select-current_setting-max_locks_per_transaction"},
			},
		},
	},
	{
		Name:        "set 'max_logical_replication_workers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_logical_replication_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0995-show-max_logical_replication_workers"},
			},
			{
				Query: "SET max_logical_replication_workers TO '4'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0996-set-max_logical_replication_workers-to-4", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_logical_replication_workers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0997-select-current_setting-max_logical_replication_workers"},
			},
		},
	},
	{
		Name:        "set 'max_parallel_apply_workers_per_subscription' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_parallel_apply_workers_per_subscription", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0998-show"},
			},
			{
				Query: "SET max_parallel_apply_workers_per_subscription TO '2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-0999-set-to-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_parallel_apply_workers_per_subscription')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1000-select-current_setting"},
			},
		},
	},
	{
		Name:        "set 'max_parallel_maintenance_workers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_parallel_maintenance_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1001-show-max_parallel_maintenance_workers"},
			},
			{
				Query: "SET max_parallel_maintenance_workers TO '3'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1002-set-max_parallel_maintenance_workers-to-3"},
			},
			{
				Query: "SHOW max_parallel_maintenance_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1003-show-max_parallel_maintenance_workers"},
			},
			{
				Query: "SET max_parallel_maintenance_workers TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1004-set-max_parallel_maintenance_workers-to-default"},
			},
			{
				Query: "SHOW max_parallel_maintenance_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1005-show-max_parallel_maintenance_workers"},
			},
			{
				Query: "SELECT current_setting('max_parallel_maintenance_workers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1006-select-current_setting-max_parallel_maintenance_workers"},
			},
		},
	},
	{
		Name:        "set 'max_parallel_workers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_parallel_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1007-show-max_parallel_workers"},
			},
			{
				Query: "SET max_parallel_workers TO 11", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1008-set-max_parallel_workers-to-11"},
			},
			{
				Query: "SHOW max_parallel_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1009-show-max_parallel_workers"},
			},
			{
				Query: "SET max_parallel_workers TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1010-set-max_parallel_workers-to-default"},
			},
			{
				Query: "SHOW max_parallel_workers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1011-show-max_parallel_workers"},
			},
			{
				Query: "SELECT current_setting('max_parallel_workers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1012-select-current_setting-max_parallel_workers"},
			},
		},
	},
	{
		Name:        "set 'max_parallel_workers_per_gather' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_parallel_workers_per_gather", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1013-show-max_parallel_workers_per_gather"},
			},
			{
				Query: "SET max_parallel_workers_per_gather TO 3", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1014-set-max_parallel_workers_per_gather-to-3"},
			},
			{
				Query: "SHOW max_parallel_workers_per_gather", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1015-show-max_parallel_workers_per_gather"},
			},
			{
				Query: "SET max_parallel_workers_per_gather TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1016-set-max_parallel_workers_per_gather-to-default"},
			},
			{
				Query: "SHOW max_parallel_workers_per_gather", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1017-show-max_parallel_workers_per_gather"},
			},
			{
				Query: "SELECT current_setting('max_parallel_workers_per_gather')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1018-select-current_setting-max_parallel_workers_per_gather"},
			},
		},
	},
	{
		Name:        "set 'max_pred_locks_per_page' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_pred_locks_per_page", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1019-show-max_pred_locks_per_page"},
			},
			{
				Query: "SET max_pred_locks_per_page TO '2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1020-set-max_pred_locks_per_page-to-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_pred_locks_per_page')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1021-select-current_setting-max_pred_locks_per_page"},
			},
		},
	},
	{
		Name:        "set 'max_pred_locks_per_relation' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_pred_locks_per_relation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1022-show-max_pred_locks_per_relation"},
			},
			{
				Query: "SET max_pred_locks_per_relation TO '-2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1023-set-max_pred_locks_per_relation-to-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_pred_locks_per_relation')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1024-select-current_setting-max_pred_locks_per_relation"},
			},
		},
	},
	{
		Name:        "set 'max_pred_locks_per_transaction' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_pred_locks_per_transaction", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1025-show-max_pred_locks_per_transaction"},
			},
			{
				Query: "SET max_pred_locks_per_transaction TO '64'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1026-set-max_pred_locks_per_transaction-to-64", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_pred_locks_per_transaction')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1027-select-current_setting-max_pred_locks_per_transaction"},
			},
		},
	},
	{
		Name:        "set 'max_prepared_transactions' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_prepared_transactions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1028-show-max_prepared_transactions"},
			},
			{
				Query: "SET max_prepared_transactions TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1029-set-max_prepared_transactions-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_prepared_transactions')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1030-select-current_setting-max_prepared_transactions"},
			},
		},
	},
	{
		Name:        "set 'max_replication_slots' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_replication_slots", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1031-show-max_replication_slots"},
			},
			{
				Query: "SET max_replication_slots TO '10'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1032-set-max_replication_slots-to-10", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_replication_slots')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1033-select-current_setting-max_replication_slots"},
			},
		},
	},
	{
		Name:        "set 'max_slot_wal_keep_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_slot_wal_keep_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1034-show-max_slot_wal_keep_size"},
			},
			{
				Query: "SET max_slot_wal_keep_size TO '-1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1035-set-max_slot_wal_keep_size-to-1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_slot_wal_keep_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1036-select-current_setting-max_slot_wal_keep_size"},
			},
		},
	},
	{
		Name:        "set 'max_stack_depth' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_stack_depth", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1037-show-max_stack_depth"},
			},
			{
				Query: "SET max_stack_depth TO '2000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1038-set-max_stack_depth-to-2000"},
			},
			{
				Query: "SHOW max_stack_depth", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1039-show-max_stack_depth"},
			},
			{
				Query: "SET max_stack_depth TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1040-set-max_stack_depth-to-default"},
			},
			{
				Query: "SHOW max_stack_depth", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1041-show-max_stack_depth"},
			},
			{
				Query: "SELECT current_setting('max_stack_depth')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1042-select-current_setting-max_stack_depth"},
			},
		},
	},
	{
		Name:        "set 'max_standby_archive_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_standby_archive_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1043-show-max_standby_archive_delay"},
			},
			{
				Query: "SET max_standby_archive_delay TO '30'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1044-set-max_standby_archive_delay-to-30", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_standby_archive_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1045-select-current_setting-max_standby_archive_delay"},
			},
		},
	},
	{
		Name:        "set 'max_standby_streaming_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_standby_streaming_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1046-show-max_standby_streaming_delay"},
			},
			{
				Query: "SET max_standby_streaming_delay TO '30'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1047-set-max_standby_streaming_delay-to-30", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_standby_streaming_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1048-select-current_setting-max_standby_streaming_delay"},
			},
		},
	},
	{
		Name:        "set 'max_sync_workers_per_subscription' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_sync_workers_per_subscription", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1049-show-max_sync_workers_per_subscription"},
			},
			{
				Query: "SET max_sync_workers_per_subscription TO '2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1050-set-max_sync_workers_per_subscription-to-2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_sync_workers_per_subscription')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1051-select-current_setting-max_sync_workers_per_subscription"},
			},
		},
	},
	{
		Name:        "set 'max_wal_senders' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_wal_senders", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1052-show-max_wal_senders"},
			},
			{
				Query: "SET max_wal_senders TO '10'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1053-set-max_wal_senders-to-10", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_wal_senders')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1054-select-current_setting-max_wal_senders"},
			},
		},
	},
	{
		Name:        "set 'max_wal_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_wal_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1055-show-max_wal_size"},
			},
			{
				Query: "SET max_wal_size TO '1000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1056-set-max_wal_size-to-1000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_wal_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1057-select-current_setting-max_wal_size"},
			},
		},
	},
	{
		Name:        "set 'max_worker_processes' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW max_worker_processes", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1058-show-max_worker_processes"},
			},
			{
				Query: "SET max_worker_processes TO '8'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1059-set-max_worker_processes-to-8", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('max_worker_processes')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1060-select-current_setting-max_worker_processes"},
			},
		},
	},
	{
		Name:        "set 'min_dynamic_shared_memory' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW min_dynamic_shared_memory", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1061-show-min_dynamic_shared_memory"},
			},
			{
				Query: "SET min_dynamic_shared_memory TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1062-set-min_dynamic_shared_memory-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('min_dynamic_shared_memory')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1063-select-current_setting-min_dynamic_shared_memory"},
			},
		},
	},
	{
		Name:        "set 'min_parallel_index_scan_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW min_parallel_index_scan_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1064-show-min_parallel_index_scan_size"},
			},
			{
				Query: "SET min_parallel_index_scan_size TO '512'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1065-set-min_parallel_index_scan_size-to-512"},
			},
			{
				Query: "SHOW min_parallel_index_scan_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1066-show-min_parallel_index_scan_size"},
			},
			{
				Query: "SET min_parallel_index_scan_size TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1067-set-min_parallel_index_scan_size-to-default"},
			},
			{
				Query: "SHOW min_parallel_index_scan_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1068-show-min_parallel_index_scan_size"},
			},
			{
				Query: "SELECT current_setting('min_parallel_index_scan_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1069-select-current_setting-min_parallel_index_scan_size"},
			},
		},
	},
	{
		Name:        "set 'min_parallel_table_scan_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW min_parallel_table_scan_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1070-show-min_parallel_table_scan_size"},
			},
			{
				Query: "SET min_parallel_table_scan_size TO '800'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1071-set-min_parallel_table_scan_size-to-800"},
			},
			{
				Query: "SHOW min_parallel_table_scan_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1072-show-min_parallel_table_scan_size"},
			},
			{
				Query: "SET min_parallel_table_scan_size TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1073-set-min_parallel_table_scan_size-to-default"},
			},
			{
				Query: "SHOW min_parallel_table_scan_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1074-show-min_parallel_table_scan_size"},
			},
			{
				Query: "SELECT current_setting('min_parallel_table_scan_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1075-select-current_setting-min_parallel_table_scan_size"},
			},
		},
	},
	{
		Name:        "set 'min_wal_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW min_wal_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1076-show-min_wal_size"},
			},
			{
				Query: "SET min_wal_size TO '8000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1077-set-min_wal_size-to-8000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('min_wal_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1078-select-current_setting-min_wal_size"},
			},
		},
	},
	{
		Name:        "set 'old_snapshot_threshold' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW old_snapshot_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1079-show-old_snapshot_threshold"},
			},
			{
				Query: "SET old_snapshot_threshold TO '-1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1080-set-old_snapshot_threshold-to-1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('old_snapshot_threshold')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1081-select-current_setting-old_snapshot_threshold"},
			},
		},
	},
	{
		Name:        "set 'parallel_leader_participation' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW parallel_leader_participation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1082-show-parallel_leader_participation"},
			},
			{
				Query: "SET parallel_leader_participation TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1083-set-parallel_leader_participation-to-off"},
			},
			{
				Query: "SHOW parallel_leader_participation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1084-show-parallel_leader_participation"},
			},
			{
				Query: "SET parallel_leader_participation TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1085-set-parallel_leader_participation-to-default"},
			},
			{
				Query: "SHOW parallel_leader_participation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1086-show-parallel_leader_participation"},
			},
			{
				Query: "SELECT current_setting('parallel_leader_participation')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1087-select-current_setting-parallel_leader_participation"},
			},
		},
	},
	{
		Name:        "set 'parallel_setup_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW parallel_setup_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1088-show-parallel_setup_cost"},
			},
			{
				Query: "SET parallel_setup_cost TO '10000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1089-set-parallel_setup_cost-to-10000"},
			},
			{
				Query: "SHOW parallel_setup_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1090-show-parallel_setup_cost"},
			},
			{
				Query: "SET parallel_setup_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1091-set-parallel_setup_cost-to-default"},
			},
			{
				Query: "SHOW parallel_setup_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1092-show-parallel_setup_cost"},
			},
			{
				Query: "SELECT current_setting('parallel_setup_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1093-select-current_setting-parallel_setup_cost"},
			},
		},
	},
	{
		Name:        "set 'parallel_tuple_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW parallel_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1094-show-parallel_tuple_cost"},
			},
			{
				Query: "SET parallel_tuple_cost TO '0.2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1095-set-parallel_tuple_cost-to-0.2"},
			},
			{
				Query: "SHOW parallel_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1096-show-parallel_tuple_cost"},
			},
			{
				Query: "SET parallel_tuple_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1097-set-parallel_tuple_cost-to-default"},
			},
			{
				Query: "SHOW parallel_tuple_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1098-show-parallel_tuple_cost"},
			},
			{
				Query: "SELECT current_setting('parallel_tuple_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1099-select-current_setting-parallel_tuple_cost"},
			},
		},
	},
	{
		Name:        "set 'password_encryption' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW password_encryption", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1100-show-password_encryption"},
			},
			{
				Query: "SET password_encryption TO 'md5'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1101-set-password_encryption-to-md5"},
			},
			{
				Query: "SHOW password_encryption", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1102-show-password_encryption"},
			},
			{
				Query: "SET password_encryption TO 'scram-sha-256'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1103-set-password_encryption-to-scram-sha-256"},
			},
			{
				Query: "SHOW password_encryption", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1104-show-password_encryption"},
			},
			{
				Query: "SELECT current_setting('password_encryption')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1105-select-current_setting-password_encryption"},
			},
		},
	},
	{
		Name:        "set 'plan_cache_mode' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW plan_cache_mode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1106-show-plan_cache_mode"},
			},
			{
				Query: "SET plan_cache_mode TO 'force_generic_plan'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1107-set-plan_cache_mode-to-force_generic_plan"},
			},
			{
				Query: "SHOW plan_cache_mode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1108-show-plan_cache_mode"},
			},
			{
				Query: "SET plan_cache_mode TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1109-set-plan_cache_mode-to-default"},
			},
			{
				Query: "SHOW plan_cache_mode", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1110-show-plan_cache_mode"},
			},
			{
				Query: "SELECT current_setting('plan_cache_mode')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1111-select-current_setting-plan_cache_mode"},
			},
		},
	},
	{
		Name:        "set 'post_auth_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW post_auth_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1112-show-post_auth_delay"},
			},
			{
				Query: "SET post_auth_delay TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1113-set-post_auth_delay-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('post_auth_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1114-select-current_setting-post_auth_delay"},
			},
		},
	},
	{
		Name:        "set 'pre_auth_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW pre_auth_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1115-show-pre_auth_delay"},
			},
			{
				Query: "SET pre_auth_delay TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1116-set-pre_auth_delay-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('pre_auth_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1117-select-current_setting-pre_auth_delay"},
			},
		},
	},
	{
		Name:        "set 'primary_conninfo' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW primary_conninfo", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1118-show-primary_conninfo"},
			},
			{
				Query: "SET primary_conninfo TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1119-set-primary_conninfo-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('primary_conninfo')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1120-select-current_setting-primary_conninfo"},
			},
		},
	},
	{
		Name:        "set 'primary_slot_name' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW primary_slot_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1121-show-primary_slot_name"},
			},
			{
				Query: "SET primary_slot_name TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1122-set-primary_slot_name-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('primary_slot_name')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1123-select-current_setting-primary_slot_name"},
			},
		},
	},
	{
		Name:        "set 'quote_all_identifiers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW quote_all_identifiers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1124-show-quote_all_identifiers"},
			},
			{
				Query: "SET quote_all_identifiers TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1125-set-quote_all_identifiers-to-on"},
			},
			{
				Query: "SHOW quote_all_identifiers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1126-show-quote_all_identifiers"},
			},
			{
				Query: "SET quote_all_identifiers TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1127-set-quote_all_identifiers-to-default"},
			},
			{
				Query: "SHOW quote_all_identifiers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1128-show-quote_all_identifiers"},
			},
			{
				Query: "SELECT current_setting('quote_all_identifiers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1129-select-current_setting-quote_all_identifiers"},
			},
		},
	},
	{
		Name:        "set 'random_page_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW random_page_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1130-show-random_page_cost"},
			},
			{
				Query: "SET random_page_cost TO 2.5", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1131-set-random_page_cost-to-2.5"},
			},
			{
				Query: "SHOW random_page_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1132-show-random_page_cost"},
			},
			{
				Query: "SET random_page_cost TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1133-set-random_page_cost-to-default"},
			},
			{
				Query: "SHOW random_page_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1134-show-random_page_cost"},
			},
			{
				Query: "SELECT current_setting('random_page_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1135-select-current_setting-random_page_cost"},
			},
		},
	},
	{
		Name:        "set 'recovery_end_command' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_end_command", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1136-show-recovery_end_command"},
			},
			{
				Query: "SET recovery_end_command TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1137-set-recovery_end_command-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_end_command')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1138-select-current_setting-recovery_end_command"},
			},
		},
	},
	{
		Name:        "set 'recovery_init_sync_method' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_init_sync_method", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1139-show-recovery_init_sync_method"},
			},
			{
				Query: "SET recovery_init_sync_method TO 'fsync'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1140-set-recovery_init_sync_method-to-fsync", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_init_sync_method')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1141-select-current_setting-recovery_init_sync_method"},
			},
		},
	},
	{
		Name:        "set 'recovery_min_apply_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_min_apply_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1142-show-recovery_min_apply_delay"},
			},
			{
				Query: "SET recovery_min_apply_delay TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1143-set-recovery_min_apply_delay-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_min_apply_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1144-select-current_setting-recovery_min_apply_delay"},
			},
		},
	},
	{
		Name:        "set 'recovery_prefetch' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_prefetch", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1145-show-recovery_prefetch"},
			},
			{
				Query: "SET recovery_prefetch TO 'try'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1146-set-recovery_prefetch-to-try", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_prefetch')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1147-select-current_setting-recovery_prefetch"},
			},
		},
	},
	{
		Name:        "set 'recovery_target' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1148-show-recovery_target"},
			},
			{
				Query: "SET recovery_target TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1149-set-recovery_target-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1150-select-current_setting-recovery_target"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_action' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_action", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1151-show-recovery_target_action"},
			},
			{
				Query: "SET recovery_target_action TO 'pause'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1152-set-recovery_target_action-to-pause", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_action')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1153-select-current_setting-recovery_target_action"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_inclusive' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_inclusive", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1154-show-recovery_target_inclusive"},
			},
			{
				Query: "SET recovery_target_inclusive TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1155-set-recovery_target_inclusive-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_inclusive')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1156-select-current_setting-recovery_target_inclusive"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_lsn' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_lsn", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1157-show-recovery_target_lsn"},
			},
			{
				Query: "SET recovery_target_lsn TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1158-set-recovery_target_lsn-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_lsn')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1159-select-current_setting-recovery_target_lsn"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_name' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_name", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1160-show-recovery_target_name"},
			},
			{
				Query: "SET recovery_target_name TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1161-set-recovery_target_name-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_name')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1162-select-current_setting-recovery_target_name"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_time' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_time", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1163-show-recovery_target_time"},
			},
			{
				Query: "SET recovery_target_time TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1164-set-recovery_target_time-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_time')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1165-select-current_setting-recovery_target_time"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_timeline' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_timeline", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1166-show-recovery_target_timeline"},
			},
			{
				Query: "SET recovery_target_timeline TO 'latest'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1167-set-recovery_target_timeline-to-latest", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_timeline')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1168-select-current_setting-recovery_target_timeline"},
			},
		},
	},
	{
		Name:        "set 'recovery_target_xid' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recovery_target_xid", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1169-show-recovery_target_xid"},
			},
			{
				Query: "SET recovery_target_xid TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1170-set-recovery_target_xid-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('recovery_target_xid')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1171-select-current_setting-recovery_target_xid"},
			},
		},
	},
	{
		Name:        "set 'recursive_worktable_factor' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW recursive_worktable_factor", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1172-show-recursive_worktable_factor"},
			},
			{
				Query: "SET recursive_worktable_factor TO '1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1173-set-recursive_worktable_factor-to-1"},
			},
			{
				Query: "SHOW recursive_worktable_factor", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1174-show-recursive_worktable_factor"},
			},
			{
				Query: "SET recursive_worktable_factor TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1175-set-recursive_worktable_factor-to-default"},
			},
			{
				Query: "SHOW recursive_worktable_factor", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1176-show-recursive_worktable_factor"},
			},
			{
				Query: "SELECT current_setting('recursive_worktable_factor')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1177-select-current_setting-recursive_worktable_factor"},
			},
		},
	},
	{
		Name:        "set 'remove_temp_files_after_crash' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW remove_temp_files_after_crash", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1178-show-remove_temp_files_after_crash"},
			},
			{
				Query: "SET remove_temp_files_after_crash TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1179-set-remove_temp_files_after_crash-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('remove_temp_files_after_crash')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1180-select-current_setting-remove_temp_files_after_crash"},
			},
		},
	},
	{
		Name:        "set 'reserved_connections' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW reserved_connections", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1181-show-reserved_connections"},
			},
			{
				Query: "SET reserved_connections TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1182-set-reserved_connections-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('reserved_connections')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1183-select-current_setting-reserved_connections"},
			},
		},
	},
	{
		Name:        "set 'restart_after_crash' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW restart_after_crash", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1184-show-restart_after_crash"},
			},
			{
				Query: "SET restart_after_crash TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1185-set-restart_after_crash-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('restart_after_crash')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1186-select-current_setting-restart_after_crash"},
			},
		},
	},
	{
		Name:        "set 'restore_command' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW restore_command", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1187-show-restore_command"},
			},
			{
				Query: "SET restore_command TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1188-set-restore_command-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('restore_command')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1189-select-current_setting-restore_command"},
			},
		},
	},
	{
		Name:        "set 'row_security' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW row_security", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1190-show-row_security"},
			},
			{
				Query: "SET row_security TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1191-set-row_security-to-off"},
			},
			{
				Query: "SHOW row_security", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1192-show-row_security"},
			},
			{
				Query: "SET row_security TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1193-set-row_security-to-default"},
			},
			{
				Query: "SHOW row_security", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1194-show-row_security"},
			},
			{
				Query: "SELECT current_setting('row_security')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1195-select-current_setting-row_security"},
			},
		},
	},
	{
		Name:        "set 'scram_iterations' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW scram_iterations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1196-show-scram_iterations"},
			},
			{
				Query: "SET scram_iterations TO '4000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1197-set-scram_iterations-to-4000"},
			},
			{
				Query: "SHOW scram_iterations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1198-show-scram_iterations"},
			},
			{
				Query: "SET scram_iterations TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1199-set-scram_iterations-to-default"},
			},
			{
				Query: "SHOW scram_iterations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1200-show-scram_iterations"},
			},
			{
				Query: "SELECT current_setting('scram_iterations')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1201-select-current_setting-scram_iterations"},
			},
		},
	},
	{
		Name:        "set 'search_path' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1202-show-search_path"},
			},
			{
				Query: "SET search_path TO 'postgres'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1203-set-search_path-to-postgres"},
			},
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1204-show-search_path"},
			},
			{
				Query: "SET search_path TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1205-set-search_path-to-default"},
			},
			{
				Query: "SHOW search_path", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1206-show-search_path"},
			},
			{
				Query: "SELECT current_setting('search_path')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1207-select-current_setting-search_path"},
			},
		},
	},
	{
		Name:        "set 'segment_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW segment_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1208-show-segment_size"},
			},
			{
				Query: "SET segment_size TO '131072'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1209-set-segment_size-to-131072", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('segment_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1210-select-current_setting-segment_size"},
			},
		},
	},
	{
		Name:        "set 'send_abort_for_crash' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW send_abort_for_crash", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1211-show-send_abort_for_crash"},
			},
			{
				Query: "SET send_abort_for_crash TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1212-set-send_abort_for_crash-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('send_abort_for_crash')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1213-select-current_setting-send_abort_for_crash"},
			},
		},
	},
	{
		Name:        "set 'send_abort_for_kill' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW send_abort_for_kill", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1214-show-send_abort_for_kill"},
			},
			{
				Query: "SET send_abort_for_kill TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1215-set-send_abort_for_kill-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('send_abort_for_kill')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1216-select-current_setting-send_abort_for_kill"},
			},
		},
	},
	{
		Name:        "set 'seq_page_cost' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW seq_page_cost", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1217-show-seq_page_cost"},
			},
			{
				Query: "SET seq_page_cost TO '1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1218-set-seq_page_cost-to-1"},
			},
			{
				Query: "SELECT current_setting('seq_page_cost')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1219-select-current_setting-seq_page_cost"},
			},
		},
	},
	{
		Name:        "set 'server_encoding' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW server_encoding", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1220-show-server_encoding"},
			},
			{
				Query: "SET server_encoding TO 'UTF8'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1221-set-server_encoding-to-utf8", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('server_encoding')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1222-select-current_setting-server_encoding"},
			},
		},
	},
	{
		Name:        "set 'server_version' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW server_version", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1223-show-server_version"},
			},
			{
				Query: "SET server_version TO '15.17 (Homebrew)'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1224-set-server_version-to-15.17-homebrew", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('server_version')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1225-select-current_setting-server_version"},
			},
		},
	},
	{
		Name:        "set 'server_version_num' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW server_version_num", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1226-show-server_version_num"},
			},
			{
				Query: "SET server_version_num TO '150017'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1227-set-server_version_num-to-150017", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('server_version_num')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1228-select-current_setting-server_version_num"},
			},
		},
	},
	{
		Name:        "set 'session_preload_libraries' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW session_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1229-show-session_preload_libraries"},
			},
			{
				Query: "SET session_preload_libraries TO '/'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1230-set-session_preload_libraries-to-/"},
			},
			{
				Query: "SHOW session_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1231-show-session_preload_libraries"},
			},
			{
				Query: "SET session_preload_libraries TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1232-set-session_preload_libraries-to-default"},
			},
			{
				Query: "SHOW session_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1233-show-session_preload_libraries"},
			},
			{
				Query: "SELECT current_setting('session_preload_libraries')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1234-select-current_setting-session_preload_libraries"},
			},
		},
	},
	{
		Name:        "set 'session_replication_role' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW session_replication_role", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1235-show-session_replication_role"},
			},
			{
				Query: "SET session_replication_role TO 'local'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1236-set-session_replication_role-to-local"},
			},
			{
				Query: "SHOW session_replication_role", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1237-show-session_replication_role"},
			},
			{
				Query: "SET session_replication_role TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1238-set-session_replication_role-to-default"},
			},
			{
				Query: "SHOW session_replication_role", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1239-show-session_replication_role"},
			},
			{
				Query: "SELECT current_setting('session_replication_role')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1240-select-current_setting-session_replication_role"},
			},
		},
	},
	{
		Name:        "set 'shared_buffers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW shared_buffers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1241-show-shared_buffers"},
			},
			{
				Query: "SET shared_buffers TO '128000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1242-set-shared_buffers-to-128000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('shared_buffers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1243-select-current_setting-shared_buffers"},
			},
		},
	},
	{
		Name:        "set 'shared_memory_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW shared_memory_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1244-show-shared_memory_size"},
			},
			{
				Query: "SET shared_memory_size TO '143000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1245-set-shared_memory_size-to-143000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('shared_memory_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1246-select-current_setting-shared_memory_size"},
			},
		},
	},
	{
		Name:        "set 'shared_memory_size_in_huge_pages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW shared_memory_size_in_huge_pages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1247-show-shared_memory_size_in_huge_pages"},
			},
			{
				Query: "SET shared_memory_size_in_huge_pages TO '-1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1248-set-shared_memory_size_in_huge_pages-to-1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('shared_memory_size_in_huge_pages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1249-select-current_setting-shared_memory_size_in_huge_pages"},
			},
		},
	},
	{
		Name:        "set 'shared_memory_type' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW shared_memory_type", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1250-show-shared_memory_type"},
			},
			{
				Query: "SET shared_memory_type TO 'mmap'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1251-set-shared_memory_type-to-mmap", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('shared_memory_type')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1252-select-current_setting-shared_memory_type"},
			},
		},
	},
	{
		Name:        "set 'shared_preload_libraries' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW shared_preload_libraries", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1253-show-shared_preload_libraries"},
			},
			{
				Query: "SET shared_preload_libraries TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1254-set-shared_preload_libraries-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('shared_preload_libraries')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1255-select-current_setting-shared_preload_libraries"},
			},
		},
	},
	{
		Name:        "set 'ssl' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1256-show-ssl"},
			},
			{
				Query: "SET ssl TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1257-set-ssl-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1258-select-current_setting-ssl"},
			},
		},
	},
	{
		Name:        "set 'ssl_ca_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_ca_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1259-show-ssl_ca_file"},
			},
			{
				Query: "SET ssl_ca_file TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1260-set-ssl_ca_file-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_ca_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1261-select-current_setting-ssl_ca_file"},
			},
		},
	},
	{
		Name:        "set 'ssl_cert_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_cert_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1262-show-ssl_cert_file"},
			},
			{
				Query: "SET ssl_cert_file TO 'server.crt'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1263-set-ssl_cert_file-to-server.crt", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_cert_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1264-select-current_setting-ssl_cert_file"},
			},
		},
	},
	{
		Name:        "set 'ssl_ciphers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_ciphers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1265-show-ssl_ciphers"},
			},
			{
				Query: "SET ssl_ciphers TO 'HIGH:MEDIUM:'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1266-set-ssl_ciphers-to-high:medium:", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_ciphers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1267-select-current_setting-ssl_ciphers"},
			},
		},
	},
	{
		Name:        "set 'ssl_crl_dir' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_crl_dir", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1268-show-ssl_crl_dir"},
			},
			{
				Query: "SET ssl_crl_dir TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1269-set-ssl_crl_dir-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_crl_dir')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1270-select-current_setting-ssl_crl_dir"},
			},
		},
	},
	{
		Name:        "set 'ssl_crl_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_crl_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1271-show-ssl_crl_file"},
			},
			{
				Query: "SET ssl_crl_file TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1272-set-ssl_crl_file-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_crl_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1273-select-current_setting-ssl_crl_file"},
			},
		},
	},
	{
		Name:        "set 'ssl_dh_params_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_dh_params_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1274-show-ssl_dh_params_file"},
			},
			{
				Query: "SET ssl_dh_params_file TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1275-set-ssl_dh_params_file-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_dh_params_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1276-select-current_setting-ssl_dh_params_file"},
			},
		},
	},
	{
		Name:        "set 'ssl_ecdh_curve' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_ecdh_curve", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1277-show-ssl_ecdh_curve"},
			},
			{
				Query: "SET ssl_ecdh_curve TO 'prime256v1'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1278-set-ssl_ecdh_curve-to-prime256v1", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_ecdh_curve')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1279-select-current_setting-ssl_ecdh_curve"},
			},
		},
	},
	{
		Name:        "set 'ssl_key_file' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_key_file", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1280-show-ssl_key_file"},
			},
			{
				Query: "SET ssl_key_file TO 'server.key'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1281-set-ssl_key_file-to-server.key", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_key_file')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1282-select-current_setting-ssl_key_file"},
			},
		},
	},
	{
		Name:        "set 'ssl_library' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_library", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1283-show-ssl_library"},
			},
			{
				Query: "SET ssl_library TO 'OpenSSL'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1284-set-ssl_library-to-openssl", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_library')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1285-select-current_setting-ssl_library"},
			},
		},
	},
	{
		Name:        "set 'ssl_max_protocol_version' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_max_protocol_version", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1286-show-ssl_max_protocol_version"},
			},
			{
				Query: "SET ssl_max_protocol_version TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1287-set-ssl_max_protocol_version-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_max_protocol_version')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1288-select-current_setting-ssl_max_protocol_version"},
			},
		},
	},
	{
		Name:        "set 'ssl_min_protocol_version' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_min_protocol_version", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1289-show-ssl_min_protocol_version"},
			},
			{
				Query: "SET ssl_min_protocol_version TO 'TLSv1.2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1290-set-ssl_min_protocol_version-to-tlsv1.2", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_min_protocol_version')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1291-select-current_setting-ssl_min_protocol_version"},
			},
		},
	},
	{
		Name:        "set 'ssl_passphrase_command' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_passphrase_command", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1292-show-ssl_passphrase_command"},
			},
			{
				Query: "SET ssl_passphrase_command TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1293-set-ssl_passphrase_command-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_passphrase_command')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1294-select-current_setting-ssl_passphrase_command"},
			},
		},
	},
	{
		Name:        "set 'ssl_passphrase_command_supports_reload' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_passphrase_command_supports_reload", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1295-show-ssl_passphrase_command_supports_reload"},
			},
			{
				Query: "SET ssl_passphrase_command_supports_reload TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1296-set-ssl_passphrase_command_supports_reload-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_passphrase_command_supports_reload')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1297-select-current_setting-ssl_passphrase_command_supports_reload"},
			},
		},
	},
	{
		Name:        "set 'ssl_prefer_server_ciphers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW ssl_prefer_server_ciphers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1298-show-ssl_prefer_server_ciphers"},
			},
			{
				Query: "SET ssl_prefer_server_ciphers TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1299-set-ssl_prefer_server_ciphers-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('ssl_prefer_server_ciphers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1300-select-current_setting-ssl_prefer_server_ciphers"},
			},
		},
	},
	{
		Name:        "set 'standard_conforming_strings' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW standard_conforming_strings", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1301-show-standard_conforming_strings"},
			},
			{
				Query: "SET standard_conforming_strings TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1302-set-standard_conforming_strings-to-off"},
			},
			{
				Query:    "SHOW standard_conforming_strings",
				Expected: []sql.Row{{int8(0)}},
			},
			{
				Query:    "SET standard_conforming_strings TO DEFAULT",
				Expected: []sql.Row{},
			},
			{
				Query: "SHOW standard_conforming_strings", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1305-show-standard_conforming_strings"},
			},
			{
				Query: "SELECT current_setting('standard_conforming_strings')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1306-select-current_setting-standard_conforming_strings"},
			},
		},
	},
	{
		Name:        "set 'statement_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW statement_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1307-show-statement_timeout"},
			},
			{
				Query: "SET statement_timeout TO '42'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1308-set-statement_timeout-to-42"},
			},
			{
				Query: "SHOW statement_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1309-show-statement_timeout"},
			},
			{
				Query: "SELECT current_setting('statement_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1310-select-current_setting-statement_timeout"},
			},
		},
	},
	{
		Name:        "set 'stats_fetch_consistency' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW stats_fetch_consistency", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1311-show-stats_fetch_consistency"},
			},
			{
				Query: "SET stats_fetch_consistency TO 'snapshot'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1312-set-stats_fetch_consistency-to-snapshot"},
			},
			{
				Query: "SHOW stats_fetch_consistency", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1313-show-stats_fetch_consistency"},
			},
			{
				Query: "SET stats_fetch_consistency TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1314-set-stats_fetch_consistency-to-default"},
			},
			{
				Query: "SHOW stats_fetch_consistency", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1315-show-stats_fetch_consistency"},
			},
			{
				Query: "SELECT current_setting('stats_fetch_consistency')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1316-select-current_setting-stats_fetch_consistency"},
			},
		},
	},
	{
		Name:        "set 'superuser_reserved_connections' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW superuser_reserved_connections", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1317-show-superuser_reserved_connections"},
			},
			{
				Query: "SET superuser_reserved_connections TO '3'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1318-set-superuser_reserved_connections-to-3", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('superuser_reserved_connections')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1319-select-current_setting-superuser_reserved_connections"},
			},
		},
	},
	{
		Name:        "set 'synchronize_seqscans' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW synchronize_seqscans", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1320-show-synchronize_seqscans"},
			},
			{
				Query: "SET synchronize_seqscans TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1321-set-synchronize_seqscans-to-off"},
			},
			{
				Query: "SHOW synchronize_seqscans", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1322-show-synchronize_seqscans"},
			},
			{
				Query: "SET synchronize_seqscans TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1323-set-synchronize_seqscans-to-default"},
			},
			{
				Query: "SHOW synchronize_seqscans", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1324-show-synchronize_seqscans"},
			},
			{
				Query: "SELECT current_setting('synchronize_seqscans')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1325-select-current_setting-synchronize_seqscans"},
			},
		},
	},
	{
		Name:        "set 'synchronous_commit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW synchronous_commit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1326-show-synchronous_commit"},
			},
			{
				Query: "SET synchronous_commit TO 'local'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1327-set-synchronous_commit-to-local"},
			},
			{
				Query: "SHOW synchronous_commit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1328-show-synchronous_commit"},
			},
			{
				Query: "SET synchronous_commit TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1329-set-synchronous_commit-to-on"},
			},
			{
				Query: "SHOW synchronous_commit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1330-show-synchronous_commit"},
			},
			{
				Query: "SELECT current_setting('synchronous_commit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1331-select-current_setting-synchronous_commit"},
			},
		},
	},
	{
		Name:        "set 'synchronous_standby_names' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW synchronous_standby_names", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1332-show-synchronous_standby_names"},
			},
			{
				Query: "SET synchronous_standby_names TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1333-set-synchronous_standby_names-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('synchronous_standby_names')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1334-select-current_setting-synchronous_standby_names"},
			},
		},
	},
	{
		Name:        "set 'syslog_facility' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW syslog_facility", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1335-show-syslog_facility"},
			},
			{
				Query: "SET syslog_facility TO 'local0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1336-set-syslog_facility-to-local0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('syslog_facility')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1337-select-current_setting-syslog_facility"},
			},
		},
	},
	{
		Name:        "set 'syslog_ident' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW syslog_ident", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1338-show-syslog_ident"},
			},
			{
				Query: "SET syslog_ident TO 'postgres'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1339-set-syslog_ident-to-postgres", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('syslog_ident')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1340-select-current_setting-syslog_ident"},
			},
		},
	},
	{
		Name:        "set 'syslog_sequence_numbers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW syslog_sequence_numbers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1341-show-syslog_sequence_numbers"},
			},
			{
				Query: "SET syslog_sequence_numbers TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1342-set-syslog_sequence_numbers-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('syslog_sequence_numbers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1343-select-current_setting-syslog_sequence_numbers"},
			},
		},
	},
	{
		Name:        "set 'syslog_split_messages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW syslog_split_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1344-show-syslog_split_messages"},
			},
			{
				Query: "SET syslog_split_messages TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1345-set-syslog_split_messages-to-on", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('syslog_split_messages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1346-select-current_setting-syslog_split_messages"},
			},
		},
	},
	{
		Name:        "set 'tcp_keepalives_count' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW tcp_keepalives_count", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1347-show-tcp_keepalives_count"},
			},
			{
				Query: "SET tcp_keepalives_count TO 100", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1348-set-tcp_keepalives_count-to-100"},
			},
			{
				Query: "SHOW tcp_keepalives_count", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1349-show-tcp_keepalives_count"},
			},
			{
				Query: "SET tcp_keepalives_count TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1350-set-tcp_keepalives_count-to-default"},
			},
			{
				Query: "SHOW tcp_keepalives_count", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1351-show-tcp_keepalives_count"},
			},
			{
				Query: "SELECT current_setting('tcp_keepalives_count')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1352-select-current_setting-tcp_keepalives_count"},
			},
		},
	},
	{
		Name:        "set 'tcp_keepalives_idle' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW tcp_keepalives_idle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1353-show-tcp_keepalives_idle"},
			},
			{
				Query: "SET tcp_keepalives_idle TO 1", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1354-set-tcp_keepalives_idle-to-1"},
			},
			{
				Query: "SHOW tcp_keepalives_idle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1355-show-tcp_keepalives_idle"},
			},
			{
				Query: "SET tcp_keepalives_idle TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1356-set-tcp_keepalives_idle-to-default"},
			},
			{
				Query: "SHOW tcp_keepalives_idle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1357-show-tcp_keepalives_idle"},
			},
			{
				Query: "SELECT current_setting('tcp_keepalives_idle')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1358-select-current_setting-tcp_keepalives_idle"},
			},
		},
	},
	{
		Name:        "set 'tcp_keepalives_interval' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW tcp_keepalives_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1359-show-tcp_keepalives_interval"},
			},
			{
				Query: "SET tcp_keepalives_interval TO 1", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1360-set-tcp_keepalives_interval-to-1"},
			},
			{
				Query: "SHOW tcp_keepalives_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1361-show-tcp_keepalives_interval"},
			},
			{
				Query: "SET tcp_keepalives_interval TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1362-set-tcp_keepalives_interval-to-default"},
			},
			{
				Query: "SHOW tcp_keepalives_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1363-show-tcp_keepalives_interval"},
			},
			{
				Query: "SELECT current_setting('tcp_keepalives_interval')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1364-select-current_setting-tcp_keepalives_interval"},
			},
		},
	},
	{
		Name:        "set 'tcp_user_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW tcp_user_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1365-show-tcp_user_timeout"},
			},
			{
				Query: "SET tcp_user_timeout TO '100000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1366-set-tcp_user_timeout-to-100000"},
			},
			{
				Query: "SHOW tcp_user_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1367-show-tcp_user_timeout"},
			},
			{
				Query: "SET tcp_user_timeout TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1368-set-tcp_user_timeout-to-default"},
			},
			{
				Query: "SHOW tcp_user_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1369-show-tcp_user_timeout"},
			},
			{
				Query: "SELECT current_setting('tcp_user_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1370-select-current_setting-tcp_user_timeout"},
			},
		},
	},
	{
		Name:        "set 'temp_buffers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW temp_buffers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1371-show-temp_buffers"},
			},
			{
				Query: "SET temp_buffers TO '8000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1372-set-temp_buffers-to-8000"},
			},
			{
				Query: "SHOW temp_buffers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1373-show-temp_buffers"},
			},
			{
				Query: "SET temp_buffers TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1374-set-temp_buffers-to-default"},
			},
			{
				Query: "SHOW temp_buffers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1375-show-temp_buffers"},
			},
			{
				Query: "SELECT current_setting('temp_buffers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1376-select-current_setting-temp_buffers"},
			},
		},
	},
	{
		Name:        "set 'temp_file_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW temp_file_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1377-show-temp_file_limit"},
			},
			{
				Query: "SET temp_file_limit TO 100", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1378-set-temp_file_limit-to-100"},
			},
			{
				Query: "SHOW temp_file_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1379-show-temp_file_limit"},
			},
			{
				Query: "SET temp_file_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1380-set-temp_file_limit-to-default"},
			},
			{
				Query: "SHOW temp_file_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1381-show-temp_file_limit"},
			},
			{
				Query: "SELECT current_setting('temp_file_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1382-select-current_setting-temp_file_limit"},
			},
		},
	},
	{
		Name:        "set 'temp_tablespaces' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW temp_tablespaces", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1383-show-temp_tablespaces"},
			},
			{
				Query: "SET temp_tablespaces TO 'pg_default'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1384-set-temp_tablespaces-to-pg_default"},
			},
			{
				Query: "SHOW temp_tablespaces", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1385-show-temp_tablespaces"},
			},
			{
				Query: "SET temp_tablespaces TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1386-set-temp_tablespaces-to-default"},
			},
			{
				Query: "SHOW temp_tablespaces", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1387-show-temp_tablespaces"},
			},
			{
				Query: "SELECT current_setting('temp_tablespaces')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1388-select-current_setting-temp_tablespaces"},
			},
		},
	},
	{
		Name:        "set 'TimeZone' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW TimeZone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1389-show-timezone"},
			},
			{
				Query: "SET TimeZone TO 'UTC'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1390-set-timezone-to-utc"},
			},
			{
				Query: "SHOW TimeZone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1391-show-timezone"},
			},
			{
				Query: "SET TimeZone TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1392-set-timezone-to-default"},
			},
			{
				Query: "SHOW TimeZone", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1393-show-timezone"},
			},
			{
				Query: "SELECT current_setting('TimeZone')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1394-select-current_setting-timezone"},
			},
		},
	},
	{
		Name:        "set 'timezone_abbreviations' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW timezone_abbreviations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1395-show-timezone_abbreviations"},
			},
			{
				Query: "SET timezone_abbreviations TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1396-set-timezone_abbreviations-to", Compare: "sqlstate"},
			},
			{
				Query: "SHOW timezone_abbreviations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1397-show-timezone_abbreviations"},
			},
			{
				Query: "SET timezone_abbreviations TO 'Default'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1398-set-timezone_abbreviations-to-default"},
			},
			{
				Query: "SHOW timezone_abbreviations", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1399-show-timezone_abbreviations"},
			},
			{
				Query: "SELECT current_setting('timezone_abbreviations')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1400-select-current_setting-timezone_abbreviations"},
			},
		},
	},
	{
		Name:        "set 'trace_notify' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW trace_notify", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1401-show-trace_notify"},
			},
			{
				Query: "SET trace_notify TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1402-set-trace_notify-to-on"},
			},
			{
				Query: "SHOW trace_notify", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1403-show-trace_notify"},
			},
			{
				Query: "SET trace_notify TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1404-set-trace_notify-to-default"},
			},
			{
				Query: "SHOW trace_notify", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1405-show-trace_notify"},
			},
			{
				Query: "SELECT current_setting('trace_notify')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1406-select-current_setting-trace_notify"},
			},
		},
	},
	{
		Name:        "set 'trace_recovery_messages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW trace_recovery_messages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1407-show-trace_recovery_messages"},
			},
			{
				Query: "SET trace_recovery_messages TO 'log'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1408-set-trace_recovery_messages-to-log", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('trace_recovery_messages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1409-select-current_setting-trace_recovery_messages"},
			},
		},
	},
	{
		Name:        "set 'trace_sort' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW trace_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1410-show-trace_sort"},
			},
			{
				Query: "SET trace_sort TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1411-set-trace_sort-to-on"},
			},
			{
				Query: "SHOW trace_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1412-show-trace_sort"},
			},
			{
				Query: "SET trace_sort TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1413-set-trace_sort-to-default"},
			},
			{
				Query: "SHOW trace_sort", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1414-show-trace_sort"},
			},
			{
				Query: "SELECT current_setting('trace_sort')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1415-select-current_setting-trace_sort"},
			},
		},
	},
	{
		Name:        "set 'track_activities' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_activities", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1416-show-track_activities"},
			},
			{
				Query: "SET track_activities TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1417-set-track_activities-to-off"},
			},
			{
				Query: "SHOW track_activities", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1418-show-track_activities"},
			},
			{
				Query: "SET track_activities TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1419-set-track_activities-to-on"},
			},
			{
				Query: "SHOW track_activities", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1420-show-track_activities"},
			},
			{
				Query: "SELECT current_setting('track_activities')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1421-select-current_setting-track_activities"},
			},
		},
	},
	{
		Name:        "set 'track_activity_query_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_activity_query_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1422-show-track_activity_query_size"},
			},
			{
				Query: "SET track_activity_query_size TO '1024'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1423-set-track_activity_query_size-to-1024", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('track_activity_query_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1424-select-current_setting-track_activity_query_size"},
			},
		},
	},
	{
		Name:        "set 'track_commit_timestamp' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_commit_timestamp", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1425-show-track_commit_timestamp"},
			},
			{
				Query: "SET track_commit_timestamp TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1426-set-track_commit_timestamp-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('track_commit_timestamp')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1427-select-current_setting-track_commit_timestamp"},
			},
		},
	},
	{
		Name:        "set 'track_counts' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_counts", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1428-show-track_counts"},
			},
			{
				Query: "SET track_counts TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1429-set-track_counts-to-off"},
			},
			{
				Query: "SHOW track_counts", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1430-show-track_counts"},
			},
			{
				Query: "SET track_counts TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1431-set-track_counts-to-default"},
			},
			{
				Query: "SHOW track_counts", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1432-show-track_counts"},
			},
			{
				Query: "SELECT current_setting('track_counts')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1433-select-current_setting-track_counts"},
			},
		},
	},
	{
		Name:        "set 'track_functions' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_functions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1434-show-track_functions"},
			},
			{
				Query: "SET track_functions TO 'all'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1435-set-track_functions-to-all"},
			},
			{
				Query: "SHOW track_functions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1436-show-track_functions"},
			},
			{
				Query: "SET track_functions TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1437-set-track_functions-to-default"},
			},
			{
				Query: "SHOW track_functions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1438-show-track_functions"},
			},
			{
				Query: "SELECT current_setting('track_functions')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1439-select-current_setting-track_functions"},
			},
		},
	},
	{
		Name:        "set 'track_io_timing' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_io_timing", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1440-show-track_io_timing"},
			},
			{
				Query: "SET track_io_timing TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1441-set-track_io_timing-to-on"},
			},
			{
				Query: "SHOW track_io_timing", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1442-show-track_io_timing"},
			},
			{
				Query: "SET track_io_timing TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1443-set-track_io_timing-to-default"},
			},
			{
				Query: "SHOW track_io_timing", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1444-show-track_io_timing"},
			},
			{
				Query: "SELECT current_setting('track_io_timing')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1445-select-current_setting-track_io_timing"},
			},
		},
	},
	{
		Name:        "set 'track_wal_io_timing' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW track_wal_io_timing", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1446-show-track_wal_io_timing"},
			},
			{
				Query: "SET track_wal_io_timing TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1447-set-track_wal_io_timing-to-on"},
			},
			{
				Query: "SHOW track_wal_io_timing", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1448-show-track_wal_io_timing"},
			},
			{
				Query: "SET track_wal_io_timing TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1449-set-track_wal_io_timing-to-default"},
			},
			{
				Query: "SHOW track_wal_io_timing", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1450-show-track_wal_io_timing"},
			},
			{
				Query: "SELECT current_setting('track_wal_io_timing')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1451-select-current_setting-track_wal_io_timing"},
			},
		},
	},
	{
		Name:        "set 'transaction_deferrable' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW transaction_deferrable", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1452-show-transaction_deferrable"},
			},
			{
				Query: "SET transaction_deferrable TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1453-set-transaction_deferrable-to-on"},
			},
			{
				Query: "SHOW transaction_deferrable", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1454-show-transaction_deferrable"},
			},
			{
				Query: "SET transaction_deferrable TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1455-set-transaction_deferrable-to-default", Compare: "sqlstate"},
			},
			{
				Query:    "SHOW transaction_deferrable",
				Expected: []sql.Row{{int8(0)}},
			},
			{
				Query: "SELECT current_setting('transaction_deferrable')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1457-select-current_setting-transaction_deferrable"},
			},
		},
	},
	{
		Name:        "set 'transaction_isolation' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW transaction_isolation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1458-show-transaction_isolation"},
			},
			{
				Query: "SET transaction_isolation TO 'serializable'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1459-set-transaction_isolation-to-serializable"},
			},
			{
				Query: "SHOW transaction_isolation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1460-show-transaction_isolation"},
			},
			{
				Query: "SET transaction_isolation TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1461-set-transaction_isolation-to-default", Compare: "sqlstate"},
			},
			{
				Query: "SHOW transaction_isolation", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1462-show-transaction_isolation"},
			},
			{
				Query: "SELECT current_setting('transaction_isolation')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1463-select-current_setting-transaction_isolation"},
			},
		},
	},
	{
		Name:        "set 'transaction_read_only' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW transaction_read_only", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1464-show-transaction_read_only"},
			},
			{
				Query: "SET transaction_read_only TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1465-set-transaction_read_only-to-on"},
			},
			{
				Query: "SHOW transaction_read_only", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1466-show-transaction_read_only"},
			},
			{
				Query: "SET transaction_read_only TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1467-set-transaction_read_only-to-default", Compare: "sqlstate"},
			},
			{
				Query:    "SHOW transaction_read_only",
				Expected: []sql.Row{{int8(0)}},
			},
			{
				Query: "SELECT current_setting('transaction_read_only')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1469-select-current_setting-transaction_read_only"},
			},
		},
	},
	{
		Name:        "set 'transform_null_equals' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW transform_null_equals", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1470-show-transform_null_equals"},
			},
			{
				Query: "SET transform_null_equals TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1471-set-transform_null_equals-to-on"},
			},
			{
				Query: "SHOW transform_null_equals", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1472-show-transform_null_equals"},
			},
			{
				Query: "SET transform_null_equals TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1473-set-transform_null_equals-to-default"},
			},
			{
				Query: "SHOW transform_null_equals", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1474-show-transform_null_equals"},
			},
			{
				Query: "SELECT current_setting('transform_null_equals')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1475-select-current_setting-transform_null_equals"},
			},
		},
	},
	{
		Name:        "set 'unix_socket_directories' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW unix_socket_directories", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1476-show-unix_socket_directories"},
			},
			{
				Query: "SET unix_socket_directories TO '/tmp'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1477-set-unix_socket_directories-to-/tmp", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('unix_socket_directories')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1478-select-current_setting-unix_socket_directories"},
			},
		},
	},
	{
		Name:        "set 'unix_socket_group' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW unix_socket_group", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1479-show-unix_socket_group"},
			},
			{
				Query: "SET unix_socket_group TO ''", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1480-set-unix_socket_group-to", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('unix_socket_group')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1481-select-current_setting-unix_socket_group"},
			},
		},
	},
	{
		Name:        "set 'unix_socket_permissions' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW unix_socket_permissions", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1482-show-unix_socket_permissions"},
			},
			{
				Query: "SET unix_socket_permissions TO '511'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1483-set-unix_socket_permissions-to-511", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('unix_socket_permissions')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1484-select-current_setting-unix_socket_permissions"},
			},
		},
	},
	{
		Name:        "set 'update_process_title' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW update_process_title", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1485-show-update_process_title"},
			},
			{
				Query: "SET update_process_title TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1486-set-update_process_title-to-off"},
			},
			{
				Query: "SHOW update_process_title", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1487-show-update_process_title"},
			},
			{
				Query: "SET update_process_title TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1488-set-update_process_title-to-default"},
			},
			{
				Query: "SHOW update_process_title", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1489-show-update_process_title"},
			},
			{
				Query: "SELECT current_setting('update_process_title')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1490-select-current_setting-update_process_title"},
			},
		},
	},
	{
		Name:        "set 'vacuum_buffer_usage_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_buffer_usage_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1491-show-vacuum_buffer_usage_limit"},
			},
			{
				Query: "SET vacuum_buffer_usage_limit TO '512'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1492-set-vacuum_buffer_usage_limit-to-512"},
			},
			{
				Query: "SHOW vacuum_buffer_usage_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1493-show-vacuum_buffer_usage_limit"},
			},
			{
				Query: "SET vacuum_buffer_usage_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1494-set-vacuum_buffer_usage_limit-to-default"},
			},
			{
				Query: "SHOW vacuum_buffer_usage_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1495-show-vacuum_buffer_usage_limit"},
			},
			{
				Query: "SELECT current_setting('vacuum_buffer_usage_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1496-select-current_setting-vacuum_buffer_usage_limit"},
			},
		},
	},
	{
		Name:        "set 'vacuum_cost_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_cost_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1497-show-vacuum_cost_delay"},
			},
			{
				Query: "SET vacuum_cost_delay TO '0.2'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1498-set-vacuum_cost_delay-to-0.2"},
			},
			{
				Query: "SHOW vacuum_cost_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1499-show-vacuum_cost_delay"},
			},
			{
				Query: "SET vacuum_cost_delay TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1500-set-vacuum_cost_delay-to-default"},
			},
			{
				Query: "SHOW vacuum_cost_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1501-show-vacuum_cost_delay"},
			},
			{
				Query: "SELECT current_setting('vacuum_cost_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1502-select-current_setting-vacuum_cost_delay"},
			},
		},
	},
	{
		Name:        "set 'vacuum_cost_limit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_cost_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1503-show-vacuum_cost_limit"},
			},
			{
				Query: "SET vacuum_cost_limit TO '400'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1504-set-vacuum_cost_limit-to-400"},
			},
			{
				Query: "SHOW vacuum_cost_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1505-show-vacuum_cost_limit"},
			},
			{
				Query: "SET vacuum_cost_limit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1506-set-vacuum_cost_limit-to-default"},
			},
			{
				Query: "SHOW vacuum_cost_limit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1507-show-vacuum_cost_limit"},
			},
			{
				Query: "SELECT current_setting('vacuum_cost_limit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1508-select-current_setting-vacuum_cost_limit"},
			},
		},
	},
	{
		Name:        "set 'vacuum_cost_page_dirty' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_cost_page_dirty", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1509-show-vacuum_cost_page_dirty"},
			},
			{
				Query: "SET vacuum_cost_page_dirty TO '200'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1510-set-vacuum_cost_page_dirty-to-200"},
			},
			{
				Query: "SHOW vacuum_cost_page_dirty", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1511-show-vacuum_cost_page_dirty"},
			},
			{
				Query: "SET vacuum_cost_page_dirty TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1512-set-vacuum_cost_page_dirty-to-default"},
			},
			{
				Query: "SHOW vacuum_cost_page_dirty", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1513-show-vacuum_cost_page_dirty"},
			},
			{
				Query: "SELECT current_setting('vacuum_cost_page_dirty')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1514-select-current_setting-vacuum_cost_page_dirty"},
			},
		},
	},
	{
		Name:        "set 'vacuum_cost_page_hit' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_cost_page_hit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1515-show-vacuum_cost_page_hit"},
			},
			{
				Query: "SET vacuum_cost_page_hit TO '100'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1516-set-vacuum_cost_page_hit-to-100"},
			},
			{
				Query: "SHOW vacuum_cost_page_hit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1517-show-vacuum_cost_page_hit"},
			},
			{
				Query: "SET vacuum_cost_page_hit TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1518-set-vacuum_cost_page_hit-to-default"},
			},
			{
				Query: "SHOW vacuum_cost_page_hit", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1519-show-vacuum_cost_page_hit"},
			},
			{
				Query: "SELECT current_setting('vacuum_cost_page_hit')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1520-select-current_setting-vacuum_cost_page_hit"},
			},
		},
	},
	{
		Name:        "set 'vacuum_cost_page_miss' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_cost_page_miss", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1521-show-vacuum_cost_page_miss"},
			},
			{
				Query: "SET vacuum_cost_page_miss TO '20'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1522-set-vacuum_cost_page_miss-to-20"},
			},
			{
				Query: "SHOW vacuum_cost_page_miss", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1523-show-vacuum_cost_page_miss"},
			},
			{
				Query: "SET vacuum_cost_page_miss TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1524-set-vacuum_cost_page_miss-to-default"},
			},
			{
				Query: "SHOW vacuum_cost_page_miss", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1525-show-vacuum_cost_page_miss"},
			},
			{
				Query: "SELECT current_setting('vacuum_cost_page_miss')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1526-select-current_setting-vacuum_cost_page_miss"},
			},
		},
	},
	{
		Name:        "set 'vacuum_failsafe_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_failsafe_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1527-show-vacuum_failsafe_age"},
			},
			{
				Query: "SET vacuum_failsafe_age TO '2100000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1528-set-vacuum_failsafe_age-to-2100000000"},
			},
			{
				Query: "SHOW vacuum_failsafe_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1529-show-vacuum_failsafe_age"},
			},
			{
				Query: "SET vacuum_failsafe_age TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1530-set-vacuum_failsafe_age-to-default"},
			},
			{
				Query: "SHOW vacuum_failsafe_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1531-show-vacuum_failsafe_age"},
			},
			{
				Query: "SELECT current_setting('vacuum_failsafe_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1532-select-current_setting-vacuum_failsafe_age"},
			},
		},
	},
	{
		Name:        "set 'vacuum_freeze_min_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_freeze_min_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1533-show-vacuum_freeze_min_age"},
			},
			{
				Query: "SET vacuum_freeze_min_age TO '20000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1534-set-vacuum_freeze_min_age-to-20000000"},
			},
			{
				Query: "SHOW vacuum_freeze_min_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1535-show-vacuum_freeze_min_age"},
			},
			{
				Query: "SET vacuum_freeze_min_age TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1536-set-vacuum_freeze_min_age-to-default"},
			},
			{
				Query: "SHOW vacuum_freeze_min_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1537-show-vacuum_freeze_min_age"},
			},
			{
				Query: "SELECT current_setting('vacuum_freeze_min_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1538-select-current_setting-vacuum_freeze_min_age"},
			},
		},
	},
	{
		Name:        "set 'vacuum_freeze_table_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_freeze_table_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1539-show-vacuum_freeze_table_age"},
			},
			{
				Query: "SET vacuum_freeze_table_age TO '100000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1540-set-vacuum_freeze_table_age-to-100000000"},
			},
			{
				Query: "SHOW vacuum_freeze_table_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1541-show-vacuum_freeze_table_age"},
			},
			{
				Query: "SET vacuum_freeze_table_age TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1542-set-vacuum_freeze_table_age-to-default"},
			},
			{
				Query: "SHOW vacuum_freeze_table_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1543-show-vacuum_freeze_table_age"},
			},
			{
				Query: "SELECT current_setting('vacuum_freeze_table_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1544-select-current_setting-vacuum_freeze_table_age"},
			},
		},
	},
	{
		Name:        "set 'vacuum_multixact_failsafe_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_multixact_failsafe_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1545-show-vacuum_multixact_failsafe_age"},
			},
			{
				Query: "SET vacuum_multixact_failsafe_age TO '1000000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1546-set-vacuum_multixact_failsafe_age-to-1000000000"},
			},
			{
				Query: "SHOW vacuum_multixact_failsafe_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1547-show-vacuum_multixact_failsafe_age"},
			},
			{
				Query: "SET vacuum_multixact_failsafe_age TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1548-set-vacuum_multixact_failsafe_age-to-default"},
			},
			{
				Query: "SHOW vacuum_multixact_failsafe_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1549-show-vacuum_multixact_failsafe_age"},
			},
			{
				Query: "SELECT current_setting('vacuum_multixact_failsafe_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1550-select-current_setting-vacuum_multixact_failsafe_age"},
			},
		},
	},
	{
		Name:        "set 'vacuum_multixact_freeze_min_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_multixact_freeze_min_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1551-show-vacuum_multixact_freeze_min_age"},
			},
			{
				Query: "SET vacuum_multixact_freeze_min_age TO '2000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1552-set-vacuum_multixact_freeze_min_age-to-2000000"},
			},
			{
				Query: "SHOW vacuum_multixact_freeze_min_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1553-show-vacuum_multixact_freeze_min_age"},
			},
			{
				Query: "SET vacuum_multixact_freeze_min_age TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1554-set-vacuum_multixact_freeze_min_age-to-default"},
			},
			{
				Query: "SHOW vacuum_multixact_freeze_min_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1555-show-vacuum_multixact_freeze_min_age"},
			},
			{
				Query: "SELECT current_setting('vacuum_multixact_freeze_min_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1556-select-current_setting-vacuum_multixact_freeze_min_age"},
			},
		},
	},
	{
		Name:        "set 'vacuum_multixact_freeze_table_age' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW vacuum_multixact_freeze_table_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1557-show-vacuum_multixact_freeze_table_age"},
			},
			{
				Query: "SET vacuum_multixact_freeze_table_age TO '120000000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1558-set-vacuum_multixact_freeze_table_age-to-120000000"},
			},
			{
				Query: "SHOW vacuum_multixact_freeze_table_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1559-show-vacuum_multixact_freeze_table_age"},
			},
			{
				Query: "SET vacuum_multixact_freeze_table_age TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1560-set-vacuum_multixact_freeze_table_age-to-default"},
			},
			{
				Query: "SHOW vacuum_multixact_freeze_table_age", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1561-show-vacuum_multixact_freeze_table_age"},
			},
			{
				Query: "SELECT current_setting('vacuum_multixact_freeze_table_age')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1562-select-current_setting-vacuum_multixact_freeze_table_age"},
			},
		},
	},
	{
		Name:        "set 'wal_block_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_block_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1563-show-wal_block_size"},
			},
			{
				Query: "SET wal_block_size TO '8192'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1564-set-wal_block_size-to-8192", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_block_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1565-select-current_setting-wal_block_size"},
			},
		},
	},
	{
		Name:        "set 'wal_buffers' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_buffers", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1566-show-wal_buffers"},
			},
			{
				Query: "SET wal_buffers TO '4000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1567-set-wal_buffers-to-4000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_buffers')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1568-select-current_setting-wal_buffers"},
			},
		},
	},
	{
		Name:        "set 'wal_compression' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_compression", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1569-show-wal_compression"},
			},
			{
				Query: "SET wal_compression TO 'lz4'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1570-set-wal_compression-to-lz4"},
			},
			{
				Query: "SHOW wal_compression", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1571-show-wal_compression"},
			},
			{
				Query: "SET wal_compression TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1572-set-wal_compression-to-off"},
			},
			{
				Query: "SHOW wal_compression", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1573-show-wal_compression"},
			},
			{
				Query: "SELECT current_setting('wal_compression')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1574-select-current_setting-wal_compression"},
			},
		},
	},
	{
		Name:        "set 'wal_consistency_checking' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_consistency_checking", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1575-show-wal_consistency_checking"},
			},
			{
				Query: "SET wal_consistency_checking TO 'generic'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1576-set-wal_consistency_checking-to-generic"},
			},
			{
				Query: "SHOW wal_consistency_checking", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1577-show-wal_consistency_checking"},
			},
			{
				Query: "SET wal_consistency_checking TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1578-set-wal_consistency_checking-to-default"},
			},
			{
				Query: "SHOW wal_consistency_checking", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1579-show-wal_consistency_checking"},
			},
			{
				Query: "SELECT current_setting('wal_consistency_checking')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1580-select-current_setting-wal_consistency_checking"},
			},
		},
	},
	{
		Name:        "set 'wal_decode_buffer_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_decode_buffer_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1581-show-wal_decode_buffer_size"},
			},
			{
				Query: "SET wal_decode_buffer_size TO '524288'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1582-set-wal_decode_buffer_size-to-524288", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_decode_buffer_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1583-select-current_setting-wal_decode_buffer_size"},
			},
		},
	},
	{
		Name:        "set 'wal_init_zero' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_init_zero", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1584-show-wal_init_zero"},
			},
			{
				Query: "SET wal_init_zero TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1585-set-wal_init_zero-to-off"},
			},
			{
				Query: "SHOW wal_init_zero", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1586-show-wal_init_zero"},
			},
			{
				Query: "SET wal_init_zero TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1587-set-wal_init_zero-to-default"},
			},
			{
				Query: "SHOW wal_init_zero", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1588-show-wal_init_zero"},
			},
			{
				Query: "SELECT current_setting('wal_init_zero')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1589-select-current_setting-wal_init_zero"},
			},
		},
	},
	{
		Name:        "set 'wal_keep_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_keep_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1590-show-wal_keep_size"},
			},
			{
				Query: "SET wal_keep_size TO '0'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1591-set-wal_keep_size-to-0", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_keep_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1592-select-current_setting-wal_keep_size"},
			},
		},
	},
	{
		Name:        "set 'wal_level' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_level", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1593-show-wal_level"},
			},
			{
				Query: "SET wal_level TO 'replica'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1594-set-wal_level-to-replica", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_level')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1595-select-current_setting-wal_level"},
			},
		},
	},
	{
		Name:        "set 'wal_log_hints' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_log_hints", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1596-show-wal_log_hints"},
			},
			{
				Query: "SET wal_log_hints TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1597-set-wal_log_hints-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_log_hints')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1598-select-current_setting-wal_log_hints"},
			},
		},
	},
	{
		Name:        "set 'wal_receiver_create_temp_slot' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_receiver_create_temp_slot", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1599-show-wal_receiver_create_temp_slot"},
			},
			{
				Query: "SET wal_receiver_create_temp_slot TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1600-set-wal_receiver_create_temp_slot-to-off", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_receiver_create_temp_slot')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1601-select-current_setting-wal_receiver_create_temp_slot"},
			},
		},
	},
	{
		Name:        "set 'wal_receiver_status_interval' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_receiver_status_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1602-show-wal_receiver_status_interval"},
			},
			{
				Query: "SET wal_receiver_status_interval TO '10'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1603-set-wal_receiver_status_interval-to-10", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_receiver_status_interval')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1604-select-current_setting-wal_receiver_status_interval"},
			},
		},
	},
	{
		Name:        "set 'wal_receiver_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_receiver_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1605-show-wal_receiver_timeout"},
			},
			{
				Query: "SET wal_receiver_timeout TO '60'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1606-set-wal_receiver_timeout-to-60", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_receiver_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1607-select-current_setting-wal_receiver_timeout"},
			},
		},
	},
	{
		Name:        "set 'wal_recycle' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_recycle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1608-show-wal_recycle"},
			},
			{
				Query: "SET wal_recycle TO 'off'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1609-set-wal_recycle-to-off"},
			},
			{
				Query: "SHOW wal_recycle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1610-show-wal_recycle"},
			},
			{
				Query: "SET wal_recycle TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1611-set-wal_recycle-to-default"},
			},
			{
				Query: "SHOW wal_recycle", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1612-show-wal_recycle"},
			},
			{
				Query: "SELECT current_setting('wal_recycle')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1613-select-current_setting-wal_recycle"},
			},
		},
	},
	{
		Name:        "set 'wal_retrieve_retry_interval' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_retrieve_retry_interval", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1614-show-wal_retrieve_retry_interval"},
			},
			{
				Query: "SET wal_retrieve_retry_interval TO '5'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1615-set-wal_retrieve_retry_interval-to-5", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_retrieve_retry_interval')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1616-select-current_setting-wal_retrieve_retry_interval"},
			},
		},
	},
	{
		Name:        "set 'wal_segment_size' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_segment_size", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1617-show-wal_segment_size"},
			},
			{
				Query: "SET wal_segment_size TO '16777216'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1618-set-wal_segment_size-to-16777216", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_segment_size')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1619-select-current_setting-wal_segment_size"},
			},
		},
	},
	{
		Name:        "set 'wal_sender_timeout' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_sender_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1620-show-wal_sender_timeout"},
			},
			{
				Query: "SET wal_sender_timeout TO '100000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1621-set-wal_sender_timeout-to-100000"},
			},
			{
				Query: "SHOW wal_sender_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1622-show-wal_sender_timeout"},
			},
			{
				Query: "SET wal_sender_timeout TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1623-set-wal_sender_timeout-to-default"},
			},
			{
				Query: "SHOW wal_sender_timeout", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1624-show-wal_sender_timeout"},
			},
			{
				Query: "SELECT current_setting('wal_sender_timeout')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1625-select-current_setting-wal_sender_timeout"},
			},
		},
	},
	{
		Name:        "set 'wal_skip_threshold' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_skip_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1626-show-wal_skip_threshold"},
			},
			{
				Query: "SET wal_skip_threshold TO '2000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1627-set-wal_skip_threshold-to-2000"},
			},
			{
				Query: "SHOW wal_skip_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1628-show-wal_skip_threshold"},
			},
			{
				Query: "SET wal_skip_threshold TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1629-set-wal_skip_threshold-to-default"},
			},
			{
				Query: "SHOW wal_skip_threshold", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1630-show-wal_skip_threshold"},
			},
			{
				Query: "SELECT current_setting('wal_skip_threshold')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1631-select-current_setting-wal_skip_threshold"},
			},
		},
	},
	{
		Name:        "set 'wal_sync_method' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_sync_method", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1632-show-wal_sync_method"},
			},
			{
				Query: "SET wal_sync_method TO 'open_datasync'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1633-set-wal_sync_method-to-open_datasync", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_sync_method')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1634-select-current_setting-wal_sync_method"},
			},
		},
	},
	{
		Name:        "set 'wal_writer_delay' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_writer_delay", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1635-show-wal_writer_delay"},
			},
			{
				Query: "SET wal_writer_delay TO '200'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1636-set-wal_writer_delay-to-200", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_writer_delay')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1637-select-current_setting-wal_writer_delay"},
			},
		},
	},
	{
		Name:        "set 'wal_writer_flush_after' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW wal_writer_flush_after", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1638-show-wal_writer_flush_after"},
			},
			{
				Query: "SET wal_writer_flush_after TO '1000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1639-set-wal_writer_flush_after-to-1000", Compare: "sqlstate"},
			},
			{
				Query: "SELECT current_setting('wal_writer_flush_after')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1640-select-current_setting-wal_writer_flush_after"},
			},
		},
	},
	{
		Name:        "set 'work_mem' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1641-show-work_mem"},
			},
			{
				Query: "SET work_mem TO '4000'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1642-set-work_mem-to-4000"},
			},
			{
				Query: "SHOW work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1643-show-work_mem"},
			},
			{
				Query: "SET work_mem TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1644-set-work_mem-to-default"},
			},
			{
				Query: "SHOW work_mem", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1645-show-work_mem"},
			},
			{
				Query: "SELECT current_setting('work_mem')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1646-select-current_setting-work_mem"},
			},
		},
	},
	{
		Name:        "set 'xmlbinary' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW xmlbinary", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1647-show-xmlbinary"},
			},
			{
				Query: "SET xmlbinary TO 'hex'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1648-set-xmlbinary-to-hex"},
			},
			{
				Query: "SHOW xmlbinary", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1649-show-xmlbinary"},
			},
			{
				Query: "SET xmlbinary TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1650-set-xmlbinary-to-default"},
			},
			{
				Query: "SHOW xmlbinary", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1651-show-xmlbinary"},
			},
			{
				Query: "SELECT current_setting('xmlbinary')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1652-select-current_setting-xmlbinary"},
			},
		},
	},
	{
		Name:        "set 'xmloption' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW xmloption", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1653-show-xmloption"},
			},
			{
				Query: "SET xmloption TO 'document'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1654-set-xmloption-to-document"},
			},
			{
				Query: "SHOW xmloption", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1655-show-xmloption"},
			},
			{
				Query: "SET xmloption TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1656-set-xmloption-to-default"},
			},
			{
				Query: "SHOW xmloption", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1657-show-xmloption"},
			},
			{
				Query: "SELECT current_setting('xmloption')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1658-select-current_setting-xmloption"},
			},
		},
	},
	{
		Name:        "set 'zero_damaged_pages' configuration variable",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW zero_damaged_pages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1659-show-zero_damaged_pages"},
			},
			{
				Query: "SET zero_damaged_pages TO 'on'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1660-set-zero_damaged_pages-to-on"},
			},
			{
				Query: "SHOW zero_damaged_pages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1661-show-zero_damaged_pages"},
			},
			{
				Query: "SET zero_damaged_pages TO DEFAULT", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1662-set-zero_damaged_pages-to-default"},
			},
			{
				Query: "SHOW zero_damaged_pages", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1663-show-zero_damaged_pages"},
			},
			{
				Query: "SELECT current_setting('zero_damaged_pages')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1664-select-current_setting-zero_damaged_pages"},
			},
		},
	},
	{
		Name: "settings with namespaces",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SET myvar.var_value TO 'value'", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1665-set-myvar.var_value-to-value"},
			},
			{
				Query: "SHOW myvar.var_value", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1666-show-myvar.var_value"},
			},
			{
				Query: "select current_setting('myvar.var_value')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1667-select-current_setting-myvar.var_value"},
			},
			{
				Query: "select current_setting('unknown_var')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1668-select-current_setting-unknown_var", Compare: "sqlstate"},
			},
			{
				Query: "show myvar.unknown_var", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1669-show-myvar.unknown_var", Compare: "sqlstate"},
			},
			{
				Query: "set myvar.var_value to (select 'a')", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1670-set-myvar.var_value-to-select-a", Compare: "sqlstate"},
			},
			{
				Query: "SHOW myvar.var_value", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1671-show-myvar.var_value"},
			},
			{
				Query: "set myvar.val2 to (select current_setting('myvar.var_value'))", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1672-set-myvar.val2-to-select-current_setting", Compare: "sqlstate"},
			},
			{
				Query: "SHOW myvar.val2", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1673-show-myvar.val2", Compare: "sqlstate"},
			},
		},
	},
	{
		Name: "set_config with is_local=true reverts at transaction end",
		Assertions: []ScriptTestAssertion{
			{
				// Pre-transaction baseline.
				Query: "SELECT current_setting('app.user_id', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1674-select-current_setting-app.user_id-true"},
			},
			{
				Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1675-begin"},
			},
			{
				Query: "SELECT set_config('app.user_id', '42', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1676-select-set_config-app.user_id-42-true"},
			},
			{
				// Visible inside the transaction.
				Query: "SELECT current_setting('app.user_id');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1677-select-current_setting-app.user_id"},
			},
			{
				Query: "COMMIT;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1678-commit"},
			},
			{
				// PostgreSQL semantics: SET LOCAL reverts on COMMIT.
				Query: "SELECT current_setting('app.user_id', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1679-select-current_setting-app.user_id-true"},
			},
		},
	},
	{
		Name: "set_config with is_local=true reverts on ROLLBACK",
		Assertions: []ScriptTestAssertion{
			{Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1680-begin"}},
			{
				Query: "SELECT set_config('app.tenant', 'rolling', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1681-select-set_config-app.tenant-rolling-true"},
			},
			{
				Query: "SELECT current_setting('app.tenant');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1682-select-current_setting-app.tenant"},
			},
			{Query: "ROLLBACK;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1683-rollback"}},
			{
				Query: "SELECT current_setting('app.tenant', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1684-select-current_setting-app.tenant-true"},
			},
		},
	},
	{
		Name: "set_config with is_local=true preserves a prior session-scope value",
		Assertions: []ScriptTestAssertion{
			{
				// Establish a session-scope baseline first.
				Query: "SELECT set_config('app.role', 'reader', false);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1685-select-set_config-app.role-reader-false"},
			},
			{Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{

				// Override transaction-locally; reader was the prior value.
				ID: "set-test-testsetstatements-1686-begin"}},
			{

				Query: "SELECT set_config('app.role', 'writer', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1687-select-set_config-app.role-writer-true"},
			},
			{
				Query: "SELECT current_setting('app.role');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1688-select-current_setting-app.role"},
			},
			{Query: "COMMIT;", PostgresOracle: ScriptTestPostgresOracle{

				// After COMMIT the original session value is restored.
				ID: "set-test-testsetstatements-1689-commit"}},
			{

				Query: "SELECT current_setting('app.role');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1690-select-current_setting-app.role"},
			},
		},
	},
	{
		Name: "set_config with is_local=true restores to first-seen value across multiple SET LOCALs",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT set_config('app.step', 'baseline', false);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1691-select-set_config-app.step-baseline-false"},
			},
			{Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1692-begin"}},
			{
				Query: "SELECT set_config('app.step', 'first', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1693-select-set_config-app.step-first-true"},
			},
			{
				Query: "SELECT set_config('app.step', 'second', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1694-select-set_config-app.step-second-true"},
			},
			{
				Query: "SELECT current_setting('app.step');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1695-select-current_setting-app.step"},
			},
			{Query: "COMMIT;", PostgresOracle: ScriptTestPostgresOracle{

				// Restored to baseline (the value before the transaction
				// started), not to "first" (an intermediate SET LOCAL).
				ID: "set-test-testsetstatements-1696-commit"}},
			{

				Query: "SELECT current_setting('app.step');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1697-select-current_setting-app.step"},
			},
		},
	},
	{
		Name: "set_config with is_local=true under autocommit reverts at statement end",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT set_config('app.transient', 'now', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1698-select-set_config-app.transient-now-true"},
			},
			{
				// In autocommit, the transaction ended with the previous
				// statement, so the variable should already be reverted.
				Query: "SELECT current_setting('app.transient', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1699-select-current_setting-app.transient-true"},
			},
		},
	},
	{
		// SET LOCAL is rewritten internally to a function call that
		// shares set_config's lifecycle bookkeeping. pgx returns no
		// rows for `SET LOCAL` so we skip the result check on those
		// statements; the lifecycle is still validated by the
		// surrounding current_setting calls.
		Name: "SET LOCAL var rolls back at COMMIT",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT set_config('app.local_session', 'baseline', false);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1700-select-set_config-app.local_session-baseline-false"},
			},
			{Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1701-begin"}},
			{
				Query:            "SET LOCAL app.local_session = 'inside';",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT current_setting('app.local_session');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1702-select-current_setting-app.local_session"},
			},
			{Query: "COMMIT;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1703-commit"}},
			{
				Query: "SELECT current_setting('app.local_session');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1704-select-current_setting-app.local_session"},
			},
		},
	},
	{
		Name: "SET LOCAL var rolls back at ROLLBACK",
		Assertions: []ScriptTestAssertion{
			{Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1705-begin"}},
			{
				Query:            "SET LOCAL app.audit_actor = 'test-user';",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT current_setting('app.audit_actor');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1706-select-current_setting-app.audit_actor"},
			},
			{Query: "ROLLBACK;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1707-rollback"}},
			{
				Query: "SELECT current_setting('app.audit_actor', true);", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1708-select-current_setting-app.audit_actor-true"},
			},
		},
	},
	{
		Name: "SET LOCAL system integer variable is stored as text",
		Assertions: []ScriptTestAssertion{
			{Query: "BEGIN;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1709-begin"}},
			{
				Query:            "SET LOCAL lock_timeout = 1000;",
				SkipResultsCheck: true,
			},
			{
				Query: "SELECT current_setting('lock_timeout');", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1710-select-current_setting-lock_timeout"},
			},
			{Query: "COMMIT;", PostgresOracle: ScriptTestPostgresOracle{ID: "set-test-testsetstatements-1711-commit"}},
		},
	},
}
