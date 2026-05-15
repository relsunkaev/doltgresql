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
)

// TestDumpVersionIdentity pins the version-identity surface that
// pg_dump versions 16+ and 17+ inspect to decide which dialect of
// schema output to emit. Doltgres reports PostgreSQL 15 today;
// pg_dump 16/17 still target a 15-compatible schema for backwards
// compatibility, but their output may include keyword forms not
// understood by 15. Per the Dump/admin/tooling TODO in
// docs/app-compatibility-checklist.md.
func TestDumpVersionIdentity(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "version() string identifies as PostgreSQL major version",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// pg_dump's first probe is `SELECT version()`;
					// the major version drives the output dialect
					// branch.
					Query: `SELECT version() LIKE 'PostgreSQL %';`, PostgresOracle: ScriptTestPostgresOracle{ID: "dump-version-identity-probe-test-testdumpversionidentity-0001-select-version-like-postgresql-%"},
				},
			},
		},
		{
			Name:        "server_version GUC is queryable",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT length(current_setting('server_version'))::text > '0'
						AS has_version;`, PostgresOracle: ScriptTestPostgresOracle{ID: "dump-version-identity-probe-test-testdumpversionidentity-0002-select-length-current_setting-server_version-::text"},
				},
			},
		},
		{
			Name:        "server_version_num GUC is numeric and >= 90000",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// pg_dump branches on server_version_num:
					// values 90000 (9.0) and up handle different
					// dump dialect quirks.
					Query: `SELECT
						current_setting('server_version_num')::int >= 90000
						AS recent_enough;`, PostgresOracle: ScriptTestPostgresOracle{ID: "dump-version-identity-probe-test-testdumpversionidentity-0003-select-current_setting-server_version_num-::int->="},
				},
			},
		},
		{
			Name:        "pg_catalog-qualified version builtins resolve",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_catalog.version() LIKE 'PostgreSQL %',
						pg_catalog.current_setting('server_version') = current_setting('server_version');`, PostgresOracle: ScriptTestPostgresOracle{ID: "dump-version-identity-probe-test-testdumpversionidentity-0004-select-pg_catalog.version-like-postgresql-%"},
				},
			},
		},
	})
}
