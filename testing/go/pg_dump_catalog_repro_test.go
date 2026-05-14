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
)

// TestPgDumpCatalogCompatibilityProbe guards pg_dump catalog probes that expect
// PostgreSQL-compatible OID types, built-in OID ranges, and qualified helpers.
func TestPgDumpCatalogCompatibilityProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_dump pg_proc transform function OID predicates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) >= 0
						FROM pg_catalog.pg_proc p
						WHERE EXISTS (
							SELECT 1
							FROM pg_catalog.pg_transform
							WHERE pg_transform.oid > 16383
								AND (p.oid = pg_transform.trffromsql
									OR p.oid = pg_transform.trftosql)
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0001-select-count-*->=-0"},
				},
				{
					Query: `SELECT count(*) >= 0
						FROM pg_catalog.pg_ts_parser
						WHERE prsstart::oid IS NOT NULL
							AND prstoken::oid IS NOT NULL
							AND prsend::oid IS NOT NULL
							AND prsheadline::oid IS NOT NULL
							AND prslextype::oid IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0002-select-count-*->=-0"},
				},
				{
					Query: `SELECT count(*) >= 0
						FROM pg_catalog.pg_ts_template
						WHERE tmplinit::oid IS NOT NULL
							AND tmpllexize::oid IS NOT NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0003-select-count-*->=-0"},
				},
				{
					Query: `SELECT NOT EXISTS (
							SELECT 1
							FROM pg_catalog.pg_proc p
							LEFT JOIN pg_catalog.pg_roles r ON r.oid = p.proowner
							WHERE r.oid IS NULL
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0004-select-not-exists-select-1"},
				},
				{
					Query: `SELECT NOT EXISTS (
							SELECT 1
							FROM pg_catalog.pg_cast
							WHERE oid > 16383
								AND castsource = 'integer'::regtype
								AND casttarget = 'bigint'::regtype
								AND castfunc = 481::oid
						);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0005-select-not-exists-select-1"},
				},
				{
					Query: `SELECT option_name, option_value
						FROM pg_catalog.pg_options_to_table(ARRAY['fillfactor=90', 'parallel_workers']::text[])
						ORDER BY option_name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0006-select-option_name-option_value-from-pg_catalog.pg_options_to_table"},
				},
				{
					Query: `SELECT pg_catalog.array_agg(v ORDER BY v)
						FROM (VALUES (2), (1)) AS src(v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-catalog-repro-test-testpgdumpcatalogcompatibilityprobe-0007-select-pg_catalog.array_agg-v-order-by"},
				},
			},
		},
	})
}
