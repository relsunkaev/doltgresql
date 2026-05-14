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

package postgres18

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestPostgres18PgAiosCatalogShapeRepro reproduces a PostgreSQL 18
// monitoring-catalog compatibility gap: pg_aios should expose active
// asynchronous I/O handles as a readable pg_catalog system view.
func TestPostgres18PgAiosCatalogShapeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_aios exposes PostgreSQL 18 columns",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT count(*) = 15
						FROM pg_catalog.pg_attribute
						WHERE attrelid = 'pg_catalog.pg_aios'::regclass
							AND NOT attisdropped
							AND attname IN (
								'pid', 'io_id', 'io_generation', 'state', 'operation',
								'off', 'length', 'target', 'handle_data_len', 'raw_result',
								'result', 'target_desc', 'f_sync', 'f_localmem', 'f_buffered'
							);`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/catalog-correctness-repro-test-testpostgres18pgaioscatalogshaperepro-0001-select-count-*-=-15"},
				},
				{
					Query: `SELECT count(*) >= 0
						FROM pg_catalog.pg_aios;`, PostgresOracle: ScriptTestPostgresOracle{ID: "postgres18/catalog-correctness-repro-test-testpostgres18pgaioscatalogshaperepro-0002-select-count-*->=-0"},
				},
			},
		},
	})
}
