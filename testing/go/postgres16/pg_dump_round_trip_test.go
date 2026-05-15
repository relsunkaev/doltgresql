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

func TestPgDumpPartitionedTableOpclassProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_dump partitioned table opclass probe resolves regnamespace in scalar subquery",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT partrelid FROM pg_catalog.pg_partitioned_table WHERE
(SELECT c.oid FROM pg_catalog.pg_opclass c JOIN pg_catalog.pg_am a ON c.opcmethod = a.oid
WHERE opcname = 'enum_ops' AND opcnamespace = 'pg_catalog'::regnamespace AND amname = 'hash') = ANY(partclass);`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-round-trip-test-testpgdumppartitionedtableopclassprobe-0001-select-partrelid-from-pg_catalog.pg_partitioned_table-where"},
				},
			},
		},
	})
}

func TestPgDumpAlterColumnSetStorageProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_dump column storage metadata is accepted",
			SetUpScript: []string{
				`CREATE TABLE storage_probe (id INT PRIMARY KEY, payload TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE ONLY public.storage_probe ALTER COLUMN payload SET STORAGE EXTENDED;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-round-trip-test-testpgdumpaltercolumnsetstorageprobe-0001-alter-table-only-public.storage_probe-alter"},
				},
				{
					Query: `INSERT INTO storage_probe VALUES (1, 'ok');`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-round-trip-test-testpgdumpaltercolumnsetstorageprobe-0002-insert-into-storage_probe-values-1"},
				},
				{
					Query: `SELECT payload FROM storage_probe WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "pg-dump-round-trip-test-testpgdumpaltercolumnsetstorageprobe-0003-select-payload-from-storage_probe-where"},
				},
			},
		},
	})
}
