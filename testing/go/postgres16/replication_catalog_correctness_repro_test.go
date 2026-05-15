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

// TestReplicationStatCatalogsExposePgAttributeLsnMetadataRepro reproduces a
// replication catalog metadata bug: PostgreSQL exposes LSN-bearing
// replication-stat columns through pg_attribute with type pg_lsn.
func TestReplicationStatCatalogsExposePgAttributeLsnMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "replication stat catalogs expose pg_attribute pg_lsn metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `
						SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
						FROM pg_catalog.pg_attribute a
						JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
						JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
						WHERE n.nspname = 'pg_catalog'
							AND (
								(c.relname = 'pg_stat_subscription' AND a.attname IN ('received_lsn', 'latest_end_lsn'))
								OR (c.relname = 'pg_stat_wal_receiver' AND a.attname IN ('receive_start_lsn', 'written_lsn', 'flushed_lsn', 'latest_end_lsn'))
								OR (c.relname = 'pg_subscription_rel' AND a.attname = 'srsublsn')
								OR (c.relname = 'pg_replication_origin_status' AND a.attname IN ('remote_lsn', 'local_lsn'))
							)
						ORDER BY c.relname, a.attnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "replication-catalog-correctness-repro-test-testreplicationstatcatalogsexposepgattributelsnmetadatarepro-0001-select-c.relname-a.attname-format_type-a.atttypid"},
				},
			},
		},
	})
}
