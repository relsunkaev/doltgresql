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

import "testing"

// TestPublicationRejectsSchemaAddAfterColumnListOrFilterRepro pins PostgreSQL's
// logical-replication publication boundary: schema membership cannot be mixed
// into a publication that already has per-table column lists or row filters.
func TestPublicationRejectsSchemaAddAfterColumnListOrFilterRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "publication rejects schema add after column list and row filter",
			SetUpScript: []string{
				"CREATE TABLE pub_filter_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE SCHEMA pub_filter_aux;",
				"CREATE TABLE pub_filter_aux.schema_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE PUBLICATION pub_filter_pub FOR TABLE pub_filter_items (tenant_id) WHERE (tenant_id > 0);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER PUBLICATION pub_filter_pub ADD TABLES IN SCHEMA pub_filter_aux;", PostgresOracle: ScriptTestPostgresOracle{ID: "publication-oracle-repro-test-testpublicationrejectsschemaaddaftercolumnlistorfilterrepro-0001-alter-publication-pub_filter_pub-add-tables", Compare: "sqlstate"},
				},
			},
		},
	})
}
