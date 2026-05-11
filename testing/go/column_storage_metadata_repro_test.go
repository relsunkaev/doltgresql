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

// TestAlterColumnSetStoragePersistsCatalogRepro reproduces a persistence bug:
// Doltgres accepts ALTER COLUMN SET STORAGE, but pg_attribute.attstorage is not
// updated.
func TestAlterColumnSetStoragePersistsCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN SET STORAGE updates pg_attribute attstorage",
			SetUpScript: []string{
				`CREATE TABLE storage_catalog_target (id INT PRIMARY KEY, payload TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE storage_catalog_target ALTER COLUMN payload SET STORAGE EXTERNAL;`,
				},
				{
					Query: `SELECT attstorage
						FROM pg_attribute
						WHERE attrelid = 'storage_catalog_target'::regclass
							AND attname = 'payload';`,
					Expected: []sql.Row{{"e"}},
				},
			},
		},
	})
}

// TestAlterColumnSetCompressionPersistsCatalogRepro reproduces a persistence
// bug: Doltgres accepts ALTER COLUMN SET COMPRESSION, but
// pg_attribute.attcompression is not updated.
func TestAlterColumnSetCompressionPersistsCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN SET COMPRESSION updates pg_attribute attcompression",
			SetUpScript: []string{
				`CREATE TABLE compression_catalog_target (id INT PRIMARY KEY, payload TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE compression_catalog_target ALTER COLUMN payload SET COMPRESSION pglz;`,
				},
				{
					Query: `SELECT attcompression
						FROM pg_attribute
						WHERE attrelid = 'compression_catalog_target'::regclass
							AND attname = 'payload';`,
					Expected: []sql.Row{{"p"}},
				},
			},
		},
	})
}

// TestAlterColumnSetStatisticsPersistsCatalogRepro reproduces a persistence
// bug: Doltgres accepts ALTER COLUMN SET STATISTICS but does not update the
// pg_attribute statistics target.
func TestAlterColumnSetStatisticsPersistsCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN SET STATISTICS updates pg_attribute attstattarget",
			SetUpScript: []string{
				`CREATE TABLE statistics_catalog_target (id INT PRIMARY KEY, payload TEXT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE statistics_catalog_target ALTER COLUMN payload SET STATISTICS 42;`,
				},
				{
					Query: `SELECT attstattarget
						FROM pg_attribute
						WHERE attrelid = 'statistics_catalog_target'::regclass
							AND attname = 'payload';`,
					Expected: []sql.Row{{int16(42)}},
				},
			},
		},
	})
}
