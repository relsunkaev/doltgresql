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

// TestSimpleUpdatableViewInsertWritesBaseTableRepro reproduces an automatic
// updatable-view correctness bug: PostgreSQL forwards INSERT through a simple
// view to the underlying base table.
func TestSimpleUpdatableViewInsertWritesBaseTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "simple updatable view INSERT writes base table",
			SetUpScript: []string{
				`CREATE TABLE updatable_view_insert_base (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE VIEW updatable_view_insert_reader AS
					SELECT id, label FROM updatable_view_insert_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO updatable_view_insert_reader
						VALUES (1, 'inserted');`,
				},
				{
					Query: `SELECT id, label
						FROM updatable_view_insert_base;`, PostgresOracle: ScriptTestPostgresOracle{ID: "updatable-view-correctness-repro-test-testsimpleupdatableviewinsertwritesbasetablerepro-0001-select-id-label-from-updatable_view_insert_base"},
				},
			},
		},
	})
}

// TestSimpleUpdatableViewUpdateWritesBaseTableRepro reproduces an automatic
// updatable-view correctness bug: PostgreSQL forwards UPDATE through a simple
// view to the underlying base table.
func TestSimpleUpdatableViewUpdateWritesBaseTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "simple updatable view UPDATE writes base table",
			SetUpScript: []string{
				`CREATE TABLE updatable_view_update_base (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO updatable_view_update_base VALUES (1, 'old');`,
				`CREATE VIEW updatable_view_update_reader AS
					SELECT id, label FROM updatable_view_update_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE updatable_view_update_reader
						SET label = 'updated'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT id, label
						FROM updatable_view_update_base;`, PostgresOracle: ScriptTestPostgresOracle{ID: "updatable-view-correctness-repro-test-testsimpleupdatableviewupdatewritesbasetablerepro-0001-select-id-label-from-updatable_view_update_base"},
				},
			},
		},
	})
}

// TestSimpleUpdatableViewDeleteWritesBaseTableRepro reproduces an automatic
// updatable-view correctness bug: PostgreSQL forwards DELETE through a simple
// view to the underlying base table.
func TestSimpleUpdatableViewDeleteWritesBaseTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "simple updatable view DELETE writes base table",
			SetUpScript: []string{
				`CREATE TABLE updatable_view_delete_base (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO updatable_view_delete_base
					VALUES (1, 'delete me'), (2, 'keep me');`,
				`CREATE VIEW updatable_view_delete_reader AS
					SELECT id, label FROM updatable_view_delete_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM updatable_view_delete_reader
						WHERE id = 1;`,
				},
				{
					Query: `SELECT id, label
						FROM updatable_view_delete_base
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "updatable-view-correctness-repro-test-testsimpleupdatableviewdeletewritesbasetablerepro-0001-select-id-label-from-updatable_view_delete_base"},
				},
			},
		},
	})
}
