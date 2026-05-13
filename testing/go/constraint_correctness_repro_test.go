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
	"github.com/stretchr/testify/require"
)

// TestPgGetConstraintdefCheckOmitsConstraintNameRepro reproduces a catalog
// correctness bug: pg_get_constraintdef() returns the constraint definition,
// not an ADD CONSTRAINT fragment that includes the constraint name.
func TestPgGetConstraintdefCheckOmitsConstraintNameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_constraintdef omits CHECK constraint name",
			SetUpScript: []string{
				`CREATE TABLE constraintdef_check_items (
					id INT PRIMARY KEY,
					amount INT CONSTRAINT amount_positive CHECK (amount > 0)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_constraintdef(oid)
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'constraintdef_check_items'::regclass
							AND conname = 'amount_positive';`,
					Expected: []sql.Row{{"CHECK ((amount > 0))"}},
				},
			},
		},
	})
}

// TestPgGetConstraintdefForeignKeyActionsRepro reproduces a schema metadata
// bug: pg_get_constraintdef() must preserve referential actions so clients can
// reconstruct the foreign key.
func TestPgGetConstraintdefForeignKeyActionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_constraintdef preserves foreign key actions",
			SetUpScript: []string{
				`CREATE TABLE constraintdef_fk_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE constraintdef_fk_child (
					id INT PRIMARY KEY,
					parent_id INT,
					CONSTRAINT constraintdef_fk_child_parent_fkey
						FOREIGN KEY (parent_id)
						REFERENCES constraintdef_fk_parent(id)
						ON UPDATE CASCADE
						ON DELETE SET NULL
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_constraintdef(oid)
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'constraintdef_fk_child'::regclass
							AND conname = 'constraintdef_fk_child_parent_fkey';`,
					Expected: []sql.Row{{
						"FOREIGN KEY (parent_id) REFERENCES constraintdef_fk_parent(id) ON UPDATE CASCADE ON DELETE SET NULL",
					}},
				},
			},
		},
	})
}

// TestPgGetConstraintdefQuotesColumnNamesRepro reproduces a catalog
// correctness bug: pg_get_constraintdef() must quote constrained column names
// that require identifier quoting.
func TestPgGetConstraintdefQuotesColumnNamesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_constraintdef quotes constrained column names",
			SetUpScript: []string{
				`CREATE TABLE constraintdef_quote_items (
					"CaseColumn" INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT pg_get_constraintdef(oid)
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'constraintdef_quote_items'::regclass
							AND conname = 'constraintdef_quote_items_pkey';`,
					Expected: []sql.Row{{`PRIMARY KEY ("CaseColumn")`}},
				},
			},
		},
	})
}

// TestUniqueConstraintIncludeColumnsRepro reproduces a schema correctness bug:
// PostgreSQL accepts UNIQUE constraints with INCLUDE columns and enforces
// uniqueness only on the key columns.
func TestUniqueConstraintIncludeColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UNIQUE constraint INCLUDE columns are accepted",
			SetUpScript: []string{
				`CREATE TABLE unique_constraint_include_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					label TEXT NOT NULL,
					UNIQUE (code) INCLUDE (label)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO unique_constraint_include_items
						VALUES (1, 10, 'first');`,
				},
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'unique_constraint_include_items'
							AND indexname <> 'unique_constraint_include_items_pkey';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX unique_constraint_include_items_code_label_key ON public.unique_constraint_include_items USING btree (code) INCLUDE (label)",
					}},
				},
			},
		},
	})
}

// TestUniqueConstraintStorageParamsRepro reproduces a schema/catalog
// correctness bug: PostgreSQL accepts index storage parameters on UNIQUE
// constraints and preserves them in pg_indexes.
func TestUniqueConstraintStorageParamsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UNIQUE constraint storage parameters are accepted",
			SetUpScript: []string{
				`CREATE TABLE unique_constraint_storage_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					UNIQUE (code) WITH (fillfactor=70)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'unique_constraint_storage_items'
							AND indexname <> 'unique_constraint_storage_items_pkey';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX unique_constraint_storage_items_code_key ON public.unique_constraint_storage_items USING btree (code) WITH (fillfactor='70')",
					}},
				},
			},
		},
	})
}

// TestUniqueConstraintDefaultTablespaceRepro reproduces a schema correctness
// bug: PostgreSQL accepts USING INDEX TABLESPACE pg_default on UNIQUE
// constraints.
func TestUniqueConstraintDefaultTablespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UNIQUE constraint default index tablespace is accepted",
			SetUpScript: []string{
				`CREATE TABLE unique_constraint_tablespace_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					UNIQUE (code) USING INDEX TABLESPACE pg_default
				);`,
				`INSERT INTO unique_constraint_tablespace_items VALUES (1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT code FROM unique_constraint_tablespace_items;`,
					Expected: []sql.Row{{10}},
				},
			},
		},
	})
}

// TestPrimaryKeyConstraintIncludeColumnsRepro reproduces a schema correctness
// bug: PostgreSQL accepts INCLUDE columns on PRIMARY KEY constraints and
// preserves them in pg_indexes.
func TestPrimaryKeyConstraintIncludeColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PRIMARY KEY constraint INCLUDE columns are accepted",
			SetUpScript: []string{
				`CREATE TABLE primary_key_constraint_include_items (
					id INT,
					label TEXT NOT NULL,
					PRIMARY KEY (id) INCLUDE (label)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'primary_key_constraint_include_items';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX primary_key_constraint_include_items_pkey ON public.primary_key_constraint_include_items USING btree (id) INCLUDE (label)",
					}},
				},
			},
		},
	})
}

// TestPrimaryKeyConstraintStorageParamsRepro reproduces a schema/catalog
// correctness bug: PostgreSQL accepts index storage parameters on PRIMARY KEY
// constraints and preserves them in pg_indexes.
func TestPrimaryKeyConstraintStorageParamsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PRIMARY KEY constraint storage parameters are accepted",
			SetUpScript: []string{
				`CREATE TABLE primary_key_constraint_storage_items (
					id INT,
					PRIMARY KEY (id) WITH (fillfactor=70)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'primary_key_constraint_storage_items';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX primary_key_constraint_storage_items_pkey ON public.primary_key_constraint_storage_items USING btree (id) WITH (fillfactor='70')",
					}},
				},
			},
		},
	})
}

// TestPrimaryKeyConstraintDefaultTablespaceRepro reproduces a schema
// correctness bug: PostgreSQL accepts USING INDEX TABLESPACE pg_default on
// PRIMARY KEY constraints.
func TestPrimaryKeyConstraintDefaultTablespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "PRIMARY KEY constraint default index tablespace is accepted",
			SetUpScript: []string{
				`CREATE TABLE primary_key_constraint_tablespace_items (
					id INT,
					PRIMARY KEY (id) USING INDEX TABLESPACE pg_default
				);`,
				`INSERT INTO primary_key_constraint_tablespace_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id FROM primary_key_constraint_tablespace_items;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestTypedTableUniqueConstraintIncludeColumnsRepro reproduces a typed-table
// schema correctness bug: PostgreSQL accepts INCLUDE columns on UNIQUE
// constraints in CREATE TABLE OF definitions.
func TestTypedTableUniqueConstraintIncludeColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF UNIQUE constraint INCLUDE columns are accepted",
			SetUpScript: []string{
				`CREATE TYPE typed_unique_include_options AS (
					id INT,
					code INT,
					label TEXT
				);`,
				`CREATE TABLE typed_unique_include_items OF typed_unique_include_options (
					UNIQUE (code) INCLUDE (id)
				);`,
				`INSERT INTO typed_unique_include_items VALUES (1, 10, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'typed_unique_include_items';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX typed_unique_include_items_code_id_key ON public.typed_unique_include_items USING btree (code) INCLUDE (id)",
					}},
				},
			},
		},
	})
}

// TestTypedTablePrimaryKeyConstraintIncludeColumnsRepro reproduces a typed-table
// schema correctness bug: PostgreSQL accepts INCLUDE columns on PRIMARY KEY
// constraints in CREATE TABLE OF definitions.
func TestTypedTablePrimaryKeyConstraintIncludeColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF PRIMARY KEY constraint INCLUDE columns are accepted",
			SetUpScript: []string{
				`CREATE TYPE typed_primary_key_include_options AS (
					id INT,
					code INT,
					label TEXT
				);`,
				`CREATE TABLE typed_primary_key_include_items OF typed_primary_key_include_options (
					PRIMARY KEY (id) INCLUDE (label)
				);`,
				`INSERT INTO typed_primary_key_include_items VALUES (1, 10, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'typed_primary_key_include_items';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX typed_primary_key_include_items_pkey ON public.typed_primary_key_include_items USING btree (id) INCLUDE (label)",
					}},
				},
			},
		},
	})
}

// TestTypedTableUniqueConstraintStorageParamsRepro reproduces a typed-table
// schema/catalog correctness bug: PostgreSQL accepts index storage parameters
// on UNIQUE constraints in CREATE TABLE OF definitions.
func TestTypedTableUniqueConstraintStorageParamsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF UNIQUE constraint storage parameters are accepted",
			SetUpScript: []string{
				`CREATE TYPE typed_unique_storage_options AS (
					id INT,
					code INT,
					label TEXT
				);`,
				`CREATE TABLE typed_unique_storage_items OF typed_unique_storage_options (
					UNIQUE (code) WITH (fillfactor=70)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'typed_unique_storage_items';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX typed_unique_storage_items_code_key ON public.typed_unique_storage_items USING btree (code) WITH (fillfactor='70')",
					}},
				},
			},
		},
	})
}

// TestTypedTablePrimaryKeyConstraintStorageParamsRepro reproduces a typed-table
// schema/catalog correctness bug: PostgreSQL accepts index storage parameters
// on PRIMARY KEY constraints in CREATE TABLE OF definitions.
func TestTypedTablePrimaryKeyConstraintStorageParamsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF PRIMARY KEY constraint storage parameters are accepted",
			SetUpScript: []string{
				`CREATE TYPE typed_primary_key_storage_options AS (
					id INT,
					code INT,
					label TEXT
				);`,
				`CREATE TABLE typed_primary_key_storage_items OF typed_primary_key_storage_options (
					PRIMARY KEY (id) WITH (fillfactor=70)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT indexdef
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'typed_primary_key_storage_items';`,
					Expected: []sql.Row{{
						"CREATE UNIQUE INDEX typed_primary_key_storage_items_pkey ON public.typed_primary_key_storage_items USING btree (id) WITH (fillfactor='70')",
					}},
				},
			},
		},
	})
}

// TestTypedTableUniqueConstraintDefaultTablespaceRepro reproduces a typed-table
// schema correctness bug: PostgreSQL accepts USING INDEX TABLESPACE pg_default
// on UNIQUE constraints in CREATE TABLE OF definitions.
func TestTypedTableUniqueConstraintDefaultTablespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF UNIQUE constraint default index tablespace is accepted",
			SetUpScript: []string{
				`CREATE TYPE typed_unique_tablespace_options AS (
					id INT,
					code INT,
					label TEXT
				);`,
				`CREATE TABLE typed_unique_tablespace_items OF typed_unique_tablespace_options (
					UNIQUE (code) USING INDEX TABLESPACE pg_default
				);`,
				`INSERT INTO typed_unique_tablespace_items VALUES (1, 10, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT code FROM typed_unique_tablespace_items;`,
					Expected: []sql.Row{{10}},
				},
			},
		},
	})
}

// TestTypedTablePrimaryKeyConstraintDefaultTablespaceRepro reproduces a
// typed-table schema correctness bug: PostgreSQL accepts USING INDEX TABLESPACE
// pg_default on PRIMARY KEY constraints in CREATE TABLE OF definitions.
func TestTypedTablePrimaryKeyConstraintDefaultTablespaceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF PRIMARY KEY constraint default index tablespace is accepted",
			SetUpScript: []string{
				`CREATE TYPE typed_primary_key_tablespace_options AS (
					id INT,
					code INT,
					label TEXT
				);`,
				`CREATE TABLE typed_primary_key_tablespace_items OF typed_primary_key_tablespace_options (
					PRIMARY KEY (id) USING INDEX TABLESPACE pg_default
				);`,
				`INSERT INTO typed_primary_key_tablespace_items VALUES (1, 10, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id FROM typed_primary_key_tablespace_items;`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestDeferrableUniqueConstraintCanBeFixedBeforeCommitRepro reproduces a
// correctness bug: DEFERRABLE INITIALLY DEFERRED unique constraints are
// accepted but still checked immediately instead of at COMMIT.
func TestDeferrableUniqueConstraintCanBeFixedBeforeCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "deferrable unique violation can be repaired before commit",
			SetUpScript: []string{
				`CREATE TABLE deferred_unique_items (
					id INT PRIMARY KEY,
					code INT UNIQUE DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO deferred_unique_items VALUES (1, 10), (2, 10);`,
				},
				{
					Query: `UPDATE deferred_unique_items SET code = 20 WHERE id = 2;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, code FROM deferred_unique_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10},
						{2, 20},
					},
				},
			},
		},
	})
}

// TestDeferrablePrimaryKeyConstraintCanBeFixedBeforeCommitRepro reproduces a
// correctness bug: DEFERRABLE INITIALLY DEFERRED primary-key constraints are
// accepted but still checked immediately instead of at COMMIT.
func TestDeferrablePrimaryKeyConstraintCanBeFixedBeforeCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "deferrable primary key violation can be repaired before commit",
			SetUpScript: []string{
				`CREATE TABLE deferred_primary_key_items (
					id INT,
					label TEXT,
					CONSTRAINT deferred_primary_key_items_pkey
						PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO deferred_primary_key_items VALUES
						(1, 'first'),
						(1, 'second');`,
				},
				{
					Query: `UPDATE deferred_primary_key_items SET id = 2 WHERE label = 'second';`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, label
						FROM deferred_primary_key_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, "first"},
						{2, "second"},
					},
				},
			},
		},
	})
}

// TestSetConstraintsDefersUniqueConstraintRepro reproduces a correctness bug:
// DEFERRABLE INITIALLY IMMEDIATE unique constraints are accepted, but SET
// CONSTRAINTS does not defer unique validation until COMMIT.
func TestSetConstraintsDefersUniqueConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET CONSTRAINTS defers unique validation",
			SetUpScript: []string{
				`CREATE TABLE set_constraints_unique_items (
					id INT PRIMARY KEY,
					code INT UNIQUE DEFERRABLE INITIALLY IMMEDIATE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SET CONSTRAINTS ALL DEFERRED;`,
				},
				{
					Query: `INSERT INTO set_constraints_unique_items VALUES (1, 10), (2, 10);`,
				},
				{
					Query: `UPDATE set_constraints_unique_items SET code = 20 WHERE id = 2;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT id, code FROM set_constraints_unique_items ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10},
						{2, 20},
					},
				},
			},
		},
	})
}

// TestDeferrableForeignKeyCanBeFixedBeforeCommitRepro guards DEFERRABLE
// INITIALLY DEFERRED foreign keys repaired before COMMIT.
func TestDeferrableForeignKeyCanBeFixedBeforeCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "deferrable foreign key violation can be repaired before commit",
			SetUpScript: []string{
				`CREATE TABLE deferred_fk_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE deferred_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES deferred_fk_parents(id) DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO deferred_fk_children VALUES (1, 42);`,
				},
				{
					Query: `INSERT INTO deferred_fk_parents VALUES (42);`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT id, parent_id FROM deferred_fk_children;`,
					Expected: []sql.Row{{1, 42}},
				},
			},
		},
	})
}

// TestSetConstraintsImmediateChecksDeferredForeignKeyRepro guards that SET
// CONSTRAINTS IMMEDIATE checks outstanding deferred foreign-key violations at
// the SET CONSTRAINTS statement.
func TestSetConstraintsImmediateChecksDeferredForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET CONSTRAINTS IMMEDIATE checks deferred foreign key violations",
			SetUpScript: []string{
				`CREATE TABLE immediate_check_fk_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE immediate_check_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES immediate_check_fk_parents(id) DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO immediate_check_fk_children VALUES (1, 42);`,
				},
				{
					Query:       `SET CONSTRAINTS ALL IMMEDIATE;`,
					ExpectedErr: `Foreign key violation`,
				},
			},
		},
	})
}

// TestSetConstraintsDefersInitiallyImmediateForeignKeyRepro guards that SET
// CONSTRAINTS DEFERRED allows an initially immediate deferrable foreign-key
// violation to be repaired before commit.
func TestSetConstraintsDefersInitiallyImmediateForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET CONSTRAINTS DEFERRED defers initially immediate foreign key",
			SetUpScript: []string{
				`CREATE TABLE deferred_immediate_fk_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE deferred_immediate_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES deferred_immediate_fk_parents(id) DEFERRABLE INITIALLY IMMEDIATE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SET CONSTRAINTS ALL DEFERRED;`,
				},
				{
					Query: `INSERT INTO deferred_immediate_fk_children VALUES (1, 42);`,
				},
				{
					Query: `INSERT INTO deferred_immediate_fk_parents VALUES (42);`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT id, parent_id FROM deferred_immediate_fk_children;`,
					Expected: []sql.Row{{1, 42}},
				},
			},
		},
	})
}

// TestRollbackToSavepointClearsDeferredForeignKeyViolationRepro guards that
// rolling back to a savepoint also clears deferred foreign-key violations
// introduced after that savepoint.
func TestRollbackToSavepointClearsDeferredForeignKeyViolationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ROLLBACK TO SAVEPOINT clears deferred foreign key violation",
			SetUpScript: []string{
				`CREATE TABLE deferred_savepoint_fk_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE deferred_savepoint_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES deferred_savepoint_fk_parents(id) DEFERRABLE INITIALLY DEFERRED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT deferred_sp;`,
				},
				{
					Query: `INSERT INTO deferred_savepoint_fk_children VALUES (1, 999);`,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT deferred_sp;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT COUNT(*) FROM deferred_savepoint_fk_children;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterForeignKeyConstraintDeferrabilityRepro reproduces a constraint
// timing correctness bug: PostgreSQL allows ALTER CONSTRAINT to change a
// foreign key's deferrability and initial timing.
func TestAlterForeignKeyConstraintDeferrabilityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER CONSTRAINT changes foreign key deferrability",
			SetUpScript: []string{
				`CREATE TABLE alter_fk_deferrability_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE alter_fk_deferrability_children (
					id INT PRIMARY KEY,
					parent_id INT,
					CONSTRAINT alter_fk_deferrability_children_parent_fk
						FOREIGN KEY (parent_id)
						REFERENCES alter_fk_deferrability_parents(id)
						NOT DEFERRABLE
				);`,
				`ALTER TABLE alter_fk_deferrability_children
					ALTER CONSTRAINT alter_fk_deferrability_children_parent_fk
					DEFERRABLE INITIALLY DEFERRED;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `INSERT INTO alter_fk_deferrability_children VALUES (1, 42);`,
				},
				{
					Query: `INSERT INTO alter_fk_deferrability_parents VALUES (42);`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT id, parent_id FROM alter_fk_deferrability_children;`,
					Expected: []sql.Row{{1, 42}},
				},
			},
		},
	})
}

// TestDeferredForeignKeyParentDeleteCanBeRepairedBeforeCommitRepro guards that
// a deferred NO ACTION foreign key allows a parent delete to be repaired by
// deleting the child row before commit.
func TestDeferredForeignKeyParentDeleteCanBeRepairedBeforeCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "deferred foreign key parent delete can be repaired before commit",
			SetUpScript: []string{
				`CREATE TABLE deferred_delete_fk_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE deferred_delete_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES deferred_delete_fk_parents(id) DEFERRABLE INITIALLY DEFERRED
				);`,
				`INSERT INTO deferred_delete_fk_parents VALUES (1);`,
				`INSERT INTO deferred_delete_fk_children VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DELETE FROM deferred_delete_fk_parents WHERE id = 1;`,
				},
				{
					Query: `DELETE FROM deferred_delete_fk_children WHERE id = 1;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT COUNT(*) FROM deferred_delete_fk_parents;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT COUNT(*) FROM deferred_delete_fk_children;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestDeferredForeignKeyParentUpdateCanBeRepairedBeforeCommitRepro guards that
// a deferred NO ACTION foreign key allows a parent key update to be repaired by
// updating the child row before commit.
func TestDeferredForeignKeyParentUpdateCanBeRepairedBeforeCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "deferred foreign key parent update can be repaired before commit",
			SetUpScript: []string{
				`CREATE TABLE deferred_update_fk_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE deferred_update_fk_children (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES deferred_update_fk_parents(id) DEFERRABLE INITIALLY DEFERRED
				);`,
				`INSERT INTO deferred_update_fk_parents VALUES (1);`,
				`INSERT INTO deferred_update_fk_children VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `UPDATE deferred_update_fk_parents SET id = 2 WHERE id = 1;`,
				},
				{
					Query: `UPDATE deferred_update_fk_children SET parent_id = 2 WHERE id = 1;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT id FROM deferred_update_fk_parents;`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT parent_id FROM deferred_update_fk_children;`,
					Expected: []sql.Row{{2}},
				},
			},
		},
	})
}

// TestRenameColumnUsedByCheckConstraintKeepsConstraintUsableRepro guards CHECK
// constraint expression rewrites when a referenced column is renamed.
func TestRenameColumnUsedByCheckConstraintKeepsConstraintUsableRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE check_rename_items (
		id INT PRIMARY KEY,
		amount INT,
		amount_total INT,
		CONSTRAINT amount_positive CHECK (amount > 0),
		CONSTRAINT amount_total_gt_amount CHECK (amount_total > amount)
	);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `ALTER TABLE check_rename_items RENAME COLUMN amount TO value;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO check_rename_items VALUES (1, 10, 15);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO check_rename_items VALUES (2, -1, 15);`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `Check constraint "amount_positive" violated`)

	_, err = conn.Current.Exec(ctx, `INSERT INTO check_rename_items VALUES (3, 10, 5);`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `Check constraint "amount_total_gt_amount" violated`)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT COUNT(*) FROM check_rename_items WHERE id = 1 AND value = 10 AND amount_total = 15;`).Scan(&count))
	require.Equal(t, int64(1), count)
}

// TestAddCheckConstraintValidatesExistingRowsRepro guards data consistency:
// adding a CHECK constraint should validate existing rows before accepting the
// new constraint.
func TestAddCheckConstraintValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD CHECK validates existing rows",
			SetUpScript: []string{
				`CREATE TABLE check_existing_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`INSERT INTO check_existing_items VALUES (1, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE check_existing_items
						ADD CONSTRAINT amount_positive CHECK (amount > 0);`,
					ExpectedErr: `Check constraint`,
				},
			},
		},
	})
}

// TestAddForeignKeyConstraintValidatesExistingRowsRepro guards data
// consistency: adding a foreign key should validate existing rows before
// accepting the new constraint.
func TestAddForeignKeyConstraintValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD FOREIGN KEY validates existing rows",
			SetUpScript: []string{
				`CREATE TABLE fk_existing_parents (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_existing_children (
					id INT PRIMARY KEY,
					parent_id INT
				);`,
				`INSERT INTO fk_existing_children VALUES (1, 42);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE fk_existing_children
						ADD CONSTRAINT fk_existing_parent_fk
						FOREIGN KEY (parent_id) REFERENCES fk_existing_parents(id);`,
					ExpectedErr: `Foreign key violation`,
				},
			},
		},
	})
}

// TestAddUniqueConstraintValidatesExistingRowsRepro guards data consistency:
// adding a unique constraint should reject existing duplicate values.
func TestAddUniqueConstraintValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD UNIQUE validates existing rows",
			SetUpScript: []string{
				`CREATE TABLE unique_existing_items (
					id INT PRIMARY KEY,
					code INT
				);`,
				`INSERT INTO unique_existing_items VALUES (1, 10), (2, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE unique_existing_items
						ADD CONSTRAINT unique_existing_code_key UNIQUE (code);`,
					ExpectedErr: `duplicate`,
				},
			},
		},
	})
}

// TestUniqueConstraintNullsNotDistinctRejectsDuplicateNullsRepro guards
// PostgreSQL UNIQUE constraints declared NULLS NOT DISTINCT treating null key
// values as equal for uniqueness.
func TestUniqueConstraintNullsNotDistinctRejectsDuplicateNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unique constraint NULLS NOT DISTINCT rejects duplicate NULLs",
			SetUpScript: []string{
				`CREATE TABLE unique_constraint_nulls_not_distinct_items (
					id INT PRIMARY KEY,
					code INT,
					CONSTRAINT unique_constraint_nulls_not_distinct_code_key
						UNIQUE NULLS NOT DISTINCT (code)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO unique_constraint_nulls_not_distinct_items VALUES (1, NULL);`,
				},
				{
					Query:       `INSERT INTO unique_constraint_nulls_not_distinct_items VALUES (2, NULL);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code
						FROM unique_constraint_nulls_not_distinct_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, nil}},
				},
			},
		},
	})
}

// TestAddUniqueConstraintNullsNotDistinctRejectsExistingDuplicateNullsRepro
// reproduces a constraint integrity bug: PostgreSQL validates existing rows
// when adding a UNIQUE NULLS NOT DISTINCT constraint.
func TestAddUniqueConstraintNullsNotDistinctRejectsExistingDuplicateNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ADD CONSTRAINT UNIQUE NULLS NOT DISTINCT rejects existing duplicate NULLs",
			SetUpScript: []string{
				`CREATE TABLE add_nulls_not_distinct_existing_items (
					id INT PRIMARY KEY,
					code INT
				);`,
				`INSERT INTO add_nulls_not_distinct_existing_items VALUES
					(1, NULL),
					(2, NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_nulls_not_distinct_existing_items
						ADD CONSTRAINT add_nulls_not_distinct_existing_code_key
						UNIQUE NULLS NOT DISTINCT (code);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.table_constraints
						WHERE table_name = 'add_nulls_not_distinct_existing_items'
							AND constraint_name = 'add_nulls_not_distinct_existing_code_key';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAddPrimaryKeyRejectsExistingNullsGuard guards that existing NULL values
// are rejected before adding a primary key.
func TestAddPrimaryKeyRejectsExistingNullsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ADD PRIMARY KEY rejects existing NULLs",
			SetUpScript: []string{
				`CREATE TABLE add_primary_key_existing_null_items (
					id INT,
					label TEXT
				);`,
				`INSERT INTO add_primary_key_existing_null_items VALUES (NULL, 'bad');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_primary_key_existing_null_items
						ADD CONSTRAINT add_primary_key_existing_null_items_pkey
						PRIMARY KEY (id);`,
					ExpectedErr: `null`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.table_constraints
						WHERE table_name = 'add_primary_key_existing_null_items'
							AND constraint_type = 'PRIMARY KEY';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAddPrimaryKeyRejectsExistingDuplicatesRepro reproduces a constraint
// integrity bug: PostgreSQL validates existing rows before adding a primary
// key, because primary-key values must be unique.
func TestAddPrimaryKeyRejectsExistingDuplicatesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ADD PRIMARY KEY rejects existing duplicates",
			SetUpScript: []string{
				`CREATE TABLE add_primary_key_existing_duplicate_items (
					id INT,
					label TEXT
				);`,
				`INSERT INTO add_primary_key_existing_duplicate_items VALUES
					(1, 'a'),
					(1, 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_primary_key_existing_duplicate_items
						ADD CONSTRAINT add_primary_key_existing_duplicate_items_pkey
						PRIMARY KEY (id);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.table_constraints
						WHERE table_name = 'add_primary_key_existing_duplicate_items'
							AND constraint_type = 'PRIMARY KEY';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestAddNotNullColumnValidatesExistingRowsRepro reproduces a data consistency
// bug: adding a NOT NULL column without a default to a non-empty table must
// reject the rewrite instead of persisting nulls in the new constrained column.
func TestAddNotNullColumnValidatesExistingRowsRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE add_not_null_existing_items (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO add_not_null_existing_items VALUES (1);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `ALTER TABLE add_not_null_existing_items
		ADD COLUMN required_value INT NOT NULL;`)
	if err == nil {
		var requiredValue string
		require.NoError(t, conn.Current.QueryRow(ctx, `SELECT required_value::text
			FROM add_not_null_existing_items
			WHERE id = 1;`).Scan(&requiredValue))
		t.Fatalf("expected ADD COLUMN NOT NULL to reject existing rows; accepted rewrite synthesized required_value=%s", requiredValue)
	}
	require.Contains(t, err.Error(), `contains null values`)
}

// TestAddUniqueNotNullColumnValidatesExistingRowsRepro guards that adding a
// UNIQUE NOT NULL column to a non-empty table validates the rewritten rows
// before accepting the new invariant.
func TestAddUniqueNotNullColumnValidatesExistingRowsRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE TABLE add_unique_not_null_existing_items (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO add_unique_not_null_existing_items VALUES (1), (2);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `ALTER TABLE add_unique_not_null_existing_items
		ADD COLUMN required_code INT UNIQUE NOT NULL;`)
	if err == nil {
		rows, queryErr := conn.Current.Query(ctx, `SELECT required_code, COUNT(*)
			FROM add_unique_not_null_existing_items
			GROUP BY required_code
			ORDER BY required_code;`)
		require.NoError(t, queryErr)
		actual, _, readErr := ReadRows(rows, true)
		require.NoError(t, readErr)
		t.Fatalf("expected ADD COLUMN UNIQUE NOT NULL to reject existing rows; accepted rewrite produced grouped values %v", actual)
	}
}

// TestAddUniqueColumnWithDuplicateDefaultValidatesExistingRowsGuard guards that
// adding a UNIQUE column with a constant default validates duplicate backfilled
// values before accepting the new invariant.
func TestAddUniqueColumnWithDuplicateDefaultValidatesExistingRowsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN UNIQUE validates duplicate defaults",
			SetUpScript: []string{
				`CREATE TABLE add_unique_default_existing_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_unique_default_existing_items VALUES (1), (2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_unique_default_existing_items
						ADD COLUMN code INT UNIQUE DEFAULT 7;`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id
						FROM add_unique_default_existing_items
						ORDER BY id;`,
					Expected: []sql.Row{{1}, {2}},
				},
			},
		},
	})
}

// TestAlterColumnSetNotNullValidatesExistingRowsRepro guards that tightening
// an existing column to NOT NULL scans existing rows and rejects persisted
// nulls.
func TestAlterColumnSetNotNullValidatesExistingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN SET NOT NULL validates existing rows",
			SetUpScript: []string{
				`CREATE TABLE set_not_null_existing_items (
					id INT PRIMARY KEY,
					required_value INT
				);`,
				`INSERT INTO set_not_null_existing_items VALUES (1, NULL);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE set_not_null_existing_items
						ALTER COLUMN required_value SET NOT NULL;`,
					ExpectedErr: `non-nullable`,
				},
			},
		},
	})
}

// TestAddColumnCheckConstraintValidatesBackfilledRowsRepro guards that adding
// a column with a default and a CHECK constraint validates the values
// backfilled into existing rows.
func TestAddColumnCheckConstraintValidatesBackfilledRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN validates backfilled CHECK values",
			SetUpScript: []string{
				`CREATE TABLE add_check_column_existing_items (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_check_column_existing_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_check_column_existing_items
						ADD COLUMN amount INT DEFAULT -1 CHECK (amount > 0);`,
					ExpectedErr: `Check constraint`,
				},
			},
		},
	})
}

// TestAddColumnForeignKeyValidatesBackfilledDefaultGuard guards that adding a
// column with a default and a REFERENCES clause validates the values backfilled
// into existing rows before accepting the new column and foreign key.
func TestAddColumnForeignKeyValidatesBackfilledDefaultGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD COLUMN validates backfilled foreign keys",
			SetUpScript: []string{
				`CREATE TABLE add_fk_column_default_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE add_fk_column_default_children (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO add_fk_column_default_children VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE add_fk_column_default_children
						ADD COLUMN parent_id INT DEFAULT 42
						REFERENCES add_fk_column_default_parents(id);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `SELECT count(*)
						FROM information_schema.columns
						WHERE table_name = 'add_fk_column_default_children'
							AND column_name = 'parent_id';`,
					Expected: []sql.Row{{int64(0)}},
				},
			},
		},
	})
}

// TestCheckConstraintsRejectNonScalarExpressionsRepro guards rejection of
// several non-scalar CHECK expressions and reproduces that set-returning
// functions are currently accepted.
func TestCheckConstraintsRejectNonScalarExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CHECK rejects subquery expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE check_subquery_items (
						id INT CHECK (id > (SELECT 0))
					);`,
					ExpectedErr: `ERROR`,
				},
			},
		},
		{
			Name: "CHECK rejects aggregate expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE check_aggregate_items (
						id INT CHECK (avg(id) > 0)
					);`,
					ExpectedErr: `ERROR`,
				},
			},
		},
		{
			Name: "CHECK rejects window expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE check_window_items (
						id INT CHECK (row_number() OVER () > 0)
					);`,
					ExpectedErr: `ERROR`,
				},
			},
		},
		{
			Name: "CHECK rejects set-returning expressions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE check_srf_items (
						id INT CHECK (generate_series(1, 2) > 0)
					);`,
					ExpectedErr: `set-returning functions are not allowed in check constraints`,
				},
			},
		},
	})
}

// TestMultipleColumnCheckConstraintsAreEnforcedRepro reproduces a data
// consistency bug: PostgreSQL allows multiple column-level CHECK constraints,
// and each one must be stored and enforced independently.
func TestMultipleColumnCheckConstraintsAreEnforcedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "multiple column check constraints are enforced",
			SetUpScript: []string{
				`CREATE TABLE multi_check_items (
					id INT PRIMARY KEY,
					amount INT CONSTRAINT amount_below_100 CHECK (amount < 100) CHECK (amount > 10)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO multi_check_items VALUES (1, 20);`,
				},
				{
					Query:       `INSERT INTO multi_check_items VALUES (2, 200);`,
					ExpectedErr: `Check constraint "amount_below_100" violated`,
				},
				{
					Query:       `INSERT INTO multi_check_items VALUES (3, 5);`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, amount FROM multi_check_items;`,
					Expected: []sql.Row{{1, 20}},
				},
			},
		},
	})
}

// TestCreateTableCheckConstraintNoInheritGuard keeps the supported CREATE TABLE
// path explicit while adjacent ALTER TABLE and CREATE TABLE OF paths are broken.
func TestCreateTableCheckConstraintNoInheritGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE accepts CHECK constraint NO INHERIT",
			SetUpScript: []string{
				`CREATE TABLE check_no_inherit_items (
					id INT,
					CONSTRAINT check_no_inherit_positive CHECK (id > 0) NO INHERIT
				);`,
				`INSERT INTO check_no_inherit_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname, connoinherit
						FROM pg_catalog.pg_constraint
						WHERE conname = 'check_no_inherit_positive';`,
					Expected: []sql.Row{{"check_no_inherit_positive", "t"}},
				},
				{
					Query:       `INSERT INTO check_no_inherit_items VALUES (-1);`,
					ExpectedErr: `Check constraint "check_no_inherit_positive" violated`,
				},
			},
		},
	})
}

// TestAlterTableAddCheckConstraintNoInheritRepro reproduces a schema
// correctness bug: PostgreSQL accepts CHECK constraints marked NO INHERIT when
// added to an existing table.
func TestAlterTableAddCheckConstraintNoInheritRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE ADD CONSTRAINT accepts CHECK constraint NO INHERIT",
			SetUpScript: []string{
				`CREATE TABLE alter_check_no_inherit_items (id INT);`,
				`ALTER TABLE alter_check_no_inherit_items
					ADD CONSTRAINT alter_check_no_inherit_positive CHECK (id > 0) NO INHERIT;`,
				`INSERT INTO alter_check_no_inherit_items VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname, connoinherit
						FROM pg_catalog.pg_constraint
						WHERE conname = 'alter_check_no_inherit_positive';`,
					Expected: []sql.Row{{"alter_check_no_inherit_positive", "t"}},
				},
				{
					Query:       `INSERT INTO alter_check_no_inherit_items VALUES (-1);`,
					ExpectedErr: `Check constraint "alter_check_no_inherit_positive" violated`,
				},
			},
		},
	})
}

// TestTypedTableCheckConstraintNoInheritRepro reproduces a typed-table schema
// correctness bug: PostgreSQL accepts CHECK constraints marked NO INHERIT in
// CREATE TABLE OF definitions.
func TestTypedTableCheckConstraintNoInheritRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE OF accepts CHECK constraint NO INHERIT",
			SetUpScript: []string{
				`CREATE TYPE typed_check_no_inherit_row AS (
					id INT,
					code TEXT
				);`,
				`CREATE TABLE typed_check_no_inherit_items OF typed_check_no_inherit_row (
					CONSTRAINT typed_check_no_inherit_positive CHECK (id > 0) NO INHERIT
				);`,
				`INSERT INTO typed_check_no_inherit_items VALUES (1, 'ok');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT conname, connoinherit
						FROM pg_catalog.pg_constraint
						WHERE conname = 'typed_check_no_inherit_positive';`,
					Expected: []sql.Row{{"typed_check_no_inherit_positive", "t"}},
				},
				{
					Query:       `INSERT INTO typed_check_no_inherit_items VALUES (-1, 'bad');`,
					ExpectedErr: `Check constraint "typed_check_no_inherit_positive" violated`,
				},
			},
		},
	})
}

// TestCheckConstraintAcceptsImmutableFunctionRepro reproduces a correctness
// bug: PostgreSQL allows CHECK constraints to call immutable functions and then
// enforces the function result for writes.
func TestCheckConstraintAcceptsImmutableFunctionRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	_, err := conn.Current.Exec(ctx, `CREATE FUNCTION check_function_positive(input_value INT) RETURNS BOOL
		LANGUAGE SQL IMMUTABLE AS $$ SELECT input_value > 0 $$;`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `CREATE TABLE check_function_items (
		id INT PRIMARY KEY,
		amount INT,
		CONSTRAINT check_function_amount_positive
			CHECK (check_function_positive(amount))
	);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO check_function_items VALUES (1, 10);`)
	require.NoError(t, err)

	_, err = conn.Current.Exec(ctx, `INSERT INTO check_function_items VALUES (2, -1);`)
	require.Error(t, err)
	require.Contains(t, err.Error(), `Check constraint "check_function_amount_positive" violated`)

	var count int64
	require.NoError(t, conn.Current.QueryRow(ctx, `SELECT COUNT(*) FROM check_function_items WHERE id = 1 AND amount = 10;`).Scan(&count))
	require.Equal(t, int64(1), count)
}

// TestNotValidCheckConstraintEnforcesNewRowsRepro reproduces a data
// consistency bug: NOT VALID CHECK constraints should skip existing rows until
// validation, but they still enforce all new writes.
func TestNotValidCheckConstraintEnforcesNewRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "NOT VALID check constraint enforces new rows",
			SetUpScript: []string{
				`CREATE TABLE not_valid_check_items (
					id INT PRIMARY KEY,
					amount INT
				);`,
				`INSERT INTO not_valid_check_items VALUES (1, -1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE not_valid_check_items
						ADD CONSTRAINT amount_positive CHECK (amount > 0) NOT VALID;`,
				},
				{
					Query:       `INSERT INTO not_valid_check_items VALUES (2, -2);`,
					ExpectedErr: `Check constraint "amount_positive" violated`,
				},
				{
					Query:       `ALTER TABLE not_valid_check_items VALIDATE CONSTRAINT amount_positive;`,
					ExpectedErr: `Check constraint "amount_positive" violated`,
				},
				{
					Query: `UPDATE not_valid_check_items SET amount = 1 WHERE id = 1;`,
				},
				{
					Query: `ALTER TABLE not_valid_check_items VALIDATE CONSTRAINT amount_positive;`,
				},
				{
					Query: `INSERT INTO not_valid_check_items VALUES (3, 3);`,
				},
				{
					Query: `SELECT id, amount
						FROM not_valid_check_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}, {3, 3}},
				},
			},
		},
	})
}

// TestNotValidForeignKeyConstraintEnforcesNewRowsRepro reproduces a data
// consistency bug: NOT VALID foreign keys should skip existing rows until
// validation, but they still enforce all new writes.
func TestNotValidForeignKeyConstraintEnforcesNewRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "NOT VALID foreign key enforces new rows",
			SetUpScript: []string{
				`CREATE TABLE not_valid_fk_parents (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE not_valid_fk_children (
					id INT PRIMARY KEY,
					parent_id INT
				);`,
				`INSERT INTO not_valid_fk_children VALUES (1, 42);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE not_valid_fk_children
						ADD CONSTRAINT not_valid_fk_children_parent_fk
						FOREIGN KEY (parent_id) REFERENCES not_valid_fk_parents(id)
						NOT VALID;`,
				},
				{
					Query:       `INSERT INTO not_valid_fk_children VALUES (2, 43);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `INSERT INTO not_valid_fk_parents VALUES (42);`,
				},
				{
					Query: `ALTER TABLE not_valid_fk_children
						VALIDATE CONSTRAINT not_valid_fk_children_parent_fk;`,
				},
				{
					Query: `INSERT INTO not_valid_fk_children VALUES (3, 42);`,
				},
				{
					Query: `SELECT id, parent_id
						FROM not_valid_fk_children
						ORDER BY id;`,
					Expected: []sql.Row{{1, 42}, {3, 42}},
				},
			},
		},
	})
}

// TestSetConstraintsRejectsNonDeferrableConstraintRepro reproduces a
// correctness bug: PostgreSQL rejects SET CONSTRAINTS DEFERRED for a
// non-deferrable constraint.
func TestSetConstraintsRejectsNonDeferrableConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET CONSTRAINTS rejects non-deferrable constraint",
			SetUpScript: []string{
				`CREATE TABLE set_constraints_nondeferrable (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SET CONSTRAINTS set_constraints_nondeferrable_code_key DEFERRED;`,
					ExpectedErr: `is not deferrable`,
				},
			},
		},
	})
}

// TestSetConstraintsRejectsNonDeferrableForeignKeyRepro reproduces a
// correctness bug: PostgreSQL rejects SET CONSTRAINTS DEFERRED for a named
// non-deferrable foreign-key constraint.
func TestSetConstraintsRejectsNonDeferrableForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET CONSTRAINTS rejects non-deferrable foreign key",
			SetUpScript: []string{
				`CREATE TABLE set_constraints_nondeferrable_fk_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE set_constraints_nondeferrable_fk_child (
					id INT PRIMARY KEY,
					parent_id INT,
					CONSTRAINT set_constraints_nondeferrable_fk
						FOREIGN KEY (parent_id) REFERENCES set_constraints_nondeferrable_fk_parent(id)
						NOT DEFERRABLE
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:       `SET CONSTRAINTS set_constraints_nondeferrable_fk DEFERRED;`,
					ExpectedErr: `is not deferrable`,
				},
			},
		},
	})
}

// TestSetConstraintsRejectsMissingConstraintRepro reproduces a correctness
// bug: PostgreSQL rejects SET CONSTRAINTS for a name that does not resolve to
// any constraint.
func TestSetConstraintsRejectsMissingConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "SET CONSTRAINTS rejects missing constraint",
			SetUpScript: []string{
				`CREATE TABLE set_constraints_missing_name_anchor (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:       `SET CONSTRAINTS set_constraints_missing_name DEFERRED;`,
					ExpectedErr: `constraint "set_constraints_missing_name" does not exist`,
				},
			},
		},
	})
}

// TestExclusionConstraintRejectsConflictingRowsRepro reproduces a data
// consistency bug: PostgreSQL exclusion constraints prevent conflicting rows.
func TestExclusionConstraintRejectsConflictingRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "EXCLUDE constraint rejects conflicting scalar values",
			SetUpScript: []string{
				`CREATE TABLE exclusion_items (
					id INT PRIMARY KEY,
					resource_id INT,
					EXCLUDE USING btree (resource_id WITH =)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO exclusion_items VALUES (1, 10);`,
				},
				{
					Query:       `INSERT INTO exclusion_items VALUES (2, 10);`,
					ExpectedErr: `conflicting key value violates exclusion constraint`,
				},
				{
					Query: `INSERT INTO exclusion_items VALUES (3, 11);`,
				},
				{
					Query: `SELECT id, resource_id
						FROM exclusion_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 10}, {3, 11}},
				},
			},
		},
	})
}
