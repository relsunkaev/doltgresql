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

// TestInheritedTableRowsVisibleThroughParentRepro reproduces a data
// correctness bug: PostgreSQL scans child table rows through the parent table
// by default, but Doltgres treats INHERITS like a one-time LIKE copy.
func TestInheritedTableRowsVisibleThroughParentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited child rows are visible through parent scans",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_scan (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_scan (
					extra TEXT
				) INHERITS (inherit_parent_scan);`,
				`INSERT INTO inherit_child_scan VALUES (1, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT id, label FROM inherit_parent_scan;`,
					Expected: []sql.Row{{1, "child"}},
				},
			},
		},
	})
}

// TestInheritedChildRowsUpdatedThroughParentRepro reproduces a data
// correctness bug: PostgreSQL UPDATEs against a parent table also scan and
// update inherited child rows by default.
func TestInheritedChildRowsUpdatedThroughParentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "UPDATE parent updates inherited child rows",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_update (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_update (
					extra TEXT
				) INHERITS (inherit_parent_update);`,
				`INSERT INTO inherit_child_update VALUES (1, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE inherit_parent_update
						SET label = 'updated'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT id, label, extra
						FROM inherit_child_update;`,
					Expected: []sql.Row{{1, "updated", "extra"}},
				},
			},
		},
	})
}

// TestInheritedChildRowsDeletedThroughParentRepro reproduces a data
// correctness bug: PostgreSQL DELETEs against a parent table also scan and
// delete inherited child rows by default.
func TestInheritedChildRowsDeletedThroughParentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DELETE parent deletes inherited child rows",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_delete (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_delete (
					extra TEXT
				) INHERITS (inherit_parent_delete);`,
				`INSERT INTO inherit_child_delete VALUES (1, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM inherit_parent_delete
						WHERE id = 1;`,
				},
				{
					Query:    `SELECT count(*) FROM inherit_child_delete;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestTruncateInheritedParentTruncatesChildRowsRepro reproduces a data
// consistency bug: PostgreSQL TRUNCATEs inherited child rows when truncating a
// parent table unless ONLY is specified.
func TestTruncateInheritedParentTruncatesChildRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE parent truncates inherited child rows",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_truncate (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_truncate (
					extra TEXT
				) INHERITS (inherit_parent_truncate);`,
				`INSERT INTO inherit_parent_truncate VALUES (1, 'parent');`,
				`INSERT INTO inherit_child_truncate VALUES (2, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `TRUNCATE inherit_parent_truncate;`,
				},
				{
					Query:    `SELECT count(*) FROM ONLY inherit_parent_truncate;`,
					Expected: []sql.Row{{0}},
				},
				{
					Query:    `SELECT count(*) FROM inherit_child_truncate;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestInheritedGeneratedColumnRejectsDefaultOverrideRepro reproduces a schema
// integrity bug: PostgreSQL rejects a child definition that tries to merge an
// inherited generated column with a regular default.
func TestInheritedGeneratedColumnRejectsDefaultOverrideRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited generated columns reject default overrides",
			SetUpScript: []string{
				`CREATE TABLE inherit_generated_parent (
					a INT PRIMARY KEY,
					b INT GENERATED ALWAYS AS (a * 2) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE inherit_generated_default_child (
						x INT,
						b INT DEFAULT 10
					) INHERITS (inherit_generated_parent);`,
					ExpectedErr: `inherits from generated column but specifies default`,
				},
			},
		},
	})
}

// TestInheritedGeneratedColumnRejectsIdentityOverrideRepro guards that a child
// table cannot merge an inherited generated column with identity generation.
func TestInheritedGeneratedColumnRejectsIdentityOverrideRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited generated columns reject identity overrides",
			SetUpScript: []string{
				`CREATE TABLE inherit_generated_identity_parent (
					a INT PRIMARY KEY,
					b INT GENERATED ALWAYS AS (a * 2) STORED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE inherit_generated_identity_child (
						x INT,
						b INT GENERATED ALWAYS AS IDENTITY
					) INHERITS (inherit_generated_identity_parent);`,
					ExpectedErr: `inherits from generated column but specifies identity`,
				},
			},
		},
	})
}

// TestAlterInheritedParentAddColumnPropagatesToChildRepro reproduces an
// inheritance schema persistence bug: PostgreSQL propagates columns added to a
// parent table to inherited child tables.
func TestAlterInheritedParentAddColumnPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent ADD COLUMN propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_add_column (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_add_column (
					extra TEXT
				) INHERITS (inherit_parent_add_column);`,
				`INSERT INTO inherit_child_add_column VALUES (1, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_parent_add_column
						ADD COLUMN marker TEXT DEFAULT 'added' NOT NULL;`,
				},
				{
					Query: `SELECT id, label, marker, extra
						FROM inherit_child_add_column;`,
					Expected: []sql.Row{{1, "child", "added", "extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentRenameColumnPropagatesToChildRepro reproduces an
// inheritance schema persistence bug: PostgreSQL renames inherited child
// columns when the parent column is renamed.
func TestAlterInheritedParentRenameColumnPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent RENAME COLUMN propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_rename_column (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_rename_column (
					extra TEXT
				) INHERITS (inherit_parent_rename_column);`,
				`INSERT INTO inherit_child_rename_column VALUES (1, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_parent_rename_column
						RENAME COLUMN label TO title;`,
				},
				{
					Query: `SELECT id, title, extra
						FROM inherit_child_rename_column;`,
					Expected: []sql.Row{{1, "child", "extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentDropColumnPropagatesToChildRepro reproduces an
// inheritance schema persistence bug: PostgreSQL drops inherited child columns
// when the parent column is dropped.
func TestAlterInheritedParentDropColumnPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent DROP COLUMN propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_drop_column (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_drop_column (
					extra TEXT
				) INHERITS (inherit_parent_drop_column);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_parent_drop_column
						DROP COLUMN label;`,
				},
				{
					Query: `SELECT column_name
						FROM information_schema.columns
						WHERE table_schema = 'public'
							AND table_name = 'inherit_child_drop_column'
						ORDER BY ordinal_position;`,
					Expected: []sql.Row{{"id"}, {"extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentColumnTypePropagatesToChildRepro reproduces an
// inheritance schema persistence bug: changing the type of an inherited parent
// column should rewrite child rows and update the child column type.
func TestAlterInheritedParentColumnTypePropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent column TYPE propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_type (
					id INT,
					amount INT
				);`,
				`CREATE TABLE inherit_child_type (
					extra TEXT
				) INHERITS (inherit_parent_type);`,
				`INSERT INTO inherit_child_type VALUES (1, 42, 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_parent_type
						ALTER COLUMN amount TYPE BIGINT;`,
				},
				{
					Query: `SELECT amount::TEXT, pg_typeof(amount)::TEXT, extra
						FROM inherit_child_type;`,
					Expected: []sql.Row{{"42", "bigint", "extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentAddCheckPropagatesToChildRepro reproduces a data
// integrity bug: CHECK constraints added to a parent table must be inherited by
// child tables and enforced on child writes.
func TestAlterInheritedParentAddCheckPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent ADD CHECK propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_add_check (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_add_check (
					extra TEXT
				) INHERITS (inherit_parent_add_check);`,
				`ALTER TABLE inherit_parent_add_check
					ADD CONSTRAINT inherit_parent_add_check_positive CHECK (id > 0);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO inherit_child_add_check VALUES (-1, 'bad', 'extra');`,
					ExpectedErr: `inherit_parent_add_check_positive`,
				},
				{
					Query:    `SELECT count(*) FROM inherit_child_add_check;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterInheritedParentSetNotNullPropagatesToChildRepro reproduces a data
// integrity bug: NOT NULL constraints added to a parent column must be
// inherited by child tables and enforced on child writes.
func TestAlterInheritedParentSetNotNullPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent SET NOT NULL propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_set_not_null (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_set_not_null (
					extra TEXT
				) INHERITS (inherit_parent_set_not_null);`,
				`ALTER TABLE inherit_parent_set_not_null
					ALTER COLUMN label SET NOT NULL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO inherit_child_set_not_null VALUES (1, NULL, 'extra');`,
					ExpectedErr: `non-nullable`,
				},
				{
					Query:    `SELECT count(*) FROM inherit_child_set_not_null;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestAlterInheritedParentAddCheckValidatesExistingChildRowsRepro reproduces a
// data integrity bug: adding an inherited parent CHECK constraint must validate
// existing child rows before accepting the new constraint.
func TestAlterInheritedParentAddCheckValidatesExistingChildRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent ADD CHECK validates existing child rows",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_existing_check (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_existing_check (
					extra TEXT
				) INHERITS (inherit_parent_existing_check);`,
				`INSERT INTO inherit_child_existing_check VALUES (-1, 'bad', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_parent_existing_check
						ADD CONSTRAINT inherit_parent_existing_positive CHECK (id > 0);`,
					ExpectedErr: `is violated by some row`,
				},
			},
		},
	})
}

// TestAlterInheritedParentSetNotNullValidatesExistingChildRowsRepro reproduces
// a data integrity bug: setting NOT NULL on an inherited parent column must
// validate existing child rows before accepting the new constraint.
func TestAlterInheritedParentSetNotNullValidatesExistingChildRowsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent SET NOT NULL validates existing child rows",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_existing_not_null (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_existing_not_null (
					extra TEXT
				) INHERITS (inherit_parent_existing_not_null);`,
				`INSERT INTO inherit_child_existing_not_null VALUES (1, NULL, 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_parent_existing_not_null
						ALTER COLUMN label SET NOT NULL;`,
					ExpectedErr: `contains null values`,
				},
			},
		},
	})
}

// TestDropInheritedParentRequiresCascadeRepro reproduces an inheritance
// dependency bug: a parent table cannot be dropped with the default RESTRICT
// behavior while an inherited child still depends on it.
func TestDropInheritedParentRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP parent table rejects inherited child dependency",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_drop_restrict (
					id INT
				);`,
				`CREATE TABLE inherit_child_drop_restrict (
					extra TEXT
				) INHERITS (inherit_parent_drop_restrict);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TABLE inherit_parent_drop_restrict;`,
					ExpectedErr: `depends on table inherit_parent_drop_restrict`,
				},
				{
					Query: `SELECT to_regclass('inherit_parent_drop_restrict') IS NOT NULL,
							to_regclass('inherit_child_drop_restrict') IS NOT NULL;`,
					Expected: []sql.Row{{"t", "t"}},
				},
			},
		},
	})
}

// TestDropInheritedParentCascadeDropsChildRepro reproduces an inheritance
// dependency bug: DROP TABLE ... CASCADE on an inherited parent should also
// remove dependent child tables.
func TestDropInheritedParentCascadeDropsChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP parent table CASCADE drops inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_drop_cascade (
					id INT
				);`,
				`CREATE TABLE inherit_child_drop_cascade (
					extra TEXT
				) INHERITS (inherit_parent_drop_cascade);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE inherit_parent_drop_cascade CASCADE;`,
				},
				{
					Query: `SELECT to_regclass('inherit_parent_drop_cascade') IS NULL,
							to_regclass('inherit_child_drop_cascade') IS NULL;`,
					Expected: []sql.Row{{"t", "t"}},
				},
			},
		},
	})
}

// TestInheritedChildUsesParentDefaultsGuard guards that default expressions on
// inherited parent columns are available when inserting into the child table.
func TestInheritedChildUsesParentDefaultsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited child uses parent defaults",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_default (
					id INT DEFAULT 7,
					label TEXT DEFAULT 'parent-default'
				);`,
				`CREATE TABLE inherit_child_default (
					extra TEXT
				) INHERITS (inherit_parent_default);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO inherit_child_default (extra)
						VALUES ('extra');`,
				},
				{
					Query: `SELECT id, label, extra
						FROM inherit_child_default;`,
					Expected: []sql.Row{{7, "parent-default", "extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentSetDefaultPropagatesToChildRepro reproduces an
// inheritance schema persistence bug: defaults set on inherited parent columns
// should propagate to child tables.
func TestAlterInheritedParentSetDefaultPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent SET DEFAULT propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_set_default (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_set_default (
					extra TEXT
				) INHERITS (inherit_parent_set_default);`,
				`ALTER TABLE inherit_parent_set_default
					ALTER COLUMN label SET DEFAULT 'new-default';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO inherit_child_set_default (id, extra)
						VALUES (1, 'extra');`,
				},
				{
					Query: `SELECT id, label, extra
						FROM inherit_child_set_default;`,
					Expected: []sql.Row{{1, "new-default", "extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentSetDefaultPropagatesToGrandchildGuard guards that
// parent default changes propagate through multiple inheritance levels.
func TestAlterInheritedParentSetDefaultPropagatesToGrandchildGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent SET DEFAULT propagates to inherited grandchild",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_set_default_grandchild (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_set_default_grandchild (
					child_extra TEXT
				) INHERITS (inherit_parent_set_default_grandchild);`,
				`CREATE TABLE inherit_grandchild_set_default (
					grandchild_extra TEXT
				) INHERITS (inherit_child_set_default_grandchild);`,
				`ALTER TABLE inherit_parent_set_default_grandchild
					ALTER COLUMN label SET DEFAULT 'grandparent-default';`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO inherit_grandchild_set_default (id, child_extra, grandchild_extra)
						VALUES (1, 'child', 'grandchild');`,
				},
				{
					Query: `SELECT id, label, child_extra, grandchild_extra
						FROM inherit_grandchild_set_default;`,
					Expected: []sql.Row{{1, "grandparent-default", "child", "grandchild"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentDropDefaultPropagatesToChildRepro reproduces an
// inheritance schema persistence bug: dropping a parent-column default should
// remove the inherited child-column default as well.
func TestAlterInheritedParentDropDefaultPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent DROP DEFAULT propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_drop_default (
					id INT,
					label TEXT DEFAULT 'parent-default'
				);`,
				`CREATE TABLE inherit_child_drop_default (
					extra TEXT
				) INHERITS (inherit_parent_drop_default);`,
				`ALTER TABLE inherit_parent_drop_default
					ALTER COLUMN label DROP DEFAULT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO inherit_child_drop_default (id, extra)
						VALUES (1, 'extra');`,
				},
				{
					Query: `SELECT id, label, extra
						FROM inherit_child_drop_default;`,
					Expected: []sql.Row{{1, nil, "extra"}},
				},
			},
		},
	})
}

// TestAlterInheritedParentDropNotNullPropagatesToChildRepro reproduces an
// inheritance schema persistence bug: dropping NOT NULL from a parent column
// should remove the inherited child-column requirement as well.
func TestAlterInheritedParentDropNotNullPropagatesToChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER parent DROP NOT NULL propagates to inherited child",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_drop_not_null (
					id INT,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE inherit_child_drop_not_null (
					extra TEXT
				) INHERITS (inherit_parent_drop_not_null);`,
				`ALTER TABLE inherit_parent_drop_not_null
					ALTER COLUMN label DROP NOT NULL;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO inherit_child_drop_not_null
						VALUES (1, NULL, 'extra');`,
				},
				{
					Query: `SELECT id, label, extra
						FROM inherit_child_drop_not_null;`,
					Expected: []sql.Row{{1, nil, "extra"}},
				},
			},
		},
	})
}

// TestAlterTableInheritAttachesChildRepro reproduces an inheritance metadata
// persistence bug: ALTER TABLE child INHERIT parent should establish a live
// parent/child relationship for compatible tables.
func TestAlterTableInheritAttachesChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE INHERIT attaches child to parent",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_attach (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_attach (
					id INT,
					label TEXT,
					extra TEXT
				);`,
				`ALTER TABLE inherit_child_attach
					INHERIT inherit_parent_attach;`,
				`INSERT INTO inherit_child_attach VALUES (1, 'child', 'extra');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label
						FROM inherit_parent_attach;`,
					Expected: []sql.Row{{1, "child"}},
				},
				{
					Query: `SELECT child.relname, parent.relname
						FROM pg_catalog.pg_inherits inh
						JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
						JOIN pg_catalog.pg_class parent ON parent.oid = inh.inhparent
						WHERE child.relname = 'inherit_child_attach';`,
					Expected: []sql.Row{{"inherit_child_attach", "inherit_parent_attach"}},
				},
			},
		},
	})
}

// TestAlterTableNoInheritDetachesChildRepro reproduces an inheritance metadata
// persistence bug: ALTER TABLE child NO INHERIT parent should remove the
// parent/child relationship.
func TestAlterTableNoInheritDetachesChildRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TABLE NO INHERIT detaches child from parent",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_detach (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_detach (
					extra TEXT
				) INHERITS (inherit_parent_detach);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE inherit_child_detach
						NO INHERIT inherit_parent_detach;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_inherits inh
						JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
						WHERE child.relname = 'inherit_child_detach';`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestInheritedChildEnforcesParentCheckConstraintGuard guards that inherited
// child tables enforce CHECK constraints inherited from the parent table.
func TestInheritedChildEnforcesParentCheckConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited child enforces parent CHECK constraint",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_check (
					id INT CONSTRAINT inherit_parent_check_positive CHECK (id > 0),
					label TEXT
				);`,
				`CREATE TABLE inherit_child_check (
					extra TEXT
				) INHERITS (inherit_parent_check);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO inherit_child_check VALUES (-1, 'bad', 'extra');`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT count(*) FROM inherit_child_check;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestInheritedChildEnforcesParentNotNullConstraintGuard guards that inherited
// child tables enforce NOT NULL constraints inherited from the parent table.
func TestInheritedChildEnforcesParentNotNullConstraintGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "inherited child enforces parent NOT NULL constraint",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_not_null (
					id INT,
					label TEXT NOT NULL
				);`,
				`CREATE TABLE inherit_child_not_null (
					extra TEXT
				) INHERITS (inherit_parent_not_null);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO inherit_child_not_null VALUES (1, NULL, 'extra');`,
					ExpectedErr: `non-nullable`,
				},
				{
					Query:    `SELECT count(*) FROM inherit_child_not_null;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestCreateTableInheritsPersistsPgInheritsMetadataRepro reproduces a catalog
// persistence bug: CREATE TABLE ... INHERITS should record the parent/child
// relationship in pg_inherits.
func TestCreateTableInheritsPersistsPgInheritsMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE INHERITS persists pg_inherits metadata",
			SetUpScript: []string{
				`CREATE TABLE inherit_parent_catalog (
					id INT,
					label TEXT
				);`,
				`CREATE TABLE inherit_child_catalog (
					extra TEXT
				) INHERITS (inherit_parent_catalog);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT child.relname, parent.relname
						FROM pg_catalog.pg_inherits inh
						JOIN pg_catalog.pg_class child ON child.oid = inh.inhrelid
						JOIN pg_catalog.pg_class parent ON parent.oid = inh.inhparent
						WHERE child.relname = 'inherit_child_catalog';`,
					Expected: []sql.Row{{"inherit_child_catalog", "inherit_parent_catalog"}},
				},
			},
		},
	})
}
