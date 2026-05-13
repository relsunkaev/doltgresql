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

// TestForeignKeyRejectsFloatReferencingIntegerRepro reproduces a schema
// integrity bug: PostgreSQL rejects a floating-point referencing column for an
// integer referenced key, but Doltgres accepts the invalid foreign key.
func TestForeignKeyRejectsFloatReferencingIntegerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "foreign key rejects float child against integer parent",
			SetUpScript: []string{
				`CREATE TABLE fk_type_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_type_child (id INT PRIMARY KEY, parent_id FLOAT);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE fk_type_child
						ADD CONSTRAINT fk_type_child_parent_id_fkey
						FOREIGN KEY (parent_id) REFERENCES fk_type_parent(id);`,
					ExpectedErr: `incompatible types`,
				},
			},
		},
	})
}

// TestTypmodForeignKeyUsesCoercedValuesRepro reproduces a referential
// integrity bug: PostgreSQL applies typmod coercion before checking foreign
// keys, so child values that coerce to the parent key are valid.
func TestTypmodForeignKeyUsesCoercedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "timestamp typmod foreign key uses rounded values",
			SetUpScript: []string{
				`CREATE TABLE timestamp_typmod_fk_parent (
					ts TIMESTAMP(0) PRIMARY KEY
				);`,
				`CREATE TABLE timestamp_typmod_fk_child (
					id INT PRIMARY KEY,
					ts TIMESTAMP(0) REFERENCES timestamp_typmod_fk_parent(ts)
				);`,
				`INSERT INTO timestamp_typmod_fk_parent VALUES
					('2021-09-15 21:43:56.600');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO timestamp_typmod_fk_child VALUES
						(1, '2021-09-15 21:43:56.700');`,
				},
				{
					Query: `SELECT c.id, c.ts::text, p.ts::text
						FROM timestamp_typmod_fk_child c
						JOIN timestamp_typmod_fk_parent p ON c.ts = p.ts
						ORDER BY c.id;`,
					Expected: []sql.Row{{1, "2021-09-15 21:43:57", "2021-09-15 21:43:57"}},
				},
			},
		},
		{
			Name: "domain typmod foreign key uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN num52_fk_domain AS NUMERIC(5,2);`,
				`CREATE TABLE numeric_domain_fk_parent (
					amount num52_fk_domain PRIMARY KEY
				);`,
				`CREATE TABLE numeric_domain_fk_child (
					id INT PRIMARY KEY,
					amount num52_fk_domain REFERENCES numeric_domain_fk_parent(amount)
				);`,
				`INSERT INTO numeric_domain_fk_parent VALUES (1.231);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO numeric_domain_fk_child VALUES (1, 1.234);`,
				},
				{
					Query: `SELECT c.id, c.amount::text, p.amount::text
						FROM numeric_domain_fk_child c
						JOIN numeric_domain_fk_parent p ON c.amount = p.amount
						ORDER BY c.id;`,
					Expected: []sql.Row{{1, "1.23", "1.23"}},
				},
			},
		},
		{
			Name: "text domain typmod foreign key uses coerced values",
			SetUpScript: []string{
				`CREATE DOMAIN varchar3_fk_domain AS varchar(3);`,
				`CREATE TABLE varchar_domain_fk_parent (
						label varchar3_fk_domain PRIMARY KEY
					);`,
				`CREATE TABLE varchar_domain_fk_child (
						id INT PRIMARY KEY,
						label varchar3_fk_domain REFERENCES varchar_domain_fk_parent(label)
					);`,
				`INSERT INTO varchar_domain_fk_parent VALUES ('abc');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO varchar_domain_fk_child VALUES (1, 'abc   ');`,
				},
				{
					Query: `SELECT c.id, c.label, length(c.label), p.label
							FROM varchar_domain_fk_child c
							JOIN varchar_domain_fk_parent p ON c.label = p.label
							ORDER BY c.id;`,
					Expected: []sql.Row{{1, "abc", 3, "abc"}},
				},
			},
		},
	})
}

// TestCompositeForeignKeyMatchFullRepro reproduces a foreign-key correctness
// bug: PostgreSQL supports MATCH FULL for composite foreign keys, where either
// all referencing columns are NULL or all are checked against the parent key.
func TestCompositeForeignKeyMatchFullRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite foreign key MATCH FULL enforces full-null rule",
			SetUpScript: []string{
				`CREATE TABLE match_full_parent (
					id1 INT,
					id2 INT,
					PRIMARY KEY (id1, id2)
				);`,
				`INSERT INTO match_full_parent VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE match_full_child (
						id INT PRIMARY KEY,
						parent_id1 INT,
						parent_id2 INT,
						FOREIGN KEY (parent_id1, parent_id2)
							REFERENCES match_full_parent(id1, id2) MATCH FULL
					);`,
				},
				{
					Query: `SELECT confmatchtype, pg_get_constraintdef(oid) LIKE '%MATCH FULL%'
						FROM pg_constraint
						WHERE conrelid = 'match_full_child'::regclass
							AND contype = 'f';`,
					Expected: []sql.Row{{"f", true}},
				},
				{
					Query: `INSERT INTO match_full_child VALUES (1, 1, 1);`,
				},
				{
					Query: `INSERT INTO match_full_child VALUES (2, NULL, NULL);`,
				},
				{
					Query:       `INSERT INTO match_full_child VALUES (3, 1, NULL);`,
					ExpectedErr: `MATCH FULL`,
				},
				{
					Query:       `INSERT INTO match_full_child VALUES (4, 2, 2);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `SELECT id, parent_id1, parent_id2
						FROM match_full_child
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 1, 1},
						{2, nil, nil},
					},
				},
			},
		},
	})
}

// TestCompositeForeignKeyMatchSimpleAllowsPartialNullsRepro guards PostgreSQL
// MATCH SIMPLE semantics for composite foreign keys: if any referencing column
// is NULL, the row does not need a matching parent.
func TestCompositeForeignKeyMatchSimpleAllowsPartialNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "composite foreign key MATCH SIMPLE allows partial nulls",
			SetUpScript: []string{
				`CREATE TABLE match_simple_parent (
					id1 INT,
					id2 INT,
					PRIMARY KEY (id1, id2)
				);`,
				`CREATE TABLE match_simple_child (
					id INT PRIMARY KEY,
					parent_id1 INT,
					parent_id2 INT,
					FOREIGN KEY (parent_id1, parent_id2)
						REFERENCES match_simple_parent(id1, id2)
				);`,
				`INSERT INTO match_simple_parent VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO match_simple_child VALUES
						(1, 1, 1),
						(2, 1, NULL),
						(3, NULL, 1),
						(4, NULL, NULL);`,
				},
				{
					Query:       `INSERT INTO match_simple_child VALUES (5, 2, 2);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `SELECT id, parent_id1, parent_id2
						FROM match_simple_child
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 1, 1},
						{2, 1, nil},
						{3, nil, 1},
						{4, nil, nil},
					},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetNullColumnListRepro reproduces a foreign-key
// correctness bug: PostgreSQL supports ON DELETE SET NULL column lists, which
// clear only selected referencing columns for composite keys.
func TestForeignKeyOnDeleteSetNullColumnListRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET NULL column list preserves tenant key",
			SetUpScript: []string{
				`CREATE TABLE fk_set_null_column_parent (
					tenant_id INT,
					id INT,
					PRIMARY KEY (tenant_id, id)
				);`,
				`CREATE TABLE fk_set_null_column_child (
					child_id INT PRIMARY KEY,
					tenant_id INT NOT NULL,
					parent_id INT,
					FOREIGN KEY (tenant_id, parent_id)
						REFERENCES fk_set_null_column_parent(tenant_id, id)
						ON DELETE SET NULL (parent_id)
				);`,
				`INSERT INTO fk_set_null_column_parent VALUES (1, 10);`,
				`INSERT INTO fk_set_null_column_child VALUES (100, 1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_set_null_column_parent
						WHERE tenant_id = 1 AND id = 10;`,
				},
				{
					Query: `SELECT child_id, tenant_id, parent_id
						FROM fk_set_null_column_child;`,
					Expected: []sql.Row{{100, 1, nil}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetNullColumnListRepro reproduces a foreign-key
// correctness bug: PostgreSQL supports ON UPDATE SET NULL column lists, which
// clear only selected referencing columns for composite keys.
func TestForeignKeyOnUpdateSetNullColumnListRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET NULL column list preserves tenant key",
			SetUpScript: []string{
				`CREATE TABLE fk_update_set_null_column_parent (
					tenant_id INT,
					id INT,
					PRIMARY KEY (tenant_id, id)
				);`,
				`CREATE TABLE fk_update_set_null_column_child (
					child_id INT PRIMARY KEY,
					tenant_id INT NOT NULL,
					parent_id INT,
					FOREIGN KEY (tenant_id, parent_id)
						REFERENCES fk_update_set_null_column_parent(tenant_id, id)
						ON UPDATE SET NULL (parent_id)
				);`,
				`INSERT INTO fk_update_set_null_column_parent VALUES (1, 10);`,
				`INSERT INTO fk_update_set_null_column_child VALUES (100, 1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_set_null_column_parent
						SET id = 11
						WHERE tenant_id = 1 AND id = 10;`,
				},
				{
					Query: `SELECT child_id, tenant_id, parent_id
						FROM fk_update_set_null_column_child;`,
					Expected: []sql.Row{{100, 1, nil}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetDefaultColumnListRepro reproduces a foreign-key
// correctness bug: PostgreSQL supports ON DELETE SET DEFAULT column lists,
// which default only selected referencing columns for composite keys.
func TestForeignKeyOnDeleteSetDefaultColumnListRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET DEFAULT column list preserves tenant key",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_set_default_column_parent (
					tenant_id INT,
					id INT,
					PRIMARY KEY (tenant_id, id)
				);`,
				`CREATE TABLE fk_delete_set_default_column_child (
					child_id INT PRIMARY KEY,
					tenant_id INT NOT NULL,
					parent_id INT DEFAULT 0,
					FOREIGN KEY (tenant_id, parent_id)
						REFERENCES fk_delete_set_default_column_parent(tenant_id, id)
						ON DELETE SET DEFAULT (parent_id)
				);`,
				`INSERT INTO fk_delete_set_default_column_parent VALUES (1, 0), (1, 10);`,
				`INSERT INTO fk_delete_set_default_column_child VALUES (100, 1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_delete_set_default_column_parent
						WHERE tenant_id = 1 AND id = 10;`,
				},
				{
					Query: `SELECT child_id, tenant_id, parent_id
						FROM fk_delete_set_default_column_child;`,
					Expected: []sql.Row{{100, 1, 0}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetDefaultColumnListRepro reproduces a foreign-key
// correctness bug: PostgreSQL supports ON UPDATE SET DEFAULT column lists,
// which default only selected referencing columns for composite keys.
func TestForeignKeyOnUpdateSetDefaultColumnListRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET DEFAULT column list preserves tenant key",
			SetUpScript: []string{
				`CREATE TABLE fk_update_set_default_column_parent (
					tenant_id INT,
					id INT,
					PRIMARY KEY (tenant_id, id)
				);`,
				`CREATE TABLE fk_update_set_default_column_child (
					child_id INT PRIMARY KEY,
					tenant_id INT NOT NULL,
					parent_id INT DEFAULT 0,
					FOREIGN KEY (tenant_id, parent_id)
						REFERENCES fk_update_set_default_column_parent(tenant_id, id)
						ON UPDATE SET DEFAULT (parent_id)
				);`,
				`INSERT INTO fk_update_set_default_column_parent VALUES (1, 0), (1, 10);`,
				`INSERT INTO fk_update_set_default_column_child VALUES (100, 1, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_set_default_column_parent
						SET id = 11
						WHERE tenant_id = 1 AND id = 10;`,
				},
				{
					Query: `SELECT child_id, tenant_id, parent_id
						FROM fk_update_set_default_column_child;`,
					Expected: []sql.Row{{100, 1, 0}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetNullValidatesCheckConstraintRepro reproduces a data
// consistency bug: referential ON DELETE SET NULL rewrites must still validate
// child-table CHECK constraints.
func TestForeignKeyOnDeleteSetNullValidatesCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET NULL validates child CHECK constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_null_check_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_null_check_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_delete_null_check_parent(id)
						ON DELETE SET NULL,
					CONSTRAINT fk_delete_null_check_present CHECK (parent_id IS NOT NULL)
				);`,
				`INSERT INTO fk_delete_null_check_parent VALUES (1);`,
				`INSERT INTO fk_delete_null_check_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM fk_delete_null_check_parent WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_null_check_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyDomainColumnReferencesBaseTypeRepro reproduces a schema
// correctness bug: PostgreSQL allows a domain-typed child foreign-key column to
// reference a parent key of the domain's base type.
func TestForeignKeyDomainColumnReferencesBaseTypeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "domain foreign key column references base type key",
			SetUpScript: []string{
				`CREATE DOMAIN positive_fk_child_domain AS INT CHECK (VALUE > 0);`,
				`CREATE TABLE domain_fk_base_parent (
					id INT PRIMARY KEY
				);`,
				`INSERT INTO domain_fk_base_parent VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE domain_fk_base_child (
					id INT PRIMARY KEY,
					parent_id positive_fk_child_domain
						REFERENCES domain_fk_base_parent(id)
				);`,
				},
				{
					Query: `INSERT INTO domain_fk_base_child VALUES (1, 1);`,
				},
				{
					Query:    `SELECT id, parent_id FROM domain_fk_base_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetNullValidatesCheckConstraintRepro reproduces a data
// consistency bug: referential ON UPDATE SET NULL rewrites must still validate
// child-table CHECK constraints.
func TestForeignKeyOnUpdateSetNullValidatesCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET NULL validates child CHECK constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_update_null_check_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_null_check_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_update_null_check_parent(id)
						ON UPDATE SET NULL,
					CONSTRAINT fk_update_null_check_present CHECK (parent_id IS NOT NULL)
				);`,
				`INSERT INTO fk_update_null_check_parent VALUES (1);`,
				`INSERT INTO fk_update_null_check_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE fk_update_null_check_parent SET id = 2 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_null_check_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetDefaultValidatesDefaultRepro reproduces a data
// consistency bug: ON DELETE SET DEFAULT must reject the parent delete if the
// default value would leave the child row without a matching parent.
func TestForeignKeyOnDeleteSetDefaultValidatesDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET DEFAULT validates default value",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_default_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_default_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 REFERENCES fk_delete_default_parent(id)
						ON DELETE SET DEFAULT
				);`,
				`INSERT INTO fk_delete_default_parent VALUES (1);`,
				`INSERT INTO fk_delete_default_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM fk_delete_default_parent WHERE id = 1;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_default_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetDefaultValidatesDefaultRepro reproduces a data
// consistency bug: ON UPDATE SET DEFAULT must reject the parent key update if
// the default value would leave the child row without a matching parent.
func TestForeignKeyOnUpdateSetDefaultValidatesDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET DEFAULT validates default value",
			SetUpScript: []string{
				`CREATE TABLE fk_update_default_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_default_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 REFERENCES fk_update_default_parent(id)
						ON UPDATE SET DEFAULT
				);`,
				`INSERT INTO fk_update_default_parent VALUES (1);`,
				`INSERT INTO fk_update_default_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE fk_update_default_parent SET id = 2 WHERE id = 1;`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_default_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetDefaultValidatesCheckConstraintRepro reproduces a
// data consistency bug: referential ON DELETE SET DEFAULT rewrites must still
// validate child-table CHECK constraints.
func TestForeignKeyOnDeleteSetDefaultValidatesCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET DEFAULT validates child CHECK constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_default_check_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_default_check_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0
						REFERENCES fk_delete_default_check_parent(id)
						ON DELETE SET DEFAULT,
					CONSTRAINT fk_delete_default_check_positive CHECK (parent_id > 0)
				);`,
				`INSERT INTO fk_delete_default_check_parent VALUES (0), (1);`,
				`INSERT INTO fk_delete_default_check_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM fk_delete_default_check_parent WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_default_check_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetDefaultValidatesCheckConstraintRepro reproduces a
// data consistency bug: referential ON UPDATE SET DEFAULT rewrites must still
// validate child-table CHECK constraints.
func TestForeignKeyOnUpdateSetDefaultValidatesCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET DEFAULT validates child CHECK constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_update_default_check_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_default_check_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0
						REFERENCES fk_update_default_check_parent(id)
						ON UPDATE SET DEFAULT,
					CONSTRAINT fk_update_default_check_positive CHECK (parent_id > 0)
				);`,
				`INSERT INTO fk_update_default_check_parent VALUES (0), (1);`,
				`INSERT INTO fk_update_default_check_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE fk_update_default_check_parent SET id = 2 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_default_check_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetDefaultValidatesUniqueConstraintRepro reproduces a
// data consistency bug: referential ON DELETE SET DEFAULT rewrites must still
// validate child-table UNIQUE constraints.
func TestForeignKeyOnDeleteSetDefaultValidatesUniqueConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET DEFAULT validates child UNIQUE constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_default_unique_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_default_unique_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 UNIQUE
						REFERENCES fk_delete_default_unique_parent(id)
						ON DELETE SET DEFAULT
				);`,
				`INSERT INTO fk_delete_default_unique_parent VALUES (0), (1), (2);`,
				`INSERT INTO fk_delete_default_unique_child VALUES (1, 1), (2, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM fk_delete_default_unique_parent WHERE id IN (1, 2);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, parent_id
						FROM fk_delete_default_unique_child
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}, {2, 2}},
				},
				{
					Query: `SELECT id
						FROM fk_delete_default_unique_parent
						ORDER BY id;`,
					Expected: []sql.Row{{0}, {1}, {2}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetDefaultValidatesUniqueConstraintRepro reproduces a
// data consistency bug: referential ON UPDATE SET DEFAULT rewrites must still
// validate child-table UNIQUE constraints.
func TestForeignKeyOnUpdateSetDefaultValidatesUniqueConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET DEFAULT validates child UNIQUE constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_update_default_unique_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_default_unique_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 UNIQUE
						REFERENCES fk_update_default_unique_parent(id)
						ON UPDATE SET DEFAULT
				);`,
				`INSERT INTO fk_update_default_unique_parent VALUES (0), (1), (2);`,
				`INSERT INTO fk_update_default_unique_child VALUES (1, 1), (2, 2);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE fk_update_default_unique_parent SET id = id + 10 WHERE id IN (1, 2);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, parent_id
						FROM fk_update_default_unique_child
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}, {2, 2}},
				},
				{
					Query: `SELECT id
						FROM fk_update_default_unique_parent
						ORDER BY id;`,
					Expected: []sql.Row{{0}, {1}, {2}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateCascadeValidatesCheckConstraintRepro reproduces a data
// consistency bug: referential ON UPDATE CASCADE rewrites must still validate
// child-table CHECK constraints.
func TestForeignKeyOnUpdateCascadeValidatesCheckConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE CASCADE validates child CHECK constraints",
			SetUpScript: []string{
				`CREATE TABLE fk_update_cascade_check_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_cascade_check_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_update_cascade_check_parent(id)
						ON UPDATE CASCADE,
					CONSTRAINT fk_update_cascade_check_small CHECK (parent_id < 10)
				);`,
				`INSERT INTO fk_update_cascade_check_parent VALUES (1);`,
				`INSERT INTO fk_update_cascade_check_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE fk_update_cascade_check_parent SET id = 11 WHERE id = 1;`,
					ExpectedErr: `Check constraint`,
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_cascade_check_child;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteCascadeValidatesGrandchildRestrictRepro verifies that
// referential ON DELETE CASCADE child deletes still enforce foreign keys that
// reference the child table.
func TestForeignKeyOnDeleteCascadeValidatesGrandchildRestrictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE CASCADE validates grandchild foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_cascade_restrict_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_cascade_restrict_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_delete_cascade_restrict_parent(id)
						ON DELETE CASCADE
				);`,
				`CREATE TABLE fk_delete_cascade_restrict_grandchild (
					id INT PRIMARY KEY,
					child_id INT REFERENCES fk_delete_cascade_restrict_child(id)
				);`,
				`INSERT INTO fk_delete_cascade_restrict_parent VALUES (1);`,
				`INSERT INTO fk_delete_cascade_restrict_child VALUES (10, 1);`,
				`INSERT INTO fk_delete_cascade_restrict_grandchild VALUES (100, 10);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DELETE FROM fk_delete_cascade_restrict_parent WHERE id = 1;`,
					ExpectedErr: `Foreign key`,
				},
				{
					Query:    `SELECT id FROM fk_delete_cascade_restrict_parent;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_cascade_restrict_child;`,
					Expected: []sql.Row{{10, 1}},
				},
				{
					Query:    `SELECT id, child_id FROM fk_delete_cascade_restrict_grandchild;`,
					Expected: []sql.Row{{100, 10}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateCascadePropagatesGrandchildCascadeRepro verifies that
// referential ON UPDATE CASCADE child updates can cascade through foreign keys
// that reference the updated child key.
func TestForeignKeyOnUpdateCascadePropagatesGrandchildCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE CASCADE propagates to grandchild foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_update_cascade_grand_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_cascade_grand_child (
					id INT PRIMARY KEY,
					parent_id INT UNIQUE REFERENCES fk_update_cascade_grand_parent(id)
						ON UPDATE CASCADE
				);`,
				`CREATE TABLE fk_update_cascade_grand_grandchild (
					id INT PRIMARY KEY,
					child_parent_id INT REFERENCES fk_update_cascade_grand_child(parent_id)
						ON UPDATE CASCADE
				);`,
				`INSERT INTO fk_update_cascade_grand_parent VALUES (1);`,
				`INSERT INTO fk_update_cascade_grand_child VALUES (10, 1);`,
				`INSERT INTO fk_update_cascade_grand_grandchild VALUES (100, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_cascade_grand_parent SET id = 2 WHERE id = 1;`,
				},
				{
					Query:    `SELECT id FROM fk_update_cascade_grand_parent;`,
					Expected: []sql.Row{{2}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_cascade_grand_child;`,
					Expected: []sql.Row{{10, 2}},
				},
				{
					Query:    `SELECT id, child_parent_id FROM fk_update_cascade_grand_grandchild;`,
					Expected: []sql.Row{{100, 2}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateCascadeValidatesGrandchildRestrictRepro verifies that
// referential ON UPDATE CASCADE child updates still enforce foreign keys that
// reference the updated child key.
func TestForeignKeyOnUpdateCascadeValidatesGrandchildRestrictRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE CASCADE validates grandchild restrict foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_update_cascade_restrict_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_cascade_restrict_child (
					id INT PRIMARY KEY,
					parent_id INT UNIQUE REFERENCES fk_update_cascade_restrict_parent(id)
						ON UPDATE CASCADE
				);`,
				`CREATE TABLE fk_update_cascade_restrict_grandchild (
					id INT PRIMARY KEY,
					child_parent_id INT REFERENCES fk_update_cascade_restrict_child(parent_id)
				);`,
				`INSERT INTO fk_update_cascade_restrict_parent VALUES (1);`,
				`INSERT INTO fk_update_cascade_restrict_child VALUES (10, 1);`,
				`INSERT INTO fk_update_cascade_restrict_grandchild VALUES (100, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `UPDATE fk_update_cascade_restrict_parent SET id = 2 WHERE id = 1;`,
					ExpectedErr: `Foreign key`,
				},
				{
					Query:    `SELECT id FROM fk_update_cascade_restrict_parent;`,
					Expected: []sql.Row{{1}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_cascade_restrict_child;`,
					Expected: []sql.Row{{10, 1}},
				},
				{
					Query:    `SELECT id, child_parent_id FROM fk_update_cascade_restrict_grandchild;`,
					Expected: []sql.Row{{100, 1}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetNullRecomputesGeneratedColumnsRepro reproduces a
// data consistency bug: referential ON DELETE SET NULL rewrites must recompute
// stored generated columns.
func TestForeignKeyOnDeleteSetNullRecomputesGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET NULL recomputes generated columns",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_null_generated_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_null_generated_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_delete_null_generated_parent(id)
						ON DELETE SET NULL,
					parent_marker INT GENERATED ALWAYS AS (parent_id + 10) STORED
				);`,
				`INSERT INTO fk_delete_null_generated_parent VALUES (1);`,
				`INSERT INTO fk_delete_null_generated_child (id, parent_id) VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_delete_null_generated_parent WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, parent_id, parent_marker FROM fk_delete_null_generated_child;`,
					Expected: []sql.Row{{1, nil, nil}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetNullRecomputesGeneratedColumnsRepro reproduces a
// data consistency bug: referential ON UPDATE SET NULL rewrites must recompute
// stored generated columns.
func TestForeignKeyOnUpdateSetNullRecomputesGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET NULL recomputes generated columns",
			SetUpScript: []string{
				`CREATE TABLE fk_update_null_generated_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_null_generated_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_update_null_generated_parent(id)
						ON UPDATE SET NULL,
					parent_marker INT GENERATED ALWAYS AS (parent_id + 10) STORED
				);`,
				`INSERT INTO fk_update_null_generated_parent VALUES (1);`,
				`INSERT INTO fk_update_null_generated_child (id, parent_id) VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_null_generated_parent SET id = 2 WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, parent_id, parent_marker FROM fk_update_null_generated_child;`,
					Expected: []sql.Row{{1, nil, nil}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetDefaultRecomputesGeneratedColumnsRepro reproduces a
// data consistency bug: referential ON DELETE SET DEFAULT rewrites must
// recompute stored generated columns.
func TestForeignKeyOnDeleteSetDefaultRecomputesGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET DEFAULT recomputes generated columns",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_default_generated_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_default_generated_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 REFERENCES fk_delete_default_generated_parent(id)
						ON DELETE SET DEFAULT,
					parent_marker INT GENERATED ALWAYS AS (parent_id + 10) STORED
				);`,
				`INSERT INTO fk_delete_default_generated_parent VALUES (0), (1);`,
				`INSERT INTO fk_delete_default_generated_child (id, parent_id) VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_delete_default_generated_parent WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, parent_id, parent_marker FROM fk_delete_default_generated_child;`,
					Expected: []sql.Row{{1, 0, 10}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetDefaultRecomputesGeneratedColumnsRepro reproduces a
// data consistency bug: referential ON UPDATE SET DEFAULT rewrites must
// recompute stored generated columns.
func TestForeignKeyOnUpdateSetDefaultRecomputesGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET DEFAULT recomputes generated columns",
			SetUpScript: []string{
				`CREATE TABLE fk_update_default_generated_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_default_generated_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 REFERENCES fk_update_default_generated_parent(id)
						ON UPDATE SET DEFAULT,
					parent_marker INT GENERATED ALWAYS AS (parent_id + 10) STORED
				);`,
				`INSERT INTO fk_update_default_generated_parent VALUES (0), (1);`,
				`INSERT INTO fk_update_default_generated_child (id, parent_id) VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_default_generated_parent SET id = 2 WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, parent_id, parent_marker FROM fk_update_default_generated_child;`,
					Expected: []sql.Row{{1, 0, 10}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateCascadeRecomputesGeneratedColumnsRepro reproduces a
// data consistency bug: referential ON UPDATE CASCADE rewrites must recompute
// stored generated columns.
func TestForeignKeyOnUpdateCascadeRecomputesGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE CASCADE recomputes generated columns",
			SetUpScript: []string{
				`CREATE TABLE fk_update_cascade_generated_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_cascade_generated_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_update_cascade_generated_parent(id)
						ON UPDATE CASCADE,
					parent_marker INT GENERATED ALWAYS AS (parent_id + 10) STORED
				);`,
				`INSERT INTO fk_update_cascade_generated_parent VALUES (1);`,
				`INSERT INTO fk_update_cascade_generated_child (id, parent_id) VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_cascade_generated_parent SET id = 3 WHERE id = 1;`,
				},
				{
					Query:    `SELECT id, parent_id, parent_marker FROM fk_update_cascade_generated_child;`,
					Expected: []sql.Row{{1, 3, 13}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateCascadeFiresChildUpdateTriggersRepro verifies that
// referential ON UPDATE CASCADE rewrites fire row-level UPDATE triggers on the
// child table.
func TestForeignKeyOnUpdateCascadeFiresChildUpdateTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE CASCADE fires child UPDATE triggers",
			SetUpScript: []string{
				`CREATE TABLE fk_update_cascade_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_cascade_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_update_cascade_trigger_parent(id)
						ON UPDATE CASCADE
				);`,
				`CREATE TABLE fk_update_cascade_trigger_audit (
					child_id INT,
					old_parent_id INT,
					new_parent_id INT
				);`,
				`CREATE FUNCTION log_fk_update_cascade_child_update() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO fk_update_cascade_trigger_audit
						VALUES (OLD.id, OLD.parent_id, NEW.parent_id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER fk_update_cascade_child_after_update
					AFTER UPDATE ON fk_update_cascade_trigger_child
					FOR EACH ROW EXECUTE FUNCTION log_fk_update_cascade_child_update();`,
				`INSERT INTO fk_update_cascade_trigger_parent VALUES (1);`,
				`INSERT INTO fk_update_cascade_trigger_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_cascade_trigger_parent SET id = 3 WHERE id = 1;`,
				},
				{
					Query: `SELECT child_id, old_parent_id, new_parent_id
						FROM fk_update_cascade_trigger_audit;`,
					Expected: []sql.Row{{1, 1, 3}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_cascade_trigger_child;`,
					Expected: []sql.Row{{1, 3}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteCascadeFiresChildDeleteTriggersRepro verifies that
// referential ON DELETE CASCADE rewrites fire row-level DELETE triggers on the
// child table.
func TestForeignKeyOnDeleteCascadeFiresChildDeleteTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE CASCADE fires child DELETE triggers",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_cascade_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_cascade_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_delete_cascade_trigger_parent(id)
						ON DELETE CASCADE
				);`,
				`CREATE TABLE fk_delete_cascade_trigger_audit (
					child_id INT,
					old_parent_id INT
				);`,
				`CREATE FUNCTION log_fk_delete_cascade_child_delete() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO fk_delete_cascade_trigger_audit
						VALUES (OLD.id, OLD.parent_id);
					RETURN OLD;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER fk_delete_cascade_child_after_delete
					AFTER DELETE ON fk_delete_cascade_trigger_child
					FOR EACH ROW EXECUTE FUNCTION log_fk_delete_cascade_child_delete();`,
				`INSERT INTO fk_delete_cascade_trigger_parent VALUES (1);`,
				`INSERT INTO fk_delete_cascade_trigger_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_delete_cascade_trigger_parent WHERE id = 1;`,
				},
				{
					Query: `SELECT child_id, old_parent_id
						FROM fk_delete_cascade_trigger_audit;`,
					Expected: []sql.Row{{1, 1}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_cascade_trigger_child;`,
					Expected: []sql.Row{},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetNullFiresChildUpdateTriggersRepro verifies that
// referential ON DELETE SET NULL rewrites fire row-level UPDATE triggers on the
// child table.
func TestForeignKeyOnDeleteSetNullFiresChildUpdateTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET NULL fires child UPDATE triggers",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_null_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_null_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_delete_null_trigger_parent(id)
						ON DELETE SET NULL
				);`,
				`CREATE TABLE fk_delete_null_trigger_audit (
					child_id INT,
					old_parent_id INT,
					new_parent_id INT
				);`,
				`CREATE FUNCTION log_fk_delete_null_child_update() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO fk_delete_null_trigger_audit
						VALUES (OLD.id, OLD.parent_id, NEW.parent_id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER fk_delete_null_child_after_update
					AFTER UPDATE ON fk_delete_null_trigger_child
					FOR EACH ROW EXECUTE FUNCTION log_fk_delete_null_child_update();`,
				`INSERT INTO fk_delete_null_trigger_parent VALUES (1);`,
				`INSERT INTO fk_delete_null_trigger_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_delete_null_trigger_parent WHERE id = 1;`,
				},
				{
					Query: `SELECT child_id, old_parent_id, new_parent_id
						FROM fk_delete_null_trigger_audit;`,
					Expected: []sql.Row{{1, 1, nil}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_null_trigger_child;`,
					Expected: []sql.Row{{1, nil}},
				},
			},
		},
	})
}

// TestForeignKeyOnDeleteSetDefaultFiresChildUpdateTriggersRepro verifies that
// referential ON DELETE SET DEFAULT rewrites fire row-level UPDATE triggers on
// the child table.
func TestForeignKeyOnDeleteSetDefaultFiresChildUpdateTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON DELETE SET DEFAULT fires child UPDATE triggers",
			SetUpScript: []string{
				`CREATE TABLE fk_delete_default_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_delete_default_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 REFERENCES fk_delete_default_trigger_parent(id)
						ON DELETE SET DEFAULT
				);`,
				`CREATE TABLE fk_delete_default_trigger_audit (
					child_id INT,
					old_parent_id INT,
					new_parent_id INT
				);`,
				`CREATE FUNCTION log_fk_delete_default_child_update() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO fk_delete_default_trigger_audit
						VALUES (OLD.id, OLD.parent_id, NEW.parent_id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER fk_delete_default_child_after_update
					AFTER UPDATE ON fk_delete_default_trigger_child
					FOR EACH ROW EXECUTE FUNCTION log_fk_delete_default_child_update();`,
				`INSERT INTO fk_delete_default_trigger_parent VALUES (0), (1);`,
				`INSERT INTO fk_delete_default_trigger_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DELETE FROM fk_delete_default_trigger_parent WHERE id = 1;`,
				},
				{
					Query: `SELECT child_id, old_parent_id, new_parent_id
						FROM fk_delete_default_trigger_audit;`,
					Expected: []sql.Row{{1, 1, 0}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_delete_default_trigger_child;`,
					Expected: []sql.Row{{1, 0}},
				},
			},
		},
	})
}

// TestForeignKeyOnUpdateSetDefaultFiresChildUpdateTriggersRepro verifies that
// referential ON UPDATE SET DEFAULT rewrites fire row-level UPDATE triggers on
// the child table.
func TestForeignKeyOnUpdateSetDefaultFiresChildUpdateTriggersRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON UPDATE SET DEFAULT fires child UPDATE triggers",
			SetUpScript: []string{
				`CREATE TABLE fk_update_default_trigger_parent (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE fk_update_default_trigger_child (
					id INT PRIMARY KEY,
					parent_id INT DEFAULT 0 REFERENCES fk_update_default_trigger_parent(id)
						ON UPDATE SET DEFAULT
				);`,
				`CREATE TABLE fk_update_default_trigger_audit (
					child_id INT,
					old_parent_id INT,
					new_parent_id INT
				);`,
				`CREATE FUNCTION log_fk_update_default_child_update() RETURNS TRIGGER AS $$
				BEGIN
					INSERT INTO fk_update_default_trigger_audit
						VALUES (OLD.id, OLD.parent_id, NEW.parent_id);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE TRIGGER fk_update_default_child_after_update
					AFTER UPDATE ON fk_update_default_trigger_child
					FOR EACH ROW EXECUTE FUNCTION log_fk_update_default_child_update();`,
				`INSERT INTO fk_update_default_trigger_parent VALUES (0), (1);`,
				`INSERT INTO fk_update_default_trigger_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE fk_update_default_trigger_parent SET id = 2 WHERE id = 1;`,
				},
				{
					Query: `SELECT child_id, old_parent_id, new_parent_id
						FROM fk_update_default_trigger_audit;`,
					Expected: []sql.Row{{1, 1, 0}},
				},
				{
					Query:    `SELECT id, parent_id FROM fk_update_default_trigger_child;`,
					Expected: []sql.Row{{1, 0}},
				},
			},
		},
	})
}

// TestDropReferencedTableRequiresCascadeRepro guards dependency enforcement for
// tables referenced by foreign keys.
func TestDropReferencedTableRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE rejects referenced tables",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_drop_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_drop_parent(id)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP TABLE fk_drop_parent;`,
					ExpectedErr: `referenced in foreign key`,
				},
			},
		},
	})
}

// TestDropReferencedTableCascadeDropsForeignKeyRepro reproduces a dependency
// correctness bug: PostgreSQL's CASCADE option drops dependent foreign-key
// constraints while preserving the referencing table.
func TestDropReferencedTableCascadeDropsForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TABLE CASCADE drops dependent foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_cascade_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_drop_cascade_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_drop_cascade_parent(id)
				);`,
				`INSERT INTO fk_drop_cascade_parent VALUES (1);`,
				`INSERT INTO fk_drop_cascade_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TABLE fk_drop_cascade_parent CASCADE;`,
				},
				{
					Query: `SELECT to_regclass('fk_drop_cascade_parent') IS NULL,
							to_regclass('fk_drop_cascade_child') IS NOT NULL,
							count(*) = 0
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'fk_drop_cascade_child'::regclass
							AND contype = 'f';`,
					Expected: []sql.Row{{"t", "t", "t"}},
				},
				{
					Query: `INSERT INTO fk_drop_cascade_child VALUES (2, 2);`,
				},
				{
					Query: `SELECT id, parent_id
						FROM fk_drop_cascade_child
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}, {2, 2}},
				},
			},
		},
	})
}

// TestDropReferencedColumnRequiresCascadeRepro reproduces a dependency
// correctness bug: Doltgres lets ALTER TABLE DROP COLUMN remove a key column
// that is still referenced by a foreign key.
func TestDropReferencedColumnRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP COLUMN rejects referenced key columns",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_column_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_drop_column_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_drop_column_parent(id)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE fk_drop_column_parent DROP COLUMN id;`,
					ExpectedErr: `depends on`,
				},
			},
		},
	})
}

// TestDropReferencedPrimaryKeyConstraintRequiresCascadeRepro reproduces a
// dependency correctness bug: Doltgres lets ALTER TABLE DROP CONSTRAINT remove
// a primary-key constraint that is still referenced by a foreign key.
func TestDropReferencedPrimaryKeyConstraintRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONSTRAINT rejects referenced primary keys",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_constraint_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_drop_constraint_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_drop_constraint_parent(id)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE fk_drop_constraint_parent DROP CONSTRAINT fk_drop_constraint_parent_pkey;`,
					ExpectedErr: `foreign key`,
				},
			},
		},
	})
}

// TestDropReferencedPrimaryKeyConstraintCascadeDropsForeignKeyRepro reproduces
// a dependency correctness bug: PostgreSQL's CASCADE option drops dependent
// foreign-key constraints when a referenced primary key is dropped.
func TestDropReferencedPrimaryKeyConstraintCascadeDropsForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONSTRAINT CASCADE drops dependent foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_constraint_cascade_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_drop_constraint_cascade_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_drop_constraint_cascade_parent(id)
				);`,
				`INSERT INTO fk_drop_constraint_cascade_parent VALUES (1);`,
				`INSERT INTO fk_drop_constraint_cascade_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE fk_drop_constraint_cascade_parent
						DROP CONSTRAINT fk_drop_constraint_cascade_parent_pkey CASCADE;`,
				},
				{
					Query: `SELECT count(*) = 0
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'fk_drop_constraint_cascade_child'::regclass
							AND contype = 'f';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `INSERT INTO fk_drop_constraint_cascade_child VALUES (2, 2);`,
				},
				{
					Query: `SELECT id, parent_id
						FROM fk_drop_constraint_cascade_child
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}, {2, 2}},
				},
			},
		},
	})
}

// TestDropReferencedUniqueConstraintRequiresCascadeRepro guards dependency
// enforcement for unique constraints referenced by foreign keys.
func TestDropReferencedUniqueConstraintRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONSTRAINT rejects referenced unique keys",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_unique_parent (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`CREATE TABLE fk_drop_unique_child (
					id INT PRIMARY KEY,
					parent_code INT REFERENCES fk_drop_unique_parent(code)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE fk_drop_unique_parent DROP CONSTRAINT fk_drop_unique_parent_code_key;`,
					ExpectedErr: `foreign key`,
				},
			},
		},
	})
}

// TestDropReferencedUniqueConstraintCascadeDropsForeignKeyRepro reproduces a
// dependency correctness bug: PostgreSQL's CASCADE option drops dependent
// foreign-key constraints when a referenced unique constraint is dropped.
func TestDropReferencedUniqueConstraintCascadeDropsForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP unique CONSTRAINT CASCADE drops dependent foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_unique_cascade_parent (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`CREATE TABLE fk_drop_unique_cascade_child (
					id INT PRIMARY KEY,
					parent_code INT REFERENCES fk_drop_unique_cascade_parent(code)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE fk_drop_unique_cascade_parent
						DROP CONSTRAINT fk_drop_unique_cascade_parent_code_key CASCADE;`,
				},
				{
					Query: `SELECT count(*) = 0
						FROM pg_catalog.pg_constraint
						WHERE conrelid = 'fk_drop_unique_cascade_child'::regclass
							AND contype = 'f';`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query: `INSERT INTO fk_drop_unique_cascade_child VALUES (1, 999);`,
				},
			},
		},
	})
}

// TestDropReferencedUniqueIndexRequiresCascadeRepro guards dependency
// enforcement for unique indexes referenced by foreign keys.
func TestDropReferencedUniqueIndexRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP INDEX rejects referenced unique indexes",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_index_parent (
					id INT PRIMARY KEY,
					code INT UNIQUE
				);`,
				`CREATE TABLE fk_drop_index_child (
					id INT PRIMARY KEY,
					parent_code INT REFERENCES fk_drop_index_parent(code)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP INDEX fk_drop_index_parent_code_key;`,
					ExpectedErr: `constraint`,
				},
			},
		},
	})
}

// TestDropReferencedStandaloneUniqueIndexRequiresCascadeRepro reproduces a
// dependency correctness bug: PostgreSQL refuses to drop a standalone unique
// index that a foreign-key constraint depends on unless CASCADE is specified.
func TestDropReferencedStandaloneUniqueIndexRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP INDEX rejects referenced standalone unique indexes",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_index_restrict_parent (
					id INT PRIMARY KEY,
					code INT NOT NULL
				);`,
				`CREATE UNIQUE INDEX fk_drop_index_restrict_parent_code_idx
					ON fk_drop_index_restrict_parent (code);`,
				`CREATE TABLE fk_drop_index_restrict_child (
					id INT PRIMARY KEY,
					parent_code INT REFERENCES fk_drop_index_restrict_parent(code)
				);`,
				`INSERT INTO fk_drop_index_restrict_parent VALUES (1, 7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP INDEX fk_drop_index_restrict_parent_code_idx;`,
					ExpectedErr: `depends on`,
				},
				{
					Query: `SELECT to_regclass('fk_drop_index_restrict_parent_code_idx') IS NOT NULL,
						EXISTS (
							SELECT 1
							FROM pg_constraint
							WHERE conrelid = 'fk_drop_index_restrict_child'::regclass
							  AND contype = 'f'
						);`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query:       `INSERT INTO fk_drop_index_restrict_parent VALUES (2, 7);`,
					ExpectedErr: `duplicate`,
				},
			},
		},
	})
}

// TestDropReferencedUniqueIndexCascadeDropsForeignKeyRepro reproduces a
// dependency correctness bug: PostgreSQL DROP INDEX ... CASCADE on a referenced
// standalone unique index removes dependent foreign-key constraints.
func TestDropReferencedUniqueIndexCascadeDropsForeignKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP INDEX CASCADE drops dependent foreign keys",
			SetUpScript: []string{
				`CREATE TABLE fk_drop_index_cascade_parent (
					id INT PRIMARY KEY,
					code INT NOT NULL
				);`,
				`CREATE UNIQUE INDEX fk_drop_index_cascade_parent_code_idx
					ON fk_drop_index_cascade_parent (code);`,
				`CREATE TABLE fk_drop_index_cascade_child (
					id INT PRIMARY KEY,
					parent_code INT REFERENCES fk_drop_index_cascade_parent(code)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP INDEX fk_drop_index_cascade_parent_code_idx CASCADE;`,
				},
				{
					Query: `SELECT to_regclass('fk_drop_index_cascade_parent_code_idx') IS NULL,
						NOT EXISTS (
							SELECT 1
							FROM pg_constraint
							WHERE conrelid = 'fk_drop_index_cascade_child'::regclass
							  AND contype = 'f'
						);`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query: `INSERT INTO fk_drop_index_cascade_child VALUES (1, 999);`,
				},
			},
		},
	})
}

// TestDropForeignKeyConstraintWithExplicitSchemaRepro reproduces a dependency
// correctness bug: ALTER TABLE with an explicitly schema-qualified child table
// should drop the foreign-key constraint even when the schema is not on the
// search path.
func TestDropForeignKeyConstraintWithExplicitSchemaRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP CONSTRAINT resolves explicitly schema-qualified child table",
			SetUpScript: []string{
				`CREATE SCHEMA explicit_fk_parent;`,
				`CREATE SCHEMA explicit_fk_child;`,
				`CREATE TABLE explicit_fk_parent.parent_items (
					id INT PRIMARY KEY
				);`,
				`CREATE TABLE explicit_fk_child.child_items (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES explicit_fk_parent.parent_items(id)
				);`,
				`INSERT INTO explicit_fk_parent.parent_items VALUES (1);`,
				`INSERT INTO explicit_fk_child.child_items VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `INSERT INTO explicit_fk_child.child_items VALUES (2, 2);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `ALTER TABLE explicit_fk_child.child_items
						DROP CONSTRAINT child_items_parent_id_fkey;`,
				},
				{
					Query: `INSERT INTO explicit_fk_child.child_items VALUES (2, 2);`,
				},
				{
					Query: `SELECT id, parent_id
						FROM explicit_fk_child.child_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 1},
						{2, 2},
					},
				},
			},
		},
	})
}

// TestTruncateReferencedTableRequiresCascadeRepro guards TRUNCATE dependency
// enforcement for tables referenced by foreign keys.
func TestTruncateReferencedTableRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE rejects referenced tables",
			SetUpScript: []string{
				`CREATE TABLE fk_truncate_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_truncate_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_truncate_parent(id)
				);`,
				`INSERT INTO fk_truncate_parent VALUES (1);`,
				`INSERT INTO fk_truncate_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `TRUNCATE fk_truncate_parent;`,
					ExpectedErr: `foreign key`,
				},
			},
		},
	})
}

// TestTruncateReferencedTableCascadeTruncatesChildrenRepro guards TRUNCATE
// dependency enforcement: PostgreSQL's CASCADE option truncates dependent
// referencing tables as part of the same operation.
func TestTruncateReferencedTableCascadeTruncatesChildrenRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "TRUNCATE CASCADE truncates referencing tables",
			SetUpScript: []string{
				`CREATE TABLE fk_truncate_cascade_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_truncate_cascade_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_truncate_cascade_parent(id)
				);`,
				`INSERT INTO fk_truncate_cascade_parent VALUES (1);`,
				`INSERT INTO fk_truncate_cascade_child VALUES (1, 1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `TRUNCATE fk_truncate_cascade_parent CASCADE;`,
				},
				{
					Query: `SELECT
							(SELECT count(*) FROM fk_truncate_cascade_parent),
							(SELECT count(*) FROM fk_truncate_cascade_child);`,
					Expected: []sql.Row{{0, 0}},
				},
			},
		},
	})
}

// TestAlterReferencedColumnTypeRequiresCascadeRepro guards type-change
// dependency enforcement for foreign-key referenced columns.
func TestAlterReferencedColumnTypeRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE rejects referenced key columns",
			SetUpScript: []string{
				`CREATE TABLE fk_alter_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_alter_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_alter_parent(id)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE fk_alter_parent ALTER COLUMN id TYPE TEXT;`,
					ExpectedErr: `foreign key`,
				},
			},
		},
	})
}

// TestAlterReferencingColumnTypeRequiresCascadeRepro guards type-change
// dependency enforcement for foreign-key referencing columns.
func TestAlterReferencingColumnTypeRequiresCascadeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER COLUMN TYPE rejects referencing columns",
			SetUpScript: []string{
				`CREATE TABLE fk_alter_child_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_alter_child_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_alter_child_parent(id)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `ALTER TABLE fk_alter_child_child ALTER COLUMN parent_id TYPE TEXT;`,
					ExpectedErr: `used in key specification`,
				},
			},
		},
	})
}

// TestRenameReferencedTableKeepsForeignKeyUsableRepro guards foreign-key
// dependency rewrites when the referenced table is renamed.
func TestRenameReferencedTableKeepsForeignKeyUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME TABLE preserves foreign key enforcement",
			SetUpScript: []string{
				`CREATE TABLE fk_rename_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_rename_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_rename_parent(id)
				);`,
				`INSERT INTO fk_rename_parent VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE fk_rename_parent RENAME TO fk_rename_parent_new;`,
				},
				{
					Query: `INSERT INTO fk_rename_child VALUES (1, 1);`,
				},
				{
					Query:       `INSERT INTO fk_rename_child VALUES (2, 2);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `SELECT id, parent_id
						FROM fk_rename_child
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}

// TestRenameReferencedColumnKeepsForeignKeyUsableRepro guards foreign-key
// dependency rewrites when the referenced column is renamed.
func TestRenameReferencedColumnKeepsForeignKeyUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "RENAME COLUMN preserves foreign key enforcement",
			SetUpScript: []string{
				`CREATE TABLE fk_rename_column_parent (id INT PRIMARY KEY);`,
				`CREATE TABLE fk_rename_column_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_rename_column_parent(id)
				);`,
				`INSERT INTO fk_rename_column_parent VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TABLE fk_rename_column_parent RENAME COLUMN id TO renamed_id;`,
				},
				{
					Query: `INSERT INTO fk_rename_column_child VALUES (1, 1);`,
				},
				{
					Query:       `INSERT INTO fk_rename_column_child VALUES (2, 2);`,
					ExpectedErr: `Foreign key violation`,
				},
				{
					Query: `SELECT id, parent_id
						FROM fk_rename_column_child
						ORDER BY id;`,
					Expected: []sql.Row{{1, 1}},
				},
			},
		},
	})
}
