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

// TestCreateIndexConcurrentlyRejectsTransactionBlockRepro reproduces a DDL
// transaction-boundary bug: PostgreSQL rejects CREATE INDEX CONCURRENTLY inside
// an explicit transaction block.
func TestCreateIndexConcurrentlyRejectsTransactionBlockRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE INDEX CONCURRENTLY rejects transaction block",
			SetUpScript: []string{
				`CREATE TABLE concurrent_index_tx_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            `BEGIN;`,
					SkipResultsCheck: true,
				},
				{
					Query: `CREATE INDEX CONCURRENTLY concurrent_index_tx_items_label_idx
						ON concurrent_index_tx_items (label);`,
					ExpectedErr: `cannot run inside a transaction block`,
				},
				{
					Query:            `ROLLBACK;`,
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT count(*)::TEXT
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'concurrent_index_tx_items'
							AND indexname = 'concurrent_index_tx_items_label_idx';`,
					Expected: []sql.Row{{"0"}},
				},
			},
		},
	})
}

// TestDropIndexConcurrentlyRejectsTransactionBlockRepro reproduces a DDL
// transaction-boundary bug: PostgreSQL rejects DROP INDEX CONCURRENTLY inside
// an explicit transaction block.
func TestDropIndexConcurrentlyRejectsTransactionBlockRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP INDEX CONCURRENTLY rejects transaction block",
			SetUpScript: []string{
				`CREATE TABLE drop_concurrent_index_tx_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`CREATE INDEX drop_concurrent_index_tx_items_label_idx
					ON drop_concurrent_index_tx_items (label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:            `BEGIN;`,
					SkipResultsCheck: true,
				},
				{
					Query:       `DROP INDEX CONCURRENTLY drop_concurrent_index_tx_items_label_idx;`,
					ExpectedErr: `cannot run inside a transaction block`,
				},
				{
					Query:            `ROLLBACK;`,
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT count(*)::TEXT
						FROM pg_catalog.pg_indexes
						WHERE tablename = 'drop_concurrent_index_tx_items'
							AND indexname = 'drop_concurrent_index_tx_items_label_idx';`,
					Expected: []sql.Row{{"1"}},
				},
			},
		},
	})
}

// TestClusterMarksIndexClusteredRepro reproduces a catalog correctness bug:
// PostgreSQL records the clustered index in pg_index.indisclustered after
// CLUSTER index ON table.
func TestClusterMarksIndexClusteredRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CLUSTER marks selected index as clustered",
			SetUpScript: []string{
				`CREATE TABLE cluster_metadata_items (
					id INT PRIMARY KEY,
					label TEXT
				);`,
				`INSERT INTO cluster_metadata_items VALUES
					(1, 'beta'),
					(2, 'alpha');`,
				`CREATE INDEX cluster_metadata_items_label_idx
					ON cluster_metadata_items (label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CLUSTER cluster_metadata_items_label_idx
						ON cluster_metadata_items;`,
				},
				{
					Query: `SELECT c.relname
						FROM pg_catalog.pg_index i
						JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
						JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
						WHERE t.relname = 'cluster_metadata_items'
							AND i.indisclustered
						ORDER BY c.relname;`,
					Expected: []sql.Row{{"cluster_metadata_items_label_idx"}},
				},
			},
		},
	})
}

// TestPartialUniqueIndexEnforcesPredicateRepro guards PostgreSQL partial unique
// index semantics: uniqueness applies only to rows matching the index predicate.
func TestPartialUniqueIndexEnforcesPredicateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "partial unique index enforces predicate",
			SetUpScript: []string{
				`CREATE TABLE partial_unique_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					active BOOL NOT NULL
				);`,
				`CREATE UNIQUE INDEX partial_unique_items_active_code_idx
					ON partial_unique_items (code)
					WHERE active;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_unique_items VALUES
						(1, 10, true),
						(2, 10, false),
						(3, 10, false);`,
				},
				{
					Query:       `INSERT INTO partial_unique_items VALUES (4, 10, true);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code, active
						FROM partial_unique_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10, "t"},
						{2, 10, "f"},
						{3, 10, "f"},
					},
				},
			},
		},
	})
}

// TestOnConflictUsesPartialUniqueIndexPredicateRepro guards PostgreSQL upsert
// inference for partial unique indexes when the conflict target includes the
// matching index predicate.
func TestOnConflictUsesPartialUniqueIndexPredicateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT infers partial unique index predicate",
			SetUpScript: []string{
				`CREATE TABLE partial_upsert_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					active BOOL NOT NULL,
					note TEXT NOT NULL
				);`,
				`CREATE UNIQUE INDEX partial_upsert_items_active_code_idx
					ON partial_upsert_items (code)
					WHERE active;`,
				`INSERT INTO partial_upsert_items VALUES
					(1, 10, true, 'old-active'),
					(2, 10, false, 'inactive');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_upsert_items VALUES (3, 10, true, 'new-active')
						ON CONFLICT (code) WHERE active
						DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, active, note
						FROM partial_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{
						{1, 10, "t", "new-active"},
						{2, 10, "f", "inactive"},
					},
				},
			},
		},
	})
}

// TestOnConflictWithoutPredicateRejectsPartialUniqueIndexRepro guards
// PostgreSQL unique-index inference: a partial unique index is not a valid
// arbiter for rows outside its predicate unless the conflict target includes a
// compatible predicate.
func TestOnConflictWithoutPredicateRejectsPartialUniqueIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT without predicate rejects partial unique index",
			SetUpScript: []string{
				`CREATE TABLE partial_upsert_missing_predicate_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					active BOOL NOT NULL
				);`,
				`CREATE UNIQUE INDEX partial_upsert_missing_predicate_code_idx
					ON partial_upsert_missing_predicate_items (code)
					WHERE active;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_upsert_missing_predicate_items VALUES (1, 10, false)
						ON CONFLICT (code) DO NOTHING;`,
					ExpectedErr: `there is no unique or exclusion constraint matching the ON CONFLICT specification`,
				},
				{
					Query:    `SELECT COUNT(*) FROM partial_upsert_missing_predicate_items;`,
					Expected: []sql.Row{{0}},
				},
			},
		},
	})
}

// TestUniqueExpressionIndexEnforcesComputedValuesRepro guards PostgreSQL unique
// expression indexes rejecting rows whose computed index values conflict.
func TestUniqueExpressionIndexEnforcesComputedValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unique expression index enforces computed values",
			SetUpScript: []string{
				`CREATE TABLE unique_expression_items (
					id INT PRIMARY KEY,
					email TEXT NOT NULL
				);`,
				`CREATE UNIQUE INDEX unique_expression_items_lower_email_idx
					ON unique_expression_items ((lower(email)));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO unique_expression_items VALUES (1, 'User@Example.com');`,
				},
				{
					Query:       `INSERT INTO unique_expression_items VALUES (2, 'user@example.com');`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, email
						FROM unique_expression_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, "User@Example.com"}},
				},
			},
		},
	})
}

// TestOnConflictUsesUniqueExpressionIndexRepro reproduces an upsert
// correctness bug: PostgreSQL can infer a unique expression index as an ON
// CONFLICT arbiter.
func TestOnConflictUsesUniqueExpressionIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT infers unique expression index",
			SetUpScript: []string{
				`CREATE TABLE expression_upsert_items (
					id INT PRIMARY KEY,
					email TEXT NOT NULL,
					note TEXT NOT NULL
				);`,
				`CREATE UNIQUE INDEX expression_upsert_items_lower_email_idx
					ON expression_upsert_items ((lower(email)));`,
				`INSERT INTO expression_upsert_items VALUES (1, 'User@Example.com', 'old');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO expression_upsert_items VALUES (2, 'user@example.com', 'new')
						ON CONFLICT ((lower(email)))
						DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, email, note
						FROM expression_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, "User@Example.com", "new"}},
				},
			},
		},
	})
}

// TestOnConflictOnConstraintUsesNamedUniqueConstraintRepro guards PostgreSQL
// upsert inference through a named unique constraint.
func TestOnConflictOnConstraintUsesNamedUniqueConstraintRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT ON CONSTRAINT uses named unique constraint",
			SetUpScript: []string{
				`CREATE TABLE named_constraint_upsert_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					note TEXT NOT NULL,
					CONSTRAINT named_constraint_upsert_items_code_key UNIQUE (code)
				);`,
				`INSERT INTO named_constraint_upsert_items VALUES (1, 10, 'old');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO named_constraint_upsert_items VALUES (2, 10, 'new')
						ON CONFLICT ON CONSTRAINT named_constraint_upsert_items_code_key
						DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, note
						FROM named_constraint_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 10, "new"}},
				},
			},
		},
	})
}

// TestUniqueIndexIncludeColumnsDoNotAffectUniquenessRepro guards PostgreSQL
// unique indexes with INCLUDE columns enforcing uniqueness only on key columns,
// not included payload columns.
func TestUniqueIndexIncludeColumnsDoNotAffectUniquenessRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unique index include columns do not affect uniqueness",
			SetUpScript: []string{
				`CREATE TABLE unique_include_items (
					id INT PRIMARY KEY,
					code INT NOT NULL,
					label TEXT NOT NULL
				);`,
				`CREATE UNIQUE INDEX unique_include_items_code_idx
					ON unique_include_items (code)
					INCLUDE (label);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO unique_include_items VALUES (1, 10, 'first');`,
				},
				{
					Query:       `INSERT INTO unique_include_items VALUES (2, 10, 'second');`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code, label
						FROM unique_include_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, 10, "first"}},
				},
			},
		},
	})
}

// TestUniqueIndexNullsNotDistinctRejectsDuplicateNullsRepro guards PostgreSQL
// unique indexes declared NULLS NOT DISTINCT treating null key values as equal
// for uniqueness.
func TestUniqueIndexNullsNotDistinctRejectsDuplicateNullsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "unique index NULLS NOT DISTINCT rejects duplicate NULLs",
			SetUpScript: []string{
				`CREATE TABLE unique_nulls_not_distinct_items (
					id INT PRIMARY KEY,
					code INT
				);`,
				`CREATE UNIQUE INDEX unique_nulls_not_distinct_code_idx
					ON unique_nulls_not_distinct_items (code)
					NULLS NOT DISTINCT;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO unique_nulls_not_distinct_items VALUES (1, NULL);`,
				},
				{
					Query:       `INSERT INTO unique_nulls_not_distinct_items VALUES (2, NULL);`,
					ExpectedErr: `duplicate`,
				},
				{
					Query: `SELECT id, code
						FROM unique_nulls_not_distinct_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, nil}},
				},
			},
		},
	})
}

// TestOnConflictUsesNullsNotDistinctUniqueIndexRepro reproduces an ON CONFLICT
// correctness bug: PostgreSQL can infer a NULLS NOT DISTINCT unique index and
// route duplicate NULL key values through DO UPDATE.
func TestOnConflictUsesNullsNotDistinctUniqueIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT uses NULLS NOT DISTINCT unique index",
			SetUpScript: []string{
				`CREATE TABLE nulls_not_distinct_upsert_items (
					id INT PRIMARY KEY,
					code INT,
					label TEXT
				);`,
				`CREATE UNIQUE INDEX nulls_not_distinct_upsert_code_idx
					ON nulls_not_distinct_upsert_items (code)
					NULLS NOT DISTINCT;`,
				`INSERT INTO nulls_not_distinct_upsert_items VALUES (1, NULL, 'old');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO nulls_not_distinct_upsert_items VALUES (2, NULL, 'new')
						ON CONFLICT (code) DO UPDATE
						SET label = EXCLUDED.label
						RETURNING id, code, label;`,
					Expected: []sql.Row{{1, nil, "new"}},
				},
				{
					Query: `SELECT id, code, label
						FROM nulls_not_distinct_upsert_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, nil, "new"}},
				},
			},
		},
	})
}

// TestIndexDefinitionsRejectInvalidExpressionsRepro reproduces index
// correctness bugs where Doltgres accepts expressions PostgreSQL rejects in
// persisted index definitions.
func TestIndexDefinitionsRejectInvalidExpressionsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "index expression rejects set-returning functions",
			SetUpScript: []string{
				`CREATE TABLE index_srf_expression_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX index_srf_expression_items_idx
						ON index_srf_expression_items ((generate_series(1, 2)));`,
					ExpectedErr: `set-returning functions are not allowed in index expressions`,
				},
			},
		},
		{
			Name: "index expression rejects volatile functions",
			SetUpScript: []string{
				`CREATE TABLE index_volatile_expression_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX index_volatile_expression_items_idx
						ON index_volatile_expression_items ((random()));`,
					ExpectedErr: `functions in index expression must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "index expression rejects stable functions",
			SetUpScript: []string{
				`CREATE TABLE index_stable_expression_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX index_stable_expression_items_idx
						ON index_stable_expression_items ((now()));`,
					ExpectedErr: `functions in index expression must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "index expression rejects user-defined volatile functions",
			SetUpScript: []string{
				`CREATE TABLE index_udf_volatile_expression_items (
					id INT,
					v INT
				);`,
				`CREATE FUNCTION index_udf_volatile_value(input_value INT)
				RETURNS INT
				LANGUAGE SQL
				VOLATILE
				AS $$ SELECT input_value $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX index_udf_volatile_expression_items_idx
						ON index_udf_volatile_expression_items ((index_udf_volatile_value(v)));`,
					ExpectedErr: `functions in index expression must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "index expression rejects user-defined stable functions",
			SetUpScript: []string{
				`CREATE TABLE index_udf_stable_expression_items (
					id INT,
					v INT
				);`,
				`CREATE FUNCTION index_udf_stable_value(input_value INT)
				RETURNS INT
				LANGUAGE SQL
				STABLE
				AS $$ SELECT input_value $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX index_udf_stable_expression_items_idx
						ON index_udf_stable_expression_items ((index_udf_stable_value(v)));`,
					ExpectedErr: `functions in index expression must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "index expression rejects aggregate functions",
			SetUpScript: []string{
				`CREATE TABLE index_aggregate_expression_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX index_aggregate_expression_items_idx
						ON index_aggregate_expression_items ((avg(v)));`,
					ExpectedErr: `ERROR`,
				},
			},
		},
		{
			Name: "partial index predicate rejects set-returning functions",
			SetUpScript: []string{
				`CREATE TABLE partial_index_srf_predicate_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX partial_index_srf_predicate_items_idx
						ON partial_index_srf_predicate_items (id)
						WHERE generate_series(1, 2) > 0;`,
					ExpectedErr: `set-returning functions are not allowed in index predicates`,
				},
			},
		},
		{
			Name: "partial index predicate rejects volatile functions",
			SetUpScript: []string{
				`CREATE TABLE partial_index_volatile_predicate_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX partial_index_volatile_predicate_items_idx
						ON partial_index_volatile_predicate_items (id)
						WHERE random() > 0;`,
					ExpectedErr: `functions in index predicate must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "partial index predicate rejects stable functions",
			SetUpScript: []string{
				`CREATE TABLE partial_index_stable_predicate_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX partial_index_stable_predicate_items_idx
						ON partial_index_stable_predicate_items (id)
						WHERE now() IS NOT NULL;`,
					ExpectedErr: `functions in index predicate must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "partial index predicate rejects user-defined volatile functions",
			SetUpScript: []string{
				`CREATE TABLE partial_index_udf_volatile_predicate_items (
					id INT,
					v INT
				);`,
				`CREATE FUNCTION partial_index_udf_volatile_keep(input_value INT)
				RETURNS BOOL
				LANGUAGE SQL
				VOLATILE
				AS $$ SELECT input_value > 0 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX partial_index_udf_volatile_predicate_items_idx
						ON partial_index_udf_volatile_predicate_items (id)
						WHERE partial_index_udf_volatile_keep(v);`,
					ExpectedErr: `functions in index predicate must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "partial index predicate rejects user-defined stable functions",
			SetUpScript: []string{
				`CREATE TABLE partial_index_udf_stable_predicate_items (
					id INT,
					v INT
				);`,
				`CREATE FUNCTION partial_index_udf_stable_keep(input_value INT)
				RETURNS BOOL
				LANGUAGE SQL
				STABLE
				AS $$ SELECT input_value > 0 $$;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX partial_index_udf_stable_predicate_items_idx
						ON partial_index_udf_stable_predicate_items (id)
						WHERE partial_index_udf_stable_keep(v);`,
					ExpectedErr: `functions in index predicate must be marked IMMUTABLE`,
				},
			},
		},
		{
			Name: "partial index predicate rejects subqueries",
			SetUpScript: []string{
				`CREATE TABLE partial_index_subquery_predicate_items (
					id INT,
					v INT
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE INDEX partial_index_subquery_predicate_items_idx
						ON partial_index_subquery_predicate_items (id)
						WHERE id > (SELECT 0);`,
					ExpectedErr: `cannot use subquery in index predicate`,
				},
			},
		},
	})
}

// TestOnConflictDoNothingHandlesUniqueExpressionIndexRepro guards targetless
// ON CONFLICT DO NOTHING against conflicts raised by unique expression indexes.
func TestOnConflictDoNothingHandlesUniqueExpressionIndexRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT DO NOTHING handles unique expression index",
			SetUpScript: []string{
				`CREATE TABLE expression_do_nothing_items (
					id INT PRIMARY KEY,
					email TEXT NOT NULL
				);`,
				`CREATE UNIQUE INDEX expression_do_nothing_lower_email_idx
					ON expression_do_nothing_items ((lower(email)));`,
				`INSERT INTO expression_do_nothing_items VALUES (1, 'User@Example.com');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO expression_do_nothing_items VALUES (2, 'user@example.com')
						ON CONFLICT DO NOTHING;`,
				},
				{
					Query: `SELECT id, email
						FROM expression_do_nothing_items
						ORDER BY id;`,
					Expected: []sql.Row{{1, "User@Example.com"}},
				},
			},
		},
	})
}
