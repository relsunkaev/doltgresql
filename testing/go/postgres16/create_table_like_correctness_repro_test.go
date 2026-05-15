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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestCreateTableLikeIncludingDefaultsCopiesDefaultsRepro reproduces a schema
// correctness bug: PostgreSQL CREATE TABLE LIKE INCLUDING DEFAULTS copies
// column defaults into the new table.
func TestCreateTableLikeIncludingDefaultsCopiesDefaultsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE LIKE INCLUDING DEFAULTS copies defaults",
			SetUpScript: []string{
				`CREATE TABLE like_defaults_source (
					id INT PRIMARY KEY,
					label TEXT DEFAULT 'copied'
				);`,
				`CREATE TABLE like_defaults_copy (
					LIKE like_defaults_source INCLUDING DEFAULTS
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO like_defaults_copy (id) VALUES (1);`,
				},
				{
					Query: `SELECT id, label FROM like_defaults_copy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-like-correctness-repro-test-testcreatetablelikeincludingdefaultscopiesdefaultsrepro-0001-select-id-label-from-like_defaults_copy"},
				},
			},
		},
	})
}

// TestCreateTableLikeExcludesDefaultsByDefaultRepro reproduces a schema
// correctness bug: column defaults are not copied unless INCLUDING DEFAULTS is
// specified.
func TestCreateTableLikeExcludesDefaultsByDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE LIKE excludes defaults by default",
			SetUpScript: []string{
				`CREATE TABLE like_no_defaults_source (
					id INT PRIMARY KEY,
					label TEXT DEFAULT 'source default'
				);`,
				`CREATE TABLE like_no_defaults_copy (
					LIKE like_no_defaults_source
				);`,
				`INSERT INTO like_no_defaults_copy (id) VALUES (1);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT id, label FROM like_no_defaults_copy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-like-correctness-repro-test-testcreatetablelikeexcludesdefaultsbydefaultrepro-0001-select-id-label-from-like_no_defaults_copy"},
				},
			},
		},
	})
}

// TestCreateTableLikeExcludesCheckConstraintsByDefaultRepro reproduces a schema
// correctness bug: CHECK constraints are not copied unless INCLUDING
// CONSTRAINTS is specified.
func TestCreateTableLikeExcludesCheckConstraintsByDefaultRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE like_no_constraints_source (
			id INT PRIMARY KEY,
			amount INT CHECK (amount > 0)
		);`,
		`CREATE TABLE like_no_constraints_copy (
			LIKE like_no_constraints_source
		);`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `INSERT INTO like_no_constraints_copy VALUES (1, -10);`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, amount FROM like_no_constraints_copy;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), int64(-10)}}, actual)
}

// TestCreateTableLikeExcludesIndexesByDefaultRepro reproduces a schema
// correctness bug: indexes and unique constraints are not copied unless
// INCLUDING INDEXES is specified.
func TestCreateTableLikeExcludesIndexesByDefaultRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE like_no_indexes_source (
			id INT PRIMARY KEY,
			code INT UNIQUE
		);`,
		`CREATE TABLE like_no_indexes_copy (
			LIKE like_no_indexes_source
		);`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `INSERT INTO like_no_indexes_copy VALUES (1, 7), (2, 7);`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, code
		FROM like_no_indexes_copy
		ORDER BY id;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), int64(7)}, {int64(2), int64(7)}}, actual)
}

// TestCreateTableLikeIncludingIdentityCopiesIdentityRepro reproduces a schema
// correctness bug: PostgreSQL CREATE TABLE LIKE INCLUDING IDENTITY copies
// identity generation into the new table.
func TestCreateTableLikeIncludingIdentityCopiesIdentityRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE LIKE INCLUDING IDENTITY copies identity",
			SetUpScript: []string{
				`CREATE TABLE like_identity_source (
					id BIGINT GENERATED BY DEFAULT AS IDENTITY,
					label TEXT
				);`,
				`CREATE TABLE like_identity_copy (
					LIKE like_identity_source INCLUDING IDENTITY
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO like_identity_copy (label) VALUES ('copied identity');`,
				},
				{
					Query: `SELECT id, label FROM like_identity_copy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-like-correctness-repro-test-testcreatetablelikeincludingidentitycopiesidentityrepro-0001-select-id-label-from-like_identity_copy"},
				},
			},
		},
	})
}

// TestCreateTableLikeExcludesIdentityByDefaultRepro reproduces a schema
// correctness bug: identity generation is not copied unless INCLUDING IDENTITY
// is specified.
func TestCreateTableLikeExcludesIdentityByDefaultRepro(t *testing.T) {
	ctx, conn, controller := CreateServer(t, "postgres")
	t.Cleanup(func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	for _, stmt := range []string{
		`CREATE TABLE like_no_identity_source (
			id BIGINT GENERATED ALWAYS AS IDENTITY,
			label TEXT
		);`,
		`CREATE TABLE like_no_identity_copy (
			LIKE like_no_identity_source
		);`,
	} {
		_, err := conn.Current.Exec(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	_, err := conn.Current.Exec(ctx, `INSERT INTO like_no_identity_copy (id, label) VALUES (100, 'explicit id');`)
	require.NoError(t, err)

	rows, err := conn.Current.Query(ctx, `SELECT id, label FROM like_no_identity_copy;`)
	require.NoError(t, err)
	actual, _, err := ReadRows(rows, true)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(100), "explicit id"}}, actual)
}

// TestCreateTableLikeIncludingGeneratedCopiesGeneratedColumnsRepro reproduces a
// schema correctness bug: PostgreSQL CREATE TABLE LIKE INCLUDING GENERATED
// preserves stored generated column expressions in the new table.
func TestCreateTableLikeIncludingGeneratedCopiesGeneratedColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE LIKE INCLUDING GENERATED copies generated columns",
			SetUpScript: []string{
				`CREATE TABLE like_generated_source (
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`CREATE TABLE like_generated_copy (
					LIKE like_generated_source INCLUDING GENERATED
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO like_generated_copy (base_value) VALUES (7);`,
				},
				{
					Query: `SELECT base_value, doubled FROM like_generated_copy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-like-correctness-repro-test-testcreatetablelikeincludinggeneratedcopiesgeneratedcolumnsrepro-0001-select-base_value-doubled-from-like_generated_copy"},
				},
			},
		},
	})
}

// TestCreateTableLikeExcludesGeneratedByDefaultRepro reproduces a schema
// correctness bug: generated column expressions are not copied unless INCLUDING
// GENERATED is specified.
func TestCreateTableLikeExcludesGeneratedByDefaultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TABLE LIKE excludes generated expressions by default",
			SetUpScript: []string{
				`CREATE TABLE like_no_generated_source (
					base_value INT,
					doubled INT GENERATED ALWAYS AS (base_value * 2) STORED
				);`,
				`CREATE TABLE like_no_generated_copy (
					LIKE like_no_generated_source
				);`,
				`INSERT INTO like_no_generated_copy (base_value) VALUES (7);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT base_value, doubled FROM like_no_generated_copy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-table-like-correctness-repro-test-testcreatetablelikeexcludesgeneratedbydefaultrepro-0001-select-base_value-doubled-from-like_no_generated_copy"},
				},
			},
		},
	})
}
