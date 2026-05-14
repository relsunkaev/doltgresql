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

	"github.com/stretchr/testify/require"
)

func TestPgGetFunctionSQLBodyExistsAndReturnsNullRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE FUNCTION pg_get_function_sqlbody_as_fn(a INT)
		RETURNS INT
		LANGUAGE SQL
		AS $$ SELECT a + 1 $$;`)
	require.NoError(t, err)

	var builtinIsNull bool
	err = connection.Current.QueryRow(ctx, `SELECT pg_get_function_sqlbody(31::oid) IS NULL;`).Scan(&builtinIsNull)
	require.NoError(t, err)
	require.True(t, builtinIsNull)

	var userFunctionIsNull bool
	err = connection.Current.QueryRow(ctx, `SELECT pg_get_function_sqlbody(p.oid) IS NULL
		FROM pg_catalog.pg_proc p
		JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'public'
			AND p.proname = 'pg_get_function_sqlbody_as_fn';`).Scan(&userFunctionIsNull)
	require.NoError(t, err)
	require.True(t, userFunctionIsNull)
}
