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

package node

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/types"
)

func TestValidateColumnAliasesDecodesRemainingPhysicalColumnNames(t *testing.T) {
	schema := sql.Schema{
		{Name: "id", Type: types.Int32},
		{Name: core.EncodePhysicalColumnName("AccountID"), Type: types.Int32},
	}

	err := ValidateColumnAliases(schema, []string{"AccountID"})
	require.Error(t, err)
	require.Equal(t, pgcode.DuplicateColumn, pgerror.GetPGCode(err))
	require.Contains(t, err.Error(), `column "AccountID" specified more than once`)
}
