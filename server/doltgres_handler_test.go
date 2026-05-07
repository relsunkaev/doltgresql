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

package server

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestExecutionResultFieldsUsesSuppliedFields(t *testing.T) {
	schema := sql.Schema{{Name: "from_schema", Type: pgtypes.Int32}}
	supplied := []pgproto3.FieldDescription{{
		Name:        []byte("from_bind"),
		DataTypeOID: 23,
		Format:      1,
	}}

	fields, err := executionResultFields(sql.NewEmptyContext(), schema, nil, nil, supplied)
	require.NoError(t, err)
	require.Equal(t, supplied, fields)
}

func TestExecutionResultFieldsRecomputesOnLengthMismatch(t *testing.T) {
	schema := sql.Schema{{Name: "from_schema", Type: pgtypes.Int32}}

	fields, err := executionResultFields(sql.NewEmptyContext(), schema, nil, nil, []pgproto3.FieldDescription{})
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, []byte("from_schema"), fields[0].Name)
}

func TestExecutionFormatCodesKeepsDefaultTextCompact(t *testing.T) {
	formatCodes, err := executionFormatCodes(3, nil)
	require.NoError(t, err)
	require.Nil(t, formatCodes)

	formatCodes, err = executionFormatCodes(3, []int16{})
	require.NoError(t, err)
	require.Nil(t, formatCodes)

	formatCodes, err = executionFormatCodes(3, []int16{0})
	require.NoError(t, err)
	require.Nil(t, formatCodes)

	formatCodes, err = executionFormatCodes(3, []int16{0, 0, 0})
	require.NoError(t, err)
	require.Nil(t, formatCodes)
}

func TestExecutionFormatCodesExpandsBinaryShortForm(t *testing.T) {
	formatCodes, err := executionFormatCodes(3, []int16{1})
	require.NoError(t, err)
	require.Equal(t, []int16{1, 1, 1}, formatCodes)
}
