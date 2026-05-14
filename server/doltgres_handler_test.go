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
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestCastSQLErrorPreservesDDLPGCodes(t *testing.T) {
	for _, code := range []pgcode.Code{
		pgcode.AmbiguousFunction,
		pgcode.CannotCoerce,
		pgcode.CardinalityViolation,
		pgcode.CaseNotFound,
		pgcode.DatatypeMismatch,
		pgcode.DependentObjectsStillExist,
		pgcode.DuplicateColumn,
		pgcode.DuplicatePreparedStatement,
		pgcode.DuplicateRelation,
		pgcode.InvalidColumnReference,
		pgcode.InvalidColumnDefinition,
		pgcode.InvalidName,
		pgcode.InvalidSchemaName,
		pgcode.InvalidObjectDefinition,
		pgcode.InvalidTextRepresentation,
		pgcode.NumericValueOutOfRange,
		pgcode.ProgramLimitExceeded,
		pgcode.QueryCanceled,
		pgcode.ReadOnlySQLTransaction,
		pgcode.UndefinedColumn,
		pgcode.UndefinedPreparedStatement,
		pgcode.UniqueViolation,
		pgcode.WrongObjectType,
	} {
		err := pgerror.New(code, "ddl validation error")
		require.Equal(t, code, pgerror.GetPGCode(castSQLError(err)))
	}
}

func TestCastSQLErrorPreservesWrappedInsertPGCodes(t *testing.T) {
	err := pgerror.WithCandidateCode(errors.New("reject before trigger insert"), pgcode.RaiseException)
	wrapped := sql.NewWrappedInsertError(sql.NewRow(1, "bad"), err)

	require.Equal(t, pgcode.RaiseException, pgerror.GetPGCode(castSQLError(wrapped)))
	require.Equal(t, "reject before trigger insert", castSQLError(wrapped).Error())
}

func TestCastSQLErrorMapsExpectedSingleRow(t *testing.T) {
	err := sql.ErrExpectedSingleRow.New()

	require.Equal(t, pgcode.CardinalityViolation, pgerror.GetPGCode(castSQLError(err)))

	wrapped := sql.NewWrappedInsertError(sql.NewRow(1, "bad"), err)
	require.Equal(t, pgcode.CardinalityViolation, pgerror.GetPGCode(castSQLError(wrapped)))
}

func TestCastSQLErrorMapsReadOnlyTransaction(t *testing.T) {
	err := sql.ErrReadOnlyTransaction.New()

	require.Equal(t, pgcode.ReadOnlySQLTransaction, pgerror.GetPGCode(castSQLError(err)))
}

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

func TestSchemaToFieldDescriptionsUsesDefaultTextFormat(t *testing.T) {
	fields, err := schemaToFieldDescriptionsWithSource(sql.NewEmptyContext(), sql.Schema{
		{Name: "a", Type: pgtypes.Int32},
		{Name: "b", Type: pgtypes.Text},
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, fields, 2)
	require.Equal(t, int16(0), fields[0].Format)
	require.Equal(t, int16(0), fields[1].Format)
}

func TestSchemaToFieldDescriptionsDecodesPhysicalColumnNames(t *testing.T) {
	fields, err := schemaToFieldDescriptionsWithSource(sql.NewEmptyContext(), sql.Schema{
		{Name: core.EncodePhysicalColumnName("tableName"), Type: pgtypes.Text},
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, []byte("tableName"), fields[0].Name)
}

func TestSchemaToFieldDescriptionsConvertsGMSNullToText(t *testing.T) {
	fields, err := schemaToFieldDescriptionsWithSource(sql.NewEmptyContext(), sql.Schema{
		{Name: "extra", Type: gmstypes.Null},
	}, nil, nil)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, []byte("extra"), fields[0].Name)
	require.Equal(t, uint32(25), fields[0].DataTypeOID)
	require.Equal(t, int32(-1), fields[0].TypeModifier)
}

func TestSchemaToFieldDescriptionsExpandsBinaryFormat(t *testing.T) {
	fields, err := schemaToFieldDescriptionsWithSource(sql.NewEmptyContext(), sql.Schema{
		{Name: "a", Type: pgtypes.Int32},
		{Name: "b", Type: pgtypes.Text},
	}, nil, []int16{1})
	require.NoError(t, err)
	require.Len(t, fields, 2)
	require.Equal(t, int16(1), fields[0].Format)
	require.Equal(t, int16(1), fields[1].Format)
}

func TestRowOutputTypeUsesResultFieldType(t *testing.T) {
	fields := []pgproto3.FieldDescription{{
		DataTypeOID:  id.Cache().ToOID(pgtypes.TextArray.ID.AsId()),
		TypeModifier: -1,
	}}
	require.Same(t, pgtypes.TextArray, rowOutputType(gmstypes.Text, fields, 0))
}

func TestConvertBindParametersReturnsNilForNoValues(t *testing.T) {
	bindings, err := (&DoltgresHandler{}).convertBindParameters(sql.NewEmptyContext(), nil, nil, nil)
	require.NoError(t, err)
	require.Nil(t, bindings)
}

func TestReceiveBindParameterWidensCompactBinaryIntegers(t *testing.T) {
	ctx := sql.NewEmptyContext()

	v, err := receiveBindParameter(ctx, pgtypes.Int32, []byte{0x00, 0x01})
	require.NoError(t, err)
	require.Equal(t, int32(1), v)

	v, err = receiveBindParameter(ctx, pgtypes.Int32, []byte{0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, int32(-1), v)

	v, err = receiveBindParameter(ctx, pgtypes.Int64, []byte{0x00, 0x00, 0x00, 0x01})
	require.NoError(t, err)
	require.Equal(t, int64(1), v)

	v, err = receiveBindParameter(ctx, pgtypes.Int64, []byte{0xff, 0xff})
	require.NoError(t, err)
	require.Equal(t, int64(-1), v)
}

func TestReceiveBindParameterUnknownUsesRawBinaryPayload(t *testing.T) {
	ctx := sql.NewEmptyContext()

	v, err := receiveBindParameter(ctx, pgtypes.Unknown, []byte("p"))
	require.NoError(t, err)
	require.Equal(t, "p", v)

	v, err = receiveBindParameter(ctx, pgtypes.Unknown, []byte{0x22, 0x7f, 0x47, 0x08})
	require.NoError(t, err)
	require.Equal(t, "578766600", v)

	v, err = receiveBindParameter(ctx, pgtypes.Unknown, []byte{0x01})
	require.NoError(t, err)
	require.Equal(t, "true", v)
}
