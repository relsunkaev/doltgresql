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

package jsonbgin

import (
	"bytes"
	"sort"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestRowReferenceSingleColumnRoundTrip(t *testing.T) {
	ctx := sql.NewEmptyContext()

	rowRef, err := EncodeRowReference(ctx, []sql.Type{pgtypes.Int32}, sql.Row{int32(-42)})
	require.NoError(t, err)
	require.Equal(t, uint8(RowReferenceFormatVersionV1), rowRef.FormatVersion)
	require.NotEmpty(t, rowRef.Bytes)
	require.Equal(t, sql.Row{int32(-42)}, rowRef.Values)

	decoded, err := DecodeRowReference(ctx, []sql.Type{pgtypes.Int32}, rowRef.Bytes)
	require.NoError(t, err)
	require.Equal(t, rowRef.FormatVersion, decoded.FormatVersion)
	require.Equal(t, rowRef.Values, decoded.Values)
	require.Equal(t, rowRef.Bytes, decoded.Bytes)
}

func TestRowReferenceCompositeRoundTrip(t *testing.T) {
	ctx := sql.NewEmptyContext()
	types := []sql.Type{pgtypes.Text, pgtypes.Int32, pgtypes.Int64}
	values := sql.Row{"tenant:west", int32(7), int64(99)}

	rowRef, err := EncodeRowReference(ctx, types, values)
	require.NoError(t, err)

	decoded, err := DecodeRowReference(ctx, types, rowRef.Bytes)
	require.NoError(t, err)
	require.Equal(t, values, decoded.Values)
}

func TestRowReferenceOrdersLikePrimaryKey(t *testing.T) {
	ctx := sql.NewEmptyContext()
	types := []sql.Type{pgtypes.Int32, pgtypes.Text}
	rows := []sql.Row{
		{int32(-2), "z"},
		{int32(-1), "a"},
		{int32(-1), "aa"},
		{int32(-1), "b"},
		{int32(0), ""},
		{int32(1), "a"},
	}
	rowRefs := make([]RowReference, len(rows))
	for i, row := range rows {
		rowRef, err := EncodeRowReference(ctx, types, row)
		require.NoError(t, err)
		rowRefs[i] = rowRef
	}

	for i := range rows {
		for j := range rows {
			want, err := compareRowReferenceValues(ctx, types, rows[i], rows[j])
			require.NoError(t, err)
			require.Equal(t, sign(want), sign(CompareRowReferences(rowRefs[i].Bytes, rowRefs[j].Bytes)))
		}
	}

	shuffled := []RowReference{rowRefs[4], rowRefs[2], rowRefs[5], rowRefs[0], rowRefs[3], rowRefs[1]}
	sort.Slice(shuffled, func(i, j int) bool {
		return CompareRowReferences(shuffled[i].Bytes, shuffled[j].Bytes) < 0
	})
	for i, rowRef := range shuffled {
		decoded, err := DecodeRowReference(ctx, types, rowRef.Bytes)
		require.NoError(t, err)
		require.Equal(t, rows[i], decoded.Values)
	}
}

func TestRowReferenceNullableComponentsRoundTripAndOrder(t *testing.T) {
	ctx := sql.NewEmptyContext()
	types := []sql.Type{pgtypes.Int32, pgtypes.Text}
	rows := []sql.Row{
		{nil, nil},
		{nil, "a"},
		{int32(0), nil},
		{int32(0), "a"},
	}

	var previous []byte
	for i, row := range rows {
		rowRef, err := EncodeRowReference(ctx, types, row)
		require.NoError(t, err)
		decoded, err := DecodeRowReference(ctx, types, rowRef.Bytes)
		require.NoError(t, err)
		require.Equal(t, row, decoded.Values)
		if i > 0 {
			require.Less(t, CompareRowReferences(previous, rowRef.Bytes), 0)
		}
		previous = rowRef.Bytes
	}
}

func TestPrimaryKeyRowReferenceUsesPrimaryKeyColumns(t *testing.T) {
	ctx := sql.NewEmptyContext()
	sch := sql.Schema{
		{Name: "tenant", Type: pgtypes.Text, PrimaryKey: true},
		{Name: "doc", Type: pgtypes.JsonB},
		{Name: "id", Type: pgtypes.Int32, PrimaryKey: true},
	}

	rowRef, ok, err := EncodePrimaryKeyRowReference(ctx, sch, sql.Row{"east", `{"vip": true}`, int32(42)})
	require.NoError(t, err)
	require.True(t, ok)

	decoded, err := DecodeRowReference(ctx, []sql.Type{pgtypes.Text, pgtypes.Int32}, rowRef.Bytes)
	require.NoError(t, err)
	require.Equal(t, sql.Row{"east", int32(42)}, decoded.Values)
}

func TestPrimaryKeyRowReferenceReportsMissingPrimaryKey(t *testing.T) {
	ctx := sql.NewEmptyContext()
	sch := sql.Schema{
		{Name: "doc", Type: pgtypes.JsonB},
	}

	_, ok, err := EncodePrimaryKeyRowReference(ctx, sch, sql.Row{`{"vip": true}`})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRowReferenceRejectsMalformedPayloads(t *testing.T) {
	ctx := sql.NewEmptyContext()
	rowRef, err := EncodeRowReference(ctx, []sql.Type{pgtypes.Int32, pgtypes.Text}, sql.Row{int32(7), "east"})
	require.NoError(t, err)

	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{name: "empty", payload: nil, wantErr: "too short"},
		{name: "bad magic", payload: append([]byte("BAD!"), rowRef.Bytes[4:]...), wantErr: "magic"},
		{name: "unsupported version", payload: unsupportedRowReferenceVersion(rowRef.Bytes), wantErr: "unsupported"},
		{name: "component count mismatch", payload: rowRef.Bytes, wantErr: "component count"},
		{name: "truncated string", payload: rowRef.Bytes[:len(rowRef.Bytes)-1], wantErr: "terminated"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			types := []sql.Type{pgtypes.Int32}
			if test.name != "component count mismatch" {
				types = []sql.Type{pgtypes.Int32, pgtypes.Text}
			}
			_, err := DecodeRowReference(ctx, types, test.payload)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestRowReferenceRejectsUnsupportedTypes(t *testing.T) {
	ctx := sql.NewEmptyContext()

	_, err := EncodeRowReference(ctx, []sql.Type{pgtypes.Numeric}, sql.Row{decimal.RequireFromString("10.5")})
	require.ErrorContains(t, err, "unsupported")
}

func compareRowReferenceValues(ctx *sql.Context, types []sql.Type, left sql.Row, right sql.Row) (int, error) {
	for i, typ := range types {
		cmp, err := typ.Compare(ctx, left[i], right[i])
		if err != nil || cmp != 0 {
			return cmp, err
		}
	}
	return 0, nil
}

func unsupportedRowReferenceVersion(payload []byte) []byte {
	copied := append([]byte(nil), payload...)
	copied[4] = 99
	return copied
}

func sign(value int) int {
	switch {
	case value < 0:
		return -1
	case value > 0:
		return 1
	default:
		return 0
	}
}

func TestComparableBytesRoundTrip(t *testing.T) {
	inputs := [][]byte{
		nil,
		[]byte(""),
		[]byte("a"),
		[]byte("a\x00b"),
		[]byte("aa"),
	}
	for _, input := range inputs {
		encoded := encodeComparableBytes(input)
		decoded, offset, err := decodeComparableBytes(encoded, 0)
		require.NoError(t, err)
		require.Equal(t, len(encoded), offset)
		require.True(t, bytes.Equal(input, decoded))
	}
}
