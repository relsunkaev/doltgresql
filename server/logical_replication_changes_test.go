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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/replicaidentity"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestEncodeRelationMessageReplicaIdentity(t *testing.T) {
	tableSchema := sql.Schema{
		{Name: "id", Type: pgtypes.Int32, PrimaryKey: true},
	}
	fields := []pgproto3.FieldDescription{
		{Name: []byte("id"), DataTypeOID: 23},
	}

	message := encodeRelationMessage(42, "public", "items", tableSchema, fields, replicaidentity.IdentityFull.Byte(), map[string]struct{}{"id": {}})
	require.Equal(t, byte(replicaidentity.IdentityFull), relationMessageIdentity(t, message))
	require.Equal(t, byte(1), relationMessageFirstColumnFlag(t, message))

	message = encodeRelationMessage(42, "public", "items", tableSchema, fields, 0, nil)
	require.Equal(t, byte(replicaidentity.IdentityDefault), relationMessageIdentity(t, message))
	require.Equal(t, byte(1), relationMessageFirstColumnFlag(t, message))
}

func relationMessageIdentity(t *testing.T, message []byte) byte {
	t.Helper()
	offset := 1 + 4
	offset += len("public") + 1
	offset += len("items") + 1
	require.Less(t, offset, len(message))
	return message[offset]
}

func relationMessageFirstColumnFlag(t *testing.T, message []byte) byte {
	t.Helper()
	offset := 1 + 4
	offset += len("public") + 1
	offset += len("items") + 1
	offset += 1 + 2
	require.Less(t, offset, len(message))
	return message[offset]
}

func TestNormalizePublicationRowFilterUnknownPredicates(t *testing.T) {
	require.Equal(t, "visible IS NULL", normalizePublicationRowFilter("visible IS UNKNOWN"))
	require.Equal(t, "visible IS NOT NULL", normalizePublicationRowFilter("visible IS NOT UNKNOWN"))
	require.Equal(t, "visible IS NULL", normalizePublicationRowFilter("visible IS NOT DISTINCT FROM NULL"))
	require.Equal(t, "visible IS NOT NULL", normalizePublicationRowFilter("visible IS DISTINCT FROM NULL"))
	require.Equal(t, "NOT (customer_id <=> 5)", normalizePublicationRowFilter("customer_id IS DISTINCT FROM 5"))
	require.Equal(t, "(status <=> 'ready')", normalizePublicationRowFilter("status IS NOT DISTINCT FROM 'ready'"))
	require.Equal(t, "code < 'm'", normalizePublicationRowFilter("code::varchar < 'm'"))
	require.Equal(t, "code = 'alpha'", normalizePublicationRowFilter("code = 'alpha'::TEXT"))
	require.Equal(t, "code = 'alpha'", normalizePublicationRowFilter("code::character varying = 'alpha'"))
}

func TestPublicationRowFilterDistinctFromPredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		value  rowFilterValue
		match  bool
	}{
		{
			name:   "distinct from different literal",
			filter: "customer_id IS DISTINCT FROM 5",
			value:  rowFilterValue{data: []byte("7")},
			match:  true,
		},
		{
			name:   "distinct from equal literal",
			filter: "customer_id IS DISTINCT FROM 5",
			value:  rowFilterValue{data: []byte("5")},
			match:  false,
		},
		{
			name:   "distinct from non-null literal matches null",
			filter: "customer_id IS DISTINCT FROM 5",
			value:  rowFilterValue{null: true},
			match:  true,
		},
		{
			name:   "not distinct from equal literal",
			filter: "customer_id IS NOT DISTINCT FROM 5",
			value:  rowFilterValue{data: []byte("5")},
			match:  true,
		},
		{
			name:   "not distinct from different literal",
			filter: "customer_id IS NOT DISTINCT FROM 5",
			value:  rowFilterValue{data: []byte("7")},
			match:  false,
		},
		{
			name:   "not distinct from non-null literal rejects null",
			filter: "customer_id IS NOT DISTINCT FROM 5",
			value:  rowFilterValue{null: true},
			match:  false,
		},
		{
			name:   "not distinct from null matches null",
			filter: "customer_id IS NOT DISTINCT FROM NULL",
			value:  rowFilterValue{null: true},
			match:  true,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
				"customer_id": tt.value,
			})
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestPublicationRowFilterCoalescePredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		values map[string]rowFilterValue
		match  bool
	}{
		{
			name:   "uses first non-null column",
			filter: "COALESCE(label, 'fallback') = 'shown'",
			values: map[string]rowFilterValue{
				"label": {data: []byte("shown")},
			},
			match: true,
		},
		{
			name:   "uses fallback literal for null column",
			filter: "COALESCE(label, 'fallback') = 'fallback'",
			values: map[string]rowFilterValue{
				"label": {null: true},
			},
			match: true,
		},
		{
			name:   "uses later non-null argument",
			filter: "COALESCE(label, backup_label, 'fallback') = 'backup'",
			values: map[string]rowFilterValue{
				"label":        {null: true},
				"backup_label": {data: []byte("backup")},
			},
			match: true,
		},
		{
			name:   "all null remains null",
			filter: "COALESCE(label, NULL) IS NULL",
			values: map[string]rowFilterValue{
				"label": {null: true},
			},
			match: true,
		},
		{
			name:   "non-matching fallback rejects row",
			filter: "COALESCE(label, 'fallback') = 'shown'",
			values: map[string]rowFilterValue{
				"label": {null: true},
			},
			match: false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, tt.values)
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestPublicationRowFilterTextCastPredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		value  rowFilterValue
		match  bool
	}{
		{
			name:   "left varchar cast compares as text",
			filter: "code::varchar < 'm'",
			value:  rowFilterValue{data: []byte("alpha")},
			match:  true,
		},
		{
			name:   "left text cast rejects non-match",
			filter: "code::text < 'm'",
			value:  rowFilterValue{data: []byte("zulu")},
			match:  false,
		},
		{
			name:   "right text cast compares as text",
			filter: "code = 'alpha'::text",
			value:  rowFilterValue{data: []byte("alpha")},
			match:  true,
		},
		{
			name:   "character varying cast compares as text",
			filter: "code::character varying = 'alpha'",
			value:  rowFilterValue{data: []byte("alpha")},
			match:  true,
		},
		{
			name:   "null cast operand does not match comparison",
			filter: "code::varchar = 'alpha'",
			value:  rowFilterValue{null: true},
			match:  false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
				"code": tt.value,
			})
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestPublicationRowFilterBetweenPredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		value  rowFilterValue
		match  bool
	}{
		{
			name:   "between includes lower bound",
			filter: "score BETWEEN 10 AND 20",
			value:  rowFilterValue{data: []byte("10")},
			match:  true,
		},
		{
			name:   "between includes upper bound",
			filter: "score BETWEEN 10 AND 20",
			value:  rowFilterValue{data: []byte("20")},
			match:  true,
		},
		{
			name:   "between matches interior",
			filter: "score BETWEEN 10 AND 20",
			value:  rowFilterValue{data: []byte("15")},
			match:  true,
		},
		{
			name:   "between rejects outside range",
			filter: "score BETWEEN 10 AND 20",
			value:  rowFilterValue{data: []byte("21")},
			match:  false,
		},
		{
			name:   "not between inverts complete comparison",
			filter: "score NOT BETWEEN 10 AND 20",
			value:  rowFilterValue{data: []byte("21")},
			match:  true,
		},
		{
			name:   "not between rejects inside range",
			filter: "score NOT BETWEEN 10 AND 20",
			value:  rowFilterValue{data: []byte("15")},
			match:  false,
		},
		{
			name:   "between rejects null",
			filter: "score BETWEEN 10 AND 20",
			value:  rowFilterValue{null: true},
			match:  false,
		},
		{
			name:   "not between rejects null",
			filter: "score NOT BETWEEN 10 AND 20",
			value:  rowFilterValue{null: true},
			match:  false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
				"score": tt.value,
			})
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestPublicationRowFilterArithmeticPredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		value  rowFilterValue
		match  bool
	}{
		{
			name:   "addition matches",
			filter: "score + 1 = 2",
			value:  rowFilterValue{data: []byte("1")},
			match:  true,
		},
		{
			name:   "addition rejects non-match",
			filter: "score + 1 = 2",
			value:  rowFilterValue{data: []byte("2")},
			match:  false,
		},
		{
			name:   "subtraction matches",
			filter: "score - 1 = 4",
			value:  rowFilterValue{data: []byte("5")},
			match:  true,
		},
		{
			name:   "multiplication matches",
			filter: "score * 2 = 10",
			value:  rowFilterValue{data: []byte("5")},
			match:  true,
		},
		{
			name:   "division matches",
			filter: "score / 2 = 3",
			value:  rowFilterValue{data: []byte("6")},
			match:  true,
		},
		{
			name:   "nested arithmetic matches",
			filter: "(score + 1) * 2 = 12",
			value:  rowFilterValue{data: []byte("5")},
			match:  true,
		},
		{
			name:   "unary minus matches",
			filter: "-score = -5",
			value:  rowFilterValue{data: []byte("5")},
			match:  true,
		},
		{
			name:   "null arithmetic rejects comparison",
			filter: "score + 1 = 2",
			value:  rowFilterValue{null: true},
			match:  false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
				"score": tt.value,
			})
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestPublicationRowFilterArithmeticErrors(t *testing.T) {
	ctx := sql.NewEmptyContext()

	expr, err := parsePublicationRowFilter("score / 0 = 1")
	require.NoError(t, err)
	_, err = evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
		"score": {data: []byte("5")},
	})
	require.ErrorContains(t, err, "division by zero")

	expr, err = parsePublicationRowFilter("label + 1 = 2")
	require.NoError(t, err)
	_, err = evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
		"label": {data: []byte("shown")},
	})
	require.ErrorContains(t, err, `requires numeric operands`)
}

func TestPublicationRowFilterTextColumnPredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		value  string
		match  bool
	}{
		{
			name:   "text equality does not coerce numeric-looking values",
			filter: "label = '1.0'",
			value:  "1",
			match:  false,
		},
		{
			name:   "text inequality does not coerce numeric-looking values",
			filter: "label <> '1.0'",
			value:  "1",
			match:  true,
		},
		{
			name:   "text ordering remains lexical",
			filter: "label < '2'",
			value:  "10",
			match:  true,
		},
		{
			name:   "text greater-than remains lexical",
			filter: "label > '2'",
			value:  "10",
			match:  false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
				"label": {
					data:    []byte(tt.value),
					typeOID: id.Cache().ToOID(pgtypes.Text.ID.AsId()),
				},
			})
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestPublicationRowFilterLikePredicates(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		value  rowFilterValue
		match  bool
	}{
		{
			name:   "like prefix matches",
			filter: "label LIKE 'show%'",
			value:  rowFilterValue{data: []byte("shown")},
			match:  true,
		},
		{
			name:   "like prefix rejects non-match",
			filter: "label LIKE 'show%'",
			value:  rowFilterValue{data: []byte("hidden")},
			match:  false,
		},
		{
			name:   "not like prefix inverts match",
			filter: "label NOT LIKE 'hid%'",
			value:  rowFilterValue{data: []byte("shown")},
			match:  true,
		},
		{
			name:   "not like prefix rejects match",
			filter: "label NOT LIKE 'hid%'",
			value:  rowFilterValue{data: []byte("hidden")},
			match:  false,
		},
		{
			name:   "like rejects null",
			filter: "label LIKE 'show%'",
			value:  rowFilterValue{null: true},
			match:  false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := parsePublicationRowFilter(tt.filter)
			require.NoError(t, err)
			match, err := evalPublicationFilterBool(ctx, expr, map[string]rowFilterValue{
				"label": tt.value,
			})
			require.NoError(t, err)
			require.Equal(t, tt.match, match)
		})
	}
}

func TestRowFilterLikePatternEscapes(t *testing.T) {
	match, ok, err := rowFilterValuesLike(
		rowFilterValue{data: []byte("show_item")},
		rowFilterValue{data: []byte(`show\_%`)},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, match)

	match, ok, err = rowFilterValuesLike(
		rowFilterValue{data: []byte("showXitem")},
		rowFilterValue{data: []byte(`show\_%`)},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, match)
}
