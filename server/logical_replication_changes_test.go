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
