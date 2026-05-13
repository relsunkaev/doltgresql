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

package compare_test

import (
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/server/compare"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/binary"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestMain(m *testing.M) {
	functions.Init()
	binary.Init()
	framework.Initialize()
	os.Exit(m.Run())
}

func TestCompareRecordsEqualityNullSemantics(t *testing.T) {
	ctx := sql.NewEmptyContext()

	tests := []struct {
		name  string
		op    framework.Operator
		left  []any
		right []any
		want  any
	}{
		{
			name:  "equal returns false when a later non-null field differs",
			op:    framework.Operator_BinaryEqual,
			left:  []any{int32(1), int32(2), int32(3)},
			right: []any{int32(1), nil, int32(4)},
			want:  false,
		},
		{
			name:  "equal returns null when only null fields are unknown",
			op:    framework.Operator_BinaryEqual,
			left:  []any{int32(1), nil},
			right: []any{int32(1), nil},
			want:  nil,
		},
		{
			name:  "not equal returns true when a later non-null field differs",
			op:    framework.Operator_BinaryNotEqual,
			left:  []any{int32(1), nil, int32(3)},
			right: []any{int32(1), int32(2), int32(4)},
			want:  true,
		},
		{
			name:  "not equal returns false when all fields are non-null and equal",
			op:    framework.Operator_BinaryNotEqual,
			left:  []any{int32(1), int32(2)},
			right: []any{int32(1), int32(2)},
			want:  false,
		},
		{
			name:  "not equal returns null when only null fields are unknown",
			op:    framework.Operator_BinaryNotEqual,
			left:  []any{int32(1), nil},
			right: []any{int32(1), nil},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compare.CompareRecords(ctx, tt.op, int32Record(tt.left...), int32Record(tt.right...))
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompareRecordsOrderingStopsAtDecisiveField(t *testing.T) {
	ctx := sql.NewEmptyContext()

	tests := []struct {
		name  string
		op    framework.Operator
		left  []any
		right []any
		want  any
	}{
		{
			name:  "less than returns false at the first greater field",
			op:    framework.Operator_BinaryLessThan,
			left:  []any{int32(1), int32(3), nil},
			right: []any{int32(1), int32(2), nil},
			want:  false,
		},
		{
			name:  "less than returns null when the decisive field is null",
			op:    framework.Operator_BinaryLessThan,
			left:  []any{int32(1), int32(2)},
			right: []any{int32(1), nil},
			want:  nil,
		},
		{
			name:  "less or equal returns true for all equal non-null fields",
			op:    framework.Operator_BinaryLessOrEqual,
			left:  []any{int32(1), int32(2)},
			right: []any{int32(1), int32(2)},
			want:  true,
		},
		{
			name:  "greater or equal returns null when the decisive field is null",
			op:    framework.Operator_BinaryGreaterOrEqual,
			left:  []any{int32(1), nil},
			right: []any{int32(1), int32(2)},
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compare.CompareRecords(ctx, tt.op, int32Record(tt.left...), int32Record(tt.right...))
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func int32Record(values ...any) []pgtypes.RecordValue {
	record := make([]pgtypes.RecordValue, len(values))
	for i, value := range values {
		record[i] = pgtypes.RecordValue{
			Type:  pgtypes.Int32,
			Value: value,
		}
	}
	return record
}
