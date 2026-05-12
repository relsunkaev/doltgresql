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

package cast

import (
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var initJSONBCastsForTest sync.Once

func ensureJSONBCastsForTest() {
	initJSONBCastsForTest.Do(func() {
		if framework.GetExplicitCast(pgtypes.JsonB, pgtypes.Int32) == nil {
			initJsonB()
		}
	})
}

func TestJsonbNullExplicitScalarCastsReturnSQLNull(t *testing.T) {
	ensureJSONBCastsForTest()

	ctx := sql.NewEmptyContext()
	jsonNull := pgtypes.JsonDocument{Value: pgtypes.JsonValueNull(0)}
	for _, targetType := range []*pgtypes.DoltgresType{
		pgtypes.Bool,
		pgtypes.Float32,
		pgtypes.Float64,
		pgtypes.Int16,
		pgtypes.Int32,
		pgtypes.Int64,
		pgtypes.Numeric,
	} {
		t.Run(targetType.String(), func(t *testing.T) {
			castFunc := framework.GetExplicitCast(pgtypes.JsonB, targetType)
			if castFunc == nil {
				t.Fatalf("expected explicit jsonb cast to %s", targetType.String())
			}
			result, err := castFunc(ctx, jsonNull, targetType)
			if err != nil {
				t.Fatal(err)
			}
			if result != nil {
				t.Fatalf("expected SQL NULL, found %v", result)
			}
		})
	}
}

func TestJsonbExplicitScalarCastsStillRejectWrongJsonKinds(t *testing.T) {
	ensureJSONBCastsForTest()

	ctx := sql.NewEmptyContext()
	tests := []struct {
		name       string
		targetType *pgtypes.DoltgresType
		value      pgtypes.JsonValue
	}{
		{
			name:       "object to int",
			targetType: pgtypes.Int32,
			value: pgtypes.JsonValueObject{
				Items: []pgtypes.JsonValueObjectItem{{Key: "a", Value: pgtypes.JsonValueNumber{}}},
				Index: map[string]int{"a": 0},
			},
		},
		{
			name:       "array to bool",
			targetType: pgtypes.Bool,
			value:      pgtypes.JsonValueArray{pgtypes.JsonValueBoolean(true)},
		},
		{
			name:       "string to numeric",
			targetType: pgtypes.Numeric,
			value:      pgtypes.JsonValueString("1"),
		},
		{
			name:       "boolean to int",
			targetType: pgtypes.Int32,
			value:      pgtypes.JsonValueBoolean(true),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			castFunc := framework.GetExplicitCast(pgtypes.JsonB, tt.targetType)
			if castFunc == nil {
				t.Fatalf("expected explicit jsonb cast to %s", tt.targetType.String())
			}
			_, err := castFunc(ctx, pgtypes.JsonDocument{Value: tt.value}, tt.targetType)
			if err == nil {
				t.Fatal("expected cast error")
			}
		})
	}
}
