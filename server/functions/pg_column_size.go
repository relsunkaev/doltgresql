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

package functions

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgColumnSize registers the functions to the catalog.
func initPgColumnSize() {
	framework.RegisterFunction(pg_column_size_any)
}

// pg_column_size_any represents the PostgreSQL function of the same name, taking the same parameters.
var pg_column_size_any = framework.Function1{
	Name:       "pg_column_size",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Strict:     true,
	Callable: func(ctx *sql.Context, resolved [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgColumnSize(ctx, resolved, nil, val)
	},
	CallableWithExpr: func(ctx *sql.Context, resolved [2]*pgtypes.DoltgresType, expr sql.Expression, val any) (any, error) {
		return pgColumnSize(ctx, resolved, expr, val)
	},
}

func pgColumnSize(ctx *sql.Context, resolved [2]*pgtypes.DoltgresType, expr sql.Expression, val any) (any, error) {
	valueType := resolved[0]
	if !valueType.IsEmptyType() && valueType.TypLength > 0 {
		return int32(valueType.TypLength), nil
	}

	if _, ok := expr.(*expression.GetField); ok {
		size, ok, err := pgColumnStoredSize(ctx, val)
		if err != nil || ok {
			return size, err
		}
	}

	unwrapped, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	if unwrapped == nil {
		return nil, nil
	}

	if !valueType.IsEmptyType() {
		switch value := unwrapped.(type) {
		case string:
			return int32(len(value) + 4), nil
		case []byte:
			return int32(len(value) + 4), nil
		}
		if serialized, err := valueType.SerializeValue(ctx, unwrapped); err == nil && serialized != nil {
			return int32(len(serialized)), nil
		}
		if output, err := valueType.IoOutput(ctx, unwrapped); err == nil {
			return int32(len(output) + 4), nil
		}
	}

	return int32(len(fmt.Sprint(unwrapped)) + 4), nil
}

func pgColumnStoredSize(ctx *sql.Context, val any) (int32, bool, error) {
	unwrapped, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return 0, false, err
	}
	switch value := unwrapped.(type) {
	case string:
		return pgVarlenaStoredSize(len(value)), true, nil
	case []byte:
		return pgVarlenaStoredSize(len(value)), true, nil
	default:
		return 0, false, nil
	}
}

func pgVarlenaStoredSize(length int) int32 {
	if length <= 126 {
		return int32(length + 1)
	}
	return int32(length + 4)
}
