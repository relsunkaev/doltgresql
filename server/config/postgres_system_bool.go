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

package config

import (
	"context"
	"reflect"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type postgresSystemBoolType struct {
	inner sql.SystemVariableType
}

var postgresSystemBoolValueType = reflect.TypeOf("")

func newPostgresSystemBoolType(name string) sql.SystemVariableType {
	return postgresSystemBoolType{inner: types.NewSystemBoolType(name)}
}

func (t postgresSystemBoolType) Compare(ctx context.Context, a any, b any) (int, error) {
	left, _, err := t.Convert(ctx, a)
	if err != nil {
		return 0, err
	}
	right, _, err := t.Convert(ctx, b)
	if err != nil {
		return 0, err
	}
	if left == right {
		return 0, nil
	}
	if left.(string) < right.(string) {
		return -1, nil
	}
	return 1, nil
}

func (t postgresSystemBoolType) Convert(ctx context.Context, v any) (any, sql.ConvertInRange, error) {
	value, inRange, err := t.inner.Convert(ctx, v)
	if err != nil {
		return nil, inRange, err
	}
	return t.format(value), inRange, nil
}

func (t postgresSystemBoolType) Equals(otherType sql.Type) bool {
	other, ok := otherType.(postgresSystemBoolType)
	return ok && t.inner.Equals(other.inner)
}

func (t postgresSystemBoolType) MaxTextResponseByteLength(ctx *sql.Context) uint32 {
	return 3
}

func (t postgresSystemBoolType) Promote() sql.Type {
	return t
}

func (t postgresSystemBoolType) SQL(ctx *sql.Context, dest []byte, v any) (sqltypes.Value, error) {
	value, _, err := t.Convert(ctx, v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(t.Type(), []byte(value.(string))), nil
}

func (t postgresSystemBoolType) Type() query.Type {
	return sqltypes.VarChar
}

func (t postgresSystemBoolType) ValueType() reflect.Type {
	return postgresSystemBoolValueType
}

func (t postgresSystemBoolType) Zero() any {
	return "off"
}

func (t postgresSystemBoolType) String() string {
	return t.inner.String()
}

func (t postgresSystemBoolType) EncodeValue(value any) (string, error) {
	converted, _, err := t.Convert(context.Background(), value)
	if err != nil {
		return "", err
	}
	return converted.(string), nil
}

func (t postgresSystemBoolType) DecodeValue(value string) (any, error) {
	switch strings.ToLower(value) {
	case "on":
		return "on", nil
	case "off":
		return "off", nil
	default:
		converted, _, err := t.Convert(context.Background(), value)
		return converted, err
	}
}

func (t postgresSystemBoolType) UnderlyingType() sql.Type {
	return types.Text
}

func (t postgresSystemBoolType) CollationCoercibility(ctx *sql.Context) (sql.CollationID, byte) {
	return sql.Collation_binary, 5
}

func (t postgresSystemBoolType) format(value any) string {
	switch v := value.(type) {
	case int8:
		if v != 0 {
			return "on"
		}
	case bool:
		if v {
			return "on"
		}
	case string:
		if strings.EqualFold(v, "on") {
			return "on"
		}
	}
	return "off"
}

func keepCurrentTransactionParameter(currVal, _ any) (any, bool) {
	if currVal == nil {
		return "off", true
	}
	return currVal, true
}
