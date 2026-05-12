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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/largeobject"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initLargeObject registers the functions to the catalog.
func initLargeObject() {
	framework.RegisterFunction(lo_create_oid)
	framework.RegisterFunction(lo_unlink_oid)
	framework.RegisterFunction(lo_from_bytea_oid_bytea)
	framework.RegisterFunction(lo_get_oid)
}

var lo_create_oid = framework.Function1{
	Name:       "lo_create",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Oid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		oid, err := largeobject.Create(oidValue(val), currentSQLUser(ctx), nil)
		if err != nil {
			return nil, err
		}
		return strconv.FormatUint(uint64(oid), 10), nil
	},
}

var lo_unlink_oid = framework.Function1{
	Name:       "lo_unlink",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Oid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return largeobject.Unlink(oidValue(val)), nil
	},
}

var lo_from_bytea_oid_bytea = framework.Function2{
	Name:       "lo_from_bytea",
	Return:     pgtypes.Oid,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, oidVal any, dataVal any) (any, error) {
		data, ok, err := sql.Unwrap[[]byte](ctx, dataVal)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("expected bytea, got %T", dataVal)
		}
		oid, err := largeobject.Create(oidValue(oidVal), currentSQLUser(ctx), data)
		if err != nil {
			return nil, err
		}
		return id.NewOID(oid).AsId(), nil
	},
}

var lo_get_oid = framework.Function1{
	Name:       "lo_get",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Oid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data, ok := largeobject.Get(oidValue(val))
		if !ok {
			return nil, errors.Errorf("large object %d does not exist", oidValue(val))
		}
		return data, nil
	},
}

func oidValue(val any) uint32 {
	return id.Cache().ToOID(val.(id.Id))
}
