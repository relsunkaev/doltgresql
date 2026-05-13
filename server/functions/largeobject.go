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
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
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
	framework.RegisterFunction(lo_get_oid_int32_int32)
	framework.RegisterFunction(lo_get_oid_int64_int32)
	framework.RegisterFunction(lo_put_oid_int32_bytea)
	framework.RegisterFunction(lo_put_oid_int64_bytea)
}

var lo_create_oid = framework.Function1{
	Name:       "lo_create",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Oid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		trackLargeObjectMutation(ctx)
		oid, err := largeobject.Create(currentDatabase(ctx), oidValue(val), currentSQLUser(ctx), nil)
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
		oid := oidValue(val)
		if err := requireLargeObjectOwner(ctx, oid); err != nil {
			return nil, err
		}
		trackLargeObjectMutation(ctx)
		count, err := largeobject.Unlink(currentDatabase(ctx), oid)
		if err != nil {
			return nil, err
		}
		if count > 0 {
			clearLargeObjectComment(oid)
		}
		return count, nil
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
		trackLargeObjectMutation(ctx)
		oid, err := largeobject.Create(currentDatabase(ctx), oidValue(oidVal), currentSQLUser(ctx), data)
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
		oid := oidValue(val)
		if err := requireLargeObjectPrivilege(ctx, oid, "SELECT"); err != nil {
			return nil, err
		}
		data, ok := largeobject.Get(currentDatabase(ctx), oid)
		if !ok {
			return nil, errors.Errorf("large object %d does not exist", oid)
		}
		return data, nil
	},
}

var lo_get_oid_int32_int32 = framework.Function3{
	Name:       "lo_get",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Int32, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, oidVal any, offsetVal any, lengthVal any) (any, error) {
		return loGetSlice(ctx, oidValue(oidVal), int64Value(offsetVal), int32Value(lengthVal))
	},
}

var lo_get_oid_int64_int32 = framework.Function3{
	Name:       "lo_get",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Int64, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, oidVal any, offsetVal any, lengthVal any) (any, error) {
		return loGetSlice(ctx, oidValue(oidVal), int64Value(offsetVal), int32Value(lengthVal))
	},
}

var lo_put_oid_int32_bytea = framework.Function3{
	Name:       "lo_put",
	Return:     pgtypes.Void,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Int32, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, oidVal any, offsetVal any, dataVal any) (any, error) {
		return "", loPut(ctx, oidValue(oidVal), int64Value(offsetVal), dataVal)
	},
}

var lo_put_oid_int64_bytea = framework.Function3{
	Name:       "lo_put",
	Return:     pgtypes.Void,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Int64, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, oidVal any, offsetVal any, dataVal any) (any, error) {
		return "", loPut(ctx, oidValue(oidVal), int64Value(offsetVal), dataVal)
	},
}

func loGetSlice(ctx *sql.Context, oid uint32, offset int64, length int32) ([]byte, error) {
	if err := requireLargeObjectPrivilege(ctx, oid, "SELECT"); err != nil {
		return nil, err
	}
	data, ok, err := largeobject.GetSlice(currentDatabase(ctx), oid, offset, length)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf("large object %d does not exist", oid)
	}
	return data, nil
}

func loPut(ctx *sql.Context, oid uint32, offset int64, dataVal any) error {
	data, ok, err := sql.Unwrap[[]byte](ctx, dataVal)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf("expected bytea, got %T", dataVal)
	}
	if err := requireLargeObjectPrivilege(ctx, oid, "UPDATE"); err != nil {
		return err
	}
	trackLargeObjectMutation(ctx)
	return largeobject.Put(currentDatabase(ctx), oid, offset, data)
}

func trackLargeObjectMutation(ctx *sql.Context) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	largeobject.TrackMutation(uint32(ctx.Session.ID()))
}

func clearLargeObjectComment(oid uint32) {
	comments.Set(comments.Key{
		ObjOID:   oid,
		ClassOID: comments.ClassOID("pg_largeobject_metadata"),
		ObjSubID: 0,
	}, nil)
}

func requireLargeObjectPrivilege(ctx *sql.Context, oid uint32, privilege string) error {
	if largeObjectCompatPrivileges(ctx) {
		return nil
	}
	allowed, err := hasLargeObjectPrivilege(currentDatabase(ctx), currentSQLUser(ctx), oid, privilege)
	if err != nil {
		return err
	}
	if !allowed {
		return errors.Errorf("permission denied for large object %d", oid)
	}
	return nil
}

func requireLargeObjectOwner(ctx *sql.Context, oid uint32) error {
	if largeObjectCompatPrivileges(ctx) {
		return nil
	}
	owner, ok := largeobject.Owner(currentDatabase(ctx), oid)
	if !ok {
		return errors.Errorf("large object %d does not exist", oid)
	}
	user := currentSQLUser(ctx)
	role := auth.GetRole(user)
	if role.IsSuperUser || owner == user {
		return nil
	}
	return errors.Errorf("permission denied for large object %d", oid)
}

func currentDatabase(ctx *sql.Context) string {
	if ctx == nil {
		return "postgres"
	}
	if database := ctx.GetCurrentDatabase(); database != "" {
		return database
	}
	return "postgres"
}

func largeObjectCompatPrivileges(ctx *sql.Context) bool {
	if ctx == nil {
		return false
	}
	value, err := ctx.GetSessionVariable(ctx, "lo_compat_privileges")
	if err != nil || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case int8:
		return v != 0
	case int:
		return v != 0
	case int64:
		return v != 0
	case string:
		return v == "on" || v == "true" || v == "1"
	default:
		return false
	}
}

func oidValue(val any) uint32 {
	return id.Cache().ToOID(val.(id.Id))
}

func int64Value(val any) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	default:
		panic(errors.Errorf("expected integer, got %T", val))
	}
}

func int32Value(val any) int32 {
	switch v := val.(type) {
	case int:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	default:
		panic(errors.Errorf("expected integer, got %T", val))
	}
}
