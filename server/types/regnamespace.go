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

package types

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

// Regnamespace is the OID alias type that resolves a namespace name to its
// pg_namespace OID. ORM introspection paths (drizzle-kit, Prisma, Alembic)
// rely on the `name::regnamespace` cast to filter pg_constraint /
// pg_class lookups by schema.
var Regnamespace = &DoltgresType{
	ID:                  toInternal("regnamespace"),
	TypLength:           int16(4),
	PassedByVal:         true,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_NumericTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_regnamespace"),
	InputFunc:           toFuncID("regnamespacein", toInternal("cstring")),
	OutputFunc:          toFuncID("regnamespaceout", toInternal("regnamespace")),
	ReceiveFunc:         toFuncID("regnamespacerecv", toInternal("internal")),
	SendFunc:            toFuncID("regnamespacesend", toInternal("regnamespace")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Plain,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeRegnamespace,
	DeserializationFunc: deserializeTypeRegnamespace,
}

// serializeTypeRegnamespace writes the canonical id form to bytes.
func serializeTypeRegnamespace(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	return []byte(val.(id.Id)), nil
}

// deserializeTypeRegnamespace reads the canonical id form from bytes.
func deserializeTypeRegnamespace(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return id.Id(data), nil
}
