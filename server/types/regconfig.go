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

// Regconfig is the OID alias type for text search configurations.
var Regconfig = &DoltgresType{
	ID:                  toInternal("regconfig"),
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
	Array:               toInternal("_regconfig"),
	InputFunc:           toFuncID("regconfigin", toInternal("cstring")),
	OutputFunc:          toFuncID("regconfigout", toInternal("regconfig")),
	ReceiveFunc:         toFuncID("regconfigrecv", toInternal("internal")),
	SendFunc:            toFuncID("regconfigsend", toInternal("regconfig")),
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
	SerializationFunc:   serializeTypeRegconfig,
	DeserializationFunc: deserializeTypeRegconfig,
}

func serializeTypeRegconfig(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	return []byte(val.(id.Id)), nil
}

func deserializeTypeRegconfig(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return id.Id(data), nil
}
