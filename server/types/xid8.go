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
	"encoding/binary"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

// Xid8 is PostgreSQL's 64-bit full transaction ID type.
var Xid8 = &DoltgresType{
	ID:                  toInternal("xid8"),
	TypLength:           int16(8),
	PassedByVal:         true,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_xid8"),
	InputFunc:           toFuncID("xid8in", toInternal("cstring")),
	OutputFunc:          toFuncID("xid8out", toInternal("xid8")),
	ReceiveFunc:         toFuncID("xid8recv", toInternal("internal")),
	SendFunc:            toFuncID("xid8send", toInternal("xid8")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Double,
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
	SerializationFunc:   serializeTypeXid8,
	DeserializationFunc: deserializeTypeXid8,
}

// serializeTypeXid8 handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypeXid8(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	retVal := make([]byte, 8)
	binary.BigEndian.PutUint64(retVal, val.(uint64))
	return retVal, nil
}

// deserializeTypeXid8 handles deserialization from the Dolt serialized format to our standard representation used by
// expressions and nodes.
func deserializeTypeXid8(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return binary.BigEndian.Uint64(data), nil
}
