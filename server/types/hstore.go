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

import "github.com/dolthub/doltgresql/core/id"

var canonicalHstore = NewHstoreType(id.NewType("public", "_hstore"), id.NewType("public", "hstore"))

// NewHstoreType returns a text-compatible hstore extension type.
func NewHstoreType(arrayID, typeID id.Type) *DoltgresType {
	hstore := *Text
	hstore.ID = typeID
	hstore.Array = arrayID
	hstore.IsPreferred = false
	hstore.InternalName = "hstore"
	return &hstore
}

// NewGhstoreType returns the GiST storage type supplied by the hstore extension.
func NewGhstoreType(arrayID, typeID id.Type) *DoltgresType {
	return &DoltgresType{
		ID:                  typeID,
		TypLength:           int16(-1),
		PassedByVal:         false,
		TypType:             TypeType_Base,
		TypCategory:         TypeCategory_UserDefinedTypes,
		IsPreferred:         false,
		IsDefined:           true,
		Delimiter:           ",",
		RelID:               id.Null,
		SubscriptFunc:       toFuncID("-"),
		Elem:                id.NullType,
		Array:               arrayID,
		InputFunc:           toFuncID("ghstore_in", toInternal("cstring")),
		OutputFunc:          toFuncID("ghstore_out", typeID),
		ReceiveFunc:         toFuncID("-"),
		SendFunc:            toFuncID("-"),
		ModInFunc:           toFuncID("-"),
		ModOutFunc:          toFuncID("-"),
		AnalyzeFunc:         toFuncID("-"),
		Align:               TypeAlignment_Int,
		Storage:             TypeStorage_Extended,
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
		InternalName:        "ghstore",
		SerializationFunc:   serializeTypeText,
		DeserializationFunc: deserializeTypeText,
	}
}

// HstoreBuiltinEquivalent returns the canonical hstore type used by compiled
// hstore functions for extension hstore types installed into any schema.
func HstoreBuiltinEquivalent(typ *DoltgresType) (*DoltgresType, bool) {
	if typ == nil {
		return nil, false
	}
	if typ.IsArrayType() {
		if typ.Elem.TypeName() != "hstore" && typ.ID.TypeName() != "_hstore" {
			return nil, false
		}
		return canonicalHstore.ToArrayType(), true
	}
	if typ.ID.TypeName() != "hstore" {
		return nil, false
	}
	if typ.IsUnresolved || typ.InternalName == "hstore" {
		return canonicalHstore, true
	}
	return nil, false
}
