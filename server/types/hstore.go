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
