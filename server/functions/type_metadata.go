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
	"regexp"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var typeModifierPattern = regexp.MustCompile(`\(\s*([0-9]+)\s*\)`)

func initTypeMetadataBuiltins() {
	framework.RegisterFunction(pg_basetype_regtype)
	framework.RegisterFunction(to_regtypemod_text)
}

var pg_basetype_regtype = framework.Function1{
	Name:       "pg_basetype",
	Return:     pgtypes.Regtype,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regtype},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgBaseType(ctx, val.(id.Id))
	},
}

var to_regtypemod_text = framework.Function1{
	Name:       "to_regtypemod",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return parseTypeModifier(val.(string)), nil
	},
}

func pgBaseType(ctx *sql.Context, typeID id.Id) (id.Id, error) {
	if typeID.Section() != id.Section_Type {
		return typeID, nil
	}
	typeColl, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	current := id.Type(typeID)
	for {
		typ, err := typeColl.GetType(ctx, current)
		if err != nil {
			return id.Null, err
		}
		if typ == nil || typ.TypType != pgtypes.TypeType_Domain {
			return current.AsId(), nil
		}
		current = typ.BaseTypeID
	}
}

func parseTypeModifier(input string) int32 {
	matches := typeModifierPattern.FindStringSubmatch(strings.TrimSpace(input))
	if matches == nil {
		return -1
	}
	value, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		return -1
	}
	return int32(value + 4)
}
