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
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initRegdictionary registers the regdictionary IO functions to the catalog.
func initRegdictionary() {
	framework.RegisterFunction(regdictionaryin)
	framework.RegisterFunction(regdictionaryout)
	framework.RegisterFunction(regdictionaryrecv)
	framework.RegisterFunction(regdictionarysend)
}

var regdictionaryin = framework.Function1{
	Name:       "regdictionaryin",
	Return:     pgtypes.Regdictionary,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if parsedOid, err := strconv.ParseUint(input, 10, 32); err == nil {
			if internalID := id.Cache().ToInternal(uint32(parsedOid)); internalID.IsValid() {
				return internalID, nil
			}
			return id.NewOID(uint32(parsedOid)).AsId(), nil
		}
		schemaName, dictionaryName, err := parseTextSearchOIDAliasInput(input)
		if err != nil {
			return id.Null, err
		}
		if schemaName != "" && schemaName != "pg_catalog" {
			return id.Null, errors.Errorf(`text search dictionary "%s" does not exist`, input)
		}
		switch dictionaryName {
		case "english_stem", "simple":
			return id.NewId(id.Section_TextSearchDictionary, "pg_catalog", dictionaryName), nil
		default:
			return id.Null, errors.Errorf(`text search dictionary "%s" does not exist`, input)
		}
	},
}

var regdictionaryout = framework.Function1{
	Name:       "regdictionaryout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regdictionary},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(id.Id)
		if input.Section() == id.Section_OID {
			return input.Segment(0), nil
		}
		return input.Segment(1), nil
	},
}

var regdictionaryrecv = framework.Function1{
	Name:       "regdictionaryrecv",
	Return:     pgtypes.Regdictionary,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		reader := utils.NewWireReader(data)
		cachedID := id.Cache().ToInternal(reader.ReadUint32())
		return cachedID, nil
	},
}

var regdictionarysend = framework.Function1{
	Name:       "regdictionarysend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regdictionary},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(id.Cache().ToOID(val.(id.Id)))
		return writer.BufferData(), nil
	},
}
