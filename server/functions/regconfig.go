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

// initRegconfig registers the regconfig IO functions to the catalog.
func initRegconfig() {
	framework.RegisterFunction(regconfigin)
	framework.RegisterFunction(regconfigout)
	framework.RegisterFunction(regconfigrecv)
	framework.RegisterFunction(regconfigsend)
}

var regconfigin = framework.Function1{
	Name:       "regconfigin",
	Return:     pgtypes.Regconfig,
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
		schemaName, configName, err := parseTextSearchOIDAliasInput(input)
		if err != nil {
			return id.Null, err
		}
		if schemaName != "" && schemaName != "pg_catalog" {
			return id.Null, errors.Errorf(`text search configuration "%s" does not exist`, input)
		}
		switch configName {
		case "english", "simple":
			return id.NewId(id.Section_TextSearchConfig, "pg_catalog", configName), nil
		default:
			return id.Null, errors.Errorf(`text search configuration "%s" does not exist`, input)
		}
	},
}

var regconfigout = framework.Function1{
	Name:       "regconfigout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regconfig},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(id.Id)
		if input.Section() == id.Section_OID {
			return input.Segment(0), nil
		}
		return input.Segment(1), nil
	},
}

var regconfigrecv = framework.Function1{
	Name:       "regconfigrecv",
	Return:     pgtypes.Regconfig,
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

var regconfigsend = framework.Function1{
	Name:       "regconfigsend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regconfig},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(id.Cache().ToOID(val.(id.Id)))
		return writer.BufferData(), nil
	},
}

func parseTextSearchOIDAliasInput(input string) (string, string, error) {
	sections, err := ioInputSections(input)
	if err != nil {
		return "", "", err
	}
	switch len(sections) {
	case 1:
		return "", sections[0], nil
	case 3:
		if sections[1] != "." {
			return "", "", errors.Errorf("invalid name syntax")
		}
		return sections[0], sections[2], nil
	default:
		return "", "", errors.Errorf("invalid name syntax")
	}
}
