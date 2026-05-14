// Copyright 2024 Dolthub, Inc.
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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lib/pq/oid"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/types"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/settings"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initRegtype registers the functions to the catalog.
func initRegtype() {
	framework.RegisterFunction(regtypein)
	framework.RegisterFunction(regtypeout)
	framework.RegisterFunction(regtyperecv)
	framework.RegisterFunction(regtypesend)
}

// regtypein represents the PostgreSQL function of regtype type IO input.
var regtypein = framework.Function1{
	Name:       "regtypein",
	Return:     pgtypes.Regtype,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		// If the string just represents a number, then we return it.
		input := val.(string)
		if parsedOid, err := strconv.ParseUint(input, 10, 32); err == nil {
			if internalID := id.Cache().ToInternal(uint32(parsedOid)); internalID.IsValid() {
				return internalID, nil
			}
			return id.NewOID(uint32(parsedOid)).AsId(), nil
		}
		sections, err := ioInputSections(input)
		if err != nil {
			return id.Null, err
		}
		if err = regtype_IoInputValidation(ctx, input, sections); err != nil {
			return id.Null, err
		}
		var searchSchemas []string
		var typeName string
		switch len(sections) {
		case 1:
			searchSchemas, err = core.SearchPath(ctx)
			if err != nil {
				return id.Null, err
			}
			typeName = sections[0]
		case 3:
			searchSchemas = []string{sections[0]}
			typeName = sections[2]
			if sections[0] == "pg_catalog" && typeName == "char" { // Sad but true
				typeName = `"char"`
			}
		default:
			return id.Null, errors.Errorf("regtype failed validation")
		}
		// Remove everything after the first parenthesis
		typeName = strings.Split(typeName, "(")[0]

		if typeName == "char" && len(searchSchemas) > 0 && searchSchemas[0] == "pg_catalog" {
			return id.NewType("pg_catalog", "bpchar").AsId(), nil
		}
		if typeName == "int" {
			typeName = "int4"
		}
		typeCollection, err := core.GetTypesCollectionFromContext(ctx)
		if err != nil {
			return id.Null, err
		}
		for _, schema := range searchSchemas {
			if internalID, ok := pgtypes.NameToInternalID[typeName]; ok && internalID.SchemaName() == schema {
				return internalID.AsId(), nil
			}
			typ, err := typeCollection.GetType(ctx, id.NewType(schema, typeName))
			if err != nil {
				return id.Null, err
			}
			if typ != nil {
				return typ.ID.AsId(), nil
			}
		}
		return id.Null, pgtypes.ErrTypeDoesNotExist.New(input)
	},
}

// regtypeout represents the PostgreSQL function of regtype type IO output.
var regtypeout = framework.Function1{
	Name:       "regtypeout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regtype},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		internalID := val.(id.Id)
		if internalID.Section() == id.Section_OID {
			return internalID.Segment(0), nil
		}
		toid := id.Cache().ToOID(internalID)
		if t, ok := types.OidToType[oid.Oid(toid)]; ok {
			return t.SQLStandardName(), nil
		}
		schemasMap, err := settings.GetCurrentSchemasAsMap(ctx)
		if err != nil {
			return "", err
		}
		schemasMap["pg_catalog"] = struct{}{}
		schemaName := internalID.Segment(0)
		typeName := internalID.Segment(1)
		if _, ok := schemasMap[schemaName]; ok {
			return typeName, nil
		}
		return schemaName + "." + typeName, nil
	},
}

// regtyperecv represents the PostgreSQL function of regtype type IO receive.
var regtyperecv = framework.Function1{
	Name:       "regtyperecv",
	Return:     pgtypes.Regtype,
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

// regtypesend represents the PostgreSQL function of regtype type IO send.
var regtypesend = framework.Function1{
	Name:       "regtypesend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regtype},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(id.Cache().ToOID(val.(id.Id)))
		return writer.BufferData(), nil
	},
}

// regtype_IoInputValidation handles the validation for the parsed sections in regtypein.
func regtype_IoInputValidation(ctx *sql.Context, input string, sections []string) error {
	if regtypeHasMisplacedQuote(input) {
		return pgerror.New(pgcode.Syntax, "invalid name syntax")
	}
	switch len(sections) {
	case 1:
		return nil
	case 3:
		// We check for name validity before checking the schema name
		if sections[1] != "." {
			return errors.Errorf("invalid name syntax")
		}
		return nil
	case 5:
		if sections[1] != "." || sections[3] != "." {
			return errors.Errorf("invalid name syntax")
		}
		return errors.Errorf("cross-database references are not implemented: %s", input)
	case 7:
		if sections[1] != "." || sections[3] != "." || sections[5] != "." {
			return errors.Errorf("invalid name syntax")
		}
		return errors.Errorf("improper qualified name (too many dotted names): %s", input)
	default:
		return errors.Errorf("invalid name syntax")
	}
}

func regtypeHasMisplacedQuote(input string) bool {
	trimmed := []rune(strings.TrimSpace(input))
	inQuotes := false
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] != '"' {
			continue
		}
		if inQuotes {
			if i < len(trimmed)-1 && trimmed[i+1] == '"' {
				i++
				continue
			}
			inQuotes = false
			continue
		}
		if i != 0 && trimmed[i-1] != '.' {
			return true
		}
		inQuotes = true
	}
	return false
}
