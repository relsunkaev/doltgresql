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
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lib/pq/oid"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/types"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initRegprocedure registers the regprocedure IO functions to the catalog.
func initRegprocedure() {
	framework.RegisterFunction(regprocedurein)
	framework.RegisterFunction(regprocedureout)
	framework.RegisterFunction(to_regprocedure_text)
	framework.RegisterFunction(regprocedurerecv)
	framework.RegisterFunction(regproceduresend)
}

// regprocedurein represents the PostgreSQL regprocedure input function.
// It resolves a function signature literal to the same pg_proc OID alias
// that pg_type.typinput and related catalog fields expose.
var regprocedurein = framework.Function1{
	Name:       "regprocedurein",
	Return:     pgtypes.Regprocedure,
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

		schemaName, functionName, paramTypes, err := parseRegprocedureInput(ctx, input)
		if err != nil {
			return id.Null, err
		}
		if schemaName == "" || schemaName == "pg_catalog" {
			for _, overload := range framework.Catalog[functionName] {
				params := overload.GetParameters()
				if len(params) != len(paramTypes) {
					continue
				}
				matched := true
				for i, param := range params {
					if param.ID != paramTypes[i] {
						matched = false
						break
					}
				}
				if matched {
					return overload.InternalID(), nil
				}
			}
		}

		functionCollection, err := core.GetFunctionsCollectionFromContext(ctx)
		if err != nil {
			return id.Null, err
		}
		searchSchemas := []string{schemaName}
		if schemaName == "" {
			searchSchemas, err = core.SearchPath(ctx)
			if err != nil {
				return id.Null, err
			}
		}
		for _, searchSchema := range searchSchemas {
			overloads, err := functionCollection.GetFunctionOverloads(ctx, id.NewFunction(searchSchema, functionName))
			if err != nil {
				return id.Null, err
			}
			for _, overload := range overloads {
				if functionIDMatchesTypes(overload.ID, paramTypes) {
					return overload.ID.AsId(), nil
				}
			}
		}
		return id.Null, errors.Errorf(`function "%s" does not exist`, input)
	},
}

func functionIDMatchesTypes(functionID id.Function, paramTypes []id.Type) bool {
	params := functionID.Parameters()
	if len(params) != len(paramTypes) {
		return false
	}
	for i, param := range params {
		if param != paramTypes[i] {
			return false
		}
	}
	return true
}

func parseRegprocedureInput(ctx *sql.Context, input string) (string, string, []id.Type, error) {
	trimmed := strings.TrimSpace(input)
	openParen := strings.IndexRune(trimmed, '(')
	if openParen < 0 {
		procID, err := regprocin.Callable(ctx, [2]*pgtypes.DoltgresType{}, trimmed)
		if err != nil {
			return "", "", nil, err
		}
		functionID := id.Function(procID.(id.Id))
		return id.Id(functionID).Segment(0), functionID.FunctionName(), functionID.Parameters(), nil
	}
	if !strings.HasSuffix(trimmed, ")") {
		return "", "", nil, errors.Errorf("invalid name syntax")
	}

	functionInput := strings.TrimSpace(trimmed[:openParen])
	sections, err := ioInputSections(functionInput)
	if err != nil {
		return "", "", nil, err
	}
	var schemaName string
	var functionName string
	switch len(sections) {
	case 1:
		functionName = sections[0]
	case 3:
		if sections[1] != "." {
			return "", "", nil, errors.Errorf("invalid name syntax")
		}
		schemaName = sections[0]
		functionName = sections[2]
	default:
		return "", "", nil, errors.Errorf("invalid name syntax")
	}
	if functionName == "" {
		return "", "", nil, errors.Errorf("invalid name syntax")
	}

	argInput := strings.TrimSpace(trimmed[openParen+1 : len(trimmed)-1])
	if argInput == "" {
		return schemaName, functionName, nil, nil
	}
	args, err := splitRegprocedureArgs(argInput)
	if err != nil {
		return "", "", nil, err
	}
	paramTypes := make([]id.Type, len(args))
	for i, arg := range args {
		typeID, err := regprocedureArgType(ctx, arg)
		if err != nil {
			return "", "", nil, err
		}
		paramTypes[i] = typeID
	}
	return schemaName, functionName, paramTypes, nil
}

func splitRegprocedureArgs(input string) ([]string, error) {
	var args []string
	var builder strings.Builder
	var inQuotes bool
	depth := 0
	runes := []rune(input)
	for i := 0; i < len(runes); i++ {
		char := runes[i]
		switch char {
		case '"':
			builder.WriteRune(char)
			if inQuotes {
				if i < len(runes)-1 && runes[i+1] == '"' {
					builder.WriteRune(runes[i+1])
					i++
				} else {
					inQuotes = false
				}
			} else {
				inQuotes = true
			}
		case '(':
			if !inQuotes {
				depth++
			}
			builder.WriteRune(char)
		case ')':
			if !inQuotes {
				depth--
				if depth < 0 {
					return nil, errors.Errorf("invalid name syntax")
				}
			}
			builder.WriteRune(char)
		case ',':
			if !inQuotes && depth == 0 {
				args = append(args, strings.TrimSpace(builder.String()))
				builder.Reset()
			} else {
				builder.WriteRune(char)
			}
		default:
			builder.WriteRune(char)
		}
	}
	if inQuotes || depth != 0 {
		return nil, errors.Errorf("invalid name syntax")
	}
	args = append(args, strings.TrimSpace(builder.String()))
	for _, arg := range args {
		if arg == "" {
			return nil, errors.Errorf("invalid name syntax")
		}
	}
	return args, nil
}

func regprocedureArgType(ctx *sql.Context, input string) (id.Type, error) {
	typeID, err := regtypein.Callable(ctx, [2]*pgtypes.DoltgresType{}, input)
	if err != nil {
		return id.NullType, err
	}
	return id.Type(typeID.(id.Id)), nil
}

// regprocedureout represents the PostgreSQL regprocedure output function.
var regprocedureout = framework.Function1{
	Name:       "regprocedureout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regprocedure},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(id.Id)
		if input.Section() == id.Section_OID {
			return input.Segment(0), nil
		}
		if input.Section() != id.Section_Function {
			return input.Segment(1), nil
		}
		functionID := id.Function(input)
		paramNames := make([]string, 0, functionID.ParameterCount())
		for _, param := range functionID.Parameters() {
			if parserType, ok := types.OidToType[oid.Oid(id.Cache().ToOID(param.AsId()))]; ok {
				paramNames = append(paramNames, parserType.SQLStandardName())
			} else if typ := pgtypes.GetTypeByID(param); typ != nil {
				paramNames = append(paramNames, typ.Name())
			} else {
				paramNames = append(paramNames, param.TypeName())
			}
		}
		return fmt.Sprintf("%s(%s)", functionID.FunctionName(), strings.Join(paramNames, ",")), nil
	},
}

// to_regprocedure_text represents the PostgreSQL function of the same name,
// returning NULL instead of raising an error when the function is missing.
var to_regprocedure_text = framework.Function1{
	Name:               "to_regprocedure",
	Return:             pgtypes.Regprocedure,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Text},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		if _, err := strconv.ParseUint(val.(string), 10, 32); err == nil {
			return nil, nil
		}
		oid, err := regprocedurein.Callable(ctx, [2]*pgtypes.DoltgresType{}, val.(string))
		if err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				return nil, nil
			}
			return nil, err
		}
		return oid, nil
	},
}

// regprocedurerecv represents the PostgreSQL regprocedure receive function.
var regprocedurerecv = framework.Function1{
	Name:       "regprocedurerecv",
	Return:     pgtypes.Regprocedure,
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

// regproceduresend represents the PostgreSQL regprocedure send function.
var regproceduresend = framework.Function1{
	Name:       "regproceduresend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regprocedure},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(id.Cache().ToOID(val.(id.Id)))
		return writer.BufferData(), nil
	},
}
