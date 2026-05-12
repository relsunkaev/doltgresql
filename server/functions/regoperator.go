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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/types"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initRegoperator registers the regoperator IO functions to the catalog.
func initRegoperator() {
	framework.RegisterFunction(regoperatorin)
	framework.RegisterFunction(regoperatorout)
	framework.RegisterFunction(regoperatorrecv)
	framework.RegisterFunction(regoperatorsend)
}

var regoperatorin = framework.Function1{
	Name:       "regoperatorin",
	Return:     pgtypes.Regoperator,
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
		schemaName, operatorName, argTypes, err := parseRegoperatorInput(ctx, input)
		if err != nil {
			return id.Null, err
		}
		if schemaName != "" && schemaName != "pg_catalog" {
			return id.Null, errors.Errorf(`operator "%s" does not exist`, input)
		}
		if _, err = framework.GetOperatorFromString(operatorName); err != nil {
			return id.Null, errors.Errorf(`operator "%s" does not exist`, input)
		}
		return id.NewId(id.Section_Operator, operatorName, string(argTypes[0]), string(argTypes[1])), nil
	},
}

func parseRegoperatorInput(ctx *sql.Context, input string) (string, string, []id.Type, error) {
	trimmed := strings.TrimSpace(input)
	openParen := strings.IndexRune(trimmed, '(')
	if openParen < 0 || !strings.HasSuffix(trimmed, ")") {
		return "", "", nil, errors.Errorf("invalid name syntax")
	}

	operatorInput := strings.TrimSpace(trimmed[:openParen])
	sections, err := ioInputSections(operatorInput)
	if err != nil {
		return "", "", nil, err
	}
	var schemaName string
	var operatorName string
	switch len(sections) {
	case 1:
		operatorName = sections[0]
	case 3:
		if sections[1] != "." {
			return "", "", nil, errors.Errorf("invalid name syntax")
		}
		schemaName = sections[0]
		operatorName = sections[2]
	default:
		return "", "", nil, errors.Errorf("invalid name syntax")
	}
	if operatorName == "" {
		return "", "", nil, errors.Errorf("invalid name syntax")
	}

	argInput := strings.TrimSpace(trimmed[openParen+1 : len(trimmed)-1])
	args, err := splitRegprocedureArgs(argInput)
	if err != nil {
		return "", "", nil, err
	}
	if len(args) != 2 {
		return "", "", nil, errors.Errorf("expected a binary operator signature")
	}
	argTypes := make([]id.Type, 2)
	for i, arg := range args {
		typeID, err := regprocedureArgType(ctx, arg)
		if err != nil {
			return "", "", nil, err
		}
		argTypes[i] = typeID
	}
	return schemaName, operatorName, argTypes, nil
}

var regoperatorout = framework.Function1{
	Name:       "regoperatorout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regoperator},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(id.Id)
		if input.Section() == id.Section_OID {
			return input.Segment(0), nil
		}
		if input.Section() != id.Section_Operator {
			return input.Segment(0), nil
		}
		leftType := regoperatorTypeOutput(id.Type(input.Segment(1)))
		rightType := regoperatorTypeOutput(id.Type(input.Segment(2)))
		return fmt.Sprintf("%s(%s,%s)", input.Segment(0), leftType, rightType), nil
	},
}

func regoperatorTypeOutput(typeID id.Type) string {
	if parserType, ok := types.OidToType[oid.Oid(id.Cache().ToOID(typeID.AsId()))]; ok {
		return parserType.SQLStandardName()
	}
	if typ := pgtypes.GetTypeByID(typeID); typ != nil {
		return typ.Name()
	}
	return typeID.TypeName()
}

var regoperatorrecv = framework.Function1{
	Name:       "regoperatorrecv",
	Return:     pgtypes.Regoperator,
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

var regoperatorsend = framework.Function1{
	Name:       "regoperatorsend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regoperator},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(id.Cache().ToOID(val.(id.Id)))
		return writer.BufferData(), nil
	},
}
