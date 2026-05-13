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
	"strings"

	"github.com/lib/pq/oid"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/procedures"
	"github.com/dolthub/doltgresql/postgres/parser/types"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type pgFunctionMetadata struct {
	SchemaName        string
	FunctionName      string
	ReturnType        id.Type
	ParameterNames    []string
	ParameterTypes    []id.Type
	ParameterModes    []string
	ParameterDefaults []string
	Language          string
	Body              string
	Volatility        string
	Strict            bool
	SetOf             bool
	InternalSymbol    string
}

func pgGetFunctionResult(ctx *sql.Context, oidVal id.Id) (string, error) {
	metadata, ok, err := pgFunctionMetadataForOID(ctx, oidVal)
	if err != nil || !ok {
		return "", err
	}
	result := pgFunctionTypeName(ctx, metadata.ReturnType)
	if metadata.SetOf {
		result = "SETOF " + result
	}
	return result, nil
}

func pgGetFunctionArguments(ctx *sql.Context, oidVal id.Id, includeDefaults bool) (string, error) {
	metadata, ok, err := pgFunctionMetadataForOID(ctx, oidVal)
	if err != nil || !ok {
		return "", err
	}
	args := make([]string, len(metadata.ParameterTypes))
	firstDefault := len(metadata.ParameterTypes) - len(metadata.ParameterDefaults)
	for i, typ := range metadata.ParameterTypes {
		parts := make([]string, 0, 4)
		if i < len(metadata.ParameterModes) && metadata.ParameterModes[i] != "" {
			parts = append(parts, metadata.ParameterModes[i])
		}
		if i < len(metadata.ParameterNames) && metadata.ParameterNames[i] != "" {
			parts = append(parts, metadata.ParameterNames[i])
		}
		parts = append(parts, pgFunctionTypeName(ctx, typ))
		if includeDefaults && i >= firstDefault && i-firstDefault < len(metadata.ParameterDefaults) && metadata.ParameterDefaults[i-firstDefault] != "" {
			parts = append(parts, "DEFAULT", metadata.ParameterDefaults[i-firstDefault])
		}
		args[i] = strings.Join(parts, " ")
	}
	return strings.Join(args, ", "), nil
}

func pgGetFunctionArgDefault(ctx *sql.Context, oidVal id.Id, argNum int32) (any, error) {
	metadata, ok, err := pgFunctionMetadataForOID(ctx, oidVal)
	if err != nil || !ok {
		return nil, err
	}
	idx := int(argNum)
	if idx < 0 || idx >= len(metadata.ParameterTypes) {
		return nil, nil
	}
	if len(metadata.ParameterDefaults) == len(metadata.ParameterTypes) {
		if metadata.ParameterDefaults[idx] == "" {
			return nil, nil
		}
		return metadata.ParameterDefaults[idx], nil
	}
	firstDefault := len(metadata.ParameterTypes) - len(metadata.ParameterDefaults)
	defaultIdx := idx - firstDefault
	if defaultIdx < 0 || defaultIdx >= len(metadata.ParameterDefaults) || metadata.ParameterDefaults[defaultIdx] == "" {
		return nil, nil
	}
	return metadata.ParameterDefaults[defaultIdx], nil
}

func pgGetFunctionDef(ctx *sql.Context, oidVal id.Id) (string, error) {
	metadata, ok, err := pgFunctionMetadataForOID(ctx, oidVal)
	if err != nil || !ok {
		return "", err
	}
	args, err := pgGetFunctionArguments(ctx, oidVal, true)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION %s.%s(%s)\n", metadata.SchemaName, metadata.FunctionName, args)
	fmt.Fprintf(&b, " RETURNS %s\n", func() string {
		result := pgFunctionTypeName(ctx, metadata.ReturnType)
		if metadata.SetOf {
			return "SETOF " + result
		}
		return result
	}())
	fmt.Fprintf(&b, " LANGUAGE %s\n", metadata.Language)
	if metadata.Volatility != "" {
		b.WriteString(pgFunctionVolatility(metadata.Volatility))
	}
	if metadata.Strict {
		if metadata.Volatility != "" {
			b.WriteByte(' ')
		}
		b.WriteString("STRICT")
	}
	if metadata.Volatility != "" || metadata.Strict {
		b.WriteByte('\n')
	}
	if metadata.InternalSymbol != "" {
		fmt.Fprintf(&b, "AS '%s';", metadata.InternalSymbol)
	} else {
		fmt.Fprintf(&b, "AS $$ %s $$;", metadata.Body)
	}
	return b.String(), nil
}

func pgFunctionMetadataForOID(ctx *sql.Context, oidVal id.Id) (pgFunctionMetadata, bool, error) {
	if metadata, ok := pgBuiltinFunctionMetadata(oidVal); ok {
		return metadata, true, nil
	}
	if oidVal.Section() != id.Section_Function {
		return pgFunctionMetadata{}, false, nil
	}
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return pgFunctionMetadata{}, false, err
	}
	function, err := funcColl.GetFunction(ctx, id.Function(oidVal))
	if err != nil {
		return pgFunctionMetadata{}, false, err
	}
	if !function.ID.IsValid() {
		return pgFunctionMetadata{}, false, nil
	}
	return pgUserFunctionMetadata(function), true, nil
}

func pgBuiltinFunctionMetadata(oidVal id.Id) (pgFunctionMetadata, bool) {
	for _, overloads := range framework.Catalog {
		for _, overload := range overloads {
			functionID := pgBuiltinFunctionID(overload)
			if functionID.AsId() != oidVal {
				continue
			}
			params := overload.GetParameters()
			paramTypes := make([]id.Type, len(params))
			for i, param := range params {
				paramTypes[i] = param.ID
			}
			return pgFunctionMetadata{
				SchemaName:     functionID.SchemaName(),
				FunctionName:   functionID.FunctionName(),
				ReturnType:     overload.GetReturn().ID,
				ParameterTypes: paramTypes,
				Language:       "internal",
				Volatility:     "v",
				Strict:         overload.IsStrict(),
				SetOf:          overload.IsSRF(),
				InternalSymbol: overload.GetName(),
			}, true
		}
	}
	return pgFunctionMetadata{}, false
}

func pgBuiltinFunctionID(function framework.FunctionInterface) id.Function {
	functionID := id.Function(function.InternalID())
	if !functionID.IsValid() || id.Id(functionID).Section() != id.Section_Function {
		params := function.GetParameters()
		paramTypes := make([]id.Type, len(params))
		for i, param := range params {
			paramTypes[i] = param.ID
		}
		functionID = id.NewFunction("pg_catalog", function.GetName(), paramTypes...)
	}
	return functionID
}

func pgFunctionParameterModes(modes []procedures.ParameterMode, count int) []string {
	if len(modes) == 0 {
		return nil
	}
	result := make([]string, count)
	hasNonInMode := false
	for i := 0; i < count; i++ {
		mode := procedures.ParameterMode_IN
		if i < len(modes) {
			mode = modes[i]
		}
		switch mode {
		case procedures.ParameterMode_OUT:
			result[i] = "OUT"
			hasNonInMode = true
		case procedures.ParameterMode_INOUT:
			result[i] = "INOUT"
			hasNonInMode = true
		case procedures.ParameterMode_VARIADIC:
			result[i] = "VARIADIC"
			hasNonInMode = true
		}
	}
	if !hasNonInMode {
		return nil
	}
	return result
}

func pgUserFunctionMetadata(function corefunctions.Function) pgFunctionMetadata {
	body := function.SQLDefinition
	language := "sql"
	if body == "" {
		body = function.GetInnerDefinition()
		language = "plpgsql"
	}
	return pgFunctionMetadata{
		SchemaName:        function.ID.SchemaName(),
		FunctionName:      function.ID.FunctionName(),
		ReturnType:        function.ReturnType,
		ParameterNames:    function.ParameterNames,
		ParameterTypes:    function.ParameterTypes,
		ParameterModes:    pgFunctionParameterModes(function.ParameterModes, len(function.ParameterTypes)),
		ParameterDefaults: function.ParameterDefaults,
		Language:          language,
		Body:              body,
		Volatility:        function.Volatility,
		Strict:            function.Strict,
		SetOf:             function.SetOf,
	}
}

func pgFunctionTypeName(ctx *sql.Context, typeID id.Type) string {
	toid := id.Cache().ToOID(typeID.AsId())
	if typ, ok := types.OidToType[oid.Oid(toid)]; ok {
		return typ.SQLStandardName()
	}
	if typ := pgtypes.GetTypeByID(typeID); typ != nil {
		return typ.Name()
	}
	typeColl, err := core.GetTypesCollectionFromContext(ctx)
	if err == nil {
		if typ, err := typeColl.GetType(ctx, typeID); err == nil && typ != nil {
			return typ.Name()
		}
	}
	return typeID.TypeName()
}

func pgFunctionVolatility(volatility string) string {
	switch volatility {
	case "i":
		return "IMMUTABLE"
	case "s":
		return "STABLE"
	case "v":
		return "VOLATILE"
	default:
		return ""
	}
}
