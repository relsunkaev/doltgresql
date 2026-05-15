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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initSqlJson() {
	framework.RegisterFunction(json_scalar)
	framework.RegisterFunction(json_serialize)
	framework.RegisterFunction(json_exists)
	framework.RegisterFunction(json_exists_passing)
	framework.RegisterFunction(json_value)
	framework.RegisterFunction(json_query)
}

var json_scalar = framework.Function1{
	Name:       "json_scalar",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		resolved, err := sql.UnwrapAny(ctx, val)
		if err != nil {
			return nil, err
		}
		if resolved == nil {
			return nil, nil
		}
		value, err := jsonValueFromAnyElement(ctx, t[0], resolved, true)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonDocument{Value: value}, nil
	},
}

var json_serialize = framework.Function1{
	Name:       "json_serialize",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		resolved, err := sql.UnwrapAny(ctx, val)
		if err != nil {
			return nil, err
		}
		if resolved == nil {
			return nil, nil
		}
		value, err := sqlJsonValueFromSerializeInput(ctx, t[0], resolved)
		if err != nil {
			return nil, err
		}
		var sb strings.Builder
		pgtypes.JsonValueFormatterCompact(&sb, value)
		return sb.String(), nil
	},
}

var json_exists = framework.Function2{
	Name:       "json_exists",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		matches, err := sqlJsonPathQuery(ctx, target, path)
		if err != nil {
			return nil, err
		}
		return len(matches) > 0, nil
	},
}

var json_exists_passing = framework.Function4{
	Name:       "json_exists",
	Return:     pgtypes.Bool,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text, pgtypes.AnyElement, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [5]*pgtypes.DoltgresType, target any, path any, passingValue any, passingName any) (any, error) {
		pathText, err := sqlJsonPathText(ctx, path)
		if err != nil {
			return nil, err
		}
		pathText, err = sqlJsonPathApplyPassing(ctx, pathText, t[2], passingValue, passingName)
		if err != nil {
			return nil, err
		}
		matches, err := sqlJsonPathQueryText(ctx, target, pathText)
		if err != nil {
			return nil, err
		}
		return len(matches) > 0, nil
	},
}

var json_value = framework.Function2{
	Name:       "json_value",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		matches, err := sqlJsonPathQuery(ctx, target, path)
		if err != nil {
			return nil, err
		}
		if len(matches) != 1 {
			return nil, nil
		}
		return sqlJsonValueAsText(ctx, matches[0])
	},
}

var json_query = framework.Function2{
	Name:       "json_query",
	Return:     pgtypes.JsonB,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		matches, err := sqlJsonPathQuery(ctx, target, path)
		if err != nil {
			return nil, err
		}
		if len(matches) != 1 {
			return nil, nil
		}
		return pgtypes.JsonDocument{Value: pgtypes.JsonValueCopy(matches[0])}, nil
	},
}

func sqlJsonValueFromSerializeInput(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (pgtypes.JsonValue, error) {
	if doc, ok := val.(pgtypes.JsonDocument); ok {
		return pgtypes.JsonValueCopy(doc.Value), nil
	}
	if value, ok := val.(pgtypes.JsonValue); ok {
		return pgtypes.JsonValueCopy(value), nil
	}
	if str, ok := val.(string); ok {
		doc, err := pgtypes.UnmarshalToJsonDocumentPreserveObjectItems([]byte(str))
		if err == nil {
			return doc.Value, nil
		}
	}
	return pgtypes.JsonValueFromSQLValue(ctx, typ, val)
}

func sqlJsonPathQuery(ctx *sql.Context, target any, path any) ([]pgtypes.JsonValue, error) {
	pathText, err := sqlJsonPathText(ctx, path)
	if err != nil {
		return nil, err
	}
	return sqlJsonPathQueryText(ctx, target, pathText)
}

func sqlJsonPathQueryText(ctx *sql.Context, target any, pathText string) ([]pgtypes.JsonValue, error) {
	doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, target)
	if err != nil {
		return nil, err
	}
	return jsonPathEval(doc.Value, pathText)
}

func sqlJsonPathText(ctx *sql.Context, path any) (string, error) {
	pathText, err := jsonPathText(ctx, path)
	if err != nil {
		return "", err
	}
	return sqlJsonNormalizePathMode(pathText), nil
}

func sqlJsonNormalizePathMode(path string) string {
	path = strings.TrimSpace(path)
	lower := strings.ToLower(path)
	for _, mode := range []string{"strict", "lax"} {
		if lower == mode {
			return "$"
		}
		prefix := mode + " "
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSpace(path[len(prefix):])
		}
	}
	return path
}

func sqlJsonPathApplyPassing(ctx *sql.Context, path string, typ *pgtypes.DoltgresType, val any, name any) (string, error) {
	resolvedName, err := sql.UnwrapAny(ctx, name)
	if err != nil {
		return "", err
	}
	nameText, ok := resolvedName.(string)
	if !ok || nameText == "" {
		return path, nil
	}
	value, err := jsonValueFromAnyElement(ctx, typ, val, true)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	pgtypes.JsonValueFormatterCompact(&sb, value)
	return strings.ReplaceAll(path, "$"+nameText, sb.String()), nil
}

func sqlJsonValueAsText(ctx *sql.Context, value pgtypes.JsonValue) (any, error) {
	value = pgtypes.JsonValueUnwrapRaw(value)
	switch value := value.(type) {
	case pgtypes.JsonValueNull:
		return nil, nil
	case pgtypes.JsonValueString:
		return pgtypes.JsonStringUnescape(value)
	default:
		var sb strings.Builder
		pgtypes.JsonValueFormatterCompact(&sb, value)
		return sb.String(), nil
	}
}
