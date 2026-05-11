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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initSetVal registers the functions to the catalog.
func initSetVal() {
	framework.RegisterFunction(setval_text_int64)
	framework.RegisterFunction(setval_text_int64_boolean)
}

// setval_text_int64 represents the PostgreSQL function of the same name, taking the same parameters.
var setval_text_int64 = framework.Function2{
	Name:               "setval",
	Return:             pgtypes.Int64,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Int64},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		var unusedTypes [4]*pgtypes.DoltgresType
		return setval_text_int64_boolean.Callable(ctx, unusedTypes, val1, val2, true)
	},
}

// setval_text_int64_boolean represents the PostgreSQL function of the same name, taking the same parameters.
var setval_text_int64_boolean = framework.Function3{
	Name:               "setval",
	Return:             pgtypes.Int64,
	Parameters:         [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Int64, pgtypes.Bool},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1 any, val2 any, val3 any) (any, error) {
		// TODO: this needs a database name to support inserts into other databases (including inserts on other branches than the current one)
		collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
		if err != nil {
			return nil, err
		}
		// TODO: this should take a regclass as the parameter to determine the schema
		schema, relation, err := ParseRelationName(ctx, val1.(string))
		if err != nil {
			return nil, err
		}
		return val2.(int64), collection.SetVal(ctx, id.NewSequence(schema, relation), val2.(int64), val3.(bool))
	},
}

// ParseRelationName parses the schema and relation name from a relation name string, including trimming any
// identifier quotes used in the name. Dots inside double-quoted identifiers are treated as part of the
// identifier rather than as schema separators, so `"my.table"` parses as a single relation name even though
// it contains a literal dot. For example, passing in 'public."MyTable"' would return 'public' and 'MyTable'.
func ParseRelationName(ctx *sql.Context, name string) (schema string, relation string, err error) {
	pathElems, err := splitQualifiedIdentifier(name)
	if err != nil {
		return "", "", err
	}
	switch len(pathElems) {
	case 1:
		schema, err = core.GetCurrentSchema(ctx)
		if err != nil {
			return "", "", err
		}
		relation = pathElems[0]
	case 2:
		schema = pathElems[0]
		relation = pathElems[1]
	case 3:
		// database is not used atm
		schema = pathElems[1]
		relation = pathElems[2]
	default:
		return "", "", errors.Errorf(`cannot parse relation: %s`, name)
	}
	return schema, relation, nil
}

// splitQualifiedIdentifier splits a qualified relation name on unquoted dots,
// honoring PostgreSQL's double-quoted identifier syntax: a dot inside `"..."`
// is part of the identifier, and `""` inside a quoted identifier escapes a
// single quote character. Unquoted identifiers preserve their original case
// (PostgreSQL would lower-case them at parse time, but the callers of this
// helper want a faithful round trip of the input string).
func splitQualifiedIdentifier(name string) ([]string, error) {
	var parts []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(name); i++ {
		c := name[i]
		if inQuote {
			if c != '"' {
				current.WriteByte(c)
				continue
			}
			// Closing quote, or `""` doubled-quote escape inside the literal.
			if i+1 < len(name) && name[i+1] == '"' {
				current.WriteByte('"')
				i++
				continue
			}
			inQuote = false
			continue
		}
		if c == '"' {
			inQuote = true
			continue
		}
		if c == '.' {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}
	if inQuote {
		return nil, errors.Errorf(`unterminated quoted identifier in relation: %s`, name)
	}
	parts = append(parts, current.String())
	return parts, nil
}
