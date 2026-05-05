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
	"bytes"
	stdjson "encoding/json"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	fastjson "github.com/goccy/go-json"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	jsonPredicateKindValue  = "value"
	jsonPredicateKindScalar = "scalar"
	jsonPredicateKindArray  = "array"
	jsonPredicateKindObject = "object"
)

// doltgres_json_predicate is the internal implementation for the PostgreSQL
// SQL/JSON `IS JSON` predicate.
var doltgres_json_predicate = framework.Function3{
	Name:       "doltgres_json_predicate",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.AnyElement, pgtypes.Text, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, t [4]*pgtypes.DoltgresType, val any, kindArg any, uniqueArg any) (any, error) {
		kind := kindArg.(string)
		requireUniqueKeys := uniqueArg.(bool)

		actualKind, valid, err := evalJSONPredicateInput(ctx, t[0], val, requireUniqueKeys)
		if err != nil || !valid {
			return false, err
		}

		switch kind {
		case jsonPredicateKindValue:
			return true, nil
		case jsonPredicateKindScalar:
			return actualKind == jsonPredicateKindScalar, nil
		case jsonPredicateKindArray:
			return actualKind == jsonPredicateKindArray, nil
		case jsonPredicateKindObject:
			return actualKind == jsonPredicateKindObject, nil
		default:
			return nil, errors.Errorf("unknown IS JSON predicate kind %q", kind)
		}
	},
}

func evalJSONPredicateInput(ctx *sql.Context, typ *pgtypes.DoltgresType, val any, requireUniqueKeys bool) (kind string, valid bool, err error) {
	if typ.ID == pgtypes.JsonB.ID {
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, typ, val)
		if err != nil {
			return "", false, err
		}
		return jsonPredicateKindFromValue(doc.Value), true, nil
	}

	data, err := jsonPredicateBytes(ctx, typ, val)
	if err != nil {
		return "", false, err
	}
	if requireUniqueKeys {
		return scanJSONPredicateUnique(data)
	}
	if !fastjson.Valid(data) {
		return "", false, nil
	}
	return jsonPredicateKindFromBytes(data), true, nil
}

func jsonPredicateBytes(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) ([]byte, error) {
	unwrapped, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	switch typ.ID {
	case pgtypes.Json.ID:
		return []byte(unwrapped.(string)), nil
	case pgtypes.Bytea.ID:
		return unwrapped.([]byte), nil
	}
	switch typ.TypCategory {
	case pgtypes.TypeCategory_StringTypes, pgtypes.TypeCategory_UnknownTypes:
		return []byte(unwrapped.(string)), nil
	default:
		return nil, errors.Errorf("cannot use type %s in IS JSON predicate", typ.String())
	}
}

func jsonPredicateKindFromValue(value pgtypes.JsonValue) string {
	switch value.(type) {
	case pgtypes.JsonValueObject:
		return jsonPredicateKindObject
	case pgtypes.JsonValueArray:
		return jsonPredicateKindArray
	default:
		return jsonPredicateKindScalar
	}
}

func jsonPredicateKindFromBytes(data []byte) string {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return jsonPredicateKindObject
		case '[':
			return jsonPredicateKindArray
		default:
			return jsonPredicateKindScalar
		}
	}
	return ""
}

func scanJSONPredicateUnique(data []byte) (kind string, valid bool, err error) {
	dec := stdjson.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	kind, valid, err = scanJSONPredicateValue(dec)
	if err != nil || !valid {
		return "", false, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return "", false, nil
	}
	return kind, true, nil
}

func scanJSONPredicateValue(dec *stdjson.Decoder) (kind string, valid bool, err error) {
	tok, err := dec.Token()
	if err != nil {
		return "", false, nil
	}
	switch tok := tok.(type) {
	case stdjson.Delim:
		switch tok {
		case '{':
			keys := make(map[string]struct{})
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return "", false, nil
				}
				key, ok := keyTok.(string)
				if !ok {
					return "", false, nil
				}
				if _, ok = keys[key]; ok {
					return "", false, nil
				}
				keys[key] = struct{}{}
				if _, valid, err = scanJSONPredicateValue(dec); err != nil || !valid {
					return "", valid, err
				}
			}
			endTok, err := dec.Token()
			if err != nil || endTok != stdjson.Delim('}') {
				return "", false, nil
			}
			return jsonPredicateKindObject, true, nil
		case '[':
			for dec.More() {
				if _, valid, err = scanJSONPredicateValue(dec); err != nil || !valid {
					return "", valid, err
				}
			}
			endTok, err := dec.Token()
			if err != nil || endTok != stdjson.Delim(']') {
				return "", false, nil
			}
			return jsonPredicateKindArray, true, nil
		default:
			return "", false, nil
		}
	default:
		return jsonPredicateKindScalar, true, nil
	}
}
