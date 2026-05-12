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

package binary

import (
	byteorder "encoding/binary"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	json "github.com/goccy/go-json"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	pgfunctions "github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

var hstoreType = pgtypes.NewUnresolvedDoltgresType("public", "hstore")
var hstoreLooseJsonNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

const (
	hstoreFlagNewVersion = uint32(0x80000000)
	hstoreEntryIsFirst   = uint32(0x80000000)
	hstoreEntryIsNull    = uint32(0x40000000)
	hstoreEntryPosMask   = uint32(0x3fffffff)
)

// initHstore registers operators and functions supplied by the hstore extension.
func initHstore() {
	initHstoreTableFunctions()
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONExtractJson, hstore_fetchval)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONExtractJson, hstore_slice_array)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONTopLevel, hstore_exist)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONTopLevelAny, hstore_exists_any)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONTopLevelAll, hstore_exists_all)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONContainsRight, hstore_contains)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONContainsLeft, hstore_contained)
	framework.RegisterBinaryFunction(framework.Operator_BinaryConcatenate, hstore_concat)
	framework.RegisterBinaryFunction(framework.Operator_BinaryMinus, hstore_delete)
	framework.RegisterBinaryFunction(framework.Operator_BinaryMinus, hstore_delete_array)
	framework.RegisterBinaryFunction(framework.Operator_BinaryMinus, hstore_delete_hstore)
	framework.RegisterBinaryFunction(framework.Operator_BinaryEqual, hstore_eq)
	framework.RegisterBinaryFunction(framework.Operator_BinaryNotEqual, hstore_ne)
	framework.RegisterBinaryOperatorFunction(framework.Operator_BinaryHstoreLess, hstore_lt)
	framework.RegisterBinaryOperatorFunction(framework.Operator_BinaryHstoreLessOrEqual, hstore_le)
	framework.RegisterBinaryOperatorFunction(framework.Operator_BinaryHstoreGreater, hstore_gt)
	framework.RegisterBinaryOperatorFunction(framework.Operator_BinaryHstoreGreaterOrEqual, hstore_ge)
	framework.RegisterBinaryOperatorFunction(framework.Operator_BinaryHstorePopulate, hstore_populate_record)
	framework.RegisterUnaryOperatorFunction(framework.Operator_UnaryHstoreToArray, hstore_to_array)
	framework.RegisterUnaryOperatorFunction(framework.Operator_UnaryHstoreToMatrix, hstore_to_matrix)
	framework.RegisterFunction(hstore_slice)
	framework.RegisterFunction(hstore_akeys)
	framework.RegisterFunction(hstore_avals)
	framework.RegisterFunction(hstore_skeys)
	framework.RegisterFunction(hstore_svals)
	framework.RegisterFunction(hstore_each)
	framework.RegisterFunction(hstore_to_array)
	framework.RegisterFunction(hstore_to_matrix)
	framework.RegisterFunction(hstore_to_json)
	framework.RegisterFunction(hstore_to_json_loose)
	framework.RegisterFunction(hstore_to_jsonb)
	framework.RegisterFunction(hstore_to_jsonb_loose)
	framework.RegisterFunction(hstore_version_diag)
	framework.RegisterFunction(hstore_in)
	framework.RegisterFunction(hstore_out)
	framework.RegisterFunction(hstore_recv)
	framework.RegisterFunction(hstore_send)
	framework.RegisterFunction(hstore_hash)
	framework.RegisterFunction(hstore_hash_extended)
	framework.RegisterFunction(hstore_tconvert)
	framework.RegisterFunction(hstore_from_record)
	framework.RegisterFunction(hstore_from_text)
	framework.RegisterFunction(hstore_from_arrays)
	framework.RegisterFunction(hstore_from_array)
	framework.RegisterFunction(hstore_isexists)
	framework.RegisterFunction(hstore_defined)
	framework.RegisterFunction(hstore_isdefined)
	framework.RegisterFunction(hstore_cmp)
	framework.RegisterFunction(hstore_lt)
	framework.RegisterFunction(hstore_le)
	framework.RegisterFunction(hstore_gt)
	framework.RegisterFunction(hstore_ge)
	framework.RegisterFunction(hstore_populate_record)
	framework.MustAddExplicitTypeCast(framework.TypeCast{
		FromType: hstoreType,
		ToType:   pgtypes.Json,
		Function: hstoreCastToJson,
	})
	framework.MustAddExplicitTypeCast(framework.TypeCast{
		FromType: hstoreType,
		ToType:   pgtypes.JsonB,
		Function: hstoreCastToJsonB,
	})
}

var hstore_fetchval = framework.Function2{
	Name:       "fetchval",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		pairs, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		value, ok := pairs[val2.(string)]
		if !ok || value == nil {
			return nil, nil
		}
		return *value, nil
	},
}

var hstore_slice_array = framework.Function2{
	Name:       "slice_array",
	Return:     pgtypes.TextArray,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		pairs, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		keys := hstoreTextArrayValues(val2)
		values := make([]any, len(keys))
		for i, key := range keys {
			if key == nil {
				continue
			}
			value, ok := pairs[*key]
			if !ok || value == nil {
				continue
			}
			values[i] = *value
		}
		return values, nil
	},
}

var hstore_slice = framework.Function2{
	Name:       "slice",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		pairs, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		sliced := make(map[string]*string)
		for _, key := range hstoreTextArrayValues(val2) {
			if key == nil {
				continue
			}
			if value, ok := pairs[*key]; ok {
				sliced[*key] = value
			}
		}
		return formatHstore(sliced), nil
	},
}

var hstore_akeys = framework.Function1{
	Name:       "akeys",
	Return:     pgtypes.TextArray,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		keys := hstoreSortedKeys(pairs)
		values := make([]any, len(keys))
		for i, key := range keys {
			values[i] = key
		}
		return values, nil
	},
}

var hstore_avals = framework.Function1{
	Name:       "avals",
	Return:     pgtypes.TextArray,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		keys := hstoreSortedKeys(pairs)
		values := make([]any, len(keys))
		for i, key := range keys {
			value := pairs[key]
			if value != nil {
				values[i] = *value
			}
		}
		return values, nil
	},
}

var hstore_skeys = framework.Function1{
	Name:       "skeys",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	SRF:        true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		return hstoreKeysRowIter(hstoreSortedKeys(pairs)), nil
	},
}

var hstore_svals = framework.Function1{
	Name:       "svals",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	SRF:        true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		keys := hstoreSortedKeys(pairs)
		values := make([]*string, len(keys))
		for i, key := range keys {
			values[i] = pairs[key]
		}
		return hstoreValuesRowIter(values), nil
	},
}

var hstore_each = framework.Function1{
	Name:       "each",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Record),
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	SRF:        true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		return hstoreEachRecordRowIter(pairs), nil
	},
}

var hstore_to_array = framework.Function1{
	Name:       "hstore_to_array",
	Return:     pgtypes.TextArray,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		keys := hstoreSortedKeys(pairs)
		values := make([]any, 0, len(keys)*2)
		for _, key := range keys {
			values = append(values, key)
			value := pairs[key]
			if value == nil {
				values = append(values, nil)
			} else {
				values = append(values, *value)
			}
		}
		return values, nil
	},
}

var hstore_to_matrix = framework.Function1{
	Name:       "hstore_to_matrix",
	Return:     pgtypes.TextArray,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		keys := hstoreSortedKeys(pairs)
		values := make([]any, 0, len(keys))
		for _, key := range keys {
			value := pairs[key]
			var itemValue any
			if value != nil {
				itemValue = *value
			}
			values = append(values, []any{key, itemValue})
		}
		return values, nil
	},
}

var hstore_to_json = framework.Function1{
	Name:       "hstore_to_json",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return hstoreToJsonString(val.(string), false)
	},
}

var hstore_to_json_loose = framework.Function1{
	Name:       "hstore_to_json_loose",
	Return:     pgtypes.Json,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return hstoreToJsonString(val.(string), true)
	},
}

var hstore_to_jsonb = framework.Function1{
	Name:       "hstore_to_jsonb",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return hstoreToJsonDocument(ctx, val.(string), false)
	},
}

var hstore_to_jsonb_loose = framework.Function1{
	Name:       "hstore_to_jsonb_loose",
	Return:     pgtypes.JsonB,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return hstoreToJsonDocument(ctx, val.(string), true)
	},
}

var hstore_version_diag = framework.Function1{
	Name:       "hstore_version_diag",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		if _, err := parseHstore(val.(string)); err != nil {
			return nil, err
		}
		return int32(2), nil
	},
}

var hstore_in = framework.Function1{
	Name:       "hstore_in",
	Return:     hstoreType,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		return formatHstore(pairs), nil
	},
}

var hstore_out = framework.Function1{
	Name:       "hstore_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		pairs, err := parseHstore(val.(string))
		if err != nil {
			return nil, err
		}
		return formatHstore(pairs), nil
	},
}

var hstore_recv = framework.Function1{
	Name:       "hstore_recv",
	Return:     hstoreType,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		return hstoreFromWirePayload(data)
	},
}

var hstore_send = framework.Function1{
	Name:       "hstore_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		if wrapper, ok := val.(sql.AnyWrapper); ok {
			var err error
			val, err = wrapper.UnwrapAny(ctx)
			if err != nil {
				return nil, err
			}
			if val == nil {
				return nil, nil
			}
		}
		return hstoreWirePayload(val.(string))
	},
}

var hstore_hash = framework.Function1{
	Name:       "hstore_hash",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		payload, err := hstoreHashPayload(val.(string))
		if err != nil {
			return nil, err
		}
		return int32(pgfunctions.PgHashBytes(payload)), nil
	},
}

var hstore_hash_extended = framework.Function2{
	Name:       "hstore_hash_extended",
	Return:     pgtypes.Int64,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Int64},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		payload, err := hstoreHashPayload(val1.(string))
		if err != nil {
			return nil, err
		}
		return int64(pgfunctions.PgHashBytesExtended(payload, uint64(val2.(int64)))), nil
	},
}

var hstore_tconvert = framework.Function2{
	Name:       "tconvert",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable:   hstoreFromTextCallable,
}

var hstore_from_record = framework.Function1{
	Name:       "hstore",
	Return:     hstoreType,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.AnyElement},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [2]*pgtypes.DoltgresType, val any) (any, error) {
		return hstoreFromRecord(ctx, t[0], val)
	},
}

var hstore_from_text = framework.Function2{
	Name:       "hstore",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable:   hstoreFromTextCallable,
}

var hstore_from_arrays = framework.Function2{
	Name:       "hstore",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.TextArray, pgtypes.TextArray},
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}
		keys := val1.([]any)
		var values []any
		if val2 != nil {
			values = val2.([]any)
			if len(keys) != len(values) {
				return nil, errors.New("arrays must have same bounds")
			}
		}
		pairs := make(map[string]*string, len(keys))
		for i, keyValue := range keys {
			if keyValue == nil {
				return nil, errors.New("null value not allowed for hstore key")
			}
			var value any
			if values != nil {
				value = values[i]
			}
			hstoreAddTextPair(pairs, keyValue.(string), value)
		}
		return formatHstore(pairs), nil
	},
}

var hstore_from_array = framework.Function1{
	Name:       "hstore",
	Return:     hstoreType,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TextArray},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		values := val.([]any)
		if len(values)%2 != 0 {
			return nil, errors.New("array must have even number of elements")
		}
		pairs := make(map[string]*string, len(values)/2)
		for i := 0; i < len(values); i += 2 {
			keyValue := values[i]
			if keyValue == nil {
				return nil, errors.New("null value not allowed for hstore key")
			}
			hstoreAddTextPair(pairs, keyValue.(string), values[i+1])
		}
		return formatHstore(pairs), nil
	},
}

var hstore_eq = framework.Function2{
	Name:       "hstore_eq",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		right, err := parseHstore(val2.(string))
		if err != nil {
			return nil, err
		}
		return hstoreEqual(left, right), nil
	},
}

var hstore_ne = framework.Function2{
	Name:       "hstore_ne",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(ctx *sql.Context, resolvedTypes [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		equal, err := hstore_eq.Callable(ctx, resolvedTypes, val1, val2)
		if err != nil {
			return nil, err
		}
		return !equal.(bool), nil
	},
}

var hstore_cmp = framework.Function2{
	Name:       "hstore_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := parseHstorePair(val1, val2)
		if err != nil {
			return nil, err
		}
		return int32(hstoreCompare(left, right)), nil
	},
}

var hstore_lt = framework.Function2{
	Name:       "hstore_lt",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable:   hstoreComparePredicate(func(cmp int) bool { return cmp < 0 }),
}

var hstore_le = framework.Function2{
	Name:       "hstore_le",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable:   hstoreComparePredicate(func(cmp int) bool { return cmp <= 0 }),
}

var hstore_gt = framework.Function2{
	Name:       "hstore_gt",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable:   hstoreComparePredicate(func(cmp int) bool { return cmp > 0 }),
}

var hstore_ge = framework.Function2{
	Name:       "hstore_ge",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable:   hstoreComparePredicate(func(cmp int) bool { return cmp >= 0 }),
}

var hstore_populate_record = framework.Function2{
	Name:       "populate_record",
	Return:     pgtypes.AnyElement,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.AnyElement, hstoreType},
	Strict:     false,
	Callable: func(ctx *sql.Context, t [3]*pgtypes.DoltgresType, base any, fromHstore any) (any, error) {
		return hstorePopulateRecord(ctx, t[2], base, fromHstore)
	},
}

var hstore_exist = framework.Function2{
	Name:       "exist",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreExistCallable,
}

var hstore_isexists = framework.Function2{
	Name:       "isexists",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreExistCallable,
}

var hstore_defined = framework.Function2{
	Name:       "defined",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreDefinedCallable,
}

var hstore_isdefined = framework.Function2{
	Name:       "isdefined",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreDefinedCallable,
}

var hstore_exists_any = framework.Function2{
	Name:       "exists_any",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable:   hstoreExistsAnyCallable,
}

var hstore_exists_all = framework.Function2{
	Name:       "exists_all",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable:   hstoreExistsAllCallable,
}

var hstore_contains = framework.Function2{
	Name:       "hs_contains",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		right, err := parseHstore(val2.(string))
		if err != nil {
			return nil, err
		}
		return hstoreContains(left, right), nil
	},
}

var hstore_contained = framework.Function2{
	Name:       "hs_contained",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		right, err := parseHstore(val2.(string))
		if err != nil {
			return nil, err
		}
		return hstoreContains(right, left), nil
	},
}

var hstore_concat = framework.Function2{
	Name:       "hs_concat",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		right, err := parseHstore(val2.(string))
		if err != nil {
			return nil, err
		}
		for key, value := range right {
			left[key] = value
		}
		return formatHstore(left), nil
	},
}

var hstore_delete = framework.Function2{
	Name:       "delete",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		pairs, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		delete(pairs, val2.(string))
		return formatHstore(pairs), nil
	},
}

var hstore_delete_array = framework.Function2{
	Name:       "delete",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		pairs, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		for _, key := range hstoreTextArrayValues(val2) {
			if key != nil {
				delete(pairs, *key)
			}
		}
		return formatHstore(pairs), nil
	},
}

var hstore_delete_hstore = framework.Function2{
	Name:       "delete",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, hstoreType},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		right, err := parseHstore(val2.(string))
		if err != nil {
			return nil, err
		}
		for key, rightValue := range right {
			if hstoreValueEqual(left[key], rightValue) {
				delete(left, key)
			}
		}
		return formatHstore(left), nil
	},
}

func hstoreExistCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	_, ok := pairs[val2.(string)]
	return ok, nil
}

func hstoreDefinedCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	value, ok := pairs[val2.(string)]
	return ok && value != nil, nil
}

func hstoreExistsAnyCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	for _, key := range hstoreTextArrayValues(val2) {
		if key == nil {
			continue
		}
		if _, ok := pairs[*key]; ok {
			return true, nil
		}
	}
	return false, nil
}

func hstoreExistsAllCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	for _, key := range hstoreTextArrayValues(val2) {
		if key == nil {
			continue
		}
		if _, ok := pairs[*key]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func hstoreTextArrayValues(val any) []*string {
	values := val.([]any)
	keys := make([]*string, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		key := value.(string)
		keys[i] = &key
	}
	return keys
}

func hstoreContains(left map[string]*string, right map[string]*string) bool {
	for key, rightValue := range right {
		leftValue, ok := left[key]
		if !ok {
			return false
		}
		if leftValue == nil || rightValue == nil {
			if leftValue != rightValue {
				return false
			}
			continue
		}
		if *leftValue != *rightValue {
			return false
		}
	}
	return true
}

func hstoreEqual(left map[string]*string, right map[string]*string) bool {
	return len(left) == len(right) && hstoreContains(left, right)
}

func hstoreValueEqual(left *string, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func parseHstorePair(val1 any, val2 any) (map[string]*string, map[string]*string, error) {
	left, err := parseHstore(val1.(string))
	if err != nil {
		return nil, nil, err
	}
	right, err := parseHstore(val2.(string))
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}

func hstoreComparePredicate(predicate func(int) bool) func(*sql.Context, [3]*pgtypes.DoltgresType, any, any) (any, error) {
	return func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		left, right, err := parseHstorePair(val1, val2)
		if err != nil {
			return nil, err
		}
		return predicate(hstoreCompare(left, right)), nil
	}
}

func hstoreCompare(left map[string]*string, right map[string]*string) int {
	leftKeys := hstoreComparisonKeys(left)
	rightKeys := hstoreComparisonKeys(right)
	count := min(len(leftKeys), len(rightKeys))
	for i := 0; i < count; i++ {
		if cmp := normalizeCompare(strings.Compare(leftKeys[i], rightKeys[i])); cmp != 0 {
			return cmp
		}
		leftValue := left[leftKeys[i]]
		rightValue := right[rightKeys[i]]
		switch {
		case leftValue == nil && rightValue == nil:
		case leftValue == nil:
			return 1
		case rightValue == nil:
			return -1
		default:
			if cmp := normalizeCompare(strings.Compare(*leftValue, *rightValue)); cmp != 0 {
				return cmp
			}
		}
	}
	return normalizeCompare(len(leftKeys) - len(rightKeys))
}

func normalizeCompare(cmp int) int {
	switch {
	case cmp < 0:
		return -1
	case cmp > 0:
		return 1
	default:
		return 0
	}
}

func hstoreComparisonKeys(pairs map[string]*string) []string {
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func hstoreAddTextPair(pairs map[string]*string, key string, value any) {
	if _, ok := pairs[key]; ok {
		return
	}
	if value == nil {
		pairs[key] = nil
		return
	}
	textValue := value.(string)
	pairs[key] = &textValue
}

func hstoreFromTextCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	if val1 == nil {
		return nil, nil
	}
	pairs := make(map[string]*string, 1)
	hstoreAddTextPair(pairs, val1.(string), val2)
	return formatHstore(pairs), nil
}

func hstoreFromRecord(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (any, error) {
	resolvedType, err := hstoreResolveType(ctx, typ)
	if err != nil {
		return nil, err
	}
	resolvedValue, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	if resolvedValue == nil {
		if resolvedType == nil || !resolvedType.IsCompositeType() || len(resolvedType.CompositeAttrs) == 0 {
			return nil, nil
		}
		pairs := make(map[string]*string, len(resolvedType.CompositeAttrs))
		for _, attr := range resolvedType.CompositeAttrs {
			pairs[attr.Name] = nil
		}
		return formatHstore(pairs), nil
	}
	record, ok := resolvedValue.([]pgtypes.RecordValue)
	if !ok {
		return nil, errors.Errorf("expected record, but got %T", resolvedValue)
	}
	pairs := make(map[string]*string, len(record))
	for i, field := range record {
		key := hstoreRecordFieldName(resolvedType, i)
		if field.Value == nil {
			pairs[key] = nil
			continue
		}
		fieldType, ok := field.Type.(*pgtypes.DoltgresType)
		if !ok {
			return nil, errors.Errorf("expected *DoltgresType, but got %T", field.Type)
		}
		output, err := fieldType.IoOutput(ctx, field.Value)
		if err != nil {
			return nil, err
		}
		if fieldType.ID == pgtypes.Bool.ID {
			output = string(output[0])
		}
		pairs[key] = &output
	}
	return formatHstore(pairs), nil
}

func hstorePopulateRecordCompositeType(ctx *sql.Context, expr sql.Expression) (*pgtypes.DoltgresType, error) {
	compositeType, ok := expr.Type(ctx).(*pgtypes.DoltgresType)
	if !ok {
		return nil, errors.New("first argument of populate_record must be a composite type")
	}
	compositeType, err := hstoreResolveType(ctx, compositeType)
	if err != nil {
		return nil, err
	}
	if compositeType == nil || !compositeType.IsCompositeType() || len(compositeType.CompositeAttrs) == 0 {
		return nil, errors.New("first argument of populate_record must be a composite type")
	}
	return compositeType, nil
}

func hstorePopulateRecord(ctx *sql.Context, compositeType *pgtypes.DoltgresType, base any, fromHstore any) ([]pgtypes.RecordValue, error) {
	compositeType, err := hstoreResolveType(ctx, compositeType)
	if err != nil {
		return nil, err
	}
	if compositeType == nil || !compositeType.IsCompositeType() || len(compositeType.CompositeAttrs) == 0 {
		return nil, errors.New("first argument of populate_record must be a composite type")
	}
	baseRecord, err := hstorePopulateRecordBase(ctx, base)
	if err != nil {
		return nil, err
	}
	resolvedHstore, err := sql.UnwrapAny(ctx, fromHstore)
	if err != nil {
		return nil, err
	}
	if resolvedHstore == nil {
		return baseRecord, nil
	}
	pairs, err := parseHstore(resolvedHstore.(string))
	if err != nil {
		return nil, err
	}
	return hstorePopulateRecordFromPairs(ctx, compositeType, baseRecord, pairs)
}

func hstorePopulateRecordBase(ctx *sql.Context, base any) ([]pgtypes.RecordValue, error) {
	resolvedBase, err := sql.UnwrapAny(ctx, base)
	if err != nil {
		return nil, err
	}
	if resolvedBase == nil {
		return nil, nil
	}
	record, ok := resolvedBase.([]pgtypes.RecordValue)
	if !ok {
		return nil, errors.Errorf("expected []RecordValue, but got %T", resolvedBase)
	}
	return record, nil
}

func hstorePopulateRecordFromPairs(
	ctx *sql.Context,
	compositeType *pgtypes.DoltgresType,
	baseRecord []pgtypes.RecordValue,
	pairs map[string]*string,
) ([]pgtypes.RecordValue, error) {
	record := make([]pgtypes.RecordValue, len(compositeType.CompositeAttrs))
	for i, attr := range compositeType.CompositeAttrs {
		attrType, err := hstoreCompositeAttributeType(ctx, attr)
		if err != nil {
			return nil, err
		}
		var baseValue any
		if i < len(baseRecord) {
			baseValue = baseRecord[i].Value
		}
		record[i] = pgtypes.RecordValue{
			Value: baseValue,
			Type:  attrType,
		}
		if value, ok := pairs[attr.Name]; ok {
			record[i].Value, err = hstorePopulateValue(ctx, attrType, value)
			if err != nil {
				return nil, err
			}
		}
	}
	return record, nil
}

func hstorePopulateValue(ctx *sql.Context, targetType *pgtypes.DoltgresType, value *string) (any, error) {
	if value == nil {
		return nil, nil
	}
	if targetType.IsCompositeType() {
		return hstorePopulateCompositeValue(ctx, targetType, *value)
	}
	return targetType.IoInput(ctx, *value)
}

func hstorePopulateCompositeValue(ctx *sql.Context, targetType *pgtypes.DoltgresType, input string) ([]pgtypes.RecordValue, error) {
	fields, err := hstoreParseRecordFields(input)
	if err != nil {
		return nil, err
	}
	if len(fields) != len(targetType.CompositeAttrs) {
		return nil, errors.Errorf("malformed record literal: \"%s\"", input)
	}
	record := make([]pgtypes.RecordValue, len(targetType.CompositeAttrs))
	for i, attr := range targetType.CompositeAttrs {
		attrType, err := hstoreCompositeAttributeType(ctx, attr)
		if err != nil {
			return nil, err
		}
		record[i] = pgtypes.RecordValue{Type: attrType}
		if fields[i] != nil {
			record[i].Value, err = hstorePopulateValue(ctx, attrType, fields[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return record, nil
}

func hstoreParseRecordFields(input string) ([]*string, error) {
	if len(input) < 2 || input[0] != '(' || input[len(input)-1] != ')' {
		return nil, errors.Errorf("malformed record literal: \"%s\"", input)
	}
	var fields []*string
	var builder strings.Builder
	inQuotes := false
	fieldQuoted := false
	parenDepth := 0
	braceDepth := 0
	for i := 1; i < len(input)-1; i++ {
		ch := input[i]
		if inQuotes {
			switch ch {
			case '\\':
				if i+1 >= len(input)-1 {
					return nil, errors.Errorf("malformed record literal: \"%s\"", input)
				}
				i++
				builder.WriteByte(input[i])
			case '"':
				inQuotes = false
			default:
				builder.WriteByte(ch)
			}
			continue
		}
		switch ch {
		case '"':
			inQuotes = true
			fieldQuoted = true
		case '(':
			parenDepth++
			builder.WriteByte(ch)
		case ')':
			if parenDepth == 0 {
				return nil, errors.Errorf("malformed record literal: \"%s\"", input)
			}
			parenDepth--
			builder.WriteByte(ch)
		case '{':
			braceDepth++
			builder.WriteByte(ch)
		case '}':
			if braceDepth == 0 {
				return nil, errors.Errorf("malformed record literal: \"%s\"", input)
			}
			braceDepth--
			builder.WriteByte(ch)
		case ',':
			if parenDepth == 0 && braceDepth == 0 {
				fields = append(fields, hstoreRecordFieldValue(builder.String(), fieldQuoted))
				builder.Reset()
				fieldQuoted = false
				continue
			}
			builder.WriteByte(ch)
		default:
			builder.WriteByte(ch)
		}
	}
	if inQuotes || parenDepth != 0 || braceDepth != 0 {
		return nil, errors.Errorf("malformed record literal: \"%s\"", input)
	}
	fields = append(fields, hstoreRecordFieldValue(builder.String(), fieldQuoted))
	return fields, nil
}

func hstoreRecordFieldValue(value string, quoted bool) *string {
	if !quoted && value == "" {
		return nil
	}
	return &value
}

func hstoreCompositeAttributeType(ctx *sql.Context, attr pgtypes.CompositeAttribute) (*pgtypes.DoltgresType, error) {
	attrType, err := hstoreResolveType(ctx, pgtypes.NewUnresolvedDoltgresType(attr.TypeID.SchemaName(), attr.TypeID.TypeName()))
	if err != nil || attrType == nil {
		return attrType, err
	}
	return attr.ApplyTypMod(attrType), nil
}

func hstoreRecordFieldName(typ *pgtypes.DoltgresType, idx int) string {
	if typ != nil && idx < len(typ.CompositeAttrs) && typ.CompositeAttrs[idx].Name != "" {
		return typ.CompositeAttrs[idx].Name
	}
	return fmt.Sprintf("f%d", idx+1)
}

func hstoreResolveType(ctx *sql.Context, typ *pgtypes.DoltgresType) (*pgtypes.DoltgresType, error) {
	if typ == nil || typ.IsResolvedType() {
		return typ, nil
	}
	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	resolved, err := typeCollection.GetType(ctx, typ.ID)
	if err != nil {
		return nil, err
	}
	if resolved != nil {
		return resolved, nil
	}
	if typ.ID.SchemaName() == "" {
		schema, err := core.GetSchemaName(ctx, nil, "")
		if err != nil {
			return nil, err
		}
		resolved, err = typeCollection.GetType(ctx, id.NewType(schema, typ.ID.TypeName()))
		if err != nil {
			return nil, err
		}
		if resolved != nil {
			return resolved, nil
		}
		resolved, err = typeCollection.GetType(ctx, id.NewType("pg_catalog", typ.ID.TypeName()))
		if err != nil {
			return nil, err
		}
		if resolved != nil {
			return resolved, nil
		}
	}
	return nil, pgtypes.ErrTypeDoesNotExist.New(typ.ID.TypeName())
}

func hstoreCastToJson(_ *sql.Context, val any, _ *pgtypes.DoltgresType) (any, error) {
	if val == nil {
		return nil, nil
	}
	return hstoreToJsonString(val.(string), false)
}

func hstoreCastToJsonB(ctx *sql.Context, val any, _ *pgtypes.DoltgresType) (any, error) {
	if val == nil {
		return nil, nil
	}
	return hstoreToJsonDocument(ctx, val.(string), false)
}

func hstoreKeysRowIter(keys []string) *pgtypes.SetReturningFunctionRowIter {
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(_ *sql.Context) (sql.Row, error) {
		if i >= len(keys) {
			return nil, io.EOF
		}
		key := keys[i]
		i++
		return sql.Row{key}, nil
	})
}

func hstoreValuesRowIter(values []*string) *pgtypes.SetReturningFunctionRowIter {
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(_ *sql.Context) (sql.Row, error) {
		if i >= len(values) {
			return nil, io.EOF
		}
		value := values[i]
		i++
		if value == nil {
			return sql.Row{nil}, nil
		}
		return sql.Row{*value}, nil
	})
}

func hstoreEachRecordRowIter(pairs map[string]*string) *pgtypes.SetReturningFunctionRowIter {
	keys := hstoreSortedKeys(pairs)
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(_ *sql.Context) (sql.Row, error) {
		if i >= len(keys) {
			return nil, io.EOF
		}
		key := keys[i]
		value := pairs[key]
		i++
		var recordValue any
		if value != nil {
			recordValue = *value
		}
		return sql.Row{[]pgtypes.RecordValue{
			{Type: pgtypes.Text, Value: key},
			{Type: pgtypes.Text, Value: recordValue},
		}}, nil
	})
}

func hstoreEachTableRowIter(pairs map[string]*string) *pgtypes.SetReturningFunctionRowIter {
	keys := hstoreSortedKeys(pairs)
	var i int
	return pgtypes.NewSetReturningFunctionRowIter(func(_ *sql.Context) (sql.Row, error) {
		if i >= len(keys) {
			return nil, io.EOF
		}
		key := keys[i]
		value := pairs[key]
		i++
		if value == nil {
			return sql.Row{key, nil}, nil
		}
		return sql.Row{key, *value}, nil
	})
}

func hstoreToJsonString(input string, loose bool) (string, error) {
	pairs, err := parseHstore(input)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	builder.WriteByte('{')
	for i, key := range hstoreSortedKeys(pairs) {
		if i > 0 {
			builder.WriteByte(',')
		}
		if err = hstoreWriteJsonString(&builder, key); err != nil {
			return "", err
		}
		builder.WriteByte(':')
		value := pairs[key]
		if value == nil {
			builder.WriteString("null")
		} else if loose && hstoreIsLooseJsonNumber(*value) {
			builder.WriteString(*value)
		} else if err = hstoreWriteJsonString(&builder, *value); err != nil {
			return "", err
		}
	}
	builder.WriteByte('}')
	return builder.String(), nil
}

func hstoreToJsonDocument(ctx *sql.Context, input string, loose bool) (pgtypes.JsonDocument, error) {
	pairs, err := parseHstore(input)
	if err != nil {
		return pgtypes.JsonDocument{}, err
	}
	items := make([]pgtypes.JsonValueObjectItem, 0, len(pairs))
	for _, key := range hstoreSortedKeys(pairs) {
		value, err := hstoreJsonValue(ctx, pairs[key], loose)
		if err != nil {
			return pgtypes.JsonDocument{}, err
		}
		items = append(items, pgtypes.JsonValueObjectItem{Key: key, Value: value})
	}
	return pgtypes.JsonDocument{
		Value: pgtypes.JsonObjectFromItems(items, false),
	}, nil
}

func hstoreJsonValue(ctx *sql.Context, value *string, loose bool) (pgtypes.JsonValue, error) {
	if value == nil {
		return pgtypes.JsonValueNull(0), nil
	}
	if loose && hstoreIsLooseJsonNumber(*value) {
		number, err := decimal.NewFromString(*value)
		if err != nil {
			return nil, err
		}
		return pgtypes.JsonValueNumber(number), nil
	}
	return pgtypes.JsonValueFromSQLValue(ctx, pgtypes.Text, *value)
}

func hstoreIsLooseJsonNumber(value string) bool {
	return hstoreLooseJsonNumberPattern.MatchString(value)
}

func hstoreWriteJsonString(builder *strings.Builder, value string) error {
	encoded, err := json.MarshalWithOption(value, json.DisableHTMLEscape())
	if err != nil {
		return err
	}
	builder.Write(encoded)
	return nil
}

func parseHstore(input string) (map[string]*string, error) {
	p := hstoreParser{input: input}
	pairs := make(map[string]*string)
	p.skipSpaces()
	if p.done() {
		return pairs, nil
	}
	for {
		key, _, ok := p.parseToken()
		if !ok || key == nil {
			return nil, invalidHstoreInput(input)
		}
		p.skipSpaces()
		if !p.consume("=>") {
			return nil, invalidHstoreInput(input)
		}
		p.skipSpaces()
		value, isNull, ok := p.parseToken()
		if !ok {
			return nil, invalidHstoreInput(input)
		}
		if _, ok := pairs[*key]; !ok {
			if isNull {
				pairs[*key] = nil
			} else {
				pairs[*key] = value
			}
		}
		p.skipSpaces()
		if p.done() {
			return pairs, nil
		}
		if !p.consume(",") {
			return nil, invalidHstoreInput(input)
		}
		p.skipSpaces()
		if p.done() {
			return nil, invalidHstoreInput(input)
		}
	}
}

type hstoreParser struct {
	input string
	pos   int
}

func (p *hstoreParser) done() bool {
	return p.pos >= len(p.input)
}

func (p *hstoreParser) skipSpaces() {
	for !p.done() {
		r, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if !unicode.IsSpace(r) {
			return
		}
		p.pos += size
	}
}

func (p *hstoreParser) consume(token string) bool {
	if !strings.HasPrefix(p.input[p.pos:], token) {
		return false
	}
	p.pos += len(token)
	return true
}

func (p *hstoreParser) parseToken() (*string, bool, bool) {
	if p.done() {
		return nil, false, false
	}
	if p.input[p.pos] == '"' {
		token, ok := p.parseQuotedToken()
		return &token, false, ok
	}
	token, ok := p.parseBareToken()
	if !ok {
		return nil, false, false
	}
	if strings.EqualFold(token, "NULL") {
		return nil, true, true
	}
	return &token, false, true
}

func (p *hstoreParser) parseQuotedToken() (string, bool) {
	p.pos++
	var builder strings.Builder
	for !p.done() {
		ch := p.input[p.pos]
		p.pos++
		switch ch {
		case '\\':
			if p.done() {
				return "", false
			}
			builder.WriteByte(p.input[p.pos])
			p.pos++
		case '"':
			return builder.String(), true
		default:
			builder.WriteByte(ch)
		}
	}
	return "", false
}

func (p *hstoreParser) parseBareToken() (string, bool) {
	start := p.pos
	for !p.done() {
		r, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if unicode.IsSpace(r) || r == ',' || r == '=' || r == '>' {
			break
		}
		p.pos += size
	}
	if p.pos == start {
		return "", false
	}
	return p.input[start:p.pos], true
}

func invalidHstoreInput(input string) error {
	return pgtypes.ErrInvalidSyntaxForType.New("hstore", input)
}

func formatHstore(pairs map[string]*string) string {
	if len(pairs) == 0 {
		return ""
	}
	keys := hstoreSortedKeys(pairs)
	parts := make([]string, len(keys))
	for i, key := range keys {
		value := pairs[key]
		if value == nil {
			parts[i] = hstoreQuote(key) + "=>NULL"
		} else {
			parts[i] = hstoreQuote(key) + "=>" + hstoreQuote(*value)
		}
	}
	return strings.Join(parts, ", ")
}

func hstoreHashPayload(input string) ([]byte, error) {
	pairs, err := parseHstore(input)
	if err != nil {
		return nil, err
	}
	keys := hstoreSortedKeys(pairs)
	stringLen, err := hstorePayloadStringLen(keys, pairs)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 4+(len(keys)*8)+stringLen)
	byteorder.LittleEndian.PutUint32(payload[0:4], uint32(len(keys))|hstoreFlagNewVersion)

	entryOffset := 4
	stringOffset := 4 + (len(keys) * 8)
	endPos := uint32(0)
	for i, key := range keys {
		endPos += uint32(len(key))
		keyEntry := endPos & hstoreEntryPosMask
		if i == 0 {
			keyEntry |= hstoreEntryIsFirst
		}
		byteorder.LittleEndian.PutUint32(payload[entryOffset:entryOffset+4], keyEntry)
		entryOffset += 4
		copy(payload[stringOffset:], key)
		stringOffset += len(key)

		value := pairs[key]
		if value == nil {
			byteorder.LittleEndian.PutUint32(payload[entryOffset:entryOffset+4], endPos|hstoreEntryIsNull)
			entryOffset += 4
			continue
		}
		endPos += uint32(len(*value))
		byteorder.LittleEndian.PutUint32(payload[entryOffset:entryOffset+4], endPos&hstoreEntryPosMask)
		entryOffset += 4
		copy(payload[stringOffset:], *value)
		stringOffset += len(*value)
	}
	return payload, nil
}

func hstoreWirePayload(input string) ([]byte, error) {
	pairs, err := parseHstore(input)
	if err != nil {
		return nil, err
	}
	keys := hstoreSortedKeys(pairs)
	writer := utils.NewWireWriter()
	writer.Reserve(uint64(hstoreWirePayloadLen(keys, pairs)))
	writer.WriteInt32(int32(len(keys)))
	for _, key := range keys {
		writer.WriteInt32(int32(len(key)))
		writer.WriteString(key)
		value := pairs[key]
		if value == nil {
			writer.WriteInt32(-1)
			continue
		}
		writer.WriteInt32(int32(len(*value)))
		writer.WriteString(*value)
	}
	return writer.BufferData(), nil
}

func hstoreWirePayloadLen(keys []string, pairs map[string]*string) int {
	total := 4
	for _, key := range keys {
		total += 8 + len(key)
		if value := pairs[key]; value != nil {
			total += len(*value)
		}
	}
	return total
}

func hstoreFromWirePayload(data []byte) (string, error) {
	reader := utils.NewWireReader(data)
	count := reader.ReadInt32()
	if count < 0 {
		return "", errors.New("number of pairs cannot be negative")
	}
	pairs := make(map[string]*string, count)
	for i := int32(0); i < count; i++ {
		keyLen := reader.ReadInt32()
		if keyLen < 0 {
			return "", errors.New("null value not allowed for hstore key")
		}
		key := reader.ReadString(uint32(keyLen))
		valueLen := reader.ReadInt32()
		if valueLen < 0 {
			hstoreAddTextPair(pairs, key, nil)
			continue
		}
		hstoreAddTextPair(pairs, key, reader.ReadString(uint32(valueLen)))
	}
	return formatHstore(pairs), nil
}

func hstorePayloadStringLen(keys []string, pairs map[string]*string) (int, error) {
	var total int
	for _, key := range keys {
		if len(key) > int(hstoreEntryPosMask)-total {
			return 0, errors.New("string too long for hstore key")
		}
		total += len(key)
		value := pairs[key]
		if value == nil {
			continue
		}
		if len(*value) > int(hstoreEntryPosMask)-total {
			return 0, errors.New("string too long for hstore value")
		}
		total += len(*value)
	}
	return total, nil
}

func hstoreSortedKeys(pairs map[string]*string) []string {
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) < len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}

func hstoreQuote(value string) string {
	var builder strings.Builder
	builder.Grow(len(value) + 2)
	builder.WriteByte('"')
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\', '"':
			builder.WriteByte('\\')
		}
		builder.WriteByte(value[i])
	}
	builder.WriteByte('"')
	return builder.String()
}
