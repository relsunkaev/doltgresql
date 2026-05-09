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
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var hstoreType = pgtypes.NewUnresolvedDoltgresType("public", "hstore")

// initHstore registers operators and functions supplied by the hstore extension.
func initHstore() {
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
	framework.RegisterFunction(hstore_slice)
	framework.RegisterFunction(hstore_akeys)
	framework.RegisterFunction(hstore_avals)
	framework.RegisterFunction(hstore_to_array)
	framework.RegisterFunction(hstore_from_text)
	framework.RegisterFunction(hstore_from_arrays)
	framework.RegisterFunction(hstore_from_array)
	framework.RegisterFunction(hstore_isexists)
	framework.RegisterFunction(hstore_defined)
	framework.RegisterFunction(hstore_isdefined)
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

var hstore_from_text = framework.Function2{
	Name:       "hstore",
	Return:     hstoreType,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		if val1 == nil {
			return nil, nil
		}
		pairs := make(map[string]*string, 1)
		hstoreAddTextPair(pairs, val1.(string), val2)
		return formatHstore(pairs), nil
	},
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
		if isNull {
			pairs[*key] = nil
		} else {
			pairs[*key] = value
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

func hstoreSortedKeys(pairs map[string]*string) []string {
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
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
