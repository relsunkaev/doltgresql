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
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// jsonb_path_exists represents the PostgreSQL function jsonb_path_exists(jsonb, jsonpath).
var jsonb_path_exists = framework.Function2{
	Name:       "jsonb_path_exists",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		return JsonPathExists(ctx, target, path)
	},
}

// JsonPathExists implements the shared JSON path exists behavior used by the
// jsonb_path_exists function and @? operator.
func JsonPathExists(ctx *sql.Context, target any, path any) (bool, error) {
	matches, err := jsonPathQuery(ctx, target, path)
	if err != nil {
		return false, err
	}
	return len(matches) > 0, nil
}

// JsonPathMatch implements the shared JSON path match behavior used by the
// jsonb_path_match function and @@ operator.
func JsonPathMatch(ctx *sql.Context, target any, path any) (any, error) {
	doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, target)
	if err != nil {
		return nil, err
	}
	pathText, err := jsonPathText(ctx, path)
	if err != nil {
		return nil, err
	}
	return jsonPathMatch(doc.Value, pathText)
}

// jsonb_path_query represents the PostgreSQL function jsonb_path_query(jsonb, jsonpath).
var jsonb_path_query = framework.Function2{
	Name:       "jsonb_path_query",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.JsonB),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		matches, err := jsonPathQuery(ctx, target, path)
		if err != nil {
			return nil, err
		}
		var idx int
		return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
			if idx >= len(matches) {
				return nil, io.EOF
			}
			value := matches[idx]
			idx++
			return sql.Row{pgtypes.JsonDocument{Value: pgtypes.JsonValueCopy(value)}}, nil
		}), nil
	},
}

// jsonb_path_query_array represents the PostgreSQL function jsonb_path_query_array(jsonb, jsonpath).
var jsonb_path_query_array = framework.Function2{
	Name:       "jsonb_path_query_array",
	Return:     pgtypes.JsonB,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		matches, err := jsonPathQuery(ctx, target, path)
		if err != nil {
			return nil, err
		}
		array := make(pgtypes.JsonValueArray, len(matches))
		for i, value := range matches {
			array[i] = pgtypes.JsonValueCopy(value)
		}
		return pgtypes.JsonDocument{Value: array}, nil
	},
}

// jsonb_path_match represents the PostgreSQL function jsonb_path_match(jsonb, jsonpath).
var jsonb_path_match = framework.Function2{
	Name:       "jsonb_path_match",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, target any, path any) (any, error) {
		return JsonPathMatch(ctx, target, path)
	},
}

func jsonPathQuery(ctx *sql.Context, target any, path any) ([]pgtypes.JsonValue, error) {
	doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.JsonB, target)
	if err != nil {
		return nil, err
	}
	pathText, err := jsonPathText(ctx, path)
	if err != nil {
		return nil, err
	}
	return jsonPathEval(doc.Value, pathText)
}

func jsonPathText(ctx *sql.Context, val any) (string, error) {
	res, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return "", err
	}
	str, ok := res.(string)
	if !ok {
		return "", errors.Errorf("expected jsonpath argument to be text, but got %T", res)
	}
	return strings.TrimSpace(str), nil
}

func jsonPathEval(root pgtypes.JsonValue, path string) ([]pgtypes.JsonValue, error) {
	if path == "" || path[0] != '$' {
		return nil, errors.Errorf("jsonpath must start with $")
	}
	current := []pgtypes.JsonValue{root}
	for i := 1; i < len(path); {
		switch path[i] {
		case '.':
			i++
			if i < len(path) && path[i] == '*' {
				current = jsonPathObjectWildcard(current)
				i++
				continue
			}
			key, next, err := jsonPathReadKey(path, i)
			if err != nil {
				return nil, err
			}
			current = jsonPathObjectKey(current, key)
			i = next
		case '[':
			if strings.HasPrefix(path[i:], "[*]") {
				current = jsonPathArrayWildcard(current)
				i += 3
				continue
			}
			end := strings.IndexByte(path[i:], ']')
			if end == -1 {
				return nil, errors.Errorf("unterminated jsonpath array subscript")
			}
			idx, err := strconv.Atoi(strings.TrimSpace(path[i+1 : i+end]))
			if err != nil {
				return nil, err
			}
			current = jsonPathArrayIndex(current, idx)
			i += end + 1
		case '?':
			predicate, next, err := jsonPathReadFilterPredicate(path, i)
			if err != nil {
				return nil, err
			}
			current, err = jsonPathFilterPredicate(current, predicate)
			if err != nil {
				return nil, err
			}
			i = next
		default:
			if unicode.IsSpace(rune(path[i])) {
				i++
				continue
			}
			return nil, errors.Errorf("unsupported jsonpath syntax near %q", path[i:])
		}
	}
	return current, nil
}

func jsonPathReadKey(path string, start int) (string, int, error) {
	if start >= len(path) {
		return "", start, errors.Errorf("expected jsonpath key")
	}
	if path[start] == '"' {
		var sb strings.Builder
		for i := start + 1; i < len(path); i++ {
			switch path[i] {
			case '\\':
				if i+1 >= len(path) {
					return "", i, errors.Errorf("unterminated jsonpath quoted key")
				}
				i++
				sb.WriteByte(path[i])
			case '"':
				return sb.String(), i + 1, nil
			default:
				sb.WriteByte(path[i])
			}
		}
		return "", len(path), errors.Errorf("unterminated jsonpath quoted key")
	}
	end := start
	for end < len(path) {
		r := rune(path[end])
		if r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) {
			end++
			continue
		}
		break
	}
	if end == start {
		return "", start, errors.Errorf("expected jsonpath key")
	}
	return path[start:end], end, nil
}

func jsonPathObjectKey(values []pgtypes.JsonValue, key string) []pgtypes.JsonValue {
	var out []pgtypes.JsonValue
	for _, value := range values {
		object, ok := value.(pgtypes.JsonValueObject)
		if !ok {
			continue
		}
		idx, ok := object.Index[key]
		if ok {
			out = append(out, object.Items[idx].Value)
		}
	}
	return out
}

func jsonPathObjectWildcard(values []pgtypes.JsonValue) []pgtypes.JsonValue {
	var out []pgtypes.JsonValue
	for _, value := range values {
		object, ok := value.(pgtypes.JsonValueObject)
		if !ok {
			continue
		}
		for _, item := range object.Items {
			out = append(out, item.Value)
		}
	}
	return out
}

func jsonPathArrayWildcard(values []pgtypes.JsonValue) []pgtypes.JsonValue {
	var out []pgtypes.JsonValue
	for _, value := range values {
		array, ok := value.(pgtypes.JsonValueArray)
		if !ok {
			continue
		}
		out = append(out, array...)
	}
	return out
}

func jsonPathArrayIndex(values []pgtypes.JsonValue, idx int) []pgtypes.JsonValue {
	var out []pgtypes.JsonValue
	for _, value := range values {
		array, ok := value.(pgtypes.JsonValueArray)
		if !ok {
			continue
		}
		resolvedIdx := idx
		if resolvedIdx < 0 {
			resolvedIdx += len(array)
		}
		if resolvedIdx >= 0 && resolvedIdx < len(array) {
			out = append(out, array[resolvedIdx])
		}
	}
	return out
}

func jsonPathReadFilterPredicate(path string, start int) (string, int, error) {
	i := start + 1
	for i < len(path) && unicode.IsSpace(rune(path[i])) {
		i++
	}
	if i >= len(path) || path[i] != '(' {
		return "", i, errors.Errorf("expected jsonpath filter predicate")
	}
	predicateStart := i + 1
	inString := false
	escaped := false
	for i = predicateStart; i < len(path); i++ {
		if escaped {
			escaped = false
			continue
		}
		if path[i] == '\\' && inString {
			escaped = true
			continue
		}
		if path[i] == '"' {
			inString = !inString
			continue
		}
		if !inString && path[i] == ')' {
			return strings.TrimSpace(path[predicateStart:i]), i + 1, nil
		}
	}
	return "", len(path), errors.Errorf("unterminated jsonpath filter predicate")
}

func jsonPathFilterPredicate(values []pgtypes.JsonValue, predicate string) ([]pgtypes.JsonValue, error) {
	lhsPath, op, rhsText, ok := jsonPathSplitComparison(predicate)
	if !ok {
		return nil, errors.Errorf("unsupported jsonpath filter predicate %q", predicate)
	}
	lhsPath = strings.TrimSpace(lhsPath)
	if !strings.HasPrefix(lhsPath, "@") {
		return nil, errors.Errorf("unsupported jsonpath filter predicate %q", predicate)
	}
	lhsPath = "$" + strings.TrimPrefix(lhsPath, "@")
	rhs, err := jsonPathLiteral(rhsText)
	if err != nil {
		return nil, err
	}
	var out []pgtypes.JsonValue
	for _, value := range values {
		lhsValues, err := jsonPathEval(value, lhsPath)
		if err != nil {
			return nil, err
		}
		for _, lhs := range lhsValues {
			if jsonPathCompare(lhs, op, rhs) {
				out = append(out, value)
				break
			}
		}
	}
	return out, nil
}

func jsonPathMatch(root pgtypes.JsonValue, path string) (any, error) {
	lhsPath, op, rhsText, ok := jsonPathSplitComparison(path)
	if !ok {
		values, err := jsonPathEval(root, path)
		if err != nil {
			return nil, err
		}
		return jsonPathSingleBooleanResult(values)
	}
	lhsValues, err := jsonPathEval(root, lhsPath)
	if err != nil {
		return nil, err
	}
	rhs, err := jsonPathLiteral(rhsText)
	if err != nil {
		return nil, err
	}
	for _, lhs := range lhsValues {
		if jsonPathCompare(lhs, op, rhs) {
			return true, nil
		}
	}
	return false, nil
}

func jsonPathCompare(lhs pgtypes.JsonValue, op string, rhs pgtypes.JsonValue) bool {
	cmp := pgtypes.JsonValueCompare(lhs, rhs)
	switch op {
	case "==":
		return cmp == 0
	case "!=":
		return cmp != 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	default:
		return false
	}
}

func jsonPathSplitComparison(path string) (string, string, string, bool) {
	operators := []string{"==", "!=", ">=", "<=", ">", "<"}
	inString := false
	escaped := false
	for i, r := range path {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && inString {
			escaped = true
			continue
		}
		if r == '"' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		for _, op := range operators {
			if strings.HasPrefix(path[i:], op) {
				return strings.TrimSpace(path[:i]), op, strings.TrimSpace(path[i+len(op):]), true
			}
		}
	}
	return "", "", "", false
}

func jsonPathLiteral(text string) (pgtypes.JsonValue, error) {
	if normalized, ok := jsonPathNormalizeNumericLiteral(text); ok {
		text = normalized
	}
	doc, err := pgtypes.UnmarshalToJsonDocument([]byte(text))
	if err != nil {
		return nil, err
	}
	return doc.Value, nil
}

func jsonPathNormalizeNumericLiteral(text string) (string, bool) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", false
	}
	sign := ""
	if text[0] == '-' || text[0] == '+' {
		sign = text[:1]
		text = text[1:]
	}
	if text == "" {
		return "", false
	}
	if len(text) > 2 && text[0] == '0' {
		switch text[1] {
		case 'x', 'X':
			return jsonPathNormalizeBasedIntegerLiteral(sign, text[2:], 16)
		case 'o', 'O':
			return jsonPathNormalizeBasedIntegerLiteral(sign, text[2:], 8)
		case 'b', 'B':
			return jsonPathNormalizeBasedIntegerLiteral(sign, text[2:], 2)
		}
	}
	if !strings.Contains(text, "_") {
		return "", false
	}
	normalized := strings.ReplaceAll(text, "_", "")
	if !jsonPathIsDecimalNumber(normalized) {
		return "", false
	}
	if sign == "+" {
		sign = ""
	}
	return sign + normalized, true
}

func jsonPathNormalizeBasedIntegerLiteral(sign string, digits string, base int) (string, bool) {
	digits = strings.ReplaceAll(digits, "_", "")
	if digits == "" {
		return "", false
	}
	value := new(big.Int)
	if _, ok := value.SetString(digits, base); !ok {
		return "", false
	}
	if sign == "-" {
		value.Neg(value)
	}
	return value.String(), true
}

func jsonPathIsDecimalNumber(text string) bool {
	i := 0
	if i >= len(text) {
		return false
	}
	switch {
	case text[i] == '0':
		i++
	case text[i] >= '1' && text[i] <= '9':
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
	default:
		return false
	}
	if i < len(text) && text[i] == '.' {
		i++
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(text) && (text[i] == 'e' || text[i] == 'E') {
		i++
		if i < len(text) && (text[i] == '-' || text[i] == '+') {
			i++
		}
		start := i
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(text)
}

func jsonPathSingleBooleanResult(values []pgtypes.JsonValue) (any, error) {
	if len(values) != 1 {
		return nil, pgerror.New(pgcode.SingletonSQLJSONItemRequired, "single boolean result is expected")
	}
	switch value := values[0].(type) {
	case pgtypes.JsonValueBoolean:
		return bool(value), nil
	case pgtypes.JsonValueNull:
		return nil, nil
	default:
		return nil, pgerror.New(pgcode.SingletonSQLJSONItemRequired, "single boolean result is expected")
	}
}
