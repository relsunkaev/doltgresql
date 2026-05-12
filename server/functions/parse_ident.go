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
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initParseIdent() {
	framework.RegisterFunction(parse_ident_text)
	framework.RegisterFunction(parse_ident_text_bool)
}

var parse_ident_text = framework.Function1{
	Name:       "parse_ident",
	Return:     pgtypes.TextArray,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return parseIdent(val.(string), true)
	},
}

var parse_ident_text_bool = framework.Function2{
	Name:       "parse_ident",
	Return:     pgtypes.TextArray,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val any, strict any) (any, error) {
		return parseIdent(val.(string), strict.(bool))
	},
}

func parseIdent(input string, strict bool) ([]any, error) {
	var parts []any
	for i := 0; i < len(input); {
		i = skipIdentifierSpace(input, i)
		if i >= len(input) {
			break
		}

		var part string
		var err error
		if input[i] == '"' {
			part, i, err = parseQuotedIdentifier(input, i)
		} else if isIdentifierStart(input[i]) {
			part, i = parseUnquotedIdentifier(input, i)
		} else {
			if strict || len(parts) == 0 {
				return nil, errors.Errorf("string is not a valid identifier: %s", input)
			}
			break
		}
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)

		i = skipIdentifierSpace(input, i)
		if i >= len(input) {
			break
		}
		if input[i] != '.' {
			if strict {
				return nil, errors.Errorf("string is not a valid identifier: %s", input)
			}
			break
		}
		i++
		if skipIdentifierSpace(input, i) >= len(input) {
			return nil, errors.Errorf("string is not a valid identifier: %s", input)
		}
	}
	if len(parts) == 0 {
		return nil, errors.Errorf("string is not a valid identifier: %s", input)
	}
	return parts, nil
}

func parseQuotedIdentifier(input string, start int) (string, int, error) {
	var sb strings.Builder
	for i := start + 1; i < len(input); i++ {
		if input[i] != '"' {
			sb.WriteByte(input[i])
			continue
		}
		if i+1 < len(input) && input[i+1] == '"' {
			sb.WriteByte('"')
			i++
			continue
		}
		if sb.Len() == 0 {
			return "", 0, errors.Errorf("zero-length delimited identifier")
		}
		return sb.String(), i + 1, nil
	}
	return "", 0, errors.Errorf("unterminated quoted identifier")
}

func parseUnquotedIdentifier(input string, start int) (string, int) {
	i := start
	for i < len(input) && isIdentifierPart(input[i]) {
		i++
	}
	return strings.ToLower(input[start:i]), i
}

func skipIdentifierSpace(input string, start int) int {
	for start < len(input) && unicode.IsSpace(rune(input[start])) {
		start++
	}
	return start
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || unicode.IsLetter(rune(ch))
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || ch == '$' || (ch >= '0' && ch <= '9')
}
