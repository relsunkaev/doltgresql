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
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/lex"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initFormat() {
	framework.RegisterFunction(format_any)
}

var format_any = framework.Function1N{
	Name:       "format",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     false,
	Callable: func(ctx *sql.Context, argTypes []*pgtypes.DoltgresType, formatStr any, args []any) (any, error) {
		if formatStr == nil {
			return nil, nil
		}
		return pgFormat(ctx, formatStr.(string), argTypes[1:], args)
	},
}

func pgFormat(ctx *sql.Context, formatStr string, argTypes []*pgtypes.DoltgresType, args []any) (string, error) {
	var sb strings.Builder
	nextArg := 0
	for i := 0; i < len(formatStr); {
		if formatStr[i] != '%' {
			sb.WriteByte(formatStr[i])
			i++
			continue
		}
		if i+1 >= len(formatStr) {
			return "", errors.New("unterminated format() type specifier")
		}
		if formatStr[i+1] == '%' {
			sb.WriteByte('%')
			i += 2
			continue
		}

		j := i + 1
		position := -1
		digitsStart := j
		for j < len(formatStr) && formatStr[j] >= '0' && formatStr[j] <= '9' {
			j++
		}
		if j > digitsStart && j < len(formatStr) && formatStr[j] == '$' {
			parsedPosition, err := strconv.Atoi(formatStr[digitsStart:j])
			if err != nil || parsedPosition <= 0 {
				return "", errors.Errorf("invalid format() argument position: %s", formatStr[digitsStart:j])
			}
			position = parsedPosition - 1
			j++
		} else {
			j = digitsStart
		}

		leftJustify := false
		if j < len(formatStr) && formatStr[j] == '-' {
			leftJustify = true
			j++
		}

		width := 0
		widthSet := false
		widthStart := j
		for j < len(formatStr) && formatStr[j] >= '0' && formatStr[j] <= '9' {
			j++
		}
		if j > widthStart {
			var err error
			width, err = strconv.Atoi(formatStr[widthStart:j])
			if err != nil {
				return "", errors.Errorf("invalid format() width: %s", formatStr[widthStart:j])
			}
			widthSet = true
		} else if j < len(formatStr) && formatStr[j] == '*' {
			return "", errors.New("format() width from arguments is not yet supported")
		}

		if j >= len(formatStr) {
			return "", errors.New("unterminated format() type specifier")
		}
		formatType := formatStr[j]
		if formatType != 's' && formatType != 'I' && formatType != 'L' {
			return "", errors.Errorf("unrecognized format() type specifier: %%%c", formatType)
		}

		argIndex := position
		if argIndex < 0 {
			argIndex = nextArg
		}
		if argIndex >= len(args) {
			return "", errors.New("too few arguments for format()")
		}
		nextArg = argIndex + 1

		formatted, err := pgFormatArg(ctx, argTypes[argIndex], args[argIndex], formatType)
		if err != nil {
			return "", err
		}
		if widthSet && len(formatted) < width {
			padding := strings.Repeat(" ", width-len(formatted))
			if leftJustify {
				formatted += padding
			} else {
				formatted = padding + formatted
			}
		}
		sb.WriteString(formatted)
		i = j + 1
	}
	return sb.String(), nil
}

func pgFormatArg(ctx *sql.Context, typ *pgtypes.DoltgresType, val any, formatType byte) (string, error) {
	switch formatType {
	case 's':
		if val == nil {
			return "", nil
		}
		return pgFormatOutput(ctx, typ, val)
	case 'I':
		if val == nil {
			return "", errors.New("null values cannot be formatted as an SQL identifier")
		}
		output, err := pgFormatOutput(ctx, typ, val)
		if err != nil {
			return "", err
		}
		return pgQuoteIdentifier(output), nil
	case 'L':
		if val == nil {
			return "NULL", nil
		}
		output, err := pgFormatOutput(ctx, typ, val)
		if err != nil {
			return "", err
		}
		return pgQuoteLiteral(output), nil
	default:
		return "", errors.Errorf("unrecognized format() type specifier: %%%c", formatType)
	}
}

func pgFormatOutput(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	if typ.ID == pgtypes.Bool.ID {
		if val.(bool) {
			return "t", nil
		}
		return "f", nil
	}
	return typ.IoOutput(ctx, val)
}

func pgQuoteIdentifier(val string) string {
	if canUseBareIdentifier(val) {
		return val
	}
	return `"` + strings.ReplaceAll(val, `"`, `""`) + `"`
}

func canUseBareIdentifier(val string) bool {
	if len(val) == 0 {
		return false
	}
	for i, r := range val {
		if i == 0 {
			if r != '_' && (r < 'a' || r > 'z') {
				return false
			}
			continue
		}
		if r != '_' && r != '$' && (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return false
		}
	}
	if category, ok := lex.KeywordsCategories[val]; ok && category == "R" {
		return false
	}
	return true
}

func pgQuoteLiteral(val string) string {
	return "'" + strings.ReplaceAll(val, `'`, `''`) + "'"
}
