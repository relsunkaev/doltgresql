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
		widthArgIndex := -1
		widthConsumesArg := false
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
			widthSet = true
			widthArgIndex = nextArg
			widthConsumesArg = true
			j++
			widthPositionStart := j
			for j < len(formatStr) && formatStr[j] >= '0' && formatStr[j] <= '9' {
				j++
			}
			if j > widthPositionStart {
				if j >= len(formatStr) || formatStr[j] != '$' {
					return "", errors.Errorf("invalid format() width argument position: %s", formatStr[widthPositionStart:j])
				}
				parsedPosition, err := strconv.Atoi(formatStr[widthPositionStart:j])
				if err != nil || parsedPosition <= 0 {
					return "", errors.Errorf("invalid format() width argument position: %s", formatStr[widthPositionStart:j])
				}
				widthArgIndex = parsedPosition - 1
				widthConsumesArg = false
				j++
			}
		}

		if j >= len(formatStr) {
			return "", errors.New("unterminated format() type specifier")
		}
		formatType := formatStr[j]
		if formatType != 's' && formatType != 'I' && formatType != 'L' {
			return "", errors.Errorf("unrecognized format() type specifier: %%%c", formatType)
		}

		if widthArgIndex >= 0 {
			if widthArgIndex >= len(args) {
				return "", errors.New("too few arguments for format()")
			}
			parsedWidth, err := pgFormatWidthArg(ctx, argTypes[widthArgIndex], args[widthArgIndex])
			if err != nil {
				return "", err
			}
			width = parsedWidth
			if width < 0 {
				leftJustify = true
				width = -width
			}
			if widthConsumesArg {
				nextArg = widthArgIndex + 1
			}
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

func pgFormatWidthArg(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (int, error) {
	if val == nil {
		return 0, errors.New("null values cannot be used as format() width")
	}
	switch width := val.(type) {
	case int:
		return width, nil
	case int8:
		return int(width), nil
	case int16:
		return int(width), nil
	case int32:
		return int(width), nil
	case int64:
		return int(width), nil
	case uint:
		return int(width), nil
	case uint8:
		return int(width), nil
	case uint16:
		return int(width), nil
	case uint32:
		return int(width), nil
	case uint64:
		return int(width), nil
	}
	output, err := pgFormatOutput(ctx, typ, val)
	if err != nil {
		return 0, err
	}
	width, err := strconv.Atoi(output)
	if err != nil {
		return 0, errors.Errorf("format() width argument is not an integer: %s", output)
	}
	return width, nil
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
	if str, ok := val.(string); ok && isFormatNativeStringType(typ) {
		return str, nil
	}
	return typ.IoOutput(ctx, val)
}

func isFormatNativeStringType(typ *pgtypes.DoltgresType) bool {
	return typ.ID == pgtypes.Text.ID ||
		typ.ID == pgtypes.VarChar.ID ||
		typ.ID == pgtypes.BpChar.ID ||
		typ.ID == pgtypes.Name.ID ||
		typ.ID == pgtypes.Cstring.ID
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
