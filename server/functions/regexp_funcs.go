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
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initRegexpSetReturning registers the SRF regex functions to the catalog.
func initRegexpSetReturning() {
	framework.RegisterFunction(regexp_matches_text_text)
	framework.RegisterFunction(regexp_matches_text_text_text)
	framework.RegisterFunction(regexp_count_text_text)
	framework.RegisterFunction(regexp_replace_text_text_text)
	framework.RegisterFunction(regexp_replace_text_text_text_text)
	framework.RegisterFunction(regexp_split_to_array_text_text)
	framework.RegisterFunction(regexp_split_to_array_text_text_text)
	framework.RegisterFunction(regexp_split_to_table_text_text)
	framework.RegisterFunction(regexp_split_to_table_text_text_text)
}

// compilePGRegex compiles a PG-style regex pattern with optional flags.
// Recognised flags include 'i' (case-insensitive), 'g' (global; consumed by
// caller), 'x' (expanded whitespace mode), and 'n' (newline-sensitive anchors).
// Other PG flags are rejected so callers do not silently get the wrong semantics.
func compilePGRegex(pattern, flags string) (re *regexp.Regexp, global bool, err error) {
	var inlineFlags strings.Builder
	expanded := false
	for _, f := range flags {
		switch f {
		case 'g':
			global = true
		case 'i':
			inlineFlags.WriteRune('i')
		case 'c':
			// case-sensitive (default in Go); no-op
		case 'n':
			inlineFlags.WriteRune('m')
		case 'x':
			expanded = true
		case 'm', 's', 'p', 'q', 'w', 'b', 'e':
			return nil, false, errors.Errorf("regex flag %q not supported", string(f))
		default:
			return nil, false, errors.Errorf("invalid regex flag %q", string(f))
		}
	}
	if expanded {
		pattern = expandPGRegex(pattern)
	}
	prefix := ""
	if inlineFlags.Len() > 0 {
		prefix = "(?" + inlineFlags.String() + ")"
	}
	re, err = regexp.Compile(prefix + pattern)
	if err != nil {
		return nil, false, errors.Errorf("invalid regular expression: %s", err.Error())
	}
	return re, global, nil
}

func expandPGRegex(pattern string) string {
	var sb strings.Builder
	inClass := false
	escaped := false
	for _, r := range pattern {
		if escaped {
			sb.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			sb.WriteRune(r)
			escaped = true
			continue
		}
		switch r {
		case '[':
			inClass = true
			sb.WriteRune(r)
		case ']':
			inClass = false
			sb.WriteRune(r)
		case ' ', '\t', '\n', '\r', '\f':
			if inClass {
				sb.WriteRune(r)
			}
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// regexpMatchesIter advances through matches; each row is text[] of capture
// groups (or the whole match if there are no capture groups), matching PG
// semantics.
func regexpMatchesIter(input string, re *regexp.Regexp, global bool) (*pgtypes.SetReturningFunctionRowIter, error) {
	var all [][]string
	if global {
		all = re.FindAllStringSubmatch(input, -1)
	} else if m := re.FindStringSubmatch(input); m != nil {
		all = [][]string{m}
	}
	idx := 0
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if idx >= len(all) {
			return nil, io.EOF
		}
		match := all[idx]
		idx++
		var groups []any
		if len(match) > 1 {
			groups = make([]any, 0, len(match)-1)
			for _, g := range match[1:] {
				groups = append(groups, g)
			}
		} else {
			groups = []any{match[0]}
		}
		return sql.Row{groups}, nil
	}), nil
}

var regexp_matches_text_text = framework.Function2{
	Name:       "regexp_matches",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.TextArray),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		re, global, err := compilePGRegex(val2.(string), "")
		if err != nil {
			return nil, err
		}
		return regexpMatchesIter(val1.(string), re, global)
	},
}

var regexp_matches_text_text_text = framework.Function3{
	Name:       "regexp_matches",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.TextArray),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		re, global, err := compilePGRegex(val2.(string), val3.(string))
		if err != nil {
			return nil, err
		}
		return regexpMatchesIter(val1.(string), re, global)
	},
}

var regexp_count_text_text = framework.Function2{
	Name:       "regexp_count",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		re, _, err := compilePGRegex(val2.(string), "g")
		if err != nil {
			return nil, err
		}
		return int32(len(re.FindAllStringIndex(val1.(string), -1))), nil
	},
}

var regexp_replace_text_text_text = framework.Function3{
	Name:       "regexp_replace",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		re, _, err := compilePGRegex(val2.(string), "")
		if err != nil {
			return nil, err
		}
		return regexpReplace(val1.(string), re, val3.(string), false), nil
	},
}

var regexp_replace_text_text_text_text = framework.Function4{
	Name:       "regexp_replace",
	Return:     pgtypes.Text,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, val1, val2, val3, val4 any) (any, error) {
		re, global, err := compilePGRegex(val2.(string), val4.(string))
		if err != nil {
			return nil, err
		}
		return regexpReplace(val1.(string), re, val3.(string), global), nil
	},
}

func regexpReplace(input string, re *regexp.Regexp, replacement string, global bool) string {
	replacement = pgRegexReplacement(replacement)
	if global {
		return re.ReplaceAllString(input, replacement)
	}
	loc := re.FindStringSubmatchIndex(input)
	if loc == nil {
		return input
	}
	var sb strings.Builder
	sb.WriteString(input[:loc[0]])
	sb.Write(re.ExpandString(nil, replacement, input, loc))
	sb.WriteString(input[loc[1]:])
	return sb.String()
}

func pgRegexReplacement(replacement string) string {
	var sb strings.Builder
	for i := 0; i < len(replacement); i++ {
		if replacement[i] != '\\' || i+1 >= len(replacement) {
			sb.WriteByte(replacement[i])
			continue
		}
		i++
		switch next := replacement[i]; {
		case next >= '0' && next <= '9':
			sb.WriteByte('$')
			sb.WriteByte(next)
		case next == '&':
			sb.WriteString("$0")
		default:
			sb.WriteByte(next)
		}
	}
	return sb.String()
}

func regexpSplitIter(input string, re *regexp.Regexp) *pgtypes.SetReturningFunctionRowIter {
	parts := re.Split(input, -1)
	idx := 0
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if idx >= len(parts) {
			return nil, io.EOF
		}
		p := parts[idx]
		idx++
		return sql.Row{p}, nil
	})
}

func regexpSplitArray(input string, re *regexp.Regexp) []any {
	parts := re.Split(input, -1)
	values := make([]any, len(parts))
	for i, part := range parts {
		values[i] = part
	}
	return values
}

var regexp_split_to_array_text_text = framework.Function2{
	Name:       "regexp_split_to_array",
	Return:     pgtypes.TextArray,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		re, _, err := compilePGRegex(val2.(string), "")
		if err != nil {
			return nil, err
		}
		return regexpSplitArray(val1.(string), re), nil
	},
}

var regexp_split_to_array_text_text_text = framework.Function3{
	Name:       "regexp_split_to_array",
	Return:     pgtypes.TextArray,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		re, _, err := compilePGRegex(val2.(string), val3.(string))
		if err != nil {
			return nil, err
		}
		return regexpSplitArray(val1.(string), re), nil
	},
}

var regexp_split_to_table_text_text = framework.Function2{
	Name:       "regexp_split_to_table",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		re, _, err := compilePGRegex(val2.(string), "")
		if err != nil {
			return nil, err
		}
		return regexpSplitIter(val1.(string), re), nil
	},
}

var regexp_split_to_table_text_text_text = framework.Function3{
	Name:       "regexp_split_to_table",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		re, _, err := compilePGRegex(val2.(string), val3.(string))
		if err != nil {
			return nil, err
		}
		return regexpSplitIter(val1.(string), re), nil
	},
}
