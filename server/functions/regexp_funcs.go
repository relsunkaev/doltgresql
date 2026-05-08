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
	framework.RegisterFunction(regexp_split_to_table_text_text)
	framework.RegisterFunction(regexp_split_to_table_text_text_text)
}

// compilePGRegex compiles a PG-style regex pattern with optional flags.
// Recognised flags: 'i' (case-insensitive), 'g' (global; consumed by caller).
// Other PG flags are rejected so callers do not silently get the wrong
// semantics.
func compilePGRegex(pattern, flags string) (re *regexp.Regexp, global bool, err error) {
	var inlineFlags strings.Builder
	for _, f := range flags {
		switch f {
		case 'g':
			global = true
		case 'i':
			inlineFlags.WriteRune('i')
		case 'c':
			// case-sensitive (default in Go); no-op
		case 'm', 's', 'n', 'x', 'p', 'q', 'w', 'b', 'e':
			return nil, false, errors.Errorf("regex flag %q not supported", string(f))
		default:
			return nil, false, errors.Errorf("invalid regex flag %q", string(f))
		}
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
