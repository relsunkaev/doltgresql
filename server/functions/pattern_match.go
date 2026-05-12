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
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPatternMatch registers helper functions used by PostgreSQL-only pattern operators.
func initPatternMatch() {
	framework.RegisterFunction(doltgres_ilike_text_text)
	framework.RegisterFunction(doltgres_similar_to_text_text)
	framework.RegisterFunction(doltgres_regex_match_ci_text_text)
}

var doltgres_ilike_text_text = framework.Function2{
	Name:       "__doltgres_ilike",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val, pattern any) (any, error) {
		re, err := regexp.Compile("(?is)" + likePatternToRegex(pattern.(string)))
		if err != nil {
			return nil, err
		}
		return re.MatchString(val.(string)), nil
	},
}

var doltgres_similar_to_text_text = framework.Function2{
	Name:       "__doltgres_similar_to",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val, pattern any) (any, error) {
		re, err := regexp.Compile("(?s)" + similarPatternToRegex(pattern.(string)))
		if err != nil {
			return nil, err
		}
		return re.MatchString(val.(string)), nil
	},
}

var doltgres_regex_match_ci_text_text = framework.Function2{
	Name:       "__doltgres_regex_match_ci",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val, pattern any) (any, error) {
		re, _, err := compilePGRegex(pattern.(string), "i")
		if err != nil {
			return nil, err
		}
		return re.MatchString(val.(string)), nil
	},
}

func likePatternToRegex(pattern string) string {
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			} else {
				b.WriteString(regexp.QuoteMeta(`\`))
			}
		default:
			b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		}
	}
	b.WriteByte('$')
	return b.String()
}

func similarPatternToRegex(pattern string) string {
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			} else {
				b.WriteString(regexp.QuoteMeta(`\`))
			}
		case '|', '*', '+', '?', '{', '}', '(', ')', '[', ']':
			b.WriteByte(pattern[i])
		default:
			b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		}
	}
	b.WriteByte('$')
	return b.String()
}
