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
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func init() {
	framework.RegisterFunction(to_tsvector_text)
	framework.RegisterFunction(to_tsvector_config_text)
	framework.RegisterFunction(to_tsquery_text)
	framework.RegisterFunction(to_tsquery_config_text)
	framework.RegisterFunction(ts_match_vq_text)
}

var textSearchTokenPattern = regexp.MustCompile(`[[:alnum:]_]+`)

var to_tsvector_text = framework.Function1{
	Name:       "to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return simpleTSVector(fmt.Sprint(val)), nil
	},
}

var to_tsvector_config_text = framework.Function2{
	Name:       "to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, config any, val any) (any, error) {
		return simpleTSVector(fmt.Sprint(val)), nil
	},
}

var to_tsquery_text = framework.Function1{
	Name:       "to_tsquery",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return simpleTSQuery(fmt.Sprint(val)), nil
	},
}

var to_tsquery_config_text = framework.Function2{
	Name:       "to_tsquery",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, config any, val any) (any, error) {
		return simpleTSQuery(fmt.Sprint(val)), nil
	},
}

var ts_match_vq_text = framework.Function2{
	Name:       "ts_match_vq",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, query any) (any, error) {
		vectorTerms := textSearchTermSet(fmt.Sprint(vector))
		for _, term := range textSearchTerms(fmt.Sprint(query)) {
			if !vectorTerms[term] {
				return false, nil
			}
		}
		return true, nil
	},
}

func simpleTSVector(input string) string {
	positionsByTerm := map[string][]int{}
	for i, term := range textSearchTerms(input) {
		positionsByTerm[term] = append(positionsByTerm[term], i+1)
	}
	terms := make([]string, 0, len(positionsByTerm))
	for term := range positionsByTerm {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	parts := make([]string, 0, len(terms))
	for _, term := range terms {
		positions := positionsByTerm[term]
		positionText := make([]string, len(positions))
		for i, pos := range positions {
			positionText[i] = fmt.Sprint(pos)
		}
		parts = append(parts, "'"+term+"':"+strings.Join(positionText, ","))
	}
	return strings.Join(parts, " ")
}

func simpleTSQuery(input string) string {
	terms := textSearchTerms(input)
	if len(terms) == 0 {
		return ""
	}
	parts := make([]string, len(terms))
	for i, term := range terms {
		parts[i] = "'" + term + "'"
	}
	return strings.Join(parts, " & ")
}

func textSearchTermSet(input string) map[string]bool {
	terms := map[string]bool{}
	for _, term := range textSearchTerms(input) {
		terms[term] = true
	}
	return terms
}

func textSearchTerms(input string) []string {
	rawTerms := textSearchTokenPattern.FindAllString(strings.ToLower(input), -1)
	terms := rawTerms[:0]
	for _, term := range rawTerms {
		if term != "" {
			terms = append(terms, term)
		}
	}
	return terms
}
