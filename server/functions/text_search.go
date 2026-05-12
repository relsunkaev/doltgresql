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
	framework.RegisterFunction(plainto_tsquery_text)
	framework.RegisterFunction(plainto_tsquery_config_text)
	framework.RegisterFunction(phraseto_tsquery_text)
	framework.RegisterFunction(phraseto_tsquery_config_text)
	framework.RegisterFunction(websearch_to_tsquery_text)
	framework.RegisterFunction(websearch_to_tsquery_config_text)
	framework.RegisterFunction(ts_headline_config_text_text)
	framework.RegisterFunction(ts_rank_text_text)
	framework.RegisterFunction(ts_rank_cd_text_text)
	framework.RegisterFunction(tsvector_to_array_text)
	framework.RegisterFunction(array_to_tsvector_text_array)
	framework.RegisterFunction(strip_text)
	framework.RegisterFunction(ts_match_vq_text)
}

var textSearchTokenPattern = regexp.MustCompile(`[[:alnum:]_]+`)
var tsVectorLexemePattern = regexp.MustCompile(`'((?:''|[^'])*)'(?:\:\d+(?:,\d+)*)?`)

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

var plainto_tsquery_text = framework.Function1{
	Name:       "plainto_tsquery",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return simpleTSQuery(fmt.Sprint(val)), nil
	},
}

var plainto_tsquery_config_text = framework.Function2{
	Name:       "plainto_tsquery",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, config any, val any) (any, error) {
		return simpleTSQuery(fmt.Sprint(val)), nil
	},
}

var phraseto_tsquery_text = framework.Function1{
	Name:       "phraseto_tsquery",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return simpleTSPhraseQuery(fmt.Sprint(val)), nil
	},
}

var phraseto_tsquery_config_text = framework.Function2{
	Name:       "phraseto_tsquery",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, config any, val any) (any, error) {
		return simpleTSPhraseQuery(fmt.Sprint(val)), nil
	},
}

var websearch_to_tsquery_text = framework.Function1{
	Name:       "websearch_to_tsquery",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return simpleTSQuery(fmt.Sprint(val)), nil
	},
}

var websearch_to_tsquery_config_text = framework.Function2{
	Name:       "websearch_to_tsquery",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, config any, val any) (any, error) {
		return simpleTSQuery(fmt.Sprint(val)), nil
	},
}

var ts_headline_config_text_text = framework.Function3{
	Name:       "ts_headline",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, config any, document any, query any) (any, error) {
		return simpleTSHeadline(fmt.Sprint(document), fmt.Sprint(query)), nil
	},
}

var ts_rank_text_text = framework.Function2{
	Name:       "ts_rank",
	Return:     pgtypes.Float32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, query any) (any, error) {
		if simpleTSMatches(fmt.Sprint(vector), fmt.Sprint(query)) {
			return float32(0.1), nil
		}
		return float32(0), nil
	},
}

var ts_rank_cd_text_text = framework.Function2{
	Name:       "ts_rank_cd",
	Return:     pgtypes.Float32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, query any) (any, error) {
		if simpleTSMatches(fmt.Sprint(vector), fmt.Sprint(query)) {
			return float32(0.1), nil
		}
		return float32(0), nil
	},
}

var tsvector_to_array_text = framework.Function1{
	Name:       "tsvector_to_array",
	Return:     pgtypes.TextArray,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, vector any) (any, error) {
		lexemes := tsVectorLexemes(fmt.Sprint(vector))
		values := make([]any, len(lexemes))
		for i, lexeme := range lexemes {
			values[i] = lexeme
		}
		return values, nil
	},
}

var array_to_tsvector_text_array = framework.Function1{
	Name:       "array_to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.TextArray},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.([]any)
		terms := make([]string, 0, len(input))
		seen := map[string]bool{}
		for _, item := range input {
			term := strings.ToLower(fmt.Sprint(item))
			if term != "" && !seen[term] {
				seen[term] = true
				terms = append(terms, term)
			}
		}
		sort.Strings(terms)
		parts := make([]string, len(terms))
		for i, term := range terms {
			parts[i] = "'" + term + "'"
		}
		return strings.Join(parts, " "), nil
	},
}

var strip_text = framework.Function1{
	Name:       "strip",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, vector any) (any, error) {
		return simpleTSStrip(fmt.Sprint(vector)), nil
	},
}

var ts_match_vq_text = framework.Function2{
	Name:       "ts_match_vq",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, query any) (any, error) {
		return simpleTSMatches(fmt.Sprint(vector), fmt.Sprint(query)), nil
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

func simpleTSPhraseQuery(input string) string {
	terms := textSearchTerms(input)
	if len(terms) == 0 {
		return ""
	}
	parts := make([]string, len(terms))
	for i, term := range terms {
		parts[i] = "'" + term + "'"
	}
	return strings.Join(parts, " <-> ")
}

func simpleTSMatches(vector string, query string) bool {
	vectorTerms := textSearchTermSet(vector)
	for _, term := range textSearchTerms(query) {
		if !vectorTerms[term] {
			return false
		}
	}
	return true
}

func simpleTSHeadline(document string, query string) string {
	queryTerms := textSearchTermSet(query)
	return textSearchTokenPattern.ReplaceAllStringFunc(document, func(token string) string {
		if queryTerms[strings.ToLower(token)] {
			return "<b>" + token + "</b>"
		}
		return token
	})
}

func simpleTSStrip(vector string) string {
	lexemes := tsVectorLexemes(vector)
	parts := make([]string, len(lexemes))
	for i, lexeme := range lexemes {
		parts[i] = "'" + lexeme + "'"
	}
	return strings.Join(parts, " ")
}

func textSearchTermSet(input string) map[string]bool {
	terms := map[string]bool{}
	for _, term := range textSearchTerms(input) {
		terms[term] = true
	}
	return terms
}

func tsVectorLexemes(input string) []string {
	matches := tsVectorLexemePattern.FindAllStringSubmatch(input, -1)
	if len(matches) == 0 {
		terms := textSearchTerms(input)
		sort.Strings(terms)
		return terms
	}
	lexemes := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		lexeme := strings.ReplaceAll(match[1], "''", "'")
		if !seen[lexeme] {
			seen[lexeme] = true
			lexemes = append(lexemes, lexeme)
		}
	}
	sort.Strings(lexemes)
	return lexemes
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
