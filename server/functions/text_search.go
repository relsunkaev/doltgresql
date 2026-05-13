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
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func init() {
	framework.RegisterFunction(to_tsvector_text)
	framework.RegisterFunction(to_tsvector_config_text)
	framework.RegisterFunction(json_to_tsvector_json_jsonb)
	framework.RegisterFunction(json_to_tsvector_config_json_jsonb)
	framework.RegisterFunction(jsonb_to_tsvector_jsonb_jsonb)
	framework.RegisterFunction(jsonb_to_tsvector_config_jsonb_jsonb)
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
	framework.RegisterFunction(ts_delete_text_text)
	framework.RegisterFunction(setweight_text_text)
	framework.RegisterFunction(numnode_text)
	framework.RegisterFunction(querytree_text)
	framework.RegisterFunction(tsquery_phrase_text_text)
	framework.RegisterFunction(ts_rewrite_text_text_text)
	framework.RegisterFunction(ts_filter_text_text)
	framework.RegisterFunction(ts_filter_tsvector_text)
	framework.RegisterFunction(ts_match_vq_text)
}

var textSearchTokenPattern = regexp.MustCompile(`[[:alnum:]_]+`)
var tsVectorLexemePattern = regexp.MustCompile(`'((?:''|[^'])*)'(?:\:([0-9A-D,]+))?`)
var tsVectorBareLexemePattern = regexp.MustCompile(`([[:alnum:]_]+)(?:\:([0-9A-D,]+))`)

var englishTextSearchStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "by": {},
	"for": {}, "from": {}, "in": {}, "is": {}, "it": {}, "of": {}, "on": {}, "or": {},
	"that": {}, "the": {}, "to": {}, "was": {}, "were": {}, "with": {},
}

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

var json_to_tsvector_json_jsonb = framework.Function2{
	Name:       "json_to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Json, pgtypes.JsonB},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, doc any, filter any) (any, error) {
		return simpleJsonToTSVector(ctx, pgtypes.Json, doc, filter, "")
	},
}

var json_to_tsvector_config_json_jsonb = framework.Function3{
	Name:       "json_to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Json, pgtypes.JsonB},
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, config any, doc any, filter any) (any, error) {
		return simpleJsonToTSVector(ctx, pgtypes.Json, doc, filter, fmt.Sprint(config))
	},
}

var jsonb_to_tsvector_jsonb_jsonb = framework.Function2{
	Name:       "jsonb_to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.JsonB, pgtypes.JsonB},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, doc any, filter any) (any, error) {
		return simpleJsonToTSVector(ctx, pgtypes.JsonB, doc, filter, "")
	},
}

var jsonb_to_tsvector_config_jsonb_jsonb = framework.Function3{
	Name:       "jsonb_to_tsvector",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.JsonB, pgtypes.JsonB},
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, config any, doc any, filter any) (any, error) {
		return simpleJsonToTSVector(ctx, pgtypes.JsonB, doc, filter, fmt.Sprint(config))
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

var ts_delete_text_text = framework.Function2{
	Name:       "ts_delete",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, lexeme any) (any, error) {
		return simpleTSDelete(fmt.Sprint(vector), strings.ToLower(fmt.Sprint(lexeme))), nil
	},
}

var setweight_text_text = framework.Function2{
	Name:       "setweight",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, weight any) (any, error) {
		return simpleTSSetWeight(fmt.Sprint(vector), strings.ToUpper(fmt.Sprint(weight))), nil
	},
}

var numnode_text = framework.Function1{
	Name:       "numnode",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, query any) (any, error) {
		return int32(simpleTSNumNode(fmt.Sprint(query))), nil
	},
}

var querytree_text = framework.Function1{
	Name:       "querytree",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, query any) (any, error) {
		return fmt.Sprint(query), nil
	},
}

var tsquery_phrase_text_text = framework.Function2{
	Name:       "tsquery_phrase",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, left any, right any) (any, error) {
		return simpleTSQueryPhrase(fmt.Sprint(left), fmt.Sprint(right)), nil
	},
}

var ts_rewrite_text_text_text = framework.Function3{
	Name:       "ts_rewrite",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, query any, target any, substitute any) (any, error) {
		return simpleTSRewrite(fmt.Sprint(query), fmt.Sprint(target), fmt.Sprint(substitute)), nil
	},
}

var ts_filter_text_text = framework.Function2{
	Name:       "ts_filter",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, weights any) (any, error) {
		return simpleTSFilter(fmt.Sprint(vector), fmt.Sprint(weights)), nil
	},
}

var ts_filter_tsvector_text = framework.Function2{
	Name:       "ts_filter",
	Return:     pgtypes.TsVector,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.TsVector, pgtypes.Text},
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, vector any, weights any) (any, error) {
		return simpleTSFilter(fmt.Sprint(vector), fmt.Sprint(weights)), nil
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
	return renderTSVectorFromTerms(textSearchTerms(input), nil)
}

func renderTSVectorFromTerms(terms []string, stopWords map[string]struct{}) string {
	positionsByTerm := map[string][]int{}
	for i, term := range terms {
		if _, ok := stopWords[term]; ok {
			continue
		}
		positionsByTerm[term] = append(positionsByTerm[term], i+1)
	}
	lexemes := make([]string, 0, len(positionsByTerm))
	for term := range positionsByTerm {
		lexemes = append(lexemes, term)
	}
	sort.Strings(lexemes)
	parts := make([]string, 0, len(lexemes))
	for _, term := range lexemes {
		positions := positionsByTerm[term]
		positionText := make([]string, len(positions))
		for i, pos := range positions {
			positionText[i] = fmt.Sprint(pos)
		}
		parts = append(parts, "'"+term+"':"+strings.Join(positionText, ","))
	}
	return strings.Join(parts, " ")
}

func simpleJsonToTSVector(ctx *sql.Context, docType *pgtypes.DoltgresType, doc any, filter any, config string) (string, error) {
	jsonDoc, err := pgtypes.JsonDocumentFromSQLValue(ctx, docType, doc)
	if err != nil {
		return "", err
	}
	filterSet, err := jsonToTSVectorFilter(ctx, filter)
	if err != nil {
		return "", err
	}
	terms, err := jsonTextSearchTerms(jsonDoc.Value, filterSet)
	if err != nil {
		return "", err
	}
	return renderTSVectorFromTerms(terms, textSearchStopWords(config)), nil
}

func jsonToTSVectorFilter(ctx *sql.Context, filter any) (map[string]bool, error) {
	doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, filter)
	if err != nil {
		return nil, err
	}
	filterSet := map[string]bool{}
	var visit func(value pgtypes.JsonValue) error
	visit = func(value pgtypes.JsonValue) error {
		switch value := value.(type) {
		case pgtypes.JsonValueString:
			option, err := pgtypes.JsonStringUnescape(value)
			if err != nil {
				return err
			}
			filterSet[strings.ToLower(option)] = true
		case pgtypes.JsonValueArray:
			for _, item := range value {
				if _, isNull := item.(pgtypes.JsonValueNull); isNull {
					continue
				}
				if err := visit(item); err != nil {
					return err
				}
			}
		case pgtypes.JsonValueNull:
		default:
			return fmt.Errorf("invalid json_to_tsvector filter")
		}
		return nil
	}
	if err := visit(doc.Value); err != nil {
		return nil, err
	}
	return filterSet, nil
}

func jsonTextSearchTerms(value pgtypes.JsonValue, filter map[string]bool) ([]string, error) {
	var terms []string
	var visit func(value pgtypes.JsonValue) error
	enabled := func(name string) bool {
		return filter["all"] || filter[name]
	}
	visit = func(value pgtypes.JsonValue) error {
		switch value := value.(type) {
		case pgtypes.JsonValueObject:
			for _, item := range value.Items {
				if enabled("key") {
					terms = append(terms, textSearchTerms(item.Key)...)
				}
				if err := visit(item.Value); err != nil {
					return err
				}
			}
		case pgtypes.JsonValueArray:
			for _, item := range value {
				if err := visit(item); err != nil {
					return err
				}
			}
		case pgtypes.JsonValueString:
			if enabled("string") {
				text, err := pgtypes.JsonStringUnescape(value)
				if err != nil {
					return err
				}
				terms = append(terms, textSearchTerms(text)...)
			}
		case pgtypes.JsonValueNumber:
			if enabled("numeric") {
				terms = append(terms, textSearchTerms(decimal.Decimal(value).String())...)
			}
		case pgtypes.JsonValueBoolean:
			if enabled("boolean") {
				terms = append(terms, textSearchTerms(fmt.Sprint(bool(value)))...)
			}
		case pgtypes.JsonValueNull:
		}
		return nil
	}
	if err := visit(value); err != nil {
		return nil, err
	}
	return terms, nil
}

func textSearchStopWords(config string) map[string]struct{} {
	if strings.EqualFold(config, "english") {
		return englishTextSearchStopWords
	}
	return nil
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
	entries := tsVectorEntries(vector)
	parts := make([]string, len(entries))
	for i, entry := range entries {
		parts[i] = "'" + entry.lexeme + "'"
	}
	return strings.Join(parts, " ")
}

func simpleTSDelete(vector string, lexeme string) string {
	entries := tsVectorEntries(vector)
	filtered := entries[:0]
	for _, entry := range entries {
		if entry.lexeme != lexeme {
			filtered = append(filtered, entry)
		}
	}
	return renderTSVectorEntries(filtered)
}

func simpleTSSetWeight(vector string, weight string) string {
	entries := tsVectorEntries(vector)
	for i := range entries {
		for j, position := range entries[i].positions {
			entries[i].positions[j] = strings.TrimRight(position, "ABCD") + weight
		}
	}
	return renderTSVectorEntries(entries)
}

func simpleTSNumNode(query string) int {
	if strings.TrimSpace(query) == "" {
		return 0
	}
	return len(textSearchTerms(query)) + strings.Count(query, "&") + strings.Count(query, "|") + strings.Count(query, "!") + strings.Count(query, "<->")
}

func simpleTSQueryPhrase(left string, right string) string {
	terms := append(textSearchTerms(left), textSearchTerms(right)...)
	parts := make([]string, len(terms))
	for i, term := range terms {
		parts[i] = "'" + term + "'"
	}
	return strings.Join(parts, " <-> ")
}

func simpleTSRewrite(query string, target string, substitute string) string {
	if query == target {
		return substitute
	}
	return strings.ReplaceAll(query, target, substitute)
}

func simpleTSFilter(vector string, weights string) string {
	allowed := map[string]bool{}
	for _, weight := range textSearchTerms(weights) {
		allowed[strings.ToUpper(weight)] = true
	}
	entries := tsVectorEntries(vector)
	filtered := entries[:0]
	for _, entry := range entries {
		keptPositions := entry.positions[:0]
		for _, position := range entry.positions {
			if len(position) == 0 {
				continue
			}
			weight := strings.ToUpper(position[len(position)-1:])
			if allowed[weight] {
				keptPositions = append(keptPositions, position)
			}
		}
		if len(keptPositions) > 0 {
			entry.positions = keptPositions
			filtered = append(filtered, entry)
		}
	}
	return renderTSVectorEntries(filtered)
}

func textSearchTermSet(input string) map[string]bool {
	terms := map[string]bool{}
	for _, term := range textSearchTerms(input) {
		terms[term] = true
	}
	return terms
}

func tsVectorLexemes(input string) []string {
	entries := tsVectorEntries(input)
	if len(entries) > 0 {
		lexemes := make([]string, len(entries))
		for i, entry := range entries {
			lexemes[i] = entry.lexeme
		}
		return lexemes
	}
	terms := textSearchTerms(input)
	sort.Strings(terms)
	return terms
}

func tsVectorEntries(input string) []tsVectorEntry {
	matches := tsVectorLexemePattern.FindAllStringSubmatch(input, -1)
	if len(matches) == 0 {
		matches = tsVectorBareLexemePattern.FindAllStringSubmatch(input, -1)
		if len(matches) == 0 {
			return nil
		}
	}
	entries := make([]tsVectorEntry, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		lexeme := strings.ReplaceAll(match[1], "''", "'")
		if !seen[lexeme] {
			seen[lexeme] = true
			var positions []string
			if len(match) > 2 && match[2] != "" {
				positions = strings.Split(match[2], ",")
			}
			entries = append(entries, tsVectorEntry{lexeme: lexeme, positions: positions})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lexeme < entries[j].lexeme
	})
	return entries
}

func renderTSVectorEntries(entries []tsVectorEntry) string {
	parts := make([]string, len(entries))
	for i, entry := range entries {
		part := "'" + entry.lexeme + "'"
		if len(entry.positions) > 0 {
			part += ":" + strings.Join(entry.positions, ",")
		}
		parts[i] = part
	}
	return strings.Join(parts, " ")
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

type tsVectorEntry struct {
	lexeme    string
	positions []string
}
