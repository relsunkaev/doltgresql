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

package types

import (
	"regexp"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// TsQuery is PostgreSQL's text-search query type.
var TsQuery = &DoltgresType{
	ID:                  toInternal("tsquery"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_tsquery"),
	InputFunc:           toFuncID("tsqueryin", toInternal("cstring")),
	OutputFunc:          toFuncID("tsqueryout", toInternal("tsquery")),
	ReceiveFunc:         toFuncID("tsqueryrecv", toInternal("internal")),
	SendFunc:            toFuncID("tsquerysend", toInternal("tsquery")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Extended,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeTextSearch,
	DeserializationFunc: deserializeTypeTextSearch,
}

// TsVector is PostgreSQL's text-search document vector type.
var TsVector = &DoltgresType{
	ID:                  toInternal("tsvector"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_tsvector"),
	InputFunc:           toFuncID("tsvectorin", toInternal("cstring")),
	OutputFunc:          toFuncID("tsvectorout", toInternal("tsvector")),
	ReceiveFunc:         toFuncID("tsvectorrecv", toInternal("internal")),
	SendFunc:            toFuncID("tsvectorsend", toInternal("tsvector")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Extended,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeTextSearch,
	DeserializationFunc: deserializeTypeTextSearch,
}

var tsLexemePattern = regexp.MustCompile(`[[:alnum:]_]+`)
var tsVectorQuotedLexemePattern = regexp.MustCompile(`'((?:''|[^'])*)'(?:\:([0-9A-D,]+))?`)
var tsVectorBareLexemePattern = regexp.MustCompile(`([[:alnum:]_]+)(?:\:([0-9A-D,]+))?`)

// CanonicalTSQuery converts a small tsquery subset to PostgreSQL-style text.
func CanonicalTSQuery(input string) string {
	var tokens []string
	for i := 0; i < len(input); {
		switch ch := input[i]; {
		case ch == '\'':
			end := i + 1
			for end < len(input) {
				if input[end] == '\'' {
					if end+1 < len(input) && input[end+1] == '\'' {
						end += 2
						continue
					}
					end++
					break
				}
				end++
			}
			tokens = append(tokens, input[i:end])
			i = end
		case isTSLexemeByte(ch):
			start := i
			for i < len(input) && isTSLexemeByte(input[i]) {
				i++
			}
			tokens = append(tokens, quoteTSLexeme(input[start:i]))
		case ch == '<' && strings.HasPrefix(input[i:], "<->"):
			tokens = append(tokens, "<->")
			i += 3
		case ch == '&' || ch == '|' || ch == '!' || ch == '(' || ch == ')':
			tokens = append(tokens, string(ch))
			i++
		default:
			i++
		}
	}
	return renderTSQueryTokens(tokens)
}

// CanonicalTSVector converts a small tsvector subset to PostgreSQL-style text.
func CanonicalTSVector(input string) string {
	entries := parseTSVectorEntries(input)
	if len(entries) > 0 {
		return renderTSVectorEntries(entries)
	}
	terms := tsLexemePattern.FindAllString(strings.ToLower(input), -1)
	entries = make([]tsVectorEntry, 0, len(terms))
	seen := map[string]bool{}
	for _, term := range terms {
		if term == "" || seen[term] {
			continue
		}
		seen[term] = true
		entries = append(entries, tsVectorEntry{lexeme: term})
	}
	sortTSVectorEntries(entries)
	return renderTSVectorEntries(entries)
}

func serializeTypeTextSearch(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	str := val.(string)
	str = canonicalTextSearchValue(t, str)
	writer := utils.NewWriter(uint64(len(str) + 4))
	writer.String(str)
	return writer.Data(), nil
}

func deserializeTypeTextSearch(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader := utils.NewReader(data)
	return reader.String(), nil
}

func CanonicalTextSearchValue(t *DoltgresType, val string) string {
	return canonicalTextSearchValue(t, val)
}

func canonicalTextSearchValue(t *DoltgresType, val string) string {
	switch t.ID.TypeName() {
	case "tsquery":
		return CanonicalTSQuery(val)
	case "tsvector":
		return CanonicalTSVector(val)
	default:
		return val
	}
}

func renderTSQueryTokens(tokens []string) string {
	var builder strings.Builder
	for i, token := range tokens {
		if i > 0 && token != ")" && tokens[i-1] != "(" && tokens[i-1] != "!" {
			builder.WriteByte(' ')
		}
		builder.WriteString(token)
	}
	return builder.String()
}

func parseTSVectorEntries(input string) []tsVectorEntry {
	matches := tsVectorQuotedLexemePattern.FindAllStringSubmatch(input, -1)
	if len(matches) == 0 {
		matches = tsVectorBareLexemePattern.FindAllStringSubmatch(input, -1)
	}
	entries := make([]tsVectorEntry, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		lexeme := strings.ToLower(strings.ReplaceAll(match[1], "''", "'"))
		if lexeme == "" || seen[lexeme] {
			continue
		}
		seen[lexeme] = true
		var positions []string
		if len(match) > 2 && match[2] != "" {
			positions = strings.Split(match[2], ",")
		}
		entries = append(entries, tsVectorEntry{lexeme: lexeme, positions: positions})
	}
	sortTSVectorEntries(entries)
	return entries
}

func sortTSVectorEntries(entries []tsVectorEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lexeme < entries[j].lexeme
	})
}

func renderTSVectorEntries(entries []tsVectorEntry) string {
	parts := make([]string, len(entries))
	for i, entry := range entries {
		part := quoteTSLexeme(entry.lexeme)
		if len(entry.positions) > 0 {
			part += ":" + strings.Join(entry.positions, ",")
		}
		parts[i] = part
	}
	return strings.Join(parts, " ")
}

func quoteTSLexeme(lexeme string) string {
	return "'" + strings.ReplaceAll(strings.ToLower(lexeme), "'", "''") + "'"
}

func isTSLexemeByte(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

type tsVectorEntry struct {
	lexeme    string
	positions []string
}
