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

package binary

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var hstoreType = pgtypes.NewUnresolvedDoltgresType("public", "hstore")

// initHstore registers operators and functions supplied by the hstore extension.
func initHstore() {
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONExtractJson, hstore_fetchval)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONTopLevel, hstore_exist)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONTopLevelAny, hstore_exists_any)
	framework.RegisterBinaryFunction(framework.Operator_BinaryJSONTopLevelAll, hstore_exists_all)
	framework.RegisterFunction(hstore_isexists)
	framework.RegisterFunction(hstore_defined)
	framework.RegisterFunction(hstore_isdefined)
}

var hstore_fetchval = framework.Function2{
	Name:       "fetchval",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable: func(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		pairs, err := parseHstore(val1.(string))
		if err != nil {
			return nil, err
		}
		value, ok := pairs[val2.(string)]
		if !ok || value == nil {
			return nil, nil
		}
		return *value, nil
	},
}

var hstore_exist = framework.Function2{
	Name:       "exist",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreExistCallable,
}

var hstore_isexists = framework.Function2{
	Name:       "isexists",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreExistCallable,
}

var hstore_defined = framework.Function2{
	Name:       "defined",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreDefinedCallable,
}

var hstore_isdefined = framework.Function2{
	Name:       "isdefined",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.Text},
	Strict:     true,
	Callable:   hstoreDefinedCallable,
}

var hstore_exists_any = framework.Function2{
	Name:       "exists_any",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable:   hstoreExistsAnyCallable,
}

var hstore_exists_all = framework.Function2{
	Name:       "exists_all",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{hstoreType, pgtypes.TextArray},
	Strict:     true,
	Callable:   hstoreExistsAllCallable,
}

func hstoreExistCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	_, ok := pairs[val2.(string)]
	return ok, nil
}

func hstoreDefinedCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	value, ok := pairs[val2.(string)]
	return ok && value != nil, nil
}

func hstoreExistsAnyCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	for _, key := range hstoreTextArrayValues(val2) {
		if key == nil {
			continue
		}
		if _, ok := pairs[*key]; ok {
			return true, nil
		}
	}
	return false, nil
}

func hstoreExistsAllCallable(_ *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
	pairs, err := parseHstore(val1.(string))
	if err != nil {
		return nil, err
	}
	for _, key := range hstoreTextArrayValues(val2) {
		if key == nil {
			continue
		}
		if _, ok := pairs[*key]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func hstoreTextArrayValues(val any) []*string {
	values := val.([]any)
	keys := make([]*string, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		key := value.(string)
		keys[i] = &key
	}
	return keys
}

func parseHstore(input string) (map[string]*string, error) {
	p := hstoreParser{input: input}
	pairs := make(map[string]*string)
	p.skipSpaces()
	if p.done() {
		return pairs, nil
	}
	for {
		key, _, ok := p.parseToken()
		if !ok || key == nil {
			return nil, invalidHstoreInput(input)
		}
		p.skipSpaces()
		if !p.consume("=>") {
			return nil, invalidHstoreInput(input)
		}
		p.skipSpaces()
		value, isNull, ok := p.parseToken()
		if !ok {
			return nil, invalidHstoreInput(input)
		}
		if isNull {
			pairs[*key] = nil
		} else {
			pairs[*key] = value
		}
		p.skipSpaces()
		if p.done() {
			return pairs, nil
		}
		if !p.consume(",") {
			return nil, invalidHstoreInput(input)
		}
		p.skipSpaces()
		if p.done() {
			return nil, invalidHstoreInput(input)
		}
	}
}

type hstoreParser struct {
	input string
	pos   int
}

func (p *hstoreParser) done() bool {
	return p.pos >= len(p.input)
}

func (p *hstoreParser) skipSpaces() {
	for !p.done() {
		r, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if !unicode.IsSpace(r) {
			return
		}
		p.pos += size
	}
}

func (p *hstoreParser) consume(token string) bool {
	if !strings.HasPrefix(p.input[p.pos:], token) {
		return false
	}
	p.pos += len(token)
	return true
}

func (p *hstoreParser) parseToken() (*string, bool, bool) {
	if p.done() {
		return nil, false, false
	}
	if p.input[p.pos] == '"' {
		token, ok := p.parseQuotedToken()
		return &token, false, ok
	}
	token, ok := p.parseBareToken()
	if !ok {
		return nil, false, false
	}
	if strings.EqualFold(token, "NULL") {
		return nil, true, true
	}
	return &token, false, true
}

func (p *hstoreParser) parseQuotedToken() (string, bool) {
	p.pos++
	var builder strings.Builder
	for !p.done() {
		ch := p.input[p.pos]
		p.pos++
		switch ch {
		case '\\':
			if p.done() {
				return "", false
			}
			builder.WriteByte(p.input[p.pos])
			p.pos++
		case '"':
			return builder.String(), true
		default:
			builder.WriteByte(ch)
		}
	}
	return "", false
}

func (p *hstoreParser) parseBareToken() (string, bool) {
	start := p.pos
	for !p.done() {
		r, size := utf8.DecodeRuneInString(p.input[p.pos:])
		if unicode.IsSpace(r) || r == ',' || r == '=' || r == '>' {
			break
		}
		p.pos += size
	}
	if p.pos == start {
		return "", false
	}
	return p.input[start:p.pos], true
}

func invalidHstoreInput(input string) error {
	return pgtypes.ErrInvalidSyntaxForType.New("hstore", input)
}
