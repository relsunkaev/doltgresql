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

package jsonbgin

import (
	"fmt"
	"sort"

	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// TokenKind is the normalized kind of a JSONB GIN lookup token.
type TokenKind string

const (
	// TokenKindKey is used by jsonb_ops for object keys and string array
	// elements. PostgreSQL treats string array elements as key-like entries so
	// top-level existence operators can use the same token family.
	TokenKindKey TokenKind = "key"

	TokenKindString      TokenKind = "string"
	TokenKindNumber      TokenKind = "number"
	TokenKindBoolean     TokenKind = "boolean"
	TokenKindNull        TokenKind = "null"
	TokenKindEmptyObject TokenKind = "empty-object"
	TokenKindEmptyArray  TokenKind = "empty-array"

	// TokenKindPathValue is used by jsonb_path_ops for a scalar value plus the
	// object-key path leading to it.
	TokenKindPathValue TokenKind = "path-value"
)

// Token is the deterministic JSONB GIN key emitted for one indexed JSONB
// document. Path is only populated for path/value opclass tokens.
type Token struct {
	OpClass string
	Kind    TokenKind
	Path    []string
	Value   string
}

// Extract returns the normalized GIN tokens for doc using opClass.
func Extract(doc pgtypes.JsonDocument, opClass string) ([]Token, error) {
	return ExtractValue(doc.Value, opClass)
}

// ExtractValue returns the normalized GIN tokens for value using opClass.
func ExtractValue(value pgtypes.JsonValue, opClass string) ([]Token, error) {
	extractor := extractor{
		opClass: indexmetadata.NormalizeOpClass(opClass),
	}

	switch extractor.opClass {
	case indexmetadata.OpClassJsonbOps:
		if err := extractor.extractJsonbOps(value, false); err != nil {
			return nil, err
		}
	case indexmetadata.OpClassJsonbPathOps:
		if err := extractor.extractJsonbPathOps(value); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported JSONB GIN opclass %q", opClass)
	}

	return normalizeTokens(extractor.tokens), nil
}

type extractor struct {
	opClass string
	path    []string
	tokens  []Token
}

func (e *extractor) extractJsonbOps(value pgtypes.JsonValue, arrayElement bool) error {
	switch value := value.(type) {
	case pgtypes.JsonValueObject:
		if len(value.Items) == 0 {
			e.addIndependent(TokenKindEmptyObject, "")
			return nil
		}
		for _, item := range value.Items {
			e.addIndependent(TokenKindKey, item.Key)
			if err := e.extractJsonbOps(item.Value, false); err != nil {
				return err
			}
		}
	case pgtypes.JsonValueArray:
		if len(value) == 0 {
			e.addIndependent(TokenKindEmptyArray, "")
			return nil
		}
		for _, item := range value {
			if err := e.extractJsonbOps(item, true); err != nil {
				return err
			}
		}
	case pgtypes.JsonValueString:
		decoded, err := pgtypes.JsonStringUnescape(value)
		if err != nil {
			return err
		}
		if arrayElement {
			e.addIndependent(TokenKindKey, decoded)
		} else {
			e.addIndependent(TokenKindString, decoded)
		}
	case pgtypes.JsonValueNumber:
		e.addIndependent(TokenKindNumber, decimal.Decimal(value).String())
	case pgtypes.JsonValueBoolean:
		if value {
			e.addIndependent(TokenKindBoolean, "true")
		} else {
			e.addIndependent(TokenKindBoolean, "false")
		}
	case pgtypes.JsonValueNull:
		e.addIndependent(TokenKindNull, "null")
	default:
		return fmt.Errorf("unexpected JSONB value type %T", value)
	}
	return nil
}

func (e *extractor) extractJsonbPathOps(value pgtypes.JsonValue) error {
	switch value := value.(type) {
	case pgtypes.JsonValueObject:
		for _, item := range value.Items {
			e.pushPath(item.Key)
			if err := e.extractJsonbPathOps(item.Value); err != nil {
				return err
			}
			e.popPath()
		}
	case pgtypes.JsonValueArray:
		for _, item := range value {
			if err := e.extractJsonbPathOps(item); err != nil {
				return err
			}
		}
	case pgtypes.JsonValueString:
		decoded, err := pgtypes.JsonStringUnescape(value)
		if err != nil {
			return err
		}
		e.addPathValue("string:" + decoded)
	case pgtypes.JsonValueNumber:
		e.addPathValue("number:" + decimal.Decimal(value).String())
	case pgtypes.JsonValueBoolean:
		if value {
			e.addPathValue("boolean:true")
		} else {
			e.addPathValue("boolean:false")
		}
	case pgtypes.JsonValueNull:
		e.addPathValue("null:null")
	default:
		return fmt.Errorf("unexpected JSONB value type %T", value)
	}
	return nil
}

func (e *extractor) addIndependent(kind TokenKind, value string) {
	e.tokens = append(e.tokens, Token{
		OpClass: e.opClass,
		Kind:    kind,
		Value:   value,
	})
}

func (e *extractor) addPathValue(value string) {
	e.tokens = append(e.tokens, Token{
		OpClass: e.opClass,
		Kind:    TokenKindPathValue,
		Path:    copyPath(e.path),
		Value:   value,
	})
}

func (e *extractor) pushPath(key string) {
	e.path = append(e.path, key)
}

func (e *extractor) popPath() {
	e.path = e.path[:len(e.path)-1]
}

func copyPath(path []string) []string {
	if len(path) == 0 {
		return nil
	}
	copied := make([]string, len(path))
	copy(copied, path)
	return copied
}

func normalizeTokens(tokens []Token) []Token {
	sort.Slice(tokens, func(i, j int) bool {
		return compareTokens(tokens[i], tokens[j]) < 0
	})

	writeIdx := 0
	for _, token := range tokens {
		if writeIdx == 0 || compareTokens(tokens[writeIdx-1], token) != 0 {
			tokens[writeIdx] = token
			writeIdx++
		}
	}
	return tokens[:writeIdx]
}

func compareTokens(left Token, right Token) int {
	if left.OpClass < right.OpClass {
		return -1
	} else if left.OpClass > right.OpClass {
		return 1
	}
	if left.Kind < right.Kind {
		return -1
	} else if left.Kind > right.Kind {
		return 1
	}
	if cmp := compareStringSlices(left.Path, right.Path); cmp != 0 {
		return cmp
	}
	if left.Value < right.Value {
		return -1
	} else if left.Value > right.Value {
		return 1
	}
	return 0
}

func compareStringSlices(left []string, right []string) int {
	for i := 0; i < len(left) && i < len(right); i++ {
		if left[i] < right[i] {
			return -1
		} else if left[i] > right[i] {
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	} else if len(left) > len(right) {
		return 1
	}
	return 0
}
