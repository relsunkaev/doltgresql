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
	"strconv"
	"strings"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// EncodeToken returns the deterministic storage key for token.
func EncodeToken(token Token) string {
	parts := make([]string, 0, 4+len(token.Path))
	parts = append(parts, indexmetadata.NormalizeOpClass(token.OpClass), string(token.Kind), strconv.Itoa(len(token.Path)))
	parts = append(parts, token.Path...)
	parts = append(parts, token.Value)

	var sb strings.Builder
	for _, part := range parts {
		sb.WriteString(strconv.Itoa(len(part)))
		sb.WriteByte(':')
		sb.WriteString(part)
	}
	return sb.String()
}

// DecodeToken decodes a storage key emitted by EncodeToken.
func DecodeToken(encoded string) (Token, error) {
	parts, err := decodeLengthPrefixedParts(encoded)
	if err != nil {
		return Token{}, err
	}
	if len(parts) < 4 {
		return Token{}, fmt.Errorf("malformed JSONB GIN token key: expected at least 4 fields, got %d", len(parts))
	}
	pathCount, err := strconv.Atoi(parts[2])
	if err != nil || pathCount < 0 {
		return Token{}, fmt.Errorf("malformed JSONB GIN token key: invalid path count %q", parts[2])
	}
	if len(parts) != 4+pathCount {
		return Token{}, fmt.Errorf("malformed JSONB GIN token key: expected %d fields, got %d", 4+pathCount, len(parts))
	}
	path := parts[3 : 3+pathCount]
	return Token{
		OpClass: indexmetadata.NormalizeOpClass(parts[0]),
		Kind:    TokenKind(parts[1]),
		Path:    copyPath(path),
		Value:   parts[len(parts)-1],
	}, nil
}

func decodeLengthPrefixedParts(encoded string) ([]string, error) {
	if encoded == "" {
		return nil, fmt.Errorf("malformed JSONB GIN token key: empty key")
	}
	var parts []string
	for offset := 0; offset < len(encoded); {
		lengthStart := offset
		for offset < len(encoded) && encoded[offset] >= '0' && encoded[offset] <= '9' {
			offset++
		}
		if offset == lengthStart || offset >= len(encoded) || encoded[offset] != ':' {
			return nil, fmt.Errorf("malformed JSONB GIN token key at byte %d", lengthStart)
		}
		length, err := strconv.Atoi(encoded[lengthStart:offset])
		if err != nil {
			return nil, err
		}
		offset++
		if length < 0 || offset+length > len(encoded) {
			return nil, fmt.Errorf("malformed JSONB GIN token key: field length %d exceeds input", length)
		}
		parts = append(parts, encoded[offset:offset+length])
		offset += length
	}
	return parts, nil
}

// PostingTableName returns the deterministic sidecar table name for a JSONB
// GIN index's persisted posting rows.
func PostingTableName(tableName string, indexName string) string {
	return "dg_gin_" + sanitizePostingNamePart(tableName) + "_" + sanitizePostingNamePart(indexName) + "_postings"
}

func sanitizePostingNamePart(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var sb strings.Builder
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			sb.WriteByte(ch)
		} else {
			sb.WriteByte('_')
		}
	}
	if sb.Len() == 0 {
		return "unnamed"
	}
	return sb.String()
}

// PostingStore is the access-method-neutral posting-list model used by JSONB
// GIN storage. The current implementation is in-memory; the same token and row
// identity contract is used when the postings are persisted to sidecar tables.
type PostingStore struct {
	postings map[string]map[string]struct{}
}

// NewPostingStore returns an empty posting store.
func NewPostingStore() *PostingStore {
	return &PostingStore{
		postings: make(map[string]map[string]struct{}),
	}
}

// Add associates rowID with each token. Duplicate tokens or duplicate row
// additions are ignored.
func (s *PostingStore) Add(rowID string, tokens []Token) error {
	if rowID == "" {
		return fmt.Errorf("JSONB GIN posting row identity cannot be empty")
	}
	for _, token := range normalizedTokenCopy(tokens) {
		key := EncodeToken(token)
		rows := s.postings[key]
		if rows == nil {
			rows = make(map[string]struct{})
			s.postings[key] = rows
		}
		rows[rowID] = struct{}{}
	}
	return nil
}

// Delete removes rowID from each token's posting list.
func (s *PostingStore) Delete(rowID string, tokens []Token) error {
	if rowID == "" {
		return fmt.Errorf("JSONB GIN posting row identity cannot be empty")
	}
	for _, token := range normalizedTokenCopy(tokens) {
		key := EncodeToken(token)
		rows := s.postings[key]
		if rows == nil {
			continue
		}
		delete(rows, rowID)
		if len(rows) == 0 {
			delete(s.postings, key)
		}
	}
	return nil
}

// Replace applies an update from oldTokens to newTokens for rowID.
func (s *PostingStore) Replace(rowID string, oldTokens []Token, newTokens []Token) error {
	if err := s.Delete(rowID, oldTokens); err != nil {
		return err
	}
	return s.Add(rowID, newTokens)
}

// Lookup returns the sorted row IDs that contain token.
func (s *PostingStore) Lookup(token Token) []string {
	return sortedRows(s.postings[EncodeToken(token)])
}

// Union returns the sorted row IDs that contain any token.
func (s *PostingStore) Union(tokens ...Token) []string {
	rows := make(map[string]struct{})
	for _, token := range tokens {
		for rowID := range s.postings[EncodeToken(token)] {
			rows[rowID] = struct{}{}
		}
	}
	return sortedRows(rows)
}

// Intersect returns the sorted row IDs that contain every token.
func (s *PostingStore) Intersect(tokens ...Token) []string {
	if len(tokens) == 0 {
		return nil
	}

	encoded := make([]string, len(tokens))
	for i, token := range tokens {
		encoded[i] = EncodeToken(token)
		if len(s.postings[encoded[i]]) == 0 {
			return nil
		}
	}
	sort.Slice(encoded, func(i, j int) bool {
		return len(s.postings[encoded[i]]) < len(s.postings[encoded[j]])
	})

	var rows []string
	for rowID := range s.postings[encoded[0]] {
		found := true
		for _, key := range encoded[1:] {
			if _, ok := s.postings[key][rowID]; !ok {
				found = false
				break
			}
		}
		if found {
			rows = append(rows, rowID)
		}
	}
	sort.Strings(rows)
	return rows
}

func normalizedTokenCopy(tokens []Token) []Token {
	if len(tokens) == 0 {
		return nil
	}
	copied := make([]Token, len(tokens))
	for i, token := range tokens {
		copied[i] = Token{
			OpClass: indexmetadata.NormalizeOpClass(token.OpClass),
			Kind:    token.Kind,
			Path:    copyPath(token.Path),
			Value:   token.Value,
		}
	}
	return normalizeTokens(copied)
}

func sortedRows(rows map[string]struct{}) []string {
	if len(rows) == 0 {
		return nil
	}
	sorted := make([]string, 0, len(rows))
	for rowID := range rows {
		sorted = append(sorted, rowID)
	}
	sort.Strings(sorted)
	return sorted
}
