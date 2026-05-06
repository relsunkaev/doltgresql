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
	"bytes"
	"fmt"
	"sort"
)

const defaultChunkedPostingStoreRowsPerChunk = 256

// ChunkedPostingStore is the in-memory model for JSONB GIN v2 posting-list
// chunks. It mirrors PostingStore's candidate semantics while materializing
// sorted row-reference chunks on demand.
type ChunkedPostingStore struct {
	rowsPerChunk int
	postings     map[string]map[string][]byte
	chunks       map[string][]PostingChunk
	dirty        map[string]struct{}
}

// NewChunkedPostingStore returns an empty chunked posting store. rowsPerChunk
// controls the test/model chunk boundary; non-positive values use the default.
func NewChunkedPostingStore(rowsPerChunk int) *ChunkedPostingStore {
	if rowsPerChunk <= 0 {
		rowsPerChunk = defaultChunkedPostingStoreRowsPerChunk
	}
	return &ChunkedPostingStore{
		rowsPerChunk: rowsPerChunk,
		postings:     make(map[string]map[string][]byte),
		chunks:       make(map[string][]PostingChunk),
		dirty:        make(map[string]struct{}),
	}
}

// Add associates rowRef with each token. Duplicate tokens or row references are
// ignored.
func (s *ChunkedPostingStore) Add(rowRef []byte, tokens []Token) error {
	if len(rowRef) == 0 {
		return fmt.Errorf("JSONB GIN posting row reference cannot be empty")
	}
	rowKey := string(rowRef)
	for _, token := range normalizedTokenCopy(tokens) {
		tokenKey := EncodeToken(token)
		rows := s.postings[tokenKey]
		if rows == nil {
			rows = make(map[string][]byte)
			s.postings[tokenKey] = rows
		}
		if _, ok := rows[rowKey]; !ok {
			rows[rowKey] = append([]byte(nil), rowRef...)
			s.dirty[tokenKey] = struct{}{}
		}
	}
	return nil
}

// Delete removes rowRef from each token's posting list.
func (s *ChunkedPostingStore) Delete(rowRef []byte, tokens []Token) error {
	if len(rowRef) == 0 {
		return fmt.Errorf("JSONB GIN posting row reference cannot be empty")
	}
	rowKey := string(rowRef)
	for _, token := range normalizedTokenCopy(tokens) {
		tokenKey := EncodeToken(token)
		rows := s.postings[tokenKey]
		if rows == nil {
			continue
		}
		if _, ok := rows[rowKey]; ok {
			delete(rows, rowKey)
			s.dirty[tokenKey] = struct{}{}
		}
		if len(rows) == 0 {
			delete(s.postings, tokenKey)
			delete(s.chunks, tokenKey)
			delete(s.dirty, tokenKey)
		}
	}
	return nil
}

// Replace applies an update from oldTokens to newTokens for rowRef.
func (s *ChunkedPostingStore) Replace(rowRef []byte, oldTokens []Token, newTokens []Token) error {
	if err := s.Delete(rowRef, oldTokens); err != nil {
		return err
	}
	return s.Add(rowRef, newTokens)
}

// Lookup returns sorted row references that contain token.
func (s *ChunkedPostingStore) Lookup(token Token) [][]byte {
	chunks, err := s.Chunks(token)
	if err != nil {
		return nil
	}
	return decodePostingChunkRowRefs(chunks)
}

// Union returns sorted row references that contain any token.
func (s *ChunkedPostingStore) Union(tokens ...Token) [][]byte {
	rows := make(map[string][]byte)
	for _, token := range tokens {
		for _, rowRef := range s.Lookup(token) {
			rows[string(rowRef)] = rowRef
		}
	}
	return sortedChunkedRowRefs(rows)
}

// Intersect returns sorted row references that contain every token.
func (s *ChunkedPostingStore) Intersect(tokens ...Token) [][]byte {
	if len(tokens) == 0 {
		return nil
	}

	lists := make([][][]byte, len(tokens))
	for i, token := range tokens {
		lists[i] = s.Lookup(token)
		if len(lists[i]) == 0 {
			return nil
		}
	}
	sort.Slice(lists, func(i, j int) bool {
		return len(lists[i]) < len(lists[j])
	})

	rows := make(map[string][]byte, len(lists[0]))
	for _, rowRef := range lists[0] {
		rows[string(rowRef)] = rowRef
	}
	for _, list := range lists[1:] {
		nextRows := make(map[string]struct{}, len(list))
		for _, rowRef := range list {
			nextRows[string(rowRef)] = struct{}{}
		}
		for rowKey := range rows {
			if _, ok := nextRows[rowKey]; !ok {
				delete(rows, rowKey)
			}
		}
		if len(rows) == 0 {
			return nil
		}
	}
	return sortedChunkedRowRefs(rows)
}

// Chunks returns the sorted posting chunks for token.
func (s *ChunkedPostingStore) Chunks(token Token) ([]PostingChunk, error) {
	tokenKey := EncodeToken(Token{
		OpClass: token.OpClass,
		Kind:    token.Kind,
		Path:    copyPath(token.Path),
		Value:   token.Value,
	})
	if err := s.ensureChunks(tokenKey); err != nil {
		return nil, err
	}
	return clonePostingChunks(s.chunks[tokenKey]), nil
}

func (s *ChunkedPostingStore) ensureChunks(tokenKey string) error {
	if _, ok := s.dirty[tokenKey]; !ok {
		return nil
	}
	delete(s.dirty, tokenKey)

	rowRefs := sortedChunkedRowRefs(s.postings[tokenKey])
	if len(rowRefs) == 0 {
		delete(s.chunks, tokenKey)
		return nil
	}

	chunks := make([]PostingChunk, 0, (len(rowRefs)+s.rowsPerChunk-1)/s.rowsPerChunk)
	for start := 0; start < len(rowRefs); start += s.rowsPerChunk {
		end := start + s.rowsPerChunk
		if end > len(rowRefs) {
			end = len(rowRefs)
		}
		chunk, err := EncodePostingChunk(rowRefs[start:end])
		if err != nil {
			return err
		}
		chunks = append(chunks, chunk)
	}
	s.chunks[tokenKey] = chunks
	return nil
}

func decodePostingChunkRowRefs(chunks []PostingChunk) [][]byte {
	var rows [][]byte
	for _, chunk := range chunks {
		decoded, err := DecodePostingChunk(chunk.Payload)
		if err != nil {
			return nil
		}
		rows = append(rows, cloneChunkedRowRefs(decoded.RowRefs)...)
	}
	return rows
}

func sortedChunkedRowRefs(rows map[string][]byte) [][]byte {
	if len(rows) == 0 {
		return nil
	}
	sorted := make([][]byte, 0, len(rows))
	for _, rowRef := range rows {
		sorted = append(sorted, append([]byte(nil), rowRef...))
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})
	return sorted
}

func cloneChunkedRowRefs(rowRefs [][]byte) [][]byte {
	if len(rowRefs) == 0 {
		return nil
	}
	copied := make([][]byte, len(rowRefs))
	for i, rowRef := range rowRefs {
		copied[i] = append([]byte(nil), rowRef...)
	}
	return copied
}

func clonePostingChunks(chunks []PostingChunk) []PostingChunk {
	if len(chunks) == 0 {
		return nil
	}
	copied := make([]PostingChunk, len(chunks))
	for i, chunk := range chunks {
		copied[i] = PostingChunk{
			FormatVersion: chunk.FormatVersion,
			RowCount:      chunk.RowCount,
			FirstRowRef:   append([]byte(nil), chunk.FirstRowRef...),
			LastRowRef:    append([]byte(nil), chunk.LastRowRef...),
			RowRefs:       cloneChunkedRowRefs(chunk.RowRefs),
			Payload:       append([]byte(nil), chunk.Payload...),
			Checksum:      chunk.Checksum,
		}
	}
	return copied
}
