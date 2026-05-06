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
	"sort"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

func TestPostingChunkRoundTrip(t *testing.T) {
	rowRefs := [][]byte{
		[]byte("\x00int4\x00\x00\x00\x01"),
		[]byte("\x00int4\x00\x00\x00\x02"),
		[]byte("tenant:01\x00id:00000005"),
		[]byte("tenant:01\x00id:00000009"),
	}

	chunk, err := EncodePostingChunk(rowRefs)
	require.NoError(t, err)
	require.Equal(t, uint16(PostingChunkFormatVersionV1), chunk.FormatVersion)
	require.Equal(t, uint32(len(rowRefs)), chunk.RowCount)
	require.Equal(t, rowRefs[0], chunk.FirstRowRef)
	require.Equal(t, rowRefs[len(rowRefs)-1], chunk.LastRowRef)
	require.NotEmpty(t, chunk.Payload)
	require.NotZero(t, chunk.Checksum)

	decoded, err := DecodePostingChunk(chunk.Payload)
	require.NoError(t, err)
	require.Equal(t, chunk.FormatVersion, decoded.FormatVersion)
	require.Equal(t, chunk.RowCount, decoded.RowCount)
	require.Equal(t, chunk.FirstRowRef, decoded.FirstRowRef)
	require.Equal(t, chunk.LastRowRef, decoded.LastRowRef)
	require.Equal(t, chunk.Checksum, decoded.Checksum)
	require.Equal(t, rowRefs, decoded.RowRefs)
}

func TestPostingChunkDoesNotRetainCallerBuffers(t *testing.T) {
	rowRefs := [][]byte{[]byte("row/1"), []byte("row/2")}

	chunk, err := EncodePostingChunk(rowRefs)
	require.NoError(t, err)

	rowRefs[0][0] = 'x'
	require.Equal(t, []byte("row/1"), chunk.RowRefs[0])
	require.Equal(t, []byte("row/1"), chunk.FirstRowRef)

	decoded, err := DecodePostingChunk(chunk.Payload)
	require.NoError(t, err)

	chunk.Payload[len(chunk.Payload)-1] ^= 0xff
	require.Equal(t, []byte("row/1"), decoded.RowRefs[0])
}

func TestPostingChunkEmptyRoundTrip(t *testing.T) {
	chunk, err := EncodePostingChunk(nil)
	require.NoError(t, err)
	require.Equal(t, uint16(PostingChunkFormatVersionV1), chunk.FormatVersion)
	require.Zero(t, chunk.RowCount)
	require.Empty(t, chunk.FirstRowRef)
	require.Empty(t, chunk.LastRowRef)
	require.Empty(t, chunk.RowRefs)
	require.NotEmpty(t, chunk.Payload)

	decoded, err := DecodePostingChunk(chunk.Payload)
	require.NoError(t, err)
	require.Zero(t, decoded.RowCount)
	require.Empty(t, decoded.FirstRowRef)
	require.Empty(t, decoded.LastRowRef)
	require.Empty(t, decoded.RowRefs)
}

func TestPostingChunkRejectsUnsortedDuplicateOrEmptyReferences(t *testing.T) {
	tests := []struct {
		name    string
		rowRefs [][]byte
		wantErr string
	}{
		{
			name:    "duplicate",
			rowRefs: [][]byte{[]byte("row/1"), []byte("row/1")},
			wantErr: "duplicate",
		},
		{
			name:    "unsorted",
			rowRefs: [][]byte{[]byte("row/2"), []byte("row/1")},
			wantErr: "sorted",
		},
		{
			name:    "empty row reference",
			rowRefs: [][]byte{[]byte("row/1"), nil},
			wantErr: "empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := EncodePostingChunk(test.rowRefs)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestPostingChunkRejectsMalformedPayloads(t *testing.T) {
	chunk, err := EncodePostingChunk([][]byte{[]byte("row/1"), []byte("row/2")})
	require.NoError(t, err)

	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{name: "empty", payload: nil, wantErr: "too short"},
		{name: "bad magic", payload: append([]byte("BAD!"), chunk.Payload[4:]...), wantErr: "magic"},
		{name: "truncated header", payload: chunk.Payload[:postingChunkV1HeaderSize-1], wantErr: "too short"},
		{name: "truncated row length", payload: chunk.Payload[:postingChunkV1HeaderSize+2], wantErr: "row length"},
		{name: "truncated row bytes", payload: truncatedPostingChunkRowRefPayload(chunk.Payload), wantErr: "row reference"},
		{name: "trailing bytes", payload: append(append([]byte(nil), chunk.Payload...), 0x00), wantErr: "trailing"},
		{name: "checksum mismatch", payload: corruptPostingChunkPayloadByte(chunk.Payload), wantErr: "checksum"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodePostingChunk(test.payload)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestPostingChunkRejectsUnsupportedVersion(t *testing.T) {
	chunk, err := EncodePostingChunk([][]byte{[]byte("row/1")})
	require.NoError(t, err)

	payload := append([]byte(nil), chunk.Payload...)
	payload[4] = 0
	payload[5] = 99

	_, err = DecodePostingChunk(payload)
	require.ErrorContains(t, err, "unsupported")
}

func TestPostingChunkQuickRoundTrip(t *testing.T) {
	property := func(inputs [][]byte) bool {
		rowRefs := sortedUniqueNonEmptyRowRefs(inputs)
		chunk, err := EncodePostingChunk(rowRefs)
		if err != nil {
			t.Logf("EncodePostingChunk(%q) returned error: %v", rowRefs, err)
			return false
		}
		decoded, err := DecodePostingChunk(chunk.Payload)
		if err != nil {
			t.Logf("DecodePostingChunk returned error: %v", err)
			return false
		}
		if decoded.FormatVersion != PostingChunkFormatVersionV1 || decoded.RowCount != uint32(len(rowRefs)) {
			t.Logf("unexpected metadata: version=%d rowCount=%d", decoded.FormatVersion, decoded.RowCount)
			return false
		}
		return equalRowRefs(rowRefs, decoded.RowRefs)
	}

	require.NoError(t, quick.Check(property, &quick.Config{MaxCount: 100}))
}

func sortedUniqueNonEmptyRowRefs(inputs [][]byte) [][]byte {
	rowRefs := make([][]byte, 0, len(inputs))
	for _, input := range inputs {
		if len(input) == 0 {
			continue
		}
		rowRefs = append(rowRefs, append([]byte(nil), input...))
	}
	sort.Slice(rowRefs, func(i, j int) bool {
		return bytes.Compare(rowRefs[i], rowRefs[j]) < 0
	})

	writeIdx := 0
	for _, rowRef := range rowRefs {
		if writeIdx == 0 || !bytes.Equal(rowRefs[writeIdx-1], rowRef) {
			rowRefs[writeIdx] = rowRef
			writeIdx++
		}
	}
	return rowRefs[:writeIdx]
}

func equalRowRefs(left [][]byte, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !bytes.Equal(left[i], right[i]) {
			return false
		}
	}
	return true
}

func corruptPostingChunkPayloadByte(payload []byte) []byte {
	corrupted := append([]byte(nil), payload...)
	bodyByte := len(corrupted) - postingChunkV1ChecksumSize - 1
	if bodyByte >= postingChunkV1HeaderSize+postingChunkV1LengthSize {
		corrupted[bodyByte] ^= 0x01
	}
	return corrupted
}

func truncatedPostingChunkRowRefPayload(payload []byte) []byte {
	bodyPrefixEnd := postingChunkV1HeaderSize + postingChunkV1LengthSize + 1
	truncated := append([]byte(nil), payload[:bodyPrefixEnd]...)
	return append(truncated, payload[len(payload)-postingChunkV1ChecksumSize:]...)
}
