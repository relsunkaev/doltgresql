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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
)

// PostingChunkFormatVersionV1 is the initial JSONB GIN posting-list payload
// format. It stores sorted, unique row references as length-prefixed byte
// strings with a CRC32 checksum over the header and row-reference body.
const PostingChunkFormatVersionV1 uint16 = 1

const (
	postingChunkV1HeaderSize   = 10
	postingChunkV1ChecksumSize = 4
	postingChunkV1LengthSize   = 4
)

var (
	postingChunkMagic      = [4]byte{'D', 'G', 'G', 'C'}
	postingChunkCRCTable   = crc32.MakeTable(crc32.Castagnoli)
	maxPostingChunkPayload = int(^uint(0) >> 1)
)

// PostingChunk is the decoded and encoded representation of one posting-list
// chunk for a single JSONB GIN token.
type PostingChunk struct {
	FormatVersion uint16
	RowCount      uint32
	FirstRowRef   []byte
	LastRowRef    []byte
	RowRefs       [][]byte
	Payload       []byte
	Checksum      uint32
}

// PostingChunkMetadata is the fixed-width metadata stored in a posting chunk
// payload header and checksum trailer.
type PostingChunkMetadata struct {
	FormatVersion uint16
	RowCount      uint32
	Checksum      uint32
}

// EncodePostingChunk validates and encodes sorted, unique row references into a
// versioned JSONB GIN posting-list chunk payload.
func EncodePostingChunk(rowRefs [][]byte) (PostingChunk, error) {
	return encodePostingChunk(rowRefs, true)
}

// EncodePostingChunkForStorage validates and encodes sorted, unique row
// references for persisted posting rows. It omits the decoded RowRefs copy
// because storage callers only need metadata and the encoded payload.
func EncodePostingChunkForStorage(rowRefs [][]byte) (PostingChunk, error) {
	return encodePostingChunk(rowRefs, false)
}

func encodePostingChunk(rowRefs [][]byte, includeRowRefs bool) (PostingChunk, error) {
	if len(rowRefs) > math.MaxUint32 {
		return PostingChunk{}, fmt.Errorf("JSONB GIN posting chunk row count %d exceeds maximum %d", len(rowRefs), uint64(math.MaxUint32))
	}

	payloadSize := postingChunkV1HeaderSize + postingChunkV1ChecksumSize
	var copied [][]byte
	if includeRowRefs {
		copied = make([][]byte, len(rowRefs))
	}
	for i, rowRef := range rowRefs {
		if len(rowRef) == 0 {
			return PostingChunk{}, fmt.Errorf("JSONB GIN posting chunk row reference at offset %d is empty", i)
		}
		if len(rowRef) > math.MaxUint32 {
			return PostingChunk{}, fmt.Errorf("JSONB GIN posting chunk row reference at offset %d is too large: %d bytes", i, len(rowRef))
		}
		if i > 0 {
			cmp := bytes.Compare(rowRefs[i-1], rowRef)
			if cmp == 0 {
				return PostingChunk{}, fmt.Errorf("JSONB GIN posting chunk row reference at offset %d is duplicate", i)
			}
			if cmp > 0 {
				return PostingChunk{}, fmt.Errorf("JSONB GIN posting chunk row references must be sorted ascending")
			}
		}
		if payloadSize > maxPostingChunkPayload-postingChunkV1LengthSize-len(rowRef) {
			return PostingChunk{}, fmt.Errorf("JSONB GIN posting chunk payload is too large")
		}
		payloadSize += postingChunkV1LengthSize + len(rowRef)
		if includeRowRefs {
			copied[i] = append([]byte(nil), rowRef...)
		}
	}

	payload := make([]byte, postingChunkV1HeaderSize, payloadSize)
	copy(payload[:4], postingChunkMagic[:])
	binary.BigEndian.PutUint16(payload[4:6], PostingChunkFormatVersionV1)
	binary.BigEndian.PutUint32(payload[6:postingChunkV1HeaderSize], uint32(len(rowRefs)))
	for _, rowRef := range rowRefs {
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(rowRef)))
		payload = append(payload, rowRef...)
	}

	checksum := crc32.Checksum(payload, postingChunkCRCTable)
	payload = binary.BigEndian.AppendUint32(payload, checksum)

	chunk := PostingChunk{
		FormatVersion: PostingChunkFormatVersionV1,
		RowCount:      uint32(len(rowRefs)),
		RowRefs:       copied,
		Payload:       payload,
		Checksum:      checksum,
	}
	if len(rowRefs) > 0 {
		chunk.FirstRowRef = append([]byte(nil), rowRefs[0]...)
		chunk.LastRowRef = append([]byte(nil), rowRefs[len(rowRefs)-1]...)
	}
	return chunk, nil
}

// InspectPostingChunkMetadata returns fixed-width posting chunk metadata without
// decoding row references.
func InspectPostingChunkMetadata(payload []byte) (PostingChunkMetadata, error) {
	if len(payload) < postingChunkV1HeaderSize {
		return PostingChunkMetadata{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: too short")
	}
	if !bytes.Equal(payload[:4], postingChunkMagic[:]) {
		return PostingChunkMetadata{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: invalid magic")
	}
	version := binary.BigEndian.Uint16(payload[4:6])
	switch version {
	case PostingChunkFormatVersionV1:
		if len(payload) < postingChunkV1HeaderSize+postingChunkV1ChecksumSize {
			return PostingChunkMetadata{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: missing checksum")
		}
		return PostingChunkMetadata{
			FormatVersion: version,
			RowCount:      binary.BigEndian.Uint32(payload[6:postingChunkV1HeaderSize]),
			Checksum:      binary.BigEndian.Uint32(payload[len(payload)-postingChunkV1ChecksumSize:]),
		}, nil
	default:
		return PostingChunkMetadata{}, fmt.Errorf("unsupported JSONB GIN posting chunk payload version %d", version)
	}
}

// DecodePostingChunk validates and decodes a versioned JSONB GIN posting-list
// chunk payload.
func DecodePostingChunk(payload []byte) (PostingChunk, error) {
	if len(payload) < postingChunkV1HeaderSize {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: too short")
	}
	if !bytes.Equal(payload[:4], postingChunkMagic[:]) {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: invalid magic")
	}

	version := binary.BigEndian.Uint16(payload[4:6])
	switch version {
	case PostingChunkFormatVersionV1:
		return decodePostingChunkV1(payload, true)
	default:
		return PostingChunk{}, fmt.Errorf("unsupported JSONB GIN posting chunk payload version %d", version)
	}
}

// DecodePostingChunkRowReferences validates payload and returns row references
// backed by payload. Callers must not retain or mutate returned RowRefs after
// payload may change.
func DecodePostingChunkRowReferences(payload []byte) (PostingChunk, error) {
	if len(payload) < postingChunkV1HeaderSize {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: too short")
	}
	if !bytes.Equal(payload[:4], postingChunkMagic[:]) {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: invalid magic")
	}

	version := binary.BigEndian.Uint16(payload[4:6])
	switch version {
	case PostingChunkFormatVersionV1:
		return decodePostingChunkV1(payload, false)
	default:
		return PostingChunk{}, fmt.Errorf("unsupported JSONB GIN posting chunk payload version %d", version)
	}
}

func decodePostingChunkV1(payload []byte, copyBuffers bool) (PostingChunk, error) {
	rowCount := binary.BigEndian.Uint32(payload[6:postingChunkV1HeaderSize])
	bodyEnd := len(payload)
	if len(payload) >= postingChunkV1HeaderSize+postingChunkV1ChecksumSize {
		bodyEnd -= postingChunkV1ChecksumSize
	}

	offset := postingChunkV1HeaderSize
	rowRefs := make([][]byte, 0, minInt(int(rowCount), 1024))
	for i := uint32(0); i < rowCount; i++ {
		if offset+postingChunkV1LengthSize > bodyEnd {
			return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: truncated row length at offset %d", i)
		}
		length := binary.BigEndian.Uint32(payload[offset : offset+postingChunkV1LengthSize])
		offset += postingChunkV1LengthSize
		if length == 0 {
			return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: empty row reference at offset %d", i)
		}
		if uint64(length) > uint64(bodyEnd-offset) {
			return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: truncated row reference at offset %d", i)
		}
		rowRef := payload[offset : offset+int(length)]
		offset += int(length)
		if copyBuffers {
			rowRef = append([]byte(nil), rowRef...)
		}
		if len(rowRefs) > 0 {
			cmp := bytes.Compare(rowRefs[len(rowRefs)-1], rowRef)
			if cmp == 0 {
				return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: duplicate row reference at offset %d", i)
			}
			if cmp > 0 {
				return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: row references are not sorted")
			}
		}
		rowRefs = append(rowRefs, rowRef)
	}

	if offset < bodyEnd {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: trailing bytes after row references")
	}
	if offset+postingChunkV1ChecksumSize > len(payload) {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: missing checksum")
	}
	storedChecksum := binary.BigEndian.Uint32(payload[offset : offset+postingChunkV1ChecksumSize])
	actualChecksum := crc32.Checksum(payload[:offset], postingChunkCRCTable)
	if storedChecksum != actualChecksum {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: checksum mismatch")
	}
	if offset+postingChunkV1ChecksumSize != len(payload) {
		return PostingChunk{}, fmt.Errorf("malformed JSONB GIN posting chunk payload: trailing bytes after checksum")
	}

	chunkPayload := payload
	if copyBuffers {
		chunkPayload = append([]byte(nil), payload...)
	}
	chunk := PostingChunk{
		FormatVersion: PostingChunkFormatVersionV1,
		RowCount:      rowCount,
		RowRefs:       rowRefs,
		Payload:       chunkPayload,
		Checksum:      storedChecksum,
	}
	if len(rowRefs) > 0 {
		if copyBuffers {
			chunk.FirstRowRef = append([]byte(nil), rowRefs[0]...)
			chunk.LastRowRef = append([]byte(nil), rowRefs[len(rowRefs)-1]...)
		} else {
			chunk.FirstRowRef = rowRefs[0]
			chunk.LastRowRef = rowRefs[len(rowRefs)-1]
		}
	}
	return chunk, nil
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}
