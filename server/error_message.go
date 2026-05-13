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

package server

import (
	"bytes"
	"strings"

	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

func sanitizeErrorMessage(message string) string {
	message = formatMissingNonNullableColumnError(message)
	message = formatColumnSpecifiedTwiceError(message)
	if strings.Contains(message, "duplicate unique key given: [") {
		message = formatSerializedJSONBInUniqueKeyError(message)
	}
	return escapeNullBytes(message)
}

func formatMissingNonNullableColumnError(message string) string {
	const prefix = "Field '"
	const suffix = "' doesn't have a default value"
	if !strings.HasPrefix(message, prefix) {
		return message
	}
	columnName, rest, ok := strings.Cut(strings.TrimPrefix(message, prefix), suffix)
	if !ok {
		return message
	}
	return `null value in column "` + columnName + `" violates not-null constraint` + rest
}

func formatColumnSpecifiedTwiceError(message string) string {
	const prefix = "column '"
	const suffix = "' specified twice"
	if !strings.HasPrefix(message, prefix) || !strings.HasSuffix(message, suffix) {
		return message
	}
	columnName := strings.TrimSuffix(strings.TrimPrefix(message, prefix), suffix)
	return `column "` + columnName + `" specified more than once`
}

func formatSerializedJSONBInUniqueKeyError(message string) string {
	var sb strings.Builder
	for i := 0; i < len(message); {
		if formatted, consumed, ok := formatSerializedJSONBAt(message[i:]); ok {
			sb.WriteString(formatted)
			i += consumed
			continue
		}
		sb.WriteByte(message[i])
		i++
	}
	return sb.String()
}

func formatSerializedJSONBAt(data string) (string, int, bool) {
	// Dolt's adaptive encoding prefixes inline extended values with a NUL byte.
	if len(data) > 1 && data[0] == 0 {
		if formatted, consumed, ok := formatSerializedJSONValue(data[1:]); ok {
			return formatted, consumed + 1, true
		}
	}
	return formatSerializedJSONValue(data)
}

func formatSerializedJSONValue(data string) (string, int, bool) {
	if len(data) == 0 || data[0] > byte(pgtypes.JsonValueType_Null) {
		return "", 0, false
	}

	var value pgtypes.JsonValue
	var err error
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		value, err = pgtypes.JsonValueDeserialize(utils.NewReader([]byte(data)))
	}()
	if panicked || err != nil {
		return "", 0, false
	}

	writer := utils.NewWriter(uint64(len(data)))
	pgtypes.JsonValueSerialize(writer, value)
	serialized := writer.Data()
	if !bytes.HasPrefix([]byte(data), serialized) {
		return "", 0, false
	}

	var sb strings.Builder
	pgtypes.JsonValueFormatter(&sb, value)
	return sb.String(), len(serialized), true
}

func escapeNullBytes(message string) string {
	if !strings.ContainsRune(message, '\x00') {
		return message
	}
	var sb strings.Builder
	for i := 0; i < len(message); i++ {
		if message[i] == 0 {
			sb.WriteString(`\x00`)
		} else {
			sb.WriteByte(message[i])
		}
	}
	return sb.String()
}
