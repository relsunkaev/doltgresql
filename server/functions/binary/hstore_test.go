// Copyright 2024 Dolthub, Inc.
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
	"encoding/hex"
	"strings"
	"testing"
)

func TestHstoreWirePayload(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "00000000",
		},
		{
			name:     "populated",
			input:    `"B"=>"5", "A"=>"2", "empty"=>NULL`,
			expected: "00000003000000014100000001320000000142000000013500000005656d707479ffffffff",
		},
		{
			name:     "escaped",
			input:    `"quote"=>"a\"b", "slash"=>"c\\d"`,
			expected: "000000020000000571756f74650000000361226200000005736c61736800000003635c64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := hstoreWirePayload(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if actual := hex.EncodeToString(payload); actual != tt.expected {
				t.Fatalf("expected payload %s, got %s", tt.expected, actual)
			}

			decoded, err := hstoreFromWirePayload(payload)
			if err != nil {
				t.Fatal(err)
			}
			pairs, err := parseHstore(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if expected := formatHstore(pairs); decoded != expected {
				t.Fatalf("expected decoded hstore %q, got %q", expected, decoded)
			}
		})
	}
}

func TestHstoreFromWirePayloadRejectsMalformedPayloads(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		contains string
	}{
		{
			name:     "negative pair count",
			payload:  "ffffffff",
			contains: "number of pairs cannot be negative",
		},
		{
			name:     "null key",
			payload:  "00000001ffffffff",
			contains: "null value not allowed for hstore key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := hex.DecodeString(tt.payload)
			if err != nil {
				t.Fatal(err)
			}
			_, err = hstoreFromWirePayload(payload)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("expected error containing %q, got %q", tt.contains, err.Error())
			}
		})
	}
}
