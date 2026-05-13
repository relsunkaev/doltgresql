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

package core

import "testing"

func TestPhysicalViewNameEncoding(t *testing.T) {
	tests := []struct {
		name    string
		encoded string
	}{
		{name: "caseview", encoded: "caseview"},
		{name: "CaseView", encoded: "__doltgres_view_name_4361736556696577"},
		{name: "__doltgres_view_name_user", encoded: "__doltgres_view_name_5f5f646f6c74677265735f766965775f6e616d655f75736572"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodePhysicalViewName(tt.name)
			if encoded != tt.encoded {
				t.Fatalf("expected encoded name %q, got %q", tt.encoded, encoded)
			}
			decoded := DecodePhysicalViewName(encoded)
			if decoded != tt.name {
				t.Fatalf("expected decoded name %q, got %q", tt.name, decoded)
			}
		})
	}

	const invalidPhysicalName = "__doltgres_view_name_not_hex"
	if decoded := DecodePhysicalViewName(invalidPhysicalName); decoded != invalidPhysicalName {
		t.Fatalf("expected invalid physical name to remain %q, got %q", invalidPhysicalName, decoded)
	}
}
