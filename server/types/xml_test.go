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

import "testing"

func TestValidateXMLDocument(t *testing.T) {
	for _, input := range []string{"<doc/>", "<?xml version=\"1.0\"?><doc><child/></doc>"} {
		if err := ValidateXMLDocument(input); err != nil {
			t.Fatalf("expected valid XML document %q: %v", input, err)
		}
	}
	for _, input := range []string{"plain text", "<doc>", "<a/><b/>"} {
		if err := ValidateXMLDocument(input); err == nil {
			t.Fatalf("expected invalid XML document %q", input)
		}
	}
}

func TestValidateXMLContent(t *testing.T) {
	for _, input := range []string{"plain text", "<a/><b/>", "<doc><child/></doc>"} {
		if err := ValidateXMLContent(input); err != nil {
			t.Fatalf("expected valid XML content %q: %v", input, err)
		}
	}
	if err := ValidateXMLContent("<doc>"); err == nil {
		t.Fatal("expected malformed XML content to fail")
	}
}
