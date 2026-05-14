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

package parser

import (
	"testing"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

func TestParseInvalidBitLiteralUsesInvalidTextRepresentation(t *testing.T) {
	_, err := Parse(`SELECT B'10012345';`)
	if err == nil {
		t.Fatal("expected invalid bit literal to fail")
	}
	if code := pgerror.GetPGCode(err); code != pgcode.InvalidTextRepresentation {
		t.Fatalf("expected %s, got %s: %v", pgcode.InvalidTextRepresentation, code, err)
	}
}
