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

package functions

import (
	"testing"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

func TestArrayFillNegativeDimensionSQLState(t *testing.T) {
	_, err := arrayFillDimensions([]any{int32(-1)})
	if err == nil {
		t.Fatal("expected error")
	}
	if code := pgerror.GetPGCode(err); code != pgcode.ProgramLimitExceeded {
		t.Fatalf("got SQLSTATE %s, want %s", code, pgcode.ProgramLimitExceeded)
	}
}
