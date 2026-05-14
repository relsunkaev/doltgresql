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
	"bytes"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestInt4OutputAcceptsIntegralRuntimeValues(t *testing.T) {
	ctx := sql.NewEmptyContext()
	var resolved [2]*pgtypes.DoltgresType

	got, err := int4out.Callable(ctx, resolved, int(7))
	if err != nil {
		t.Fatal(err)
	}
	if got != "7" {
		t.Fatalf("got %q, want 7", got)
	}

	sent, err := int4send.Callable(ctx, resolved, int(7))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sent.([]byte), []byte{0, 0, 0, 7}) {
		t.Fatalf("got %v, want [0 0 0 7]", sent)
	}
}

func TestInt4OutputRejectsOutOfRangeRuntimeValues(t *testing.T) {
	ctx := sql.NewEmptyContext()
	var resolved [2]*pgtypes.DoltgresType

	if _, err := int4out.Callable(ctx, resolved, int64(2147483648)); err == nil {
		t.Fatal("expected out-of-range error")
	}
}
