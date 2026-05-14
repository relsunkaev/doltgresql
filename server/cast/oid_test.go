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

package cast

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestInt64ToOidOutOfRangeSQLState(t *testing.T) {
	if framework.GetExplicitCast(pgtypes.Int64, pgtypes.Oid) == nil {
		initInt64()
	}
	castFn := framework.GetExplicitCast(pgtypes.Int64, pgtypes.Oid)
	if castFn == nil {
		t.Fatal("expected int8 to oid cast")
	}

	for _, value := range []int64{-1, pgtypes.MaxUint32 + 1} {
		t.Run("", func(t *testing.T) {
			_, err := castFn(sql.NewEmptyContext(), value, pgtypes.Oid)
			if err == nil {
				t.Fatal("expected error")
			}
			if code := pgerror.GetPGCode(err); code != pgcode.NumericValueOutOfRange {
				t.Fatalf("got SQLSTATE %s, want %s", code, pgcode.NumericValueOutOfRange)
			}
		})
	}
}
