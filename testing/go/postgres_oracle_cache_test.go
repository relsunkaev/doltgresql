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

package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestPostgresOracleCacheNormalizesArrayTextFields(t *testing.T) {
	entry := &postgresOracleCachedEntry{}
	fields := []pgconn.FieldDescription{{
		DataTypeOID: id.Cache().ToOID(pgtypes.TextArray.ID.AsId()),
	}}
	rows := []sql.Row{
		{`{"\\x68656c6c6f","\\\\x776f726c64"}`},
		{`{\x68656c6c6f,\\x776f726c64}`},
	}

	got := entry.stringRowsForFields(rows, fields)
	want := []sql.Row{
		{`{\x68656c6c6f,\\x776f726c64}`},
		{`{\x68656c6c6f,\\x776f726c64}`},
	}
	if len(got) != len(want) || len(got[0]) != len(want[0]) || len(got[1]) != len(want[1]) || got[0][0] != want[0][0] || got[1][0] != want[1][0] {
		t.Fatalf("stringRowsForFields() = %#v, want %#v", got, want)
	}
}
