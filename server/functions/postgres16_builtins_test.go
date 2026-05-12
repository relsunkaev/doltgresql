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
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestPgInputValidationInteger(t *testing.T) {
	ctx := sql.NewEmptyContext()
	params := [3]*pgtypes.DoltgresType{}
	valid, err := pg_input_is_valid_text_text.Callable(ctx, params, "42", "integer")
	if err != nil {
		t.Fatal(err)
	}
	if valid != true {
		t.Fatalf("got %v, want true", valid)
	}
	code, err := pg_input_error_info_sql_error_code_text_text.Callable(ctx, params, "42000000000", "integer")
	if err != nil {
		t.Fatal(err)
	}
	if code != "22003" {
		t.Fatalf("got %v, want 22003", code)
	}
}

func TestArrayRandomPreservesCardinality(t *testing.T) {
	array := []any{int32(1), int32(2), int32(3), int32(4)}
	sampled, err := array_sample_anyarray_int32.Callable(nil, [3]*pgtypes.DoltgresType{}, array, int32(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(sampled.([]any)) != 2 {
		t.Fatalf("got length %d, want 2", len(sampled.([]any)))
	}
	shuffled, err := array_shuffle_anyarray.Callable(nil, [2]*pgtypes.DoltgresType{}, array)
	if err != nil {
		t.Fatal(err)
	}
	if len(shuffled.([]any)) != 4 {
		t.Fatalf("got length %d, want 4", len(shuffled.([]any)))
	}
}

func TestDateAddSubtractInTimezone(t *testing.T) {
	start := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	oneDay := duration.MakeDuration(0, 1, 0)
	added, err := date_add_timestamptz_interval_text.Callable(nil, [4]*pgtypes.DoltgresType{}, start, oneDay, "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if !added.(time.Time).Equal(start.AddDate(0, 0, 1)) {
		t.Fatalf("got %v, want one day later", added)
	}
	subtracted, err := date_subtract_timestamptz_interval_text.Callable(nil, [4]*pgtypes.DoltgresType{}, added, oneDay, "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if !subtracted.(time.Time).Equal(start) {
		t.Fatalf("got %v, want %v", subtracted, start)
	}
}
