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
	"io"
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestUnnestMultiRowIterPadsShorterArrays(t *testing.T) {
	iter := &unnestMultiRowIter{
		arrays: [][]any{
			{int32(10), int32(20)},
			{"foo", "bar", "baz"},
			nil,
		},
		maxLen: 3,
	}

	want := []sql.Row{
		{int32(10), "foo", nil},
		{int32(20), "bar", nil},
		{nil, "baz", nil},
	}
	for i, wantRow := range want {
		got, err := iter.Next(nil)
		if err != nil {
			t.Fatalf("row %d returned error: %v", i, err)
		}
		if !reflect.DeepEqual(got, wantRow) {
			t.Fatalf("row %d: got %#v, want %#v", i, got, wantRow)
		}
	}

	got, err := iter.Next(nil)
	if err != io.EOF {
		t.Fatalf("after final row got (%#v, %v), want io.EOF", got, err)
	}
}
