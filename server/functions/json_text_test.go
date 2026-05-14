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

	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestJsonValueAsTextDecodesEscapedStrings(t *testing.T) {
	ctx := sql.NewEmptyContext()
	doc, err := jsonDocumentFromFunctionValue(ctx, pgtypes.Json, `{"a":"x\ny"}`)
	if err != nil {
		t.Fatal(err)
	}
	object, err := jsonValueAsObjectForKeys("json_each_text", doc.Value)
	if err != nil {
		t.Fatal(err)
	}
	got, err := jsonValueAsText(ctx, object.Items[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	if got != "x\ny" {
		t.Fatalf("jsonValueAsText() = %#v, want %#v", got, "x\ny")
	}
}
