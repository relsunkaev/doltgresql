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

func TestXmlConstructorHelpers(t *testing.T) {
	if got, want := escapeXMLText(`< foo & bar >`), `&lt; foo &amp; bar &gt;`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	ctx := sql.NewEmptyContext()
	got, err := xmlforest_any.Callable(
		ctx,
		[]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text, pgtypes.Int32},
		"foo",
		[]any{"abc", "bar", int32(123)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := `<foo>abc</foo><bar>123</bar>`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
