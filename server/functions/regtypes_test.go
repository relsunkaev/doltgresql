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

	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestAdditionalRegTypeIO(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		name string
		in   func(*sql.Context, [2]*pgtypes.DoltgresType, any) (any, error)
		out  func(*sql.Context, [2]*pgtypes.DoltgresType, any) (any, error)
		text string
		want string
	}{
		{
			name: "regconfig",
			in:   regconfigin.Callable,
			out:  regconfigout.Callable,
			text: "english",
			want: "english",
		},
		{
			name: "regdictionary",
			in:   regdictionaryin.Callable,
			out:  regdictionaryout.Callable,
			text: "simple",
			want: "simple",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oidAlias, err := tt.in(ctx, [2]*pgtypes.DoltgresType{}, tt.text)
			if err != nil {
				t.Fatal(err)
			}
			output, err := tt.out(ctx, [2]*pgtypes.DoltgresType{}, oidAlias)
			if err != nil {
				t.Fatal(err)
			}
			if output != tt.want {
				t.Fatalf("expected %q, found %q", tt.want, output)
			}
		})
	}

	t.Run("regoperator", func(t *testing.T) {
		operatorID := id.NewId(
			id.Section_Operator,
			"+",
			string(id.NewType("pg_catalog", "int4")),
			string(id.NewType("pg_catalog", "int4")),
		)
		output, err := regoperatorout.Callable(ctx, [2]*pgtypes.DoltgresType{}, operatorID)
		if err != nil {
			t.Fatal(err)
		}
		if output != "+(integer,integer)" {
			t.Fatalf("expected %q, found %q", "+(integer,integer)", output)
		}
	})

	t.Run("regrole", func(t *testing.T) {
		output, err := regroleout.Callable(ctx, [2]*pgtypes.DoltgresType{}, id.NewId(id.Section_User, "postgres"))
		if err != nil {
			t.Fatal(err)
		}
		if output != "postgres" {
			t.Fatalf("expected %q, found %q", "postgres", output)
		}
		output, err = regroleout.Callable(ctx, [2]*pgtypes.DoltgresType{}, id.NewOID(0).AsId())
		if err != nil {
			t.Fatal(err)
		}
		if output != "-" {
			t.Fatalf("expected %q, found %q", "-", output)
		}
	})
}

func TestAdditionalRegTypeIORejectsMissingNames(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		name string
		in   func(*sql.Context, [2]*pgtypes.DoltgresType, any) (any, error)
		text string
	}{
		{name: "regoperator", in: regoperatorin.Callable, text: "missing"},
		{name: "regconfig", in: regconfigin.Callable, text: "missing_config"},
		{name: "regdictionary", in: regdictionaryin.Callable, text: "missing_dictionary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.in(ctx, [2]*pgtypes.DoltgresType{}, tt.text); err == nil {
				t.Fatal("expected lookup error")
			}
		})
	}
}
