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
	"strconv"
	"testing"

	"github.com/shopspring/decimal"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestPowerNumericFractionalExponentScale(t *testing.T) {
	tests := []struct {
		name string
		base decimal.Decimal
		want string
	}{
		{
			name: "integer result keeps numeric scale",
			base: decimal.NewFromInt(4),
			want: "2.0000000000000000",
		},
		{
			name: "float8 cast preserves PostgreSQL precision",
			base: decimal.NewFromInt(2),
			want: "1.4142135623730951",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := power_numeric_numeric.Callable(
				nil,
				[3]*pgtypes.DoltgresType{},
				test.base,
				decimal.NewFromFloat(0.5),
			)
			if err != nil {
				t.Fatal(err)
			}
			if text := got.(decimal.Decimal).StringFixed(16); text != test.want {
				t.Fatalf("got %q, want %q", text, test.want)
			}
			if test.base.Equal(decimal.NewFromInt(2)) {
				f, _ := got.(decimal.Decimal).Float64()
				if text := strconv.FormatFloat(f, 'f', -1, 64); text != test.want {
					t.Fatalf("float8 cast got %q, want %q", text, test.want)
				}
			}
		})
	}
}
