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

package types

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestNumericTypmodRoundingAndOverflow(t *testing.T) {
	negativeScaleTypmod, err := GetTypmodFromNumericPrecisionAndScale(2, -3)
	if err != nil {
		t.Fatal(err)
	}
	precision, scale := GetPrecisionAndScaleFromTypmod(negativeScaleTypmod)
	if precision != 2 || scale != -3 {
		t.Fatalf("expected precision 2 scale -3, found precision %d scale %d", precision, scale)
	}
	rounded, err := GetNumericValueWithTypmod(decimal.RequireFromString("12345"), negativeScaleTypmod)
	if err != nil {
		t.Fatal(err)
	}
	if got := rounded.String(); got != "12000" {
		t.Fatalf("expected 12000, found %s", got)
	}
	if _, err = GetNumericValueWithTypmod(decimal.RequireFromString("99500"), negativeScaleTypmod); err == nil {
		t.Fatal("expected overflow for rounded negative-scale value")
	}

	largeScaleTypmod, err := GetTypmodFromNumericPrecisionAndScale(3, 5)
	if err != nil {
		t.Fatal(err)
	}
	rounded, err = GetNumericValueWithTypmod(decimal.RequireFromString("0.001234"), largeScaleTypmod)
	if err != nil {
		t.Fatal(err)
	}
	if got := rounded.String(); got != "0.00123" {
		t.Fatalf("expected 0.00123, found %s", got)
	}
	if _, err = GetNumericValueWithTypmod(decimal.RequireFromString("0.09999"), largeScaleTypmod); err == nil {
		t.Fatal("expected overflow for scale-greater-than-precision value")
	}
}

func TestNumericSpecialValuesCompareAndSerialize(t *testing.T) {
	nan, ok := ParseNumericSpecialValue("NaN")
	if !ok {
		t.Fatal("expected NaN to parse as a numeric special value")
	}
	posInf, ok := ParseNumericSpecialValue("Infinity")
	if !ok {
		t.Fatal("expected Infinity to parse as a numeric special value")
	}
	negInf, ok := ParseNumericSpecialValue("-Infinity")
	if !ok {
		t.Fatal("expected -Infinity to parse as a numeric special value")
	}
	onePointFive := decimal.RequireFromString("1.5")

	for _, tc := range []struct {
		name  string
		left  any
		right any
		want  int
	}{
		{"negative infinity sorts before finite", negInf, onePointFive, -1},
		{"finite sorts before infinity", onePointFive, posInf, -1},
		{"infinity sorts before nan", posInf, nan, -1},
		{"nan equals nan", nan, nan, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CompareNumericValues(tc.left, tc.right)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("expected compare result %d, found %d", tc.want, got)
			}
		})
	}

	data, err := serializeTypeNumeric(nil, Numeric, posInf)
	if err != nil {
		t.Fatal(err)
	}
	roundTripped, err := deserializeTypeNumeric(nil, Numeric, data)
	if err != nil {
		t.Fatal(err)
	}
	formatted, err := FormatNumericValue(roundTripped, -1)
	if err != nil {
		t.Fatal(err)
	}
	if formatted != "Infinity" {
		t.Fatalf("expected Infinity, found %s", formatted)
	}
}
