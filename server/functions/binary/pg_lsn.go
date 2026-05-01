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

package binary

import (
	"math/big"

	"github.com/cockroachdb/errors"
	"github.com/shopspring/decimal"
)

var maxPgLsn = new(big.Int).SetUint64(^uint64(0))

func pgLsnDiffDecimal(left uint64, right uint64) decimal.Decimal {
	leftInt := new(big.Int).SetUint64(left)
	rightInt := new(big.Int).SetUint64(right)
	return decimal.NewFromBigInt(leftInt.Sub(leftInt, rightInt), 0)
}

func pgLsnApplyNumericOffset(lsn uint64, offset decimal.Decimal) (uint64, error) {
	if !offset.IsInteger() {
		return 0, errors.Errorf("pg_lsn offset must be an integer")
	}
	result := new(big.Int).SetUint64(lsn)
	result.Add(result, offset.BigInt())
	if result.Sign() < 0 || result.Cmp(maxPgLsn) > 0 {
		return 0, errors.Errorf("pg_lsn out of range")
	}
	return result.Uint64(), nil
}

func pgLsnSubtractNumericOffset(lsn uint64, offset decimal.Decimal) (uint64, error) {
	negativeOffset := new(big.Int).Neg(offset.BigInt())
	return pgLsnApplyNumericOffset(lsn, decimal.NewFromBigInt(negativeOffset, 0))
}
