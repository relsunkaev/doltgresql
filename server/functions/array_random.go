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
	"math/rand"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initArrayRandom() {
	framework.RegisterFunction(array_sample_anyarray_int32)
	framework.RegisterFunction(array_shuffle_anyarray)
}

var array_sample_anyarray_int32 = framework.Function2{
	Name:               "array_sample",
	Return:             pgtypes.AnyArray,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.AnyArray, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val any, count any) (any, error) {
		array := append([]any(nil), val.([]any)...)
		n := int(count.(int32))
		if n < 0 || n > len(array) {
			return nil, errors.Errorf("sample size must be between 0 and array length")
		}
		rand.Shuffle(len(array), func(i, j int) {
			array[i], array[j] = array[j], array[i]
		})
		return array[:n], nil
	},
}

var array_shuffle_anyarray = framework.Function1{
	Name:               "array_shuffle",
	Return:             pgtypes.AnyArray,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.AnyArray},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		array := append([]any(nil), val.([]any)...)
		rand.Shuffle(len(array), func(i, j int) {
			array[i], array[j] = array[j], array[i]
		})
		return array, nil
	},
}
