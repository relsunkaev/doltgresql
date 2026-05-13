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

package expression

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestJsonAggFormatPlainArrayIsCompact(t *testing.T) {
	var sb strings.Builder
	jsonAggFormatPlain(&sb, pgtypes.JsonValueArray{
		pgtypes.JsonValueString("first"),
		pgtypes.JsonValueString("second"),
	})

	require.Equal(t, `["first","second"]`, sb.String())
}

func TestJsonAggFormatPlainArrayPreservesRawJsonSeparators(t *testing.T) {
	var sb strings.Builder
	jsonAggFormatPlain(&sb, pgtypes.JsonValueArray{
		pgtypes.JsonValueRaw{Raw: `{"b":1,"a":2}`, Value: pgtypes.JsonValueObject{}},
		pgtypes.JsonValueRaw{Raw: `{"a":1,"a":2}`, Value: pgtypes.JsonValueObject{}},
		pgtypes.JsonValueRaw{Raw: `{ "c" : 3 }`, Value: pgtypes.JsonValueObject{}},
	})

	require.Equal(t, `[{"b":1,"a":2}, {"a":1,"a":2}, { "c" : 3 }]`, sb.String())
}
