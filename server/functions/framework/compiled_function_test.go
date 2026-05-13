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

package framework

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestCompiledFunctionStringSchemaQualifiesResolvedUserFunction(t *testing.T) {
	fn := SQLFunction{
		ID:           id.NewFunction(`mixed"schema`, "lookup_default"),
		ReturnType:   pgtypes.Int32,
		SqlStatement: "SELECT 1",
	}
	compiled := &CompiledFunction{
		Name: "lookup_default",
		overload: overloadMatch{
			params: Overload{function: fn},
		},
	}

	require.Equal(t, `"mixed""schema"."lookup_default"()`, compiled.String())
}
