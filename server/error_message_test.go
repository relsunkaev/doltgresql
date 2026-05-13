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

package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

func TestSanitizeErrorMessageFormatsInlineAdaptiveJSONB(t *testing.T) {
	writer := utils.NewWriter(32)
	pgtypes.JsonValueSerialize(writer, pgtypes.JsonObjectFromItems([]pgtypes.JsonValueObjectItem{
		{Key: "key", Value: pgtypes.JsonValueString("value")},
	}, true))
	inlineAdaptiveValue := "\x00" + string(writer.Data())

	message := "duplicate unique key given: [" + inlineAdaptiveValue + ",2] (errno 1062) (sqlstate HY000)"
	sanitized := sanitizeErrorMessage(message)

	require.NotContains(t, sanitized, "\x00")
	require.Contains(t, sanitized, `{"key": "value"}`)
	require.True(t, strings.HasPrefix(sanitized, `duplicate unique key given: [{"key": "value"},2]`))
}

func TestSanitizeErrorMessageFormatsDuplicateTargetColumn(t *testing.T) {
	sanitized := sanitizeErrorMessage("column 'a' specified twice")

	require.Equal(t, `column "a" specified more than once`, sanitized)

	wrapped := sanitizeErrorMessage("column 'a' specified twice (errno 1110) (sqlstate HY000)")
	require.Equal(t, `column "a" specified more than once (errno 1110) (sqlstate HY000)`, wrapped)
}

func TestSanitizeErrorMessageFormatsMissingNonNullableColumn(t *testing.T) {
	sanitized := sanitizeErrorMessage("Field 'id' doesn't have a default value (errno 1105) (sqlstate HY000)")

	require.Equal(t, `null value in column "id" violates not-null constraint (errno 1105) (sqlstate HY000)`, sanitized)
}
