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
	"errors"
	"strings"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
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

func TestSanitizeErrorMessageFormatsExclusionConstraintViolation(t *testing.T) {
	sanitized := sanitizeErrorMessage("duplicate unique key given: [10] (exclusion_items_resource_id_excl) (errno 1062) (sqlstate HY000)")

	require.Equal(t, `conflicting key value violates exclusion constraint "exclusion_items_resource_id_excl" (errno 1062) (sqlstate HY000)`, sanitized)
}

func TestErrorResponseCodeFormatsExclusionConstraintViolation(t *testing.T) {
	code := errorResponseCode(errors.New("duplicate unique key given: [10] (exclusion_items_resource_id_excl) (errno 1062) (sqlstate HY000)"))

	require.Equal(t, pgcode.ExclusionViolation.String(), code)
}

func TestCastSQLErrorPreservesExplicitPGCodes(t *testing.T) {
	for _, code := range []pgcode.Code{pgcode.Syntax, pgcode.RaiseException, pgcode.CheckViolation} {
		err := pgerror.New(code, "plpgsql error")
		require.Equal(t, code, pgerror.GetPGCode(castSQLError(err)))
	}
}

func TestSanitizeErrorMessageFormatsMissingNonNullableColumn(t *testing.T) {
	sanitized := sanitizeErrorMessage("Field 'id' doesn't have a default value (errno 1105) (sqlstate HY000)")

	require.Equal(t, `null value in column "id" violates not-null constraint (errno 1105) (sqlstate HY000)`, sanitized)
}

func TestSanitizeErrorMessageFormatsProvidedNullNonNullableColumn(t *testing.T) {
	sanitized := sanitizeErrorMessage("column name 'label' is non-nullable but attempted to set a value of null (errno 1048) (sqlstate HY000)")

	require.Equal(t, `null value in column "label" violates not-null constraint: column name 'label' is non-nullable but attempted to set a value of null (errno 1048) (sqlstate HY000)`, sanitized)
}

func TestErrMessageToSQLStateFormatsMaterializedViewAliasErrors(t *testing.T) {
	code, ok := errMessageToSQLState(`too many column names were specified`)
	require.True(t, ok)
	require.Equal(t, pgcode.Syntax.String(), code)

	code, ok = errMessageToSQLState(`column "account_id" specified more than once`)
	require.True(t, ok)
	require.Equal(t, pgcode.DuplicateColumn.String(), code)
}

func TestErrMessageToSQLStateFormatsMissingAlterTableColumn(t *testing.T) {
	code, ok := errMessageToSQLState(`table "drop_missing_strict_items" does not have column "missing_col"`)
	require.True(t, ok)
	require.Equal(t, pgcode.UndefinedColumn.String(), code)
}

func TestErrMessageToSQLStateFormatsAlterTableRowTypeDependency(t *testing.T) {
	code, ok := errMessageToSQLState(`cannot alter table "row_type_parent" because column "row_type_child.parent_row" uses its row type`)
	require.True(t, ok)
	require.Equal(t, pgcode.FeatureNotSupported.String(), code)
}

func TestErrMessageToSQLStateFormatsTruncateForeignKeyDependency(t *testing.T) {
	code, ok := errMessageToSQLState(`cannot truncate table fk_truncate_parent as it is referenced in foreign key fk_truncate_child_parent_id_fkey on table fk_truncate_child`)
	require.True(t, ok)
	require.Equal(t, pgcode.FeatureNotSupported.String(), code)
}

func TestErrMessageToSQLStateFormatsTypmodOverflow(t *testing.T) {
	code, ok := errMessageToSQLState(`numeric field overflow - A field with precision 5, scale 2 must round to an absolute value less than 10^3`)
	require.True(t, ok)
	require.Equal(t, pgcode.NumericValueOutOfRange.String(), code)

	code, ok = errMessageToSQLState(`value too long for type varying(3): out of range`)
	require.True(t, ok)
	require.Equal(t, pgcode.StringDataRightTruncation.String(), code)
}

func TestMysqlErrnoToSQLStateFormatsDuplicateInsertTargetColumns(t *testing.T) {
	code, ok := mysqlErrnoToSQLState(mysql.ERFieldSpecifiedTwice)
	require.True(t, ok)
	require.Equal(t, pgcode.DuplicateColumn.String(), code)
}

func TestErrMessageToSQLStateFormatsUnpopulatedMaterializedView(t *testing.T) {
	code, ok := errMessageToSQLState(`materialized view "source_mv" has not been populated`)
	require.True(t, ok)
	require.Equal(t, pgcode.ObjectNotInPrerequisiteState.String(), code)
}

func TestErrMessageToSQLStateFormatsRefreshMaterializedViewErrors(t *testing.T) {
	code, ok := errMessageToSQLState("REFRESH options CONCURRENTLY and WITH NO DATA cannot be used together")
	require.True(t, ok)
	require.Equal(t, pgcode.Syntax.String(), code)

	code, ok = errMessageToSQLState("CONCURRENTLY cannot be used when the materialized view is not populated")
	require.True(t, ok)
	require.Equal(t, pgcode.FeatureNotSupported.String(), code)

	code, ok = errMessageToSQLState(`relation "plain_table" is not a materialized view`)
	require.True(t, ok)
	require.Equal(t, pgcode.FeatureNotSupported.String(), code)

	code, ok = errMessageToSQLState(`cannot refresh materialized view "public.no_unique_mv" concurrently`)
	require.True(t, ok)
	require.Equal(t, pgcode.ObjectNotInPrerequisiteState.String(), code)
}

func TestErrMessageToSQLStateFormatsUndefinedFunction(t *testing.T) {
	code, ok := errMessageToSQLState(`function: 'missing_sum' not found; function does not exist`)
	require.True(t, ok)
	require.Equal(t, pgcode.UndefinedFunction.String(), code)

	code, ok = errMessageToSQLState(`function missing_sum(integer) does not exist`)
	require.True(t, ok)
	require.Equal(t, pgcode.UndefinedFunction.String(), code)

	code, ok = errMessageToSQLState(`stored procedure "missing_proc" does not exist`)
	require.True(t, ok)
	require.Equal(t, pgcode.UndefinedFunction.String(), code)

	code, ok = errMessageToSQLState(`procedure "missing_proc" does not exist`)
	require.True(t, ok)
	require.Equal(t, pgcode.UndefinedFunction.String(), code)
}
