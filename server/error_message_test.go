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
	for _, code := range []pgcode.Code{pgcode.Syntax, pgcode.RaiseException, pgcode.CheckViolation, pgcode.InsufficientPrivilege} {
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

func TestErrMessageToSQLStateFormatsTemporaryTablePersistentSchema(t *testing.T) {
	msg := `cannot create temporary relation in non-temporary schema`
	code, ok := errMessageToSQLState(msg)
	require.True(t, ok)
	require.Equal(t, pgcode.InvalidTableDefinition.String(), code)
	require.Equal(t, pgcode.InvalidTableDefinition.String(), errorResponseCode(errors.New(msg)))
}

func TestErrMessageToSQLStateFormatsInsufficientPrivilege(t *testing.T) {
	for _, msg := range []string{
		`permission denied to create database`,
		`permission denied: must be owner of database protected_db`,
		`must be owner of table protected_items`,
	} {
		code, ok := errMessageToSQLState(msg)
		require.True(t, ok)
		require.Equal(t, pgcode.InsufficientPrivilege.String(), code)
		require.Equal(t, pgcode.InsufficientPrivilege.String(), errorResponseCode(errors.New(msg)))
	}
}

func TestErrMessageToSQLStateFormatsCommonRuntimeErrors(t *testing.T) {
	for _, tt := range []struct {
		msg  string
		code pgcode.Code
	}{
		{msg: `role "missing_database_owner" does not exist`, code: pgcode.UndefinedObject},
		{msg: `extension "plpgsql" must be installed in schema "pg_catalog"`, code: pgcode.DuplicateObject},
		{msg: `division by zero`, code: pgcode.DivisionByZero},
	} {
		code, ok := errMessageToSQLState(tt.msg)
		require.True(t, ok)
		require.Equal(t, tt.code.String(), code)
		require.Equal(t, tt.code.String(), errorResponseCode(errors.New(tt.msg)))
	}
}

func TestErrMessageToSQLStateFormatsMissingCommentObjects(t *testing.T) {
	for _, tt := range []struct {
		msg  string
		code pgcode.Code
	}{
		{msg: `relation "missing_comment_table" does not exist`, code: pgcode.UndefinedTable},
		{msg: `operator "+" does not exist`, code: pgcode.UndefinedFunction},
		{msg: `access method "missing_comment_am" does not exist`, code: pgcode.UndefinedObject},
		{msg: `collation "missing_comment_collation" does not exist`, code: pgcode.UndefinedObject},
		{msg: `extension "missing_comment_extension" does not exist`, code: pgcode.UndefinedObject},
		{msg: `language "missing_comment_language" does not exist`, code: pgcode.UndefinedObject},
		{msg: `large object 987654321 does not exist`, code: pgcode.UndefinedObject},
		{msg: `policy "missing_comment_policy" for relation "comment_policy_target" does not exist`, code: pgcode.UndefinedObject},
		{msg: `publication "missing_comment_publication" does not exist`, code: pgcode.UndefinedObject},
		{msg: `subscription "missing_comment_subscription" does not exist`, code: pgcode.UndefinedObject},
		{msg: `tablespace "missing_comment_tablespace" does not exist`, code: pgcode.UndefinedObject},
		{msg: `text search configuration "missing_comment_ts_config" does not exist`, code: pgcode.UndefinedObject},
		{msg: `text search dictionary "missing_comment_ts_dict" does not exist`, code: pgcode.UndefinedObject},
		{msg: `text search parser "missing_comment_ts_parser" does not exist`, code: pgcode.UndefinedObject},
		{msg: `text search template "missing_comment_ts_template" does not exist`, code: pgcode.UndefinedObject},
		{msg: `type "missing_comment_domain" does not exist`, code: pgcode.UndefinedObject},
	} {
		code, ok := errMessageToSQLState(tt.msg)
		require.True(t, ok)
		require.Equal(t, tt.code.String(), code)
		require.Equal(t, tt.code.String(), errorResponseCode(errors.New(tt.msg)))
	}
}

func TestErrMessageToSQLStateFormatsDomainConstraintErrors(t *testing.T) {
	for _, tt := range []struct {
		msg  string
		code pgcode.Code
	}{
		{msg: `domain not_null_cast_domain does not allow null values`, code: pgcode.NotNullViolation},
		{msg: `cannot use subquery in check constraint`, code: pgcode.FeatureNotSupported},
		{msg: `aggregate functions are not allowed in check constraints`, code: pgcode.Grouping},
		{msg: `window functions are not allowed in check constraints`, code: pgcode.Windowing},
		{msg: `set-returning functions are not allowed in check constraints`, code: pgcode.FeatureNotSupported},
	} {
		code, ok := errMessageToSQLState(tt.msg)
		require.True(t, ok)
		require.Equal(t, tt.code.String(), code)
		require.Equal(t, tt.code.String(), errorResponseCode(errors.New(tt.msg)))
	}
}

func TestErrMessageToSQLStateFormatsSubqueryTooManyColumns(t *testing.T) {
	msg := "subquery has too many columns"
	code, ok := errMessageToSQLState(msg)
	require.True(t, ok)
	require.Equal(t, pgcode.Syntax.String(), code)
	require.Equal(t, pgcode.Syntax.String(), errorResponseCode(errors.New(msg)))
}

func TestErrMessageToSQLStateFormatsQueryShapeErrors(t *testing.T) {
	for _, tt := range []struct {
		msg  string
		code pgcode.Code
	}{
		{
			msg:  "SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
			code: pgcode.InvalidColumnReference,
		},
		{msg: "WITH TIES cannot be specified without ORDER BY", code: pgcode.Syntax},
		{
			msg:  "FOR UPDATE is not allowed with DISTINCT, GROUP BY, aggregate, or HAVING query results",
			code: pgcode.FeatureNotSupported,
		},
		{
			msg:  "FOR UPDATE is not allowed with set operation query results",
			code: pgcode.FeatureNotSupported,
		},
	} {
		code, ok := errMessageToSQLState(tt.msg)
		require.True(t, ok)
		require.Equal(t, tt.code.String(), code)
		require.Equal(t, tt.code.String(), errorResponseCode(errors.New(tt.msg)))
	}
}

func TestErrMessageToSQLStateFormatsPgcryptoErrors(t *testing.T) {
	for _, tt := range []struct {
		msg  string
		code pgcode.Code
	}{
		{msg: "unsupported pgcrypto digest algorithm: unknown", code: pgcode.InvalidParameterValue},
		{msg: "Length not in range", code: pgcode.ExternalRoutineInvocationException},
		{msg: "mismatched array dimensions", code: pgcode.ArraySubscript},
		{msg: "Corrupt ascii-armor", code: pgcode.ExternalRoutineInvocationException},
		{msg: "data not a multiple of block size", code: pgcode.ExternalRoutineInvocationException},
	} {
		code, ok := errMessageToSQLState(tt.msg)
		require.True(t, ok)
		require.Equal(t, tt.code.String(), code)
		require.Equal(t, tt.code.String(), errorResponseCode(errors.New(tt.msg)))
	}
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
