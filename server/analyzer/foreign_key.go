// Copyright 2025 Dolthub, Inc.
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

package analyzer

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/types"
)

// validateForeignKeyDefinition validates that the given foreign key definition is valid for creation
func validateForeignKeyDefinition(ctx *sql.Context, fkDef sql.ForeignKeyConstraint, cols map[string]*sql.Column, parentCols map[string]*sql.Column) error {
	if err := validateForeignKeyReferencePrivileges(ctx, fkDef); err != nil {
		return err
	}
	for i := range fkDef.Columns {
		col := cols[strings.ToLower(fkDef.Columns[i])]
		if col.Generated != nil {
			if foreignKeyUpdateActionWritesReferencingColumn(fkDef.OnUpdate) {
				return errors.Errorf("generated column %q cannot be used with ON UPDATE %s in a foreign key", col.Name, fkDef.OnUpdate)
			}
			if foreignKeyDeleteActionWritesReferencingColumn(fkDef.OnDelete) {
				return errors.Errorf("generated column %q cannot be used with ON DELETE %s in a foreign key", col.Name, fkDef.OnDelete)
			}
		}
	}
	if err := validateForeignKeyReferenceHasUniqueIndex(ctx, fkDef); err != nil {
		return err
	}
	for i := range fkDef.Columns {
		col := cols[strings.ToLower(fkDef.Columns[i])]
		parentCol := parentCols[strings.ToLower(fkDef.ParentColumns[i])]
		if !foreignKeyComparableTypes(ctx, col.Type, parentCol.Type) {
			return pgerror.Newf(pgcode.DatatypeMismatch, "Key columns %q and %q are of incompatible types: %s and %s", col.Name, parentCol.Name, col.Type.String(), parentCol.Type.String())
		}
	}
	return nil
}

func foreignKeyUpdateActionWritesReferencingColumn(action sql.ForeignKeyReferentialAction) bool {
	switch action {
	case sql.ForeignKeyReferentialAction_Cascade, sql.ForeignKeyReferentialAction_SetNull, sql.ForeignKeyReferentialAction_SetDefault:
		return true
	default:
		return false
	}
}

func foreignKeyDeleteActionWritesReferencingColumn(action sql.ForeignKeyReferentialAction) bool {
	switch action {
	case sql.ForeignKeyReferentialAction_SetNull, sql.ForeignKeyReferentialAction_SetDefault:
		return true
	default:
		return false
	}
}

func validateForeignKeyReferencePrivileges(ctx *sql.Context, fkDef sql.ForeignKeyConstraint) error {
	if fkDef.IsSelfReferential() {
		return nil
	}

	schemaName, err := core.GetSchemaName(ctx, nil, fkDef.ParentSchema)
	if err != nil {
		return err
	}
	var denied bool
	auth.LockRead(func() {
		role := auth.GetRole(ctx.Client().User)
		public := auth.GetRole("public")
		denied = !roleHasReferencesOnColumns(role.ID(), schemaName, fkDef.ParentTable, fkDef.ParentColumns) &&
			!roleHasReferencesOnColumns(public.ID(), schemaName, fkDef.ParentTable, fkDef.ParentColumns)
	})
	if denied {
		return errors.Errorf("permission denied for table %s", fkDef.ParentTable)
	}
	return nil
}

func roleHasReferencesOnColumns(role auth.RoleID, schemaName, tableName string, columns []string) bool {
	if !role.IsValid() {
		return false
	}
	table := doltdb.TableName{Name: tableName, Schema: schemaName}
	if len(columns) == 0 {
		return auth.HasTablePrivilege(auth.TablePrivilegeKey{
			Role:  role,
			Table: table,
		}, auth.Privilege_REFERENCES)
	}
	for _, column := range columns {
		if !auth.HasTablePrivilege(auth.TablePrivilegeKey{
			Role:   role,
			Table:  table,
			Column: column,
		}, auth.Privilege_REFERENCES) {
			return false
		}
	}
	return true
}

func validateForeignKeyReferenceHasUniqueIndex(ctx *sql.Context, fkDef sql.ForeignKeyConstraint) error {
	parentSchema, err := core.GetSchemaName(ctx, nil, fkDef.ParentSchema)
	if err != nil {
		return err
	}

	var foundTable bool
	var foundUniqueIndex bool
	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (bool, error) {
			if foreignKeyReferenceTargetsTable(schema, table, parentSchema, fkDef.ParentTable) {
				foundTable = true
			}
			return true, nil
		},
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (bool, error) {
			if !foreignKeyReferenceTargetsTable(schema, table, parentSchema, fkDef.ParentTable) {
				return true, nil
			}
			foundTable = true
			if foreignKeyReferenceIndexMatches(index.Item, fkDef.ParentColumns) {
				foundUniqueIndex = true
				return false, nil
			}
			return true, nil
		},
	})
	if err != nil {
		return err
	}
	if foundTable && !foundUniqueIndex {
		return pgerror.Newf(pgcode.InvalidForeignKey, `there is no unique constraint matching given keys for referenced table "%s"`, fkDef.ParentTable)
	}
	return nil
}

func foreignKeyReferenceTargetsTable(schema functions.ItemSchema, table functions.ItemTable, schemaName, tableName string) bool {
	return strings.EqualFold(schema.Item.SchemaName(), schemaName) && strings.EqualFold(table.Item.Name(), tableName)
}

func foreignKeyReferenceIndexMatches(index sql.Index, parentColumns []string) bool {
	if !index.IsUnique() || index.IsSpatial() || index.IsFullText() || len(index.PrefixLengths()) > 0 {
		return false
	}
	expressions := index.Expressions()
	if len(expressions) != len(parentColumns) {
		return false
	}
	expectedColumns := make(map[string]int, len(parentColumns))
	for _, column := range parentColumns {
		expectedColumns[strings.ToLower(column)]++
	}
	for _, expression := range expressions {
		column := foreignKeyReferenceIndexExpressionColumn(index.Table(), expression)
		if expectedColumns[column] == 0 {
			return false
		}
		expectedColumns[column]--
	}
	return true
}

func foreignKeyReferenceIndexExpressionColumn(tableName string, expression string) string {
	expression = strings.ToLower(strings.TrimSpace(expression))
	tablePrefix := strings.ToLower(tableName) + "."
	if strings.HasPrefix(expression, tablePrefix) {
		expression = strings.TrimPrefix(expression, tablePrefix)
	}
	parts := strings.Split(expression, ".")
	return strings.Trim(parts[len(parts)-1], "`\"")
}

// foreignKeyComparableTypes returns whether the two given types are able to be used as parent/child columns in a
// foreign key.
func foreignKeyComparableTypes(ctx *sql.Context, from sql.Type, to sql.Type) bool {
	dtFrom, ok := from.(*types.DoltgresType)
	if !ok {
		return false // should never be possible
	}

	dtTo, ok := to.(*types.DoltgresType)
	if !ok {
		return false // should never be possible
	}

	if dtFrom.Equals(dtTo) {
		return true
	}

	if foreignKeyFloatReferencesInteger(ctx, dtFrom, dtTo) {
		return false
	}

	fromLiteral := expression.NewLiteral(dtFrom.Zero(), from)
	toLiteral := expression.NewLiteral(dtTo.Zero(), to)

	// a foreign key between two different types is valid if there is an equality operator on the two types
	eq := framework.GetBinaryFunction(framework.Operator_BinaryEqual).Compile(ctx, "=", fromLiteral, toLiteral)
	if eq == nil || eq.StashedError() != nil {
		return false
	}

	// Additionally, we need to be able to convert freely between the two types in both directions, since we do this
	// during the process of enforcing the constraints
	forwardConversion := types.GetAssignmentCast(dtFrom, dtTo)
	reverseConversion := types.GetAssignmentCast(dtTo, dtFrom)

	return forwardConversion != nil && reverseConversion != nil
}

func foreignKeyFloatReferencesInteger(ctx *sql.Context, child *types.DoltgresType, parent *types.DoltgresType) bool {
	child = foreignKeyComparableBaseType(ctx, child)
	parent = foreignKeyComparableBaseType(ctx, parent)
	return foreignKeyBinaryFloatType(child) && foreignKeyIntegerType(parent)
}

func foreignKeyComparableBaseType(ctx *sql.Context, typ *types.DoltgresType) *types.DoltgresType {
	if typ.TypType != types.TypeType_Domain {
		return typ
	}
	baseType, err := typ.DomainUnderlyingBaseTypeWithContext(ctx)
	if err != nil {
		return typ
	}
	return baseType
}

func foreignKeyBinaryFloatType(typ *types.DoltgresType) bool {
	switch typ.ID.TypeName() {
	case "float4", "float8":
		return true
	default:
		return false
	}
}

func foreignKeyIntegerType(typ *types.DoltgresType) bool {
	switch typ.ID.TypeName() {
	case "int2", "int4", "int8", "smallserial", "serial", "bigserial":
		return true
	default:
		return false
	}
}
