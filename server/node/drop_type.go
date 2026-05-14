// Copyright 2024 Dolthub, Inc.
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

package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	coreprocedures "github.com/dolthub/doltgresql/core/procedures"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	"github.com/dolthub/doltgresql/server/types"
)

// DropType handles the DROP TYPE statement.
type DropType struct {
	database string
	schName  string
	typName  string
	ifExists bool
	cascade  bool
}

var _ sql.ExecSourceRel = (*DropType)(nil)
var _ vitess.Injectable = (*DropType)(nil)

// NewDropType returns a new *DropType.
func NewDropType(ifExists bool, db, sch, typ string, cascade bool) *DropType {
	return &DropType{
		database: db,
		schName:  sch,
		typName:  typ,
		ifExists: ifExists,
		cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *DropType) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *DropType) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *DropType) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *DropType) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	if !userRole.IsValid() {
		return nil, errors.Errorf(`role "%s" does not exist`, ctx.Client().User)
	}

	currentDb := ctx.GetCurrentDatabase()
	if len(c.database) > 0 && c.database != currentDb {
		return nil, errors.Errorf("DROP TYPE is currently only supported for the current database")
	}
	schema, err := core.GetSchemaName(ctx, nil, c.schName)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	typeID := id.NewType(schema, c.typName)
	typ, err := collection.GetType(ctx, typeID)
	if err != nil {
		return nil, err
	}
	if typ == nil {
		if c.ifExists {
			// TODO: issue a notice
			return sql.RowsToRowIter(), nil
		} else {
			return nil, types.ErrTypeDoesNotExist.New(c.typName)
		}
	}
	if extension, ok, err := extensionOwningType(ctx, typeID); err != nil {
		return nil, err
	} else if ok {
		return nil, errors.Errorf("cannot drop type %s because extension %s requires it", typ.String(), extension)
	}
	if _, ok := types.IDToBuiltInDoltgresType[typ.ID]; ok {
		return nil, types.ErrCannotDropSystemType.New(typ.String())
	}
	if err = checkTypeOwnership(ctx, typ); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	// TODO: use .IsArrayType() when we support OIDs, so Elem OID isn't 0
	if typ.TypCategory == types.TypeCategory_ArrayTypes {
		// TODO: get the base type name
		//  add HINT:  You can drop type ___ instead. (base type)
		arrTypeName := typ.String()
		return nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
			`cannot drop type %s because type %s requires it`, arrTypeName, strings.TrimSuffix(arrTypeName, "[]"))
	}

	// iterate on all table columns to check if this type is currently used.
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	for _, tableName := range tableNames {
		t, ok, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if ok {
			if ofTypeID, typedTable := tablemetadata.OfType(unwrappedTableComment(t)); typedTable && ofTypeID == typeID {
				if c.cascade {
					// TODO: handle cascade
					return nil, errors.Errorf(`cascading type drops are not yet supported`)
				}
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - table %s depends on type %s'`, c.typName, t.Name(), c.typName)
			}
			for _, col := range t.Schema(ctx) {
				if col.HiddenSystem {
					continue
				}
				if dt, isDoltgresType := col.Type.(*types.DoltgresType); isDoltgresType {
					if dt.ID == typ.ID {
						if c.cascade {
							// TODO: handle cascade
							return nil, errors.Errorf(`cascading type drops are not yet supported`)
						}
						// TODO: issue a detail (list of all columns and tables that uses this type)
						//  and a hint (when we support CASCADE)
						return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - column %s of table %s depends on type %s'`, c.typName, col.Name, t.Name(), c.typName)
					}
				}
				if depends, err := columnDefaultReferencesType(ctx, col.Default, typ); err != nil || depends {
					if err != nil {
						return nil, err
					}
					if c.cascade {
						// TODO: handle cascade
						return nil, errors.Errorf(`cascading type drops are not yet supported`)
					}
					return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - default for column %s of table %s depends on type %s`, c.typName, col.Name, t.Name(), c.typName)
				}
				if depends, err := columnDefaultReferencesType(ctx, col.Generated, typ); err != nil || depends {
					if err != nil {
						return nil, err
					}
					if c.cascade {
						// TODO: handle cascade
						return nil, errors.Errorf(`cascading type drops are not yet supported`)
					}
					return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - generated column %s of table %s depends on type %s`, c.typName, col.Name, t.Name(), c.typName)
				}
				if depends, err := columnDefaultReferencesType(ctx, col.OnUpdate, typ); err != nil || depends {
					if err != nil {
						return nil, err
					}
					if c.cascade {
						// TODO: handle cascade
						return nil, errors.Errorf(`cascading type drops are not yet supported`)
					}
					return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - on update expression for column %s of table %s depends on type %s`, c.typName, col.Name, t.Name(), c.typName)
				}
			}
			if dependency, err := checkConstraintTypeDependency(ctx, t, typ); err != nil || dependency != "" {
				if err != nil {
					return nil, err
				}
				if c.cascade {
					// TODO: handle cascade
					return nil, errors.Errorf(`cascading type drops are not yet supported`)
				}
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - %s`, c.typName, dependency)
			}
			if dependency, err := indexTypeDependency(ctx, t, typ); err != nil || dependency != "" {
				if err != nil {
					return nil, err
				}
				if c.cascade {
					// TODO: handle cascade
					return nil, errors.Errorf(`cascading type drops are not yet supported`)
				}
				return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - %s`, c.typName, dependency)
			}
		}
	}

	if dependency, err := viewTypeDependency(ctx, db, typ); err != nil {
		return nil, err
	} else if dependency != "" {
		if c.cascade {
			// TODO: handle cascade
			return nil, errors.Errorf(`cascading type drops are not yet supported`)
		}
		return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - %s`, c.typName, dependency)
	}

	if dependency, err := routineTypeDependency(ctx, typ.ID); err != nil {
		return nil, err
	} else if dependency != "" {
		if c.cascade {
			// TODO: handle cascade
			return nil, errors.Errorf(`cascading type drops are not yet supported`)
		}
		return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - %s`, c.typName, dependency)
	}

	if err = collection.DropType(ctx, typeID); err != nil {
		return nil, err
	}
	clearTypeComment(typeID)

	// undefined/shell type doesn't create array type.
	if typ.IsDefined {
		arrayTypeName := fmt.Sprintf(`_%s`, c.typName)
		arrayID := id.NewType(schema, arrayTypeName)
		if err = collection.DropType(ctx, arrayID); err != nil {
			return nil, err
		}
		clearTypeComment(arrayID)
	}
	if err = core.MarkTypesCollectionDirty(ctx, ""); err != nil {
		return nil, err
	}
	auth.LockWrite(func() {
		auth.RemoveAllTypePrivileges(schema, c.typName)
		if typ.IsDefined {
			auth.RemoveAllTypePrivileges(schema, fmt.Sprintf(`_%s`, c.typName))
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func clearTypeComment(typeID id.Type) {
	comments.Set(comments.Key{
		ObjOID:   id.Cache().ToOID(typeID.AsId()),
		ClassOID: comments.ClassOID("pg_type"),
		ObjSubID: 0,
	}, nil)
}

func routineTypeDependency(ctx *sql.Context, typeID id.Type) (string, error) {
	funcsColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return "", err
	}
	dependency := ""
	err = funcsColl.IterateFunctions(ctx, func(function corefunctions.Function) (stop bool, err error) {
		if function.ReturnType == typeID || function.AggregateStateType == typeID {
			dependency = fmt.Sprintf("function %s depends on type %s", function.ID.FunctionName(), typeID.TypeName())
			return true, nil
		}
		for _, paramType := range function.ParameterTypes {
			if paramType == typeID {
				dependency = fmt.Sprintf("function %s depends on type %s", function.ID.FunctionName(), typeID.TypeName())
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil || dependency != "" {
		return dependency, err
	}

	procsColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return "", err
	}
	err = procsColl.IterateProcedures(ctx, func(procedure coreprocedures.Procedure) (stop bool, err error) {
		for _, paramType := range procedure.ParameterTypes {
			if paramType == typeID {
				dependency = fmt.Sprintf("procedure %s depends on type %s", procedure.ID.ProcedureName(), typeID.TypeName())
				return true, nil
			}
		}
		return false, nil
	})
	return dependency, err
}

func columnDefaultReferencesType(ctx *sql.Context, defaultValue *sql.ColumnDefaultValue, typ *types.DoltgresType) (bool, error) {
	if defaultValue == nil || defaultValue.Expr == nil {
		return false, nil
	}
	if defaultValue.Expr.Resolved() && expressionTreeReferencesType(ctx, defaultValue.Expr, typ) {
		return true, nil
	}
	return expressionReferencesType(defaultValue.Expr.String(), typ)
}

func expressionTreeReferencesType(ctx *sql.Context, expr sql.Expression, typ *types.DoltgresType) bool {
	if expr == nil {
		return false
	}
	found := false
	sql.Inspect(ctx, expr, func(ctx *sql.Context, expr sql.Expression) bool {
		if expr == nil {
			return !found
		}
		if !expr.Resolved() {
			return true
		}
		if sqlTypeReferencesType(expr.Type(ctx), typ) {
			found = true
			return false
		}
		if defaultExpr, ok := expr.(*sql.ColumnDefaultValue); ok && sqlTypeReferencesType(defaultExpr.OutType, typ) {
			found = true
			return false
		}
		return true
	})
	return found
}

func sqlTypeReferencesType(sqlType sql.Type, typ *types.DoltgresType) bool {
	doltgresType, ok := sqlType.(*types.DoltgresType)
	if !ok {
		return false
	}
	return typeIDReferencesType(doltgresType.ID, typ) ||
		typeIDReferencesType(doltgresType.Array, typ) ||
		typeIDReferencesType(doltgresType.Elem, typ) ||
		typeIDReferencesType(doltgresType.BaseTypeID, typ)
}

func checkConstraintTypeDependency(ctx *sql.Context, table sql.Table, typ *types.DoltgresType) (string, error) {
	checkTable, ok := table.(sql.CheckTable)
	if !ok {
		checkTable, ok = sql.GetUnderlyingTable(table).(sql.CheckTable)
	}
	if !ok {
		return "", nil
	}
	checks, err := checkTable.GetChecks(ctx)
	if err != nil {
		return "", err
	}
	for _, check := range checks {
		depends, err := expressionReferencesType(check.CheckExpression, typ)
		if err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("check constraint %s on table %s depends on type %s", check.Name, table.Name(), typ.Name()), nil
		}
	}
	return "", nil
}

func indexTypeDependency(ctx *sql.Context, table sql.Table, typ *types.DoltgresType) (string, error) {
	indexed, ok := table.(sql.IndexAddressable)
	if !ok {
		indexed, ok = sql.GetUnderlyingTable(table).(sql.IndexAddressable)
	}
	if !ok {
		return "", nil
	}
	indexes, err := indexed.GetIndexes(ctx)
	if err != nil {
		return "", err
	}
	for _, index := range indexes {
		logicalColumns := indexmetadata.LogicalColumns(index, table.Schema(ctx))
		for _, column := range logicalColumns {
			if !column.Expression {
				continue
			}
			depends, err := expressionReferencesType(column.Definition, typ)
			if err != nil || depends {
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("index %s on table %s depends on type %s", index.ID(), table.Name(), typ.Name()), nil
			}
		}
		if predicate := indexmetadata.Predicate(index.Comment()); predicate != "" {
			depends, err := expressionReferencesType(predicate, typ)
			if err != nil || depends {
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("index %s on table %s depends on type %s", index.ID(), table.Name(), typ.Name()), nil
			}
		}
	}
	return "", nil
}

func viewTypeDependency(ctx *sql.Context, db sql.Database, typ *types.DoltgresType) (string, error) {
	viewDatabase, ok := db.(sql.ViewDatabase)
	if !ok {
		return "", nil
	}
	views, err := viewDatabase.AllViews(ctx)
	if err != nil {
		return "", err
	}
	for _, view := range views {
		depends, err := sqlStatementReferencesType(view.CreateViewStatement, typ)
		if err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("view %s depends on type %s", view.Name, typ.Name()), nil
		}
	}
	return "", nil
}

func expressionReferencesType(expr string, typ *types.DoltgresType) (bool, error) {
	if strings.TrimSpace(expr) == "" {
		return false, nil
	}
	_, changed, err := rewriteExpressionTypeReferences(
		expr,
		typ.ID,
		typ.Array,
		dropTypeProbeType(typ.ID),
		dropTypeProbeType(typ.Array),
	)
	return changed, err
}

func sqlStatementReferencesType(statement string, typ *types.DoltgresType) (bool, error) {
	_, changed, err := rewriteSQLTypeReferences(
		statement,
		typ.ID,
		typ.Array,
		dropTypeProbeType(typ.ID),
		dropTypeProbeType(typ.Array),
	)
	return changed, err
}

func dropTypeProbeType(typeID id.Type) id.Type {
	if !typeID.IsValid() {
		return id.NullType
	}
	return id.NewType(typeID.SchemaName(), "__doltgresql_drop_type_probe_"+typeID.TypeName())
}

func typeIDReferencesType(typeID id.Type, typ *types.DoltgresType) bool {
	if !typeID.IsValid() {
		return false
	}
	if typeID == typ.ID {
		return true
	}
	if typ.Array.IsValid() && typeID == typ.Array {
		return true
	}
	if strings.EqualFold(typeID.TypeName(), typ.ID.TypeName()) {
		return typeID.SchemaName() == "" || strings.EqualFold(typeID.SchemaName(), typ.ID.SchemaName())
	}
	if typ.Array.IsValid() && strings.EqualFold(typeID.TypeName(), typ.Array.TypeName()) {
		return typeID.SchemaName() == "" || strings.EqualFold(typeID.SchemaName(), typ.Array.SchemaName())
	}
	return false
}

// Schema implements the interface sql.ExecSourceRel.
func (c *DropType) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *DropType) String() string {
	return "DROP TYPE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *DropType) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *DropType) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
