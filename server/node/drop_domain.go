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
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/types"
)

// DropDomain handles the DROP DOMAIN statement.
type DropDomain struct {
	database string
	schema   string
	domain   string
	ifExists bool
	cascade  bool
}

var _ sql.ExecSourceRel = (*DropDomain)(nil)
var _ vitess.Injectable = (*DropDomain)(nil)

// NewDropDomain returns a new *DropDomain.
func NewDropDomain(ifExists bool, db string, schema string, domain string, cascade bool) *DropDomain {
	return &DropDomain{
		database: db,
		schema:   schema,
		domain:   domain,
		ifExists: ifExists,
		cascade:  cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *DropDomain) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *DropDomain) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *DropDomain) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *DropDomain) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	currentDb := ctx.GetCurrentDatabase()
	if len(c.database) > 0 && c.database != currentDb {
		return nil, errors.Errorf("DROP DOMAIN is currently only supported for the current database")
	}
	schema, err := core.GetSchemaName(ctx, nil, c.schema)
	if err != nil {
		return nil, err
	}
	collection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	typeID := id.NewType(schema, c.domain)
	domain, err := collection.GetDomainType(ctx, typeID)
	if err != nil {
		return nil, err
	}
	if domain == nil {
		if c.ifExists {
			// TODO: issue a notice
			return sql.RowsToRowIter(), nil
		} else {
			return nil, types.ErrTypeDoesNotExist.New(c.domain)
		}
	}
	if err = checkTypeOwnership(ctx, domain); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	dependency, err := c.domainDependency(ctx, db, domain)
	if err != nil {
		return nil, err
	}
	if dependency != "" {
		if c.cascade {
			// TODO: handle cascade
			return nil, errors.Errorf(`cascading domain drops are not yet supported`)
		}
		return nil, pgerror.Newf(pgcode.DependentObjectsStillExist, `cannot drop type %s because other objects depend on it - %s`, c.domain, dependency)
	}

	if err = collection.DropType(ctx, typeID); err != nil {
		return nil, err
	}

	// drop array type of this type
	arrayTypeName := fmt.Sprintf(`_%s`, c.domain)
	arrayID := id.NewType(schema, arrayTypeName)
	if err = collection.DropType(ctx, arrayID); err != nil {
		return nil, err
	}
	if err = core.MarkTypesCollectionDirty(ctx, ""); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (c *DropDomain) domainDependency(ctx *sql.Context, db sql.Database, domain *types.DoltgresType) (string, error) {
	tableNames, err := db.GetTableNames(ctx)
	if err != nil {
		return "", err
	}
	for _, tableName := range tableNames {
		t, ok, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return "", err
		}
		if !ok {
			continue
		}
		if dependency, err := tableDomainDependency(ctx, t, domain); err != nil || dependency != "" {
			return dependency, err
		}
	}
	if dependency, err := viewDomainDependency(ctx, db, domain); err != nil || dependency != "" {
		return dependency, err
	}
	return routineDomainDependency(ctx, domain)
}

func tableDomainDependency(ctx *sql.Context, table sql.Table, domain *types.DoltgresType) (string, error) {
	tableName := table.Name()
	for _, col := range table.Schema(ctx) {
		if sqlTypeReferencesDomain(col.Type, domain) {
			return fmt.Sprintf("column %s of table %s depends on type %s", col.Name, tableName, domain.Name()), nil
		}
		if depends, err := columnDefaultReferencesDomain(col.Default, domain); err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("default for column %s of table %s depends on type %s", col.Name, tableName, domain.Name()), nil
		}
		if depends, err := columnDefaultReferencesDomain(col.Generated, domain); err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("generated column %s of table %s depends on type %s", col.Name, tableName, domain.Name()), nil
		}
		if depends, err := columnDefaultReferencesDomain(col.OnUpdate, domain); err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("on update expression for column %s of table %s depends on type %s", col.Name, tableName, domain.Name()), nil
		}
	}
	if dependency, err := checkConstraintDomainDependency(ctx, table, domain); err != nil || dependency != "" {
		return dependency, err
	}
	return indexDomainDependency(ctx, table, domain)
}

func checkConstraintDomainDependency(ctx *sql.Context, table sql.Table, domain *types.DoltgresType) (string, error) {
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
		depends, err := expressionReferencesDomain(check.CheckExpression, domain)
		if err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("check constraint %s on table %s depends on type %s", check.Name, table.Name(), domain.Name()), nil
		}
	}
	return "", nil
}

func indexDomainDependency(ctx *sql.Context, table sql.Table, domain *types.DoltgresType) (string, error) {
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
			depends, err := expressionReferencesDomain(column.Definition, domain)
			if err != nil || depends {
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("index %s on table %s depends on type %s", index.ID(), table.Name(), domain.Name()), nil
			}
		}
		if predicate := indexmetadata.Predicate(index.Comment()); predicate != "" {
			depends, err := expressionReferencesDomain(predicate, domain)
			if err != nil || depends {
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("index %s on table %s depends on type %s", index.ID(), table.Name(), domain.Name()), nil
			}
		}
	}
	return "", nil
}

func viewDomainDependency(ctx *sql.Context, db sql.Database, domain *types.DoltgresType) (string, error) {
	viewDatabase, ok := db.(sql.ViewDatabase)
	if !ok {
		return "", nil
	}
	views, err := viewDatabase.AllViews(ctx)
	if err != nil {
		return "", err
	}
	for _, view := range views {
		depends, err := sqlStatementReferencesDomain(view.CreateViewStatement, domain)
		if err != nil || depends {
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("view %s depends on type %s", view.Name, domain.Name()), nil
		}
	}
	return "", nil
}

func routineDomainDependency(ctx *sql.Context, domain *types.DoltgresType) (string, error) {
	funcsColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return "", err
	}
	dependency := ""
	err = funcsColl.IterateFunctions(ctx, func(function corefunctions.Function) (stop bool, err error) {
		if typeIDReferencesDomain(function.ReturnType, domain) || typeIDReferencesDomain(function.AggregateStateType, domain) {
			dependency = fmt.Sprintf("function %s depends on type %s", function.ID.FunctionName(), domain.Name())
			return true, nil
		}
		for _, paramType := range function.ParameterTypes {
			if typeIDReferencesDomain(paramType, domain) {
				dependency = fmt.Sprintf("function %s depends on type %s", function.ID.FunctionName(), domain.Name())
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
			if typeIDReferencesDomain(paramType, domain) {
				dependency = fmt.Sprintf("procedure %s depends on type %s", procedure.ID.ProcedureName(), domain.Name())
				return true, nil
			}
		}
		return false, nil
	})
	return dependency, err
}

func sqlTypeReferencesDomain(typ sql.Type, domain *types.DoltgresType) bool {
	doltgresType, ok := typ.(*types.DoltgresType)
	if !ok {
		return false
	}
	return typeIDReferencesDomain(doltgresType.ID, domain) ||
		typeIDReferencesDomain(doltgresType.Array, domain) ||
		typeIDReferencesDomain(doltgresType.BaseTypeID, domain)
}

func columnDefaultReferencesDomain(defaultValue *sql.ColumnDefaultValue, domain *types.DoltgresType) (bool, error) {
	if defaultValue == nil || defaultValue.Expr == nil {
		return false, nil
	}
	return expressionReferencesDomain(defaultValue.Expr.String(), domain)
}

func expressionReferencesDomain(expr string, domain *types.DoltgresType) (bool, error) {
	if strings.TrimSpace(expr) == "" {
		return false, nil
	}
	_, changed, err := rewriteExpressionTypeReferences(
		expr,
		domain.ID,
		domain.Array,
		dropDomainProbeType(domain.ID),
		dropDomainProbeType(domain.Array),
	)
	return changed, err
}

func sqlStatementReferencesDomain(statement string, domain *types.DoltgresType) (bool, error) {
	_, changed, err := rewriteSQLTypeReferences(
		statement,
		domain.ID,
		domain.Array,
		dropDomainProbeType(domain.ID),
		dropDomainProbeType(domain.Array),
	)
	return changed, err
}

func dropDomainProbeType(typeID id.Type) id.Type {
	if !typeID.IsValid() {
		return id.NullType
	}
	return id.NewType(typeID.SchemaName(), "__doltgresql_drop_domain_probe_"+typeID.TypeName())
}

func typeIDReferencesDomain(typeID id.Type, domain *types.DoltgresType) bool {
	if !typeID.IsValid() {
		return false
	}
	if typeID == domain.ID {
		return true
	}
	if domain.Array.IsValid() && typeID == domain.Array {
		return true
	}
	if strings.EqualFold(typeID.TypeName(), domain.ID.TypeName()) {
		return typeID.SchemaName() == "" || strings.EqualFold(typeID.SchemaName(), domain.ID.SchemaName())
	}
	if domain.Array.IsValid() && strings.EqualFold(typeID.TypeName(), domain.Array.TypeName()) {
		return typeID.SchemaName() == "" || strings.EqualFold(typeID.SchemaName(), domain.Array.SchemaName())
	}
	return false
}

// Schema implements the interface sql.ExecSourceRel.
func (c *DropDomain) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *DropDomain) String() string {
	return "DROP DOMAIN"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *DropDomain) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *DropDomain) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
