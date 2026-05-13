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

package node

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/types"
)

type AlterDomainAction string

const (
	AlterDomainSetDefault    AlterDomainAction = "set_default"
	AlterDomainDropDefault   AlterDomainAction = "drop_default"
	AlterDomainSetNotNull    AlterDomainAction = "set_not_null"
	AlterDomainDropNotNull   AlterDomainAction = "drop_not_null"
	AlterDomainAddConstraint AlterDomainAction = "add_constraint"
)

// AlterDomain handles ALTER DOMAIN commands that mutate domain metadata.
type AlterDomain struct {
	DatabaseName   string
	SchemaName     string
	Name           string
	Action         AlterDomainAction
	DefaultExpr    sql.Expression
	ConstraintName string
	CheckExpr      sql.Expression
	NotValid       bool
	overrides      sql.EngineOverrides
}

var _ sql.ExecSourceRel = (*AlterDomain)(nil)
var _ sql.NodeOverriding = (*AlterDomain)(nil)
var _ vitess.Injectable = (*AlterDomain)(nil)

func NewAlterDomainSetDefault(databaseName, schemaName, name string) *AlterDomain {
	return &AlterDomain{DatabaseName: databaseName, SchemaName: schemaName, Name: name, Action: AlterDomainSetDefault}
}

func NewAlterDomainDropDefault(databaseName, schemaName, name string) *AlterDomain {
	return &AlterDomain{DatabaseName: databaseName, SchemaName: schemaName, Name: name, Action: AlterDomainDropDefault}
}

func NewAlterDomainSetNotNull(databaseName, schemaName, name string) *AlterDomain {
	return &AlterDomain{DatabaseName: databaseName, SchemaName: schemaName, Name: name, Action: AlterDomainSetNotNull}
}

func NewAlterDomainDropNotNull(databaseName, schemaName, name string) *AlterDomain {
	return &AlterDomain{DatabaseName: databaseName, SchemaName: schemaName, Name: name, Action: AlterDomainDropNotNull}
}

func NewAlterDomainAddConstraint(databaseName, schemaName, name, constraintName string, notValid bool) *AlterDomain {
	return &AlterDomain{
		DatabaseName:   databaseName,
		SchemaName:     schemaName,
		Name:           name,
		Action:         AlterDomainAddConstraint,
		ConstraintName: constraintName,
		NotValid:       notValid,
	}
}

func (a *AlterDomain) Children() []sql.Node { return nil }

func (a *AlterDomain) IsReadOnly() bool { return false }

func (a *AlterDomain) Resolved() bool { return true }

func (a *AlterDomain) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	collection, domain, err := a.resolveDomain(ctx)
	if err != nil {
		return nil, err
	}
	if err = checkTypeOwnership(ctx, domain); err != nil {
		return nil, err
	}

	updated := *domain
	switch a.Action {
	case AlterDomainSetDefault:
		if a.DefaultExpr == nil {
			return nil, errors.New("ALTER DOMAIN SET DEFAULT missing default expression")
		}
		updated.Default = a.DefaultExpr.String()
	case AlterDomainDropDefault:
		updated.Default = ""
	case AlterDomainSetNotNull:
		if err = a.validateNoNullDomainUsages(ctx, domain.ID); err != nil {
			return nil, err
		}
		updated.NotNull = true
	case AlterDomainDropNotNull:
		updated.NotNull = false
	case AlterDomainAddConstraint:
		if a.CheckExpr == nil {
			return nil, errors.New("ALTER DOMAIN ADD CONSTRAINT currently requires a CHECK expression")
		}
		checkName := a.ConstraintName
		if checkName == "" {
			checkName = generateCheckNameForDomain(a.Name, domainCheckNames(domain))
		}
		check := &sql.CheckConstraint{Name: checkName, Expr: a.CheckExpr, Enforced: true}
		if err = validateCheckConstraintExpression(ctx, check); err != nil {
			return nil, err
		}
		checkDef, err := plan.NewCheckDefinition(ctx, check, sql.GetSchemaFormatter(a.overrides))
		if err != nil {
			return nil, err
		}
		if !a.NotValid {
			if err = a.validateDomainCheckUsages(ctx, domain.ID, domain, check); err != nil {
				return nil, err
			}
		}
		updated.Checks = append(append([]*sql.CheckDefinition(nil), domain.Checks...), checkDef)
	default:
		return nil, errors.Errorf("unsupported ALTER DOMAIN action %q", a.Action)
	}

	if err = a.replaceDomainType(ctx, collection, domain, &updated); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterDomain) resolveDomain(ctx *sql.Context) (interface {
	DropType(context.Context, ...id.Type) error
	CreateType(context.Context, *types.DoltgresType) error
}, *types.DoltgresType, error) {
	schema, err := core.GetSchemaName(ctx, nil, a.SchemaName)
	if err != nil {
		return nil, nil, err
	}
	collection, err := core.GetTypesCollectionFromContextForDatabase(ctx, a.DatabaseName)
	if err != nil {
		return nil, nil, err
	}
	typeID := id.NewType(schema, a.Name)
	typ, err := collection.GetType(ctx, typeID)
	if err != nil {
		return nil, nil, err
	}
	if typ == nil {
		return nil, nil, types.ErrTypeDoesNotExist.New(a.Name)
	}
	if typ.TypType != types.TypeType_Domain {
		return nil, nil, errors.Errorf(`type "%s" is not a domain`, a.Name)
	}
	return collection, typ, nil
}

func (a *AlterDomain) replaceDomainType(ctx *sql.Context, collection interface {
	DropType(context.Context, ...id.Type) error
	CreateType(context.Context, *types.DoltgresType) error
}, oldDomain *types.DoltgresType, updated *types.DoltgresType) error {
	if err := collection.DropType(ctx, oldDomain.ID); err != nil {
		return err
	}
	if err := collection.CreateType(ctx, updated); err != nil {
		return err
	}
	if err := core.MarkTypesCollectionDirty(ctx, a.DatabaseName); err != nil {
		return err
	}
	return (&AlterTypeRename{DatabaseName: a.DatabaseName}).renameDependentTableColumns(ctx, oldDomain.ID, oldDomain.Array, updated)
}

func domainCheckNames(domain *types.DoltgresType) []string {
	names := make([]string, 0, len(domain.Checks))
	for _, check := range domain.Checks {
		names = append(names, check.Name)
	}
	return names
}

func (a *AlterDomain) validateNoNullDomainUsages(ctx *sql.Context, domainID id.Type) error {
	return a.forEachDomainColumnValue(ctx, domainID, func(tableName string, columnName string, _ *types.DoltgresType, value any) error {
		if value == nil {
			return errors.Errorf(`column "%s" of table "%s" contains null values`, columnName, tableName)
		}
		return nil
	})
}

func (a *AlterDomain) validateDomainCheckUsages(ctx *sql.Context, domainID id.Type, domain *types.DoltgresType, check *sql.CheckConstraint) error {
	baseType, err := domain.DomainUnderlyingBaseTypeWithContext(ctx)
	if err != nil {
		return err
	}
	return a.forEachDomainColumnValue(ctx, domainID, func(_ string, _ string, _ *types.DoltgresType, value any) error {
		valueExpr := gmsexpression.NewLiteral(value, baseType)
		expr, _, err := transform.Expr(ctx, check.Expr, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			if _, ok := expr.(*DomainColumn); ok {
				return valueExpr, transform.NewTree, nil
			}
			return expr, transform.SameTree, nil
		})
		if err != nil {
			return err
		}
		result, err := expr.Eval(ctx, nil)
		if err != nil {
			return err
		}
		if pass, ok := result.(bool); ok && !pass {
			return types.ErrDomainValueViolatesCheckConstraint.New(a.Name, check.Name)
		}
		return nil
	})
}

func (a *AlterDomain) forEachDomainColumnValue(ctx *sql.Context, domainID id.Type, cb func(tableName string, columnName string, columnType *types.DoltgresType, value any) error) error {
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	tableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	for _, tableName := range tableNames {
		if doltdb.IsSystemTable(tableName) {
			continue
		}
		sqlTable, err := core.GetSqlTableFromContext(ctx, a.DatabaseName, tableName)
		if err != nil {
			return err
		}
		if sqlTable == nil {
			continue
		}
		schema := sqlTable.Schema(ctx)
		for ordinal, col := range schema {
			colType, ok := col.Type.(*types.DoltgresType)
			if !ok || colType.ID != domainID {
				continue
			}
			if err = scanColumnValues(ctx, sqlTable, ordinal, col.Name, colType, cb); err != nil {
				return err
			}
		}
	}
	return nil
}

func scanColumnValues(ctx *sql.Context, table sql.Table, ordinal int, columnName string, columnType *types.DoltgresType, cb func(tableName string, columnName string, columnType *types.DoltgresType, value any) error) error {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return err
		}
		for {
			row, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return err
			}
			if err = cb(table.Name(), columnName, columnType, row[ordinal]); err != nil {
				_ = rows.Close(ctx)
				return err
			}
		}
		if err = rows.Close(ctx); err != nil {
			return err
		}
	}
}

func (a *AlterDomain) Schema(ctx *sql.Context) sql.Schema { return nil }

func (a *AlterDomain) String() string { return "ALTER DOMAIN" }

func (a *AlterDomain) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterDomain) WithOverrides(overrides sql.EngineOverrides) sql.Node {
	a.overrides = overrides
	return a
}

func (a *AlterDomain) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	expected := 0
	if a.Action == AlterDomainSetDefault || a.Action == AlterDomainAddConstraint {
		expected = 1
	}
	if len(children) != expected {
		return nil, ErrVitessChildCount.New(expected, len(children))
	}
	if len(children) == 0 {
		return a, nil
	}
	expr, ok := children[0].(sql.Expression)
	if !ok {
		return nil, errors.Errorf("invalid vitess child, expected sql.Expression but got %T", children[0])
	}
	copy := *a
	if a.Action == AlterDomainSetDefault {
		copy.DefaultExpr = expr
	} else {
		copy.CheckExpr = expr
	}
	return &copy, nil
}
