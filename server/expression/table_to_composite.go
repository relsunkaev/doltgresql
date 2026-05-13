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

package expression

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const duplicateCompositeFieldAliasPrefix = "__doltgres_duplicate_field_"

// TableToComposite is a set of sql.Expressions wrapped together in a single value.
type TableToComposite struct {
	tableName string
	fields    []sql.Expression
	typ       *pgtypes.DoltgresType
}

var _ sql.Expression = (*TableToComposite)(nil)
var _ vitess.Injectable = (*TableToComposite)(nil)

// NewTableToComposite creates a new composite table type.
func NewTableToComposite(ctx *sql.Context, tableName string, fields []sql.Expression) (sql.Expression, error) {
	coll, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: we need to get the schema, but the GMS builder doesn't have that information
	typ, err := coll.GetType(ctx, id.NewType("", tableName))
	if err != nil {
		return nil, err
	}
	if typ == nil {
		if len(fields) == 1 {
			return fields[0], nil
		}
		typ, err = compositeTypeFromFields(ctx, tableName, fields)
		if err != nil {
			return nil, err
		}
	}
	return &TableToComposite{
		fields: fields,
		typ:    typ,
	}, nil
}

// NewTableToCompositeExpr creates an unresolved injectable expression for a
// whole-row reference whose field expressions will be resolved by the builder.
func NewTableToCompositeExpr(tableName string) *TableToComposite {
	return &TableToComposite{tableName: tableName}
}

// EncodeDuplicateCompositeFieldAlias creates a unique planner-visible alias for
// duplicate subquery output names that still carries the PostgreSQL field name.
func EncodeDuplicateCompositeFieldAlias(tableIndex, fieldIndex int, fieldName string) string {
	return fmt.Sprintf("%s%d_%d_%s", duplicateCompositeFieldAliasPrefix, tableIndex, fieldIndex, hex.EncodeToString([]byte(fieldName)))
}

func decodeDuplicateCompositeFieldAlias(fieldName string) (string, bool) {
	if !strings.HasPrefix(fieldName, duplicateCompositeFieldAliasPrefix) {
		return "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(fieldName, duplicateCompositeFieldAliasPrefix), "_", 3)
	if len(parts) != 3 {
		return "", false
	}
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return "", false
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return "", false
	}
	decoded, err := hex.DecodeString(parts[2])
	if err != nil {
		return "", false
	}
	return string(decoded), true
}

func compositeTypeFromFields(ctx *sql.Context, tableName string, fields []sql.Expression) (*pgtypes.DoltgresType, error) {
	typeID := id.NewType("", tableName)
	relID := id.NewTable("", tableName).AsId()
	arrayID := id.NewType("", "_"+tableName)
	attrs := make([]pgtypes.CompositeAttribute, len(fields))
	for i, field := range fields {
		colType, ok := field.Type(ctx).(*pgtypes.DoltgresType)
		if !ok {
			var err error
			colType, err = pgtypes.FromGmsTypeToDoltgresType(field.Type(ctx))
			if err != nil {
				return nil, err
			}
		}
		fieldName := fmt.Sprintf("f%d", i+1)
		if nameable, ok := field.(sql.Nameable); ok && nameable.Name() != "" {
			fieldName = nameable.Name()
		}
		if decodedFieldName, ok := decodeDuplicateCompositeFieldAlias(fieldName); ok {
			fieldName = decodedFieldName
		}
		attrs[i] = pgtypes.NewCompositeAttribute(ctx, relID, fieldName, colType.ID, colType.GetAttTypMod(), int16(i+1), "")
	}
	return pgtypes.NewCompositeType(ctx, relID, arrayID, typeID, attrs), nil
}

// Resolved implements the sql.Expression interface.
func (t *TableToComposite) Resolved() bool {
	if t.fields == nil {
		return false
	}
	for _, expr := range t.fields {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// String implements the sql.Expression interface.
func (t *TableToComposite) String() string {
	return "TABLE TO COMPOSITE"
}

// Type implements the sql.Expression interface.
func (t *TableToComposite) Type(ctx *sql.Context) sql.Type {
	if t.typ == nil {
		return pgtypes.Record
	}
	return t.typ
}

// IsNullable implements the sql.Expression interface.
func (t *TableToComposite) IsNullable(ctx *sql.Context) bool {
	return false
}

// Eval implements the sql.Expression interface.
func (t *TableToComposite) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	vals := make([]pgtypes.RecordValue, len(t.fields))
	for i, expr := range t.fields {
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		typ, ok := expr.Type(ctx).(*pgtypes.DoltgresType)
		if !ok {
			return nil, fmt.Errorf("expected a DoltgresType, but got %T", expr.Type(ctx))
		}
		vals[i] = pgtypes.RecordValue{
			Value: val,
			Type:  typ,
		}
	}

	return vals, nil
}

// Children implements the sql.Expression interface.
func (t *TableToComposite) Children() []sql.Expression {
	return t.fields
}

// WithChildren implements the sql.Expression interface.
func (t *TableToComposite) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	tCopy := *t
	tCopy.fields = children
	return &tCopy, nil
}

// WithResolvedChildren implements the vitess.Injectable interface.
func (t *TableToComposite) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		return nil, fmt.Errorf("expected *sql.Context but found %T", ctx)
	}
	fields := make([]sql.Expression, len(children))
	for i, child := range children {
		expr, ok := child.(sql.Expression)
		if !ok {
			return nil, fmt.Errorf("expected sql.Expression child but found %T", child)
		}
		fields[i] = expr
	}
	return NewTableToComposite(sqlCtx, t.tableName, fields)
}
