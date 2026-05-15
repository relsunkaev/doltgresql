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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmsexpression "github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PostgresExplain renders PostgreSQL-facing EXPLAIN output for translated SELECT
// plans that Doltgres can describe safely, while preserving the GMS fallback for
// plan shapes we do not translate yet.
type PostgresExplain struct {
	Child  sql.Node
	Format sql.DescribeOptions
}

var _ sql.ExecBuilderNode = (*PostgresExplain)(nil)

// NewPostgresExplain returns a new *PostgresExplain.
func NewPostgresExplain(format sql.DescribeOptions, child sql.Node) *PostgresExplain {
	return &PostgresExplain{Child: child, Format: format}
}

// Resolved implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) Resolved() bool {
	return p.Child.Resolved()
}

// String implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) String() string {
	return "EXPLAIN"
}

// Schema implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) Schema(ctx *sql.Context) sql.Schema {
	if p.Format.Plan {
		return plan.DescribePlanSchema
	}
	return plan.DescribeSchema
}

// Children implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) IsReadOnly() bool {
	return true
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (p *PostgresExplain) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	if p.Format.Analyze {
		if !p.Child.IsReadOnly() {
			return nil, fmt.Errorf("cannot analyze statement that could have side effects")
		}
		childIter, err := b.Build(ctx, p.Child, row)
		if err != nil {
			return nil, err
		}
		defer childIter.Close(ctx)
		for {
			_, err = childIter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
		}
	}

	if p.Format.Plan {
		if rows, ok := renderPostgresPlan(ctx, p.Child); ok {
			return rowsToExplainIter(rows), nil
		}
		return rowsToExplainIter(fallbackExplainRows(ctx, p.Child, p.Format)), nil
	}

	ctx.Warn(0, "EXPLAIN Output is currently a placeholder; use EXPLAIN PLAN for old behavior")
	return sql.RowsToRowIter(sql.Row{1, "SELECT", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", ""}), nil
}

func rowsToExplainIter(rows []string) sql.RowIter {
	sqlRows := make([]sql.Row, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row) == "" {
			continue
		}
		sqlRows = append(sqlRows, sql.NewRow(row))
	}
	return sql.RowsToRowIter(sqlRows...)
}

func fallbackExplainRows(ctx *sql.Context, child sql.Node, format sql.DescribeOptions) []string {
	formatString := sql.Describe(ctx, child, format)
	formatString = strings.ReplaceAll(formatString, "\r", "")
	return strings.Split(formatString, "\n")
}

func renderPostgresPlan(ctx *sql.Context, node sql.Node) ([]string, bool) {
	if rows, ok := renderPostgresIndexScan(ctx, node); ok {
		return rows, true
	}
	return nil, false
}

type postgresSimpleSelect struct {
	project *plan.Project
	filter  *plan.Filter
	access  *plan.IndexedTableAccess
}

func simpleIndexedSelect(node sql.Node) (postgresSimpleSelect, bool) {
	var result postgresSimpleSelect
	node = unwrapPostgresExplainWrapper(node)
	if project, ok := node.(*plan.Project); ok {
		result.project = project
		node = unwrapPostgresExplainWrapper(project.Child)
	}
	if filter, ok := node.(*plan.Filter); ok {
		result.filter = filter
		node = unwrapPostgresExplainWrapper(filter.Child)
	}
	access, ok := node.(*plan.IndexedTableAccess)
	if !ok {
		return postgresSimpleSelect{}, false
	}
	result.access = access
	return result, true
}

func unwrapPostgresExplainWrapper(node sql.Node) sql.Node {
	for {
		switch n := node.(type) {
		case *ContextRootFinalizer:
			node = n.Child()
		case *RelationLockingNode:
			node = n.Child()
		default:
			return node
		}
	}
}

func renderPostgresIndexScan(ctx *sql.Context, node sql.Node) ([]string, bool) {
	shape, ok := simpleIndexedSelect(node)
	if !ok || shape.filter == nil {
		return nil, false
	}
	index := shape.access.Index()
	if index == nil || indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return nil, false
	}
	condition, ok := postgresExplainCondition(ctx, shape.filter.Expression)
	if !ok {
		condition, ok = postgresExplainStringCondition(ctx, shape.filter.Expression, shape.access.Schema(ctx))
		if !ok {
			return nil, false
		}
	}

	tableName := shape.access.Name()
	indexName := indexmetadata.DisplayNameForTable(index, shape.access.UnderlyingTable())
	width := postgresExplainWidth(ctx, shape.project, shape.access.Schema(ctx))
	return []string{
		fmt.Sprintf("Index Scan using %s on %s  (cost=0.15..8.17 rows=1 width=%d)", indexName, tableName, width),
		fmt.Sprintf("  Index Cond: (%s)", condition),
	}, true
}

func postgresExplainCondition(ctx *sql.Context, expr sql.Expression) (string, bool) {
	eq, ok := expr.(*gmsexpression.Equals)
	if !ok {
		return "", false
	}
	left, right := eq.Left(), eq.Right()
	if field, ok := left.(*gmsexpression.GetField); ok {
		if literal, ok := right.(*gmsexpression.Literal); ok {
			return postgresExplainComparison(ctx, field, "=", literal), true
		}
	}
	if field, ok := right.(*gmsexpression.GetField); ok {
		if literal, ok := left.(*gmsexpression.Literal); ok {
			return postgresExplainComparison(ctx, field, "=", literal), true
		}
	}
	return "", false
}

func postgresExplainComparison(ctx *sql.Context, field *gmsexpression.GetField, op string, literal *gmsexpression.Literal) string {
	fieldName := core.DecodePhysicalColumnName(field.Name())
	literalString := literal.String()
	if typeName := postgresExplainTypeName(ctx, field.Type(ctx)); typeName != "" {
		literalString = fmt.Sprintf("%s::%s", literalString, typeName)
	}
	return fmt.Sprintf("%s %s %s", fieldName, op, literalString)
}

func postgresExplainStringCondition(ctx *sql.Context, expr sql.Expression, schema sql.Schema) (string, bool) {
	condition := strings.TrimSpace(expr.String())
	condition = strings.TrimPrefix(condition, "(")
	condition = strings.TrimSuffix(condition, ")")
	left, right, ok := strings.Cut(condition, " = ")
	if !ok {
		return "", false
	}
	left = postgresExplainColumnName(left)
	right = strings.TrimSpace(right)
	if strings.HasPrefix(right, "'") && !strings.Contains(right, "::") {
		if typeName := postgresExplainSchemaTypeName(ctx, schema, left); typeName != "" {
			right = fmt.Sprintf("%s::%s", right, typeName)
		}
	}
	return fmt.Sprintf("%s = %s", left, right), true
}

func postgresExplainColumnName(name string) string {
	name = strings.TrimSpace(name)
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		name = name[dot+1:]
	}
	return core.DecodePhysicalColumnName(strings.Trim(name, "`\""))
}

func postgresExplainSchemaTypeName(ctx *sql.Context, schema sql.Schema, columnName string) string {
	for _, column := range schema {
		if strings.EqualFold(core.DecodePhysicalColumnName(column.Name), columnName) {
			return postgresExplainTypeName(ctx, column.Type)
		}
	}
	return ""
}

func postgresExplainTypeName(ctx *sql.Context, typ sql.Type) string {
	if typ == nil {
		return ""
	}
	if doltgresType, ok := typ.(*pgtypes.DoltgresType); ok {
		return doltgresType.ID.TypeName()
	}
	return ""
}

func postgresExplainWidth(ctx *sql.Context, project *plan.Project, fallback sql.Schema) int {
	if project != nil {
		width := 0
		for _, expr := range project.Projections {
			width += postgresExplainTypeWidth(ctx, expr.Type(ctx))
		}
		if width > 0 {
			return width
		}
	}
	width := 0
	for _, col := range fallback {
		width += postgresExplainTypeWidth(ctx, col.Type)
	}
	if width > 0 {
		return width
	}
	return 4
}

func postgresExplainTypeWidth(ctx *sql.Context, typ sql.Type) int {
	if typ == nil {
		return 0
	}
	if doltgresType, ok := typ.(*pgtypes.DoltgresType); ok {
		switch doltgresType.ID.TypeName() {
		case "bool", "char":
			return 1
		case "int2":
			return 2
		case "int4", "float4", "oid":
			return 4
		case "int8", "float8":
			return 8
		case "text", "varchar", "citext", "name":
			return 32
		}
	}
	return 4
}
