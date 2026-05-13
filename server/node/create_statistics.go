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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// CreateStatistics handles CREATE STATISTICS metadata.
type CreateStatistics struct {
	name    string
	target  alterTableStorageTarget
	columns []string
	kinds   []string
}

var _ sql.ExecSourceRel = (*CreateStatistics)(nil)
var _ vitess.Injectable = (*CreateStatistics)(nil)

// NewCreateStatistics returns a new *CreateStatistics.
func NewCreateStatistics(name string, schema string, table string, columns []string, kinds []string) *CreateStatistics {
	if len(kinds) == 0 {
		kinds = []string{"d", "f", "m"}
	}
	return &CreateStatistics{
		name: strings.TrimSpace(name),
		target: alterTableStorageTarget{
			schema: strings.TrimSpace(schema),
			table:  strings.TrimSpace(table),
		},
		columns: append([]string(nil), columns...),
		kinds:   append([]string(nil), kinds...),
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	table, err := c.target.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, sql.ErrTableNotFound.New(c.target.table)
	}
	if c.name == "" {
		return nil, errors.Errorf("statistics name cannot be empty")
	}
	if len(c.columns) == 0 {
		return nil, errors.Errorf("extended statistics require at least one column")
	}

	columns := make([]string, 0, len(c.columns))
	for _, column := range c.columns {
		column = strings.TrimSpace(column)
		found, ok := columnByName(table.Schema(ctx), column)
		if !ok {
			return nil, errors.Errorf(`column "%s" of relation "%s" does not exist`, column, c.target.table)
		}
		columns = append(columns, found.Name)
	}

	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return nil, sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	if err = alterable.ModifyComment(ctx, tablemetadata.AddExtendedStatistic(commented.Comment(), tablemetadata.ExtendedStatistic{
		Name:    c.name,
		Columns: columns,
		Kinds:   c.kinds,
	})); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) String() string {
	return "CREATE STATISTICS"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateStatistics) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateStatistics) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
