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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/deferrable"
)

// AlterForeignKeyConstraintTiming handles ALTER TABLE ... ALTER CONSTRAINT for foreign keys.
type AlterForeignKeyConstraintTiming struct {
	schema     string
	table      string
	constraint string
	timing     deferrable.Timing
}

var _ sql.ExecSourceRel = (*AlterForeignKeyConstraintTiming)(nil)
var _ vitess.Injectable = (*AlterForeignKeyConstraintTiming)(nil)

func NewAlterForeignKeyConstraintTiming(schema string, table string, constraint string, timing deferrable.Timing) *AlterForeignKeyConstraintTiming {
	return &AlterForeignKeyConstraintTiming{
		schema:     schema,
		table:      table,
		constraint: constraint,
		timing:     timing,
	}
}

func (a *AlterForeignKeyConstraintTiming) Children() []sql.Node {
	return nil
}

func (a *AlterForeignKeyConstraintTiming) IsReadOnly() bool {
	return false
}

func (a *AlterForeignKeyConstraintTiming) Resolved() bool {
	return true
}

func (a *AlterForeignKeyConstraintTiming) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, a.schema)
	if err != nil {
		return nil, err
	}
	tableName := doltdb.TableName{Name: a.table, Schema: schemaName}
	if err = checkTableOwnership(ctx, tableName); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}
	table, err := core.GetSqlTableFromContext(ctx, ctx.GetCurrentDatabase(), tableName)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, errors.Errorf(`relation "%s" does not exist`, a.table)
	}
	fkTable, ok := sql.GetUnderlyingTable(table).(sql.ForeignKeyTable)
	if !ok {
		return nil, errors.Errorf(`relation "%s" does not support foreign keys`, a.table)
	}
	foreignKeys, err := fkTable.GetDeclaredForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, fk := range foreignKeys {
		if strings.EqualFold(fk.Name, a.constraint) {
			if fk.SchemaName == "" {
				fk.SchemaName = schemaName
			}
			return sql.RowsToRowIter(), deferrable.SetForeignKeyTiming(ctx, fk, a.timing)
		}
	}
	return nil, errors.Errorf(`constraint "%s" of relation "%s" is not a foreign key constraint`, a.constraint, a.table)
}

func (a *AlterForeignKeyConstraintTiming) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (a *AlterForeignKeyConstraintTiming) String() string {
	return "ALTER TABLE ALTER CONSTRAINT"
}

func (a *AlterForeignKeyConstraintTiming) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterForeignKeyConstraintTiming) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}
