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
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/replicaidentity"
)

// AlterTableReplicaIdentity handles ALTER TABLE ... REPLICA IDENTITY.
type AlterTableReplicaIdentity struct {
	SchemaName string
	Table      string
	Identity   replicaidentity.Identity
	IndexName  string
	IfExists   bool
}

var _ sql.ExecSourceRel = (*AlterTableReplicaIdentity)(nil)
var _ vitess.Injectable = (*AlterTableReplicaIdentity)(nil)

func (a *AlterTableReplicaIdentity) Children() []sql.Node               { return nil }
func (a *AlterTableReplicaIdentity) IsReadOnly() bool                   { return false }
func (a *AlterTableReplicaIdentity) Resolved() bool                     { return true }
func (a *AlterTableReplicaIdentity) Schema(ctx *sql.Context) sql.Schema { return nil }
func (a *AlterTableReplicaIdentity) String() string                     { return "ALTER TABLE REPLICA IDENTITY" }

func (a *AlterTableReplicaIdentity) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schema, tableName, sqlTable, err := a.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	if sqlTable == nil {
		if a.IfExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, a.Table)
	}
	if err = checkTableOwnership(ctx, doltdb.TableName{Name: tableName, Schema: schema}); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	indexName := ""
	if a.Identity == replicaidentity.IdentityUsingIndex {
		indexName, err = a.resolveIndex(ctx, schema, tableName, sqlTable)
		if err != nil {
			return nil, err
		}
	}

	if err = replicaidentity.Set(ctx.GetCurrentDatabase(), schema, tableName, a.Identity, indexName); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterTableReplicaIdentity) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterTableReplicaIdentity) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func (a *AlterTableReplicaIdentity) resolveTable(ctx *sql.Context) (string, string, sql.Table, error) {
	if a.SchemaName != "" {
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: a.Table, Schema: a.SchemaName})
		if err != nil || table == nil {
			return a.SchemaName, a.Table, table, err
		}
		return a.SchemaName, table.Name(), table, nil
	}

	searchPaths, err := core.SearchPath(ctx)
	if err != nil {
		return "", "", nil, err
	}
	for _, schema := range searchPaths {
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: a.Table, Schema: schema})
		if err != nil {
			return "", "", nil, err
		}
		if table != nil {
			return schema, table.Name(), table, nil
		}
	}
	return "", a.Table, nil, nil
}

func (a *AlterTableReplicaIdentity) resolveIndex(ctx *sql.Context, schema string, tableName string, sqlTable sql.Table) (string, error) {
	var foundName string
	var nonUnique bool
	var nullableColumn bool
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		SearchSchemas: []string{schema},
		Index: func(ctx *sql.Context, itemSchema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			if !strings.EqualFold(table.Item.Name(), tableName) {
				return true, nil
			}
			displayName := replicaIdentityIndexName(index.Item)
			if !strings.EqualFold(displayName, a.IndexName) && !strings.EqualFold(index.Item.ID(), a.IndexName) {
				return true, nil
			}
			if !indexmetadata.IsUnique(index.Item) {
				nonUnique = true
				return false, nil
			}
			if indexmetadata.Deferrable(index.Item.Comment()) {
				return false, errors.Errorf(`cannot use non-immediate index "%s" as replica identity`, a.IndexName)
			}
			if !replicaIdentityIndexColumnsAreNotNull(ctx, sqlTable, index.Item) {
				nullableColumn = true
				return false, nil
			}
			foundName = displayName
			return false, nil
		},
	})
	if err != nil {
		return "", err
	}
	if nonUnique {
		return "", errors.Errorf(`cannot use non-unique index "%s" as replica identity`, a.IndexName)
	}
	if nullableColumn {
		return "", errors.Errorf(`index "%s" cannot be used as replica identity because not all columns are marked NOT NULL`, a.IndexName)
	}
	if foundName == "" {
		return "", pgerror.Newf(pgcode.UndefinedObject, `index "%s" does not exist`, a.IndexName)
	}
	return foundName, nil
}

func replicaIdentityIndexName(index sql.Index) string {
	if strings.EqualFold(index.ID(), "PRIMARY") {
		return fmt.Sprintf("%s_pkey", index.Table())
	}
	return index.ID()
}

func replicaIdentityIndexColumnsAreNotNull(ctx *sql.Context, table sql.Table, index sql.Index) bool {
	schema := table.Schema(ctx)
	for _, expr := range index.Expressions() {
		colName := replicaIdentityIndexColumnName(expr)
		colIdx := schema.IndexOfColName(colName)
		if colIdx < 0 || schema[colIdx].Nullable {
			return false
		}
	}
	return true
}

func replicaIdentityIndexColumnName(expr string) string {
	lastDot := strings.LastIndex(expr, ".")
	return expr[lastDot+1:]
}
