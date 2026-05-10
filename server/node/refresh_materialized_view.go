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
	"io"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/settings"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// RefreshMaterializedView implements PostgreSQL's non-concurrent
// REFRESH MATERIALIZED VIEW ... WITH DATA path for table-backed snapshots.
type RefreshMaterializedView struct {
	name         string
	schema       string
	concurrently bool
	withNoData   bool
	Runner       pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*RefreshMaterializedView)(nil)
var _ sql.Expressioner = (*RefreshMaterializedView)(nil)
var _ vitess.Injectable = (*RefreshMaterializedView)(nil)

var materializedViewRefreshTempCounter uint64

// testHookAfterConcurrentRefreshBuild, when non-nil, runs after a
// CONCURRENTLY refresh has built its replacement snapshot and before it
// starts the target swap. It lets cross-session tests deterministically
// inspect the old materialized view while the refresh is mid-flight.
var testHookAfterConcurrentRefreshBuild func(ctx *sql.Context)

// SetTestHookAfterConcurrentRefreshBuild installs a hook that fires between
// the build and swap phases of REFRESH MATERIALIZED VIEW CONCURRENTLY. The
// intended caller is the cross-session integration test in testing/go.
func SetTestHookAfterConcurrentRefreshBuild(hook func(ctx *sql.Context)) {
	testHookAfterConcurrentRefreshBuild = hook
}

var testHookBeforeConcurrentRefreshSwap func(ctx *sql.Context) error

// SetTestHookBeforeConcurrentRefreshSwap installs a hook immediately before
// REFRESH MATERIALIZED VIEW CONCURRENTLY publishes staged rows to the target.
// The intended caller is the cross-session integration test in testing/go.
func SetTestHookBeforeConcurrentRefreshSwap(hook func(ctx *sql.Context) error) {
	testHookBeforeConcurrentRefreshSwap = hook
}

// NewRefreshMaterializedView returns a new *RefreshMaterializedView.
func NewRefreshMaterializedView(name string, schema string, concurrently bool, withNoData bool) *RefreshMaterializedView {
	return &RefreshMaterializedView{
		name:         name,
		schema:       schema,
		concurrently: concurrently,
		withNoData:   withNoData,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (r *RefreshMaterializedView) Expressions() []sql.Expression {
	return []sql.Expression{r.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	target, err := r.resolveTarget(ctx)
	if err != nil {
		return nil, err
	}
	if r.concurrently && !tablemetadata.IsMaterializedViewPopulated(tableComment(target.table)) {
		return nil, errors.Errorf("CONCURRENTLY cannot be used when the materialized view is not populated")
	}
	if r.concurrently && r.withNoData {
		return nil, errors.Errorf("REFRESH options CONCURRENTLY and WITH NO DATA cannot be used together")
	}
	if r.concurrently {
		hasUniqueIndex, err := hasUsableConcurrentRefreshUniqueIndex(ctx, target.table)
		if err != nil {
			return nil, err
		}
		if !hasUniqueIndex {
			return nil, errors.Errorf(
				`cannot refresh materialized view "%s" concurrently`,
				materializedViewDisplayName(target.schema, target.table.Name()),
			)
		}
	}
	definition := tablemetadata.MaterializedViewDefinition(tableComment(target.table))
	if strings.TrimSpace(definition) == "" {
		return nil, errors.Errorf(`materialized view "%s" does not have a stored definition`, target.table.Name())
	}

	if r.withNoData {
		if err = r.refreshWithNoData(ctx, target, definition); err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(), nil
	}
	if r.concurrently {
		if err = r.refreshConcurrently(ctx, target, definition); err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(), nil
	}
	if err = r.refreshSynchronously(ctx, target, definition); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (r *RefreshMaterializedView) refreshWithNoData(ctx *sql.Context, target refreshMaterializedViewTarget, definition string) error {
	qualifiedName := quoteQualifiedIdentifier(target.schema, target.table.Name())
	if err := r.runRefreshStatement(ctx, "TRUNCATE TABLE "+qualifiedName); err != nil {
		return err
	}
	return r.setTargetPopulated(ctx, target, definition, false)
}

func (r *RefreshMaterializedView) refreshSynchronously(ctx *sql.Context, target refreshMaterializedViewTarget, definition string) error {
	qualifiedName := quoteQualifiedIdentifier(target.schema, target.table.Name())
	columnList := quoteColumnList(target.table.Schema(ctx))
	if err := r.runRefreshStatement(ctx, "TRUNCATE TABLE "+qualifiedName); err != nil {
		return err
	}
	if err := r.runRefreshStatement(ctx, fmt.Sprintf("INSERT INTO %s (%s) %s", qualifiedName, columnList, definition)); err != nil {
		return err
	}
	return r.setTargetPopulated(ctx, target, definition, true)
}

func (r *RefreshMaterializedView) refreshConcurrently(ctx *sql.Context, target refreshMaterializedViewTarget, definition string) error {
	columnList := quoteColumnList(target.table.Schema(ctx))
	tempName := fmt.Sprintf("__doltgres_refresh_%d", atomic.AddUint64(&materializedViewRefreshTempCounter, 1))
	quotedTempName := quoteIdentifier(tempName)
	stagedDefinition := fmt.Sprintf("SELECT * FROM (%s) AS refresh_rows (%s)", definition, columnList)

	if err := r.runRefreshStatement(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", quotedTempName, stagedDefinition)); err != nil {
		return err
	}
	defer func() {
		_ = r.runRefreshStatement(ctx, "DROP TABLE IF EXISTS "+quotedTempName)
	}()

	if testHookAfterConcurrentRefreshBuild != nil {
		testHookAfterConcurrentRefreshBuild(ctx)
	}

	replacement, err := r.buildConcurrentRefreshReplacement(ctx, target, quotedTempName, columnList)
	if err != nil {
		return err
	}
	return r.swapConcurrentRefreshReplacement(ctx, target, replacement)
}

func (r *RefreshMaterializedView) buildConcurrentRefreshReplacement(
	ctx *sql.Context,
	target refreshMaterializedViewTarget,
	quotedTempName string,
	columnList string,
) (*doltdb.Table, error) {
	targetDoltTable, err := sqlTableDoltTable(ctx, target.table)
	if err != nil {
		return nil, err
	}
	emptyTargetTable, err := emptyMaterializedViewTable(ctx, targetDoltTable)
	if err != nil {
		return nil, err
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return nil, err
	}
	tableName := targetDoltTableName(target)
	buildRoot, err := root.PutTable(ctx, tableName, emptyTargetTable)
	if err != nil {
		return nil, err
	}

	session := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()
	currentWorkingSet, err := session.WorkingSet(ctx, dbName)
	if err != nil {
		return nil, err
	}
	buildWorkingSet := currentWorkingSet.WithWorkingRoot(buildRoot).WithStagedRoot(buildRoot)
	autoIncrementTracker, err := dsess.NewAutoIncrementTracker(ctx, dbName, buildWorkingSet)
	if err != nil {
		return nil, err
	}
	defer autoIncrementTracker.Close()

	writeSession := writer.NewWriteSession(buildWorkingSet, autoIncrementTracker, editor.Options{})
	tableWriter, err := writeSession.GetTableWriter(ctx, tableName, dbName, func(*sql.Context, string, doltdb.RootValue) error {
		return nil
	}, false)
	if err != nil {
		return nil, err
	}

	tableWriter.StatementBegin(ctx)
	err = r.streamRefreshQuery(ctx, fmt.Sprintf("SELECT %s FROM %s", columnList, quotedTempName), func(row sql.Row) error {
		return tableWriter.Insert(ctx, row)
	})
	if err != nil {
		_ = tableWriter.DiscardChanges(ctx, err)
		return nil, err
	}
	if err = tableWriter.StatementComplete(ctx); err != nil {
		_ = tableWriter.DiscardChanges(ctx, err)
		return nil, err
	}
	flushedWorkingSet, err := writeSession.Flush(ctx)
	if err != nil {
		return nil, err
	}
	replacementTable, ok, err := flushedWorkingSet.WorkingRoot().GetTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf(`materialized view "%s" replacement table was not built`, target.table.Name())
	}
	return replacementTable, nil
}

func (r *RefreshMaterializedView) swapConcurrentRefreshReplacement(ctx *sql.Context, target refreshMaterializedViewTarget, replacement *doltdb.Table) error {
	if testHookBeforeConcurrentRefreshSwap != nil {
		if err := testHookBeforeConcurrentRefreshSwap(ctx); err != nil {
			return err
		}
	}
	session, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, targetDoltTableName(target), replacement)
	if err != nil {
		return err
	}
	return session.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), newRoot)
}

func sqlTableDoltTable(ctx *sql.Context, table sql.Table) (*doltdb.Table, error) {
	doltTable := core.SQLTableToDoltTable(table)
	if doltTable == nil {
		return nil, errors.Errorf(`materialized view "%s" is not backed by a Dolt table`, table.Name())
	}
	return doltTable.DoltTable(ctx)
}

func emptyMaterializedViewTable(ctx *sql.Context, table *doltdb.Table) (*doltdb.Table, error) {
	tableSchema, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	indexSet, err := table.GetIndexSet(ctx)
	if err != nil {
		return nil, err
	}
	for _, index := range tableSchema.Indexes().AllIndexes() {
		emptyIndex, err := durable.NewEmptyIndexFromTableSchema(ctx, table.ValueReadWriter(), table.NodeStore(), index, tableSchema)
		if err != nil {
			return nil, err
		}
		indexSet, err = indexSet.PutIndex(ctx, index.Name(), emptyIndex)
		if err != nil {
			return nil, err
		}
	}
	emptyRows, err := durable.NewEmptyPrimaryIndex(ctx, table.ValueReadWriter(), table.NodeStore(), tableSchema)
	if err != nil {
		return nil, err
	}
	emptyTable, err := doltdb.NewTable(ctx, table.ValueReadWriter(), table.NodeStore(), tableSchema, emptyRows, indexSet, nil)
	if err != nil {
		return nil, err
	}
	artifacts, err := table.GetArtifacts(ctx)
	if err != nil {
		return nil, err
	}
	return emptyTable.SetArtifacts(ctx, artifacts)
}

func targetDoltTableName(target refreshMaterializedViewTarget) doltdb.TableName {
	schemaName := target.schema
	if schemaTable, ok := target.table.(sql.DatabaseSchemaTable); ok {
		if databaseSchema := schemaTable.DatabaseSchema(); databaseSchema != nil {
			schemaName = databaseSchema.SchemaName()
		}
	}
	return doltdb.TableName{Name: target.table.Name(), Schema: schemaName}
}

// Schema implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) String() string {
	return "REFRESH MATERIALIZED VIEW"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *RefreshMaterializedView) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithExpressions implements the interface sql.Expressioner.
func (r *RefreshMaterializedView) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(expressions), 1)
	}
	newR := *r
	newR.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newR, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *RefreshMaterializedView) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

type refreshMaterializedViewTarget struct {
	schema string
	db     sql.Database
	table  sql.Table
}

func (r *RefreshMaterializedView) resolveTarget(ctx *sql.Context) (refreshMaterializedViewTarget, error) {
	return resolveMaterializedViewTarget(ctx, r.name, r.schema)
}

func findMaterializedViewRelation(ctx *sql.Context, name string, schema string) (refreshMaterializedViewTarget, bool, error) {
	searchSchemas, err := materializedViewSearchSchemas(ctx, schema)
	if err != nil {
		return refreshMaterializedViewTarget{}, false, err
	}
	var found refreshMaterializedViewTarget
	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		SearchSchemas: searchSchemas,
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			if table.Item.Name() != name {
				return true, nil
			}
			found = refreshMaterializedViewTarget{
				schema: schema.Item.SchemaName(),
				db:     schema.Item,
				table:  table.Item,
			}
			return false, nil
		},
	})
	if err != nil {
		return refreshMaterializedViewTarget{}, false, err
	}
	if found.table == nil {
		return refreshMaterializedViewTarget{}, false, nil
	}
	return found, true, nil
}

func resolveMaterializedViewTarget(ctx *sql.Context, name string, schema string) (refreshMaterializedViewTarget, error) {
	found, ok, err := findMaterializedViewRelation(ctx, name, schema)
	if err != nil {
		return refreshMaterializedViewTarget{}, err
	}
	if !ok {
		return refreshMaterializedViewTarget{}, errors.Errorf(`relation "%s" does not exist`, name)
	}
	if !tablemetadata.IsMaterializedView(tableComment(found.table)) {
		return refreshMaterializedViewTarget{}, errors.Errorf(`relation "%s" is not a materialized view`, name)
	}
	return found, nil
}

func (r *RefreshMaterializedView) setTargetPopulated(ctx *sql.Context, target refreshMaterializedViewTarget, definition string, populated bool) error {
	comment := tablemetadata.SetMaterializedViewDefinitionWithPopulated(tableComment(target.table), definition, populated)
	return modifyTableComment(ctx, target.db, target.table.Name(), comment)
}

func materializedViewSearchSchemas(ctx *sql.Context, schema string) ([]string, error) {
	if schema != "" {
		return []string{schema}, nil
	}
	return settings.GetCurrentSchemas(ctx)
}

func (r *RefreshMaterializedView) runRefreshStatement(ctx *sql.Context, query string) error {
	_, err := r.runRefreshQuery(ctx, query)
	return err
}

func (r *RefreshMaterializedView) runRefreshQuery(ctx *sql.Context, query string) ([]sql.Row, error) {
	if r.Runner.Runner == nil {
		return nil, errors.Errorf("statement runner is not available")
	}
	return sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := r.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
}

func (r *RefreshMaterializedView) streamRefreshQuery(ctx *sql.Context, query string, consume func(sql.Row) error) error {
	if r.Runner.Runner == nil {
		return errors.Errorf("statement runner is not available")
	}
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := r.Runner.Runner.QueryWithBindings(subCtx, query, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		defer rowIter.Close(subCtx)
		for {
			row, err := rowIter.Next(subCtx)
			if err == io.EOF {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			if err = consume(row); err != nil {
				return nil, err
			}
		}
	})
	return err
}

func hasUsableConcurrentRefreshUniqueIndex(ctx *sql.Context, table sql.Table) (bool, error) {
	indexed, ok := table.(sql.IndexAddressable)
	if !ok {
		return false, nil
	}
	indexes, err := indexed.GetIndexes(ctx)
	if err != nil {
		return false, err
	}
	tableSchema := table.Schema(ctx)
	for _, index := range indexes {
		if usableConcurrentRefreshUniqueIndex(index, tableSchema) {
			return true, nil
		}
	}
	return false, nil
}

func usableConcurrentRefreshUniqueIndex(index sql.Index, tableSchema sql.Schema) bool {
	if !indexmetadata.IsUnique(index) || !indexmetadata.IsReady(index.Comment()) || !indexmetadata.IsValid(index.Comment()) {
		return false
	}
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
	}
	if indexmetadata.Predicate(index.Comment()) != "" {
		return false
	}
	logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
	if len(logicalColumns) == 0 {
		return false
	}
	for _, column := range logicalColumns {
		if column.Expression || tableSchema.IndexOfColName(column.StorageName) < 0 {
			return false
		}
	}
	for _, column := range indexmetadata.IncludeColumns(index.Comment()) {
		if tableSchema.IndexOfColName(column) < 0 {
			return false
		}
	}
	return true
}

func materializedViewDisplayName(schema string, name string) string {
	if schema == "" {
		return name
	}
	return schema + "." + name
}

func quoteQualifiedIdentifier(schema string, name string) string {
	if schema == "" {
		return quoteIdentifier(name)
	}
	return quoteIdentifier(schema) + "." + quoteIdentifier(name)
}

func quoteColumnList(schema sql.Schema) string {
	quoted := make([]string, len(schema))
	for i, col := range schema {
		quoted[i] = quoteIdentifier(col.Name)
	}
	return strings.Join(quoted, ", ")
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func tableComment(table sql.Table) string {
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return ""
	}
	return commented.Comment()
}
