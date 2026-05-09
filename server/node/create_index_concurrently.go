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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// CreateIndexConcurrently is the InjectedStatement that drives PostgreSQL's
// CREATE INDEX CONCURRENTLY two-phase build for supported btree indexes:
//
//  1. **Register**: build the index under (indisready=false, indisvalid=false)
//     so the planner cannot use it and the catalog reflects an in-progress
//     build to any other session that inspects pg_index. The build runs in
//     this transaction while concurrent writers commit normally; Dolt's
//     working-set merge folds those writes into the committing schema change.
//  2. **Flip**: update only the encoded PostgreSQL build-state bits, surfacing
//     the existing prolly index under steady-state (indisready=true,
//     indisvalid=true) where the planner picks it up.
//
// Workloads that emit CREATE INDEX CONCURRENTLY (Drizzle Kit, Alembic, Prisma,
// Rails) get observable pg_index state, non-blocking writer behavior during
// the phase 1 build, and a metadata-only final flip.
//
// Unsupported edge cases are rejected at the AST level before this node is
// constructed.
type CreateIndexConcurrently struct {
	ifNotExists     bool
	schema          string
	table           string
	indexName       string
	unique          bool
	columns         []sql.IndexColumn
	metadata        indexmetadata.Metadata
	createStatement string
	Runner          pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*CreateIndexConcurrently)(nil)
var _ sql.Expressioner = (*CreateIndexConcurrently)(nil)
var _ vitess.Injectable = (*CreateIndexConcurrently)(nil)

// testHookBetweenPhases, when non-nil, runs after Phase 1 succeeds and
// before Phase 2 begins. It exists so the cross-session contention
// test can deterministically observe the in-progress state through
// pg_index (indisready=false, indisvalid=false) instead of having to
// race the synchronous flip. Production code must never set it.
var testHookBetweenPhases func(ctx *sql.Context)

// SetTestHookBetweenPhases installs a hook that fires between the two
// CONCURRENTLY phases. The intended caller is the cross-session
// integration test in testing/go; nothing else.
func SetTestHookBetweenPhases(hook func(ctx *sql.Context)) {
	testHookBetweenPhases = hook
}

// NewCreateIndexConcurrently constructs the two-phase CREATE INDEX
// CONCURRENTLY node. columns and metadata are taken by value so the node
// can carry them through the analyzer phase without aliasing the AST's
// shared slices.
func NewCreateIndexConcurrently(
	ifNotExists bool,
	schema string,
	table string,
	indexName string,
	unique bool,
	columns []sql.IndexColumn,
	metadata indexmetadata.Metadata,
	createStatement string,
) *CreateIndexConcurrently {
	cols := append([]sql.IndexColumn(nil), columns...)
	return &CreateIndexConcurrently{
		ifNotExists:     ifNotExists,
		schema:          schema,
		table:           table,
		indexName:       indexName,
		unique:          unique,
		columns:         cols,
		metadata:        metadata,
		createStatement: createStatement,
	}
}

// Children implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) Children() []sql.Node { return nil }

// Expressions implements sql.Expressioner.
func (c *CreateIndexConcurrently) Expressions() []sql.Expression {
	return []sql.Expression{c.Runner}
}

// IsReadOnly implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) IsReadOnly() bool { return false }

// Resolved implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) Resolved() bool { return true }

// Schema implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) Schema(*sql.Context) sql.Schema { return nil }

// String implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) String() string { return "CREATE INDEX CONCURRENTLY" }

// WithChildren implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) WithChildren(_ *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (c *CreateIndexConcurrently) WithResolvedChildren(_ context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// WithExpressions implements sql.Expressioner.
func (c *CreateIndexConcurrently) WithExpressions(_ *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(expressions), 1)
	}
	newC := *c
	newC.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newC, nil
}

// RowIter executes the two-phase state machine.
//
// Note: PostgreSQL refuses CREATE INDEX CONCURRENTLY inside an
// explicit transaction block because the build needs its own commits.
// Doltgres does not currently distinguish reliably between "inside an
// explicit BEGIN/COMMIT" and "inside an SQLAlchemy/psycopg3 implicit
// auto-begun transaction" — both set the IgnoreAutoCommit signal
// under realistic ORM connections. Erroring on either would block the
// canonical Alembic CONCURRENTLY migration patterns. We therefore let
// the build proceed and let the inter-phase commit absorb whatever
// transaction state was open. A user who intentionally batches DML
// before CONCURRENTLY in the same explicit transaction will see that
// work flushed by the inter-phase commit; that is the documented
// limitation.
func (c *CreateIndexConcurrently) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, c.schema)
	if err != nil {
		return nil, err
	}
	db, err := indexDDLDatabase(ctx, schemaName, false)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.Errorf(`schema "%s" does not exist`, c.schema)
	}
	table, ok, err := db.GetTableInsensitive(ctx, c.table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(c.table)
	}
	alterable, ok := table.(sql.IndexAlterableTable)
	if !ok {
		return nil, errors.Errorf(`relation "%s" does not support index alteration`, c.table)
	}

	if c.createStatement != "" {
		return c.rowIterWithResolvedCreateStatement(ctx, schemaName)
	}

	// Phase 1: register-with-Building. The planner skips the index
	// (indisready=false, indisvalid=false), while concurrent writers can still
	// commit against the table. If those writers commit before this transaction,
	// Dolt's working-set merge reconciles their row changes with this schema
	// change and the newly built secondary index.
	pendingMetadata := c.metadata
	pendingMetadata.NotReady = true
	pendingMetadata.Invalid = true

	indexDef := sql.IndexDef{
		Name:       c.indexName,
		Columns:    append([]sql.IndexColumn(nil), c.columns...),
		Constraint: sql.IndexConstraint_None,
		Storage:    sql.IndexUsing_BTree,
		Comment:    indexmetadata.EncodeComment(pendingMetadata),
	}
	if c.unique {
		indexDef.Constraint = sql.IndexConstraint_Unique
	}
	if err = alterable.CreateIndex(ctx, indexDef); err != nil {
		if c.ifNotExists && sql.ErrDuplicateKey.Is(err) {
			return sql.RowsToRowIter(), nil
		}
		return nil, err
	}

	// Commit Phase 1 so other sessions can observe the in-progress
	// (indisready=false, indisvalid=false) catalog state. This mirrors
	// PostgreSQL's invariant that CREATE INDEX CONCURRENTLY emits
	// multiple separate transactions; without the commit, every step of
	// the state machine would happen inside one statement-scoped
	// transaction and other sessions would only ever see the final
	// (true, true) result.
	if err = commitInterPhaseTransaction(ctx); err != nil {
		return nil, err
	}

	if testHookBetweenPhases != nil {
		testHookBetweenPhases(ctx)
	}

	// Phase 2: flip the bits via a metadata-only update — the index's
	// prolly tree from Phase 1 is preserved verbatim. This used to be
	// a drop-and-rebuild because the IndexAlterableTable surface had
	// no comment-only path; flipIndexComment goes one level lower
	// (doltdb.Table.UpdateSchema), which the upstream contract
	// documents as data-preserving.
	finalMetadata := c.metadata
	finalMetadata.NotReady = false
	finalMetadata.Invalid = false
	if err = flipIndexComment(ctx, c.schema, c.table, c.indexName, alteredIndexComment(finalMetadata)); err != nil {
		return nil, err
	}
	// Commit Phase 2 explicitly. The auto-commit path that wraps the
	// outer statement would also commit, but Phase 1 already opened a
	// fresh transaction (after our inter-phase commit), and we want
	// the (indisready=true, indisvalid=true) flip durable before this
	// node returns control to the caller.
	if err = commitInterPhaseTransaction(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateIndexConcurrently) rowIterWithResolvedCreateStatement(ctx *sql.Context, schemaName string) (sql.RowIter, error) {
	if c.ifNotExists {
		_, exists, err := locateIndex(ctx, schemaName, c.table, c.indexName, true)
		if err != nil {
			return nil, err
		}
		if exists {
			return sql.RowsToRowIter(), nil
		}
	}

	if c.Runner.Runner == nil {
		return nil, errors.Errorf("statement runner is not available")
	}
	if err := c.runCreateStatement(ctx); err != nil {
		return nil, err
	}

	pendingMetadata := c.metadata
	pendingMetadata.NotReady = true
	pendingMetadata.Invalid = true
	if err := flipIndexComment(ctx, schemaName, c.table, c.indexName, alteredIndexComment(pendingMetadata)); err != nil {
		return nil, err
	}

	if err := commitInterPhaseTransaction(ctx); err != nil {
		return nil, err
	}
	if testHookBetweenPhases != nil {
		testHookBetweenPhases(ctx)
	}

	finalMetadata := c.metadata
	finalMetadata.NotReady = false
	finalMetadata.Invalid = false
	if err := flipIndexComment(ctx, schemaName, c.table, c.indexName, alteredIndexComment(finalMetadata)); err != nil {
		return nil, err
	}
	if err := commitInterPhaseTransaction(ctx); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateIndexConcurrently) runCreateStatement(ctx *sql.Context) error {
	_, err := sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
		_, rowIter, _, err := c.Runner.Runner.QueryWithBindings(subCtx, c.createStatement, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		return sql.RowIterToRows(subCtx, rowIter)
	})
	return err
}

// commitInterPhaseTransaction commits the current transaction and
// starts a fresh one, so the next phase of the CONCURRENTLY state
// machine runs against new transaction state. This matches
// PostgreSQL's CREATE INDEX CONCURRENTLY pattern of emitting multiple
// separate commits during the build.
func commitInterPhaseTransaction(ctx *sql.Context) error {
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return errors.Errorf("session does not implement sql.TransactionSession; cannot drive concurrent index build")
	}
	if err := txSession.CommitTransaction(ctx, tx); err != nil {
		return err
	}
	if _, err := txSession.StartTransaction(ctx, sql.ReadWrite); err != nil {
		return err
	}
	return nil
}
