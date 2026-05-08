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
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// CreateIndexConcurrently is the InjectedStatement that drives PostgreSQL's
// CREATE INDEX CONCURRENTLY two-phase build for plain btree indexes:
//
//  1. **Register**: build the index under (indisready=false, indisvalid=false)
//     so the planner cannot use it and the catalog reflects an in-progress
//     build to any other session that inspects pg_index. Today the build
//     itself is synchronous because Dolt has no comment-only update path —
//     the value of this phase is the catalog state, not concurrency.
//  2. **Flip**: drop and recreate the index with the build-state bits
//     cleared, surfacing it under steady-state (indisready=true,
//     indisvalid=true) where the planner picks it up.
//
// The double-build cost is the deliberate stepping-stone trade-off: the
// state machine becomes observable to outside sessions today, and Phase 3
// will replace step 2 with a Dolt-side metadata-only flip once that API
// lands. Workloads that emit CREATE INDEX CONCURRENTLY (Drizzle Kit,
// Alembic, Prisma, Rails) get correct catalog semantics now; the build
// performance characteristics improve later without a re-spec of the SQL
// surface.
//
// Edge cases that fall back to a regular synchronous CREATE INDEX (no
// state-machine wrapping) are handled at the AST level — GIN, expression
// indexes, partial indexes, INCLUDE columns, and unique-with-expression
// shapes all keep their existing build path.
type CreateIndexConcurrently struct {
	ifNotExists bool
	schema      string
	table       string
	indexName   string
	unique      bool
	columns     []sql.IndexColumn
	metadata    indexmetadata.Metadata
}

var _ sql.ExecSourceRel = (*CreateIndexConcurrently)(nil)
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
) *CreateIndexConcurrently {
	cols := append([]sql.IndexColumn(nil), columns...)
	return &CreateIndexConcurrently{
		ifNotExists: ifNotExists,
		schema:      schema,
		table:       table,
		indexName:   indexName,
		unique:      unique,
		columns:     cols,
		metadata:    metadata,
	}
}

// Children implements sql.ExecSourceRel.
func (c *CreateIndexConcurrently) Children() []sql.Node { return nil }

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

// RowIter executes the two-phase state machine.
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

	// Phase 1: register-with-Building. Writers maintain the index, but
	// the planner skips it (indisready=false, indisvalid=false). In real
	// PostgreSQL this is a 3-step dance — register pending, wait, flip
	// indisready, wait, build, flip indisvalid — but the wait phases
	// exist to drain transactions started under the old visibility, and
	// doltgres does not expose the same multi-version snapshot view, so
	// we collapse them into a single "register-and-build under both bits
	// false" step.
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

	// Phase 2: flip the bits. Re-locate (the schema mutation may have
	// rewrapped the underlying table reference) and rebuild with the
	// state-machine bits cleared so the index becomes planner-visible.
	located, ok, err := locateIndex(ctx, c.schema, c.table, c.indexName, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf(`could not relocate index "%s" after concurrent build`, c.indexName)
	}
	finalMetadata := c.metadata
	finalMetadata.NotReady = false
	finalMetadata.Invalid = false
	if err = rebuildIndexWithMetadata(ctx, located, finalMetadata); err != nil {
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
