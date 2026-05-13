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
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/deferrable"
)

// ContextRootFinalizer is a node that finalizes any changes persisted within the context.
type ContextRootFinalizer struct {
	child sql.Node
}

var _ sql.DebugStringer = (*ContextRootFinalizer)(nil)
var _ sql.ExecBuilderNode = (*ContextRootFinalizer)(nil)

// NewContextRootFinalizer returns a new *ContextRootFinalizer.
func NewContextRootFinalizer(child sql.Node) *ContextRootFinalizer {
	return &ContextRootFinalizer{
		child: child,
	}
}

// Child returns the single child of this node
func (rf *ContextRootFinalizer) Child() sql.Node {
	return rf.child
}

// Children implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) Children() []sql.Node {
	return []sql.Node{rf.child}
}

// DebugString implements the interface sql.DebugStringer.
func (rf *ContextRootFinalizer) DebugString(ctx *sql.Context) string {
	return sql.DebugString(ctx, rf.child)
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) Resolved() bool {
	return rf.child.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	iter := &rootFinalizerIter{
		clearContextAfterClose: clearsContextOnSuccess(rf.child),
		useStatementSavepoint:  usesStatementSavepoint(rf.child),
	}
	if err := iter.ensureStatementSavepoint(ctx); err != nil {
		iter.hadErr = true
		iter.err = err
		return nil, err
	}
	childIter, err := b.Build(ctx, rf.child, r)
	if err != nil {
		err = iter.rollbackStatementSavepoint(ctx, err)
		deferrable.DiscardPendingForeignKeys(ctx)
		core.ClearContextValues(ctx)
		return nil, err
	}
	if childIter == nil {
		childIter = sql.RowsToRowIter()
	}
	iter.childIter = childIter
	return iter, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) Schema(ctx *sql.Context) sql.Schema {
	return rf.child.Schema(ctx)
}

// String implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) String() string {
	return rf.child.String()
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (rf *ContextRootFinalizer) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(rf, len(children), 1)
	}
	return NewContextRootFinalizer(children[0]), nil
}

// rootFinalizerIter is the iterator for *ContextRootFinalizer that finalizes the context.
type rootFinalizerIter struct {
	childIter              sql.RowIter
	clearContextAfterClose bool
	useStatementSavepoint  bool
	savepointInitialized   bool
	savepointName          string
	savepointTx            sql.Transaction
	hadErr                 bool
	err                    error
}

var _ sql.MutableRowIter = (*rootFinalizerIter)(nil)

var statementSavepointCounter uint64

// Next implements the interface sql.RowIter.
func (r *rootFinalizerIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := r.ensureStatementSavepoint(ctx); err != nil {
		r.hadErr = true
		r.err = err
		return nil, err
	}
	row, err := r.childIter.Next(ctx)
	if err != nil && err != io.EOF {
		r.hadErr = true
		r.err = err
	}
	return row, err
}

// Close implements the interface sql.RowIter.
func (r *rootFinalizerIter) Close(ctx *sql.Context) error {
	err := r.childIter.Close(ctx)
	if r.hadErr {
		if err == nil {
			err = r.err
		}
		err = r.rollbackStatementSavepoint(ctx, err)
		deferrable.DiscardPendingForeignKeys(ctx)
		core.ClearContextValues(ctx)
		return err
	}
	if err != nil {
		err = r.rollbackStatementSavepoint(ctx, err)
		_ = deferrable.FlushPendingForeignKeys(ctx)
		_ = core.CloseContextRootFinalizer(ctx)
		return err
	}
	if r.clearContextAfterClose {
		if err := r.releaseStatementSavepoint(ctx); err != nil {
			return err
		}
		deferrable.DiscardPendingForeignKeys(ctx)
		core.ClearContextValues(ctx)
		return nil
	}
	if err := deferrable.FlushPendingForeignKeys(ctx); err != nil {
		err = r.rollbackStatementSavepoint(ctx, err)
		core.ClearContextValues(ctx)
		return err
	}
	if err := core.CloseContextRootFinalizer(ctx); err != nil {
		return r.rollbackStatementSavepoint(ctx, err)
	}
	return r.releaseStatementSavepoint(ctx)
}

// GetChildIter implements the interface sql.CustomRowIter.
func (r *rootFinalizerIter) GetChildIter() sql.RowIter {
	return r.childIter
}

// WithChildIter implements the interface sql.CustomRowIter.
func (r *rootFinalizerIter) WithChildIter(childIter sql.RowIter) sql.RowIter {
	nr := *r
	nr.childIter = childIter
	return &nr
}

func (r *rootFinalizerIter) ensureStatementSavepoint(ctx *sql.Context) error {
	if r.savepointInitialized {
		return nil
	}
	r.savepointInitialized = true
	if !r.useStatementSavepoint {
		return nil
	}
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil
	}
	r.savepointName = fmt.Sprintf("__doltgresql_statement_%d", atomic.AddUint64(&statementSavepointCounter, 1))
	r.savepointTx = tx
	return txSession.CreateSavepoint(ctx, tx, r.savepointName)
}

func (r *rootFinalizerIter) rollbackStatementSavepoint(ctx *sql.Context, err error) error {
	if r.savepointName == "" {
		return err
	}
	savepoint := r.savepointName
	r.savepointName = ""
	savepointTx := r.savepointTx
	r.savepointTx = nil
	tx := ctx.GetTransaction()
	if tx == nil {
		return err
	}
	if tx != savepointTx {
		return err
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return err
	}
	if rollbackErr := txSession.RollbackToSavepoint(ctx, tx, savepoint); rollbackErr != nil {
		if err != nil {
			return fmt.Errorf("%w; rollback to statement savepoint failed: %v", err, rollbackErr)
		}
		return rollbackErr
	}
	if releaseErr := txSession.ReleaseSavepoint(ctx, tx, savepoint); releaseErr != nil {
		if err != nil {
			return fmt.Errorf("%w; release statement savepoint failed: %v", err, releaseErr)
		}
		return releaseErr
	}
	return err
}

func (r *rootFinalizerIter) releaseStatementSavepoint(ctx *sql.Context) error {
	if r.savepointName == "" {
		return nil
	}
	savepoint := r.savepointName
	r.savepointName = ""
	savepointTx := r.savepointTx
	r.savepointTx = nil
	tx := ctx.GetTransaction()
	if tx == nil {
		return nil
	}
	if tx != savepointTx {
		return nil
	}
	txSession, ok := ctx.Session.(sql.TransactionSession)
	if !ok {
		return nil
	}
	return txSession.ReleaseSavepoint(ctx, tx, savepoint)
}

func clearsContextOnSuccess(child sql.Node) bool {
	switch child.(type) {
	case *plan.Rollback, *plan.RollbackSavepoint:
		return true
	default:
		return strings.HasPrefix(strings.ToUpper(child.String()), "ROLLBACK")
	}
}

func usesStatementSavepoint(child sql.Node) bool {
	switch child.(type) {
	case *plan.Commit, *plan.Rollback, *plan.StartTransaction, *plan.CreateSavepoint, *plan.RollbackSavepoint, *plan.ReleaseSavepoint, *RenameDatabase:
		return false
	default:
		return true
	}
}
