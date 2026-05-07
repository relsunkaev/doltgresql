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
	"hash/fnv"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
)

// RowLockingPolicy mirrors PostgreSQL's row-locking wait modes:
//
//	Block     - the default; SELECT ... FOR UPDATE blocks until the
//	            holder commits.
//	NoWait    - SELECT ... FOR UPDATE NOWAIT raises immediately if
//	            another session holds the row.
//	SkipLocked - SELECT ... FOR UPDATE SKIP LOCKED elides rows held
//	            by another session and continues with the rest.
type RowLockingPolicy int

const (
	RowLockingPolicyBlock RowLockingPolicy = iota
	RowLockingPolicyNoWait
	RowLockingPolicySkipLocked
)

// RowLockerFuncs supplies the advisory-lock primitives the row-
// locking iterator uses. The functions package owns the underlying
// implementations; injecting them lets the node package stay free
// of an upward dependency on functions and avoids an import cycle
// (functions already imports node).
type RowLockerFuncs struct {
	// Acquire blocks until the lock at lockName is held by the
	// current session. Returns an error if the wait is interrupted.
	Acquire func(ctx *sql.Context, lockName string) error
	// TryAcquire attempts a non-blocking acquire. Returns (true, nil)
	// on success; (false, nil) when the lock is held elsewhere.
	TryAcquire func(ctx *sql.Context, lockName string) (bool, error)
}

// rowLockerFuncs holds the global lock-primitive bindings the row-
// locking iterator dispatches through. Set via SetRowLockerFuncs
// from the engine-initialization path.
var rowLockerFuncs RowLockerFuncs

// SetRowLockerFuncs registers the advisory-lock primitives.
func SetRowLockerFuncs(funcs RowLockerFuncs) {
	rowLockerFuncs = funcs
}

// RowLockingTable wraps a destination table so reads acquire a
// transaction-scoped advisory lock on each (tableOID, primary-key)
// pair before the row is handed back. PostgreSQL's
// `SELECT ... FOR UPDATE` semantics demand that two concurrent
// sessions racing for the same row queue up; doltgres has no
// storage-level row lock, so we synthesize it by piggy-backing on
// the existing pg_advisory_xact_lock infrastructure with a
// deterministic key derived from the row's primary key.
//
// Wait policy is per the PostgreSQL spec:
//
//   - default          -> Acquire (block until released)
//   - NOWAIT           -> TryAcquire; raise on miss
//   - SKIP LOCKED      -> TryAcquire; skip the row on miss
//
// Tables without a primary key surface no key to lock on, so the
// wrapper transparently degrades to the unwrapped read path
// rather than failing.
type RowLockingTable struct {
	underlying sql.Table
	tableOID   uint32
	pkColumns  []int
	policy     RowLockingPolicy
}

var _ sql.TableWrapper = (*RowLockingTable)(nil)
var _ sql.MutableTableWrapper = (*RowLockingTable)(nil)
var _ sql.Table = (*RowLockingTable)(nil)
var _ sql.IndexAddressable = (*RowLockingTable)(nil)
var _ sql.IndexedTable = (*RowLockingTable)(nil)

// WrapRowLockingTable wraps table when its schema declares a
// primary key. tableOID identifies the relation in the lock-key
// hash; without a unique relation identifier two distinct tables
// could collide on the same PK value and serialize each other.
func WrapRowLockingTable(table sql.Table, tableOID uint32, schema sql.Schema, policy RowLockingPolicy) (sql.Table, bool) {
	if _, ok := table.(*RowLockingTable); ok {
		return table, false
	}
	pkCols := make([]int, 0, len(schema))
	for i, col := range schema {
		if col.PrimaryKey {
			pkCols = append(pkCols, i)
		}
	}
	if len(pkCols) == 0 {
		// Without a PK we cannot key the lock to a specific row.
		// PG's default UPDATE behavior in this case is to lock by
		// ctid; we degrade to no locking, which matches doltgres'
		// existing advisory-only semantics for keyless tables.
		return table, false
	}
	if rowLockerFuncs.Acquire == nil || rowLockerFuncs.TryAcquire == nil {
		return table, false
	}
	return &RowLockingTable{
		underlying: table,
		tableOID:   tableOID,
		pkColumns:  pkCols,
		policy:     policy,
	}, true
}

func (t *RowLockingTable) Underlying() sql.Table { return t.underlying }

func (t *RowLockingTable) WithUnderlying(table sql.Table) sql.Table {
	out := *t
	out.underlying = table
	return &out
}

func (t *RowLockingTable) Name() string             { return t.underlying.Name() }
func (t *RowLockingTable) String() string           { return t.underlying.String() }
func (t *RowLockingTable) Schema(c *sql.Context) sql.Schema { return t.underlying.Schema(c) }
func (t *RowLockingTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *RowLockingTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *RowLockingTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	iter, err := t.underlying.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}
	return t.wrap(ctx, iter), nil
}

func (t *RowLockingTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *RowLockingTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		inner := indexAddressable.IndexedAccess(ctx, lookup)
		if inner == nil {
			return nil
		}
		return &rowLockingIndexedTable{RowLockingTable: t, indexed: inner}
	}
	return nil
}

func (t *RowLockingTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *RowLockingTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *RowLockingTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

// rowLockingIndexedTable adapts an IndexedTable so its
// LookupPartitions iterator also runs through the row-locking
// wrapper, picking up rows fetched via index lookups (the common
// case for `SELECT ... WHERE pk = N FOR UPDATE`).
type rowLockingIndexedTable struct {
	*RowLockingTable
	indexed sql.IndexedTable
}

var _ sql.IndexedTable = (*rowLockingIndexedTable)(nil)

func (t *rowLockingIndexedTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	iter, err := t.indexed.PartitionRows(ctx, partition)
	if err != nil {
		return nil, err
	}
	return t.RowLockingTable.wrap(ctx, iter), nil
}

func (t *rowLockingIndexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return t.indexed.LookupPartitions(ctx, lookup)
}

// wrap returns a row iterator that acquires the advisory lock for
// each row's primary key before yielding it.
func (t *RowLockingTable) wrap(ctx *sql.Context, inner sql.RowIter) sql.RowIter {
	return &rowLockingIter{
		ctx:       ctx,
		inner:     inner,
		tableOID:  t.tableOID,
		pkColumns: t.pkColumns,
		policy:    t.policy,
	}
}

type rowLockingIter struct {
	ctx       *sql.Context
	inner     sql.RowIter
	tableOID  uint32
	pkColumns []int
	policy    RowLockingPolicy
}

var _ sql.RowIter = (*rowLockingIter)(nil)

func (r *rowLockingIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := r.inner.Next(ctx)
		if err != nil {
			return nil, err
		}
		key := rowLockName(r.tableOID, row, r.pkColumns)
		switch r.policy {
		case RowLockingPolicyNoWait:
			ok, err := rowLockerFuncs.TryAcquire(ctx, key)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errors.Errorf("could not obtain lock on row in relation %d", r.tableOID)
			}
			return row, nil
		case RowLockingPolicySkipLocked:
			ok, err := rowLockerFuncs.TryAcquire(ctx, key)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			return row, nil
		default:
			if err := rowLockerFuncs.Acquire(ctx, key); err != nil {
				return nil, err
			}
			return row, nil
		}
	}
}

func (r *rowLockingIter) Close(ctx *sql.Context) error {
	return r.inner.Close(ctx)
}

// rowLockName builds the advisory-lock name for a (relationOID, PK)
// pair. The hash includes the OID so two unrelated tables sharing a
// PK value (1, 1) cannot accidentally serialize on each other.
func rowLockName(tableOID uint32, row sql.Row, pkColumns []int) string {
	h := fnv.New64a()
	var oidBytes [4]byte
	oidBytes[0] = byte(tableOID >> 24)
	oidBytes[1] = byte(tableOID >> 16)
	oidBytes[2] = byte(tableOID >> 8)
	oidBytes[3] = byte(tableOID)
	_, _ = h.Write(oidBytes[:])
	for _, idx := range pkColumns {
		if idx >= len(row) {
			continue
		}
		fmt.Fprintf(h, "\x00%v", row[idx])
	}
	// Match the "row:<key>" namespace used by other rowLock callers
	// so a future user-visible advisory lock cannot collide with a
	// row-level FOR UPDATE acquisition.
	return fmt.Sprintf("row:%d", h.Sum64())
}
