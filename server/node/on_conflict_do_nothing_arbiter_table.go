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
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// OnConflictDoNothingArbiterTable wraps a destination table for the
// `INSERT ... ON CONFLICT (target) DO NOTHING` shape on tables with
// multiple unique indexes. GMS implements DO NOTHING as INSERT
// IGNORE, which swallows every unique-violation error including
// ones from indexes the user did NOT name as the arbiter.
// PostgreSQL's semantics require that a non-target unique
// violation raise a duplicate-key error rather than be silently
// dropped.
//
// This wrapper pre-checks every non-target unique index on each
// row before the underlying inserter sees it. A hit on a
// non-target index returns a plain (non-UniqueKeyError) error so
// GMS's IGNORE swallow does not catch it. Target-index hits flow
// through to the underlying inserter, which raises the
// UniqueKeyError that DO NOTHING is meant to suppress.
type OnConflictDoNothingArbiterTable struct {
	underlying  sql.Table
	nonTargets  []arbiterIndexCheck
	constraint  string // human-friendly name for the violated index
	schemaWidth int
}

// arbiterIndexCheck records the bits of a non-target unique index
// the wrapper needs to look up a row in it without re-running GMS's
// metadata derivation per call.
type arbiterIndexCheck struct {
	index         sql.Index
	columnIndexes []int
	columnTypes   []sql.Type
	name          string
	predicate     *partialIndexPredicate
}

var _ sql.TableWrapper = (*OnConflictDoNothingArbiterTable)(nil)
var _ sql.MutableTableWrapper = (*OnConflictDoNothingArbiterTable)(nil)
var _ sql.InsertableTable = (*OnConflictDoNothingArbiterTable)(nil)
var _ sql.IndexAddressable = (*OnConflictDoNothingArbiterTable)(nil)
var _ sql.IndexedTable = (*OnConflictDoNothingArbiterTable)(nil)

// WrapOnConflictDoNothingArbiterTable wraps table when the analyzer
// has decided that a multi-unique DO NOTHING needs runtime
// non-target conflict pre-checks. targetIndexNames are the unique
// indexes the user named as the arbiter; non-target uniques are
// any other unique indexes on the table. Returns the unwrapped
// table when nothing to check.
func WrapOnConflictDoNothingArbiterTable(
	ctx *sql.Context,
	table sql.Table,
	targetIndexIDs map[string]struct{},
) (sql.Table, bool, error) {
	if _, ok := table.(*OnConflictDoNothingArbiterTable); ok {
		return table, false, nil
	}
	indexedTable, ok := table.(sql.IndexAddressable)
	if !ok {
		return table, false, nil
	}
	indexes, err := indexedTable.GetIndexes(ctx)
	if err != nil {
		return nil, false, err
	}
	tableSchema := table.Schema(ctx)
	checks := make([]arbiterIndexCheck, 0)
	for _, index := range indexes {
		if !indexmetadata.IsUnique(index) {
			continue
		}
		if _, isTarget := targetIndexIDs[index.ID()]; isTarget {
			continue
		}
		logicalColumns := indexmetadata.LogicalColumns(index, tableSchema)
		if len(logicalColumns) == 0 {
			continue
		}
		colIndexes := make([]int, 0, len(logicalColumns))
		colTypes := make([]sql.Type, 0, len(logicalColumns))
		colTypeMeta := index.ColumnExpressionTypes(ctx)
		for i, column := range logicalColumns {
			if column.Expression {
				return nil, false, errors.Errorf("ON CONFLICT non-target arbiter expression indexes are not yet supported")
			}
			columnName := column.StorageName
			colIdx := tableSchema.IndexOfColName(columnName)
			if colIdx < 0 {
				return nil, false, errors.Errorf("ON CONFLICT non-target arbiter column %q does not exist", columnName)
			}
			colIndexes = append(colIndexes, colIdx)
			if i < len(colTypeMeta) {
				colTypes = append(colTypes, colTypeMeta[i].Type)
			} else {
				colTypes = append(colTypes, tableSchema[colIdx].Type)
			}
		}
		var predicate *partialIndexPredicate
		if indexmetadata.IsPartialUnique(index) {
			predicateText := indexmetadata.Predicate(index.Comment())
			var err error
			predicate, err = parsePartialUniquePredicate(predicateText, table.Name(), tableSchema)
			if err != nil {
				return nil, false, err
			}
		}
		checks = append(checks, arbiterIndexCheck{
			index:         index,
			columnIndexes: colIndexes,
			columnTypes:   colTypes,
			name:          index.ID(),
			predicate:     predicate,
		})
	}
	if len(checks) == 0 {
		return table, false, nil
	}
	return &OnConflictDoNothingArbiterTable{
		underlying:  table,
		nonTargets:  checks,
		schemaWidth: len(tableSchema),
	}, true, nil
}

func (t *OnConflictDoNothingArbiterTable) Underlying() sql.Table {
	return t.underlying
}

func (t *OnConflictDoNothingArbiterTable) WithUnderlying(table sql.Table) sql.Table {
	return &OnConflictDoNothingArbiterTable{
		underlying:  table,
		nonTargets:  t.nonTargets,
		schemaWidth: t.schemaWidth,
	}
}

func (t *OnConflictDoNothingArbiterTable) Name() string {
	return t.underlying.Name()
}

func (t *OnConflictDoNothingArbiterTable) String() string {
	return t.underlying.String()
}

func (t *OnConflictDoNothingArbiterTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *OnConflictDoNothingArbiterTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *OnConflictDoNothingArbiterTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *OnConflictDoNothingArbiterTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *OnConflictDoNothingArbiterTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *OnConflictDoNothingArbiterTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *OnConflictDoNothingArbiterTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.GetIndexes(ctx)
	}
	return nil, nil
}

func (t *OnConflictDoNothingArbiterTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *OnConflictDoNothingArbiterTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func (t *OnConflictDoNothingArbiterTable) Inserter(ctx *sql.Context) sql.RowInserter {
	insertable, ok := t.underlying.(sql.InsertableTable)
	if !ok {
		return sqlutil.NewStaticErrorEditor(errors.Errorf("table %s is not insertable", t.Name()))
	}
	return &onConflictDoNothingArbiterInserter{
		table:   t,
		primary: insertable.Inserter(ctx),
	}
}

// onConflictDoNothingArbiterInserter pre-checks every non-target
// unique index and rejects rows that would violate one before the
// underlying inserter sees them. The rejection error is a plain
// errors value so GMS's INSERT IGNORE handler does not swallow
// it (only sql.UniqueKeyError / sql.PrimaryKeyError are
// swallowed). Target-index conflicts pass through to the
// underlying inserter, which raises the UniqueKeyError that
// IGNORE deliberately suppresses.
type onConflictDoNothingArbiterInserter struct {
	table   *OnConflictDoNothingArbiterTable
	primary sql.RowInserter
}

var _ sql.RowInserter = (*onConflictDoNothingArbiterInserter)(nil)

func (e *onConflictDoNothingArbiterInserter) StatementBegin(ctx *sql.Context) {
	e.primary.StatementBegin(ctx)
}

func (e *onConflictDoNothingArbiterInserter) DiscardChanges(ctx *sql.Context, err error) error {
	return e.primary.DiscardChanges(ctx, err)
}

func (e *onConflictDoNothingArbiterInserter) StatementComplete(ctx *sql.Context) error {
	return e.primary.StatementComplete(ctx)
}

func (e *onConflictDoNothingArbiterInserter) Insert(ctx *sql.Context, row sql.Row) error {
	for _, check := range e.table.nonTargets {
		if check.predicate != nil {
			matches, err := check.predicate.matches(ctx, row)
			if err != nil {
				return err
			}
			if !matches {
				continue
			}
		}
		key, hasNull := extractIndexKey(row, check.columnIndexes)
		if hasNull {
			// PG's default unique indexes treat NULLs as distinct,
			// so a row with a NULL in a key column never collides
			// on that index.
			continue
		}
		hit, err := check.hasMatch(ctx, e.table.underlying, key)
		if err != nil {
			return err
		}
		if hit {
			return errors.Errorf(
				"duplicate key value violates unique constraint %q", check.name)
		}
	}
	return e.primary.Insert(ctx, row)
}

func (e *onConflictDoNothingArbiterInserter) Close(ctx *sql.Context) error {
	return e.primary.Close(ctx)
}

func extractIndexKey(row sql.Row, columnIndexes []int) (sql.Row, bool) {
	key := make(sql.Row, len(columnIndexes))
	for i, colIdx := range columnIndexes {
		if colIdx >= len(row) {
			return nil, true
		}
		if row[colIdx] == nil {
			return nil, true
		}
		key[i] = row[colIdx]
	}
	return key, false
}

func (c arbiterIndexCheck) hasMatch(ctx *sql.Context, table sql.Table, key sql.Row) (bool, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return false, nil
	}
	lookup, err := c.lookup(key)
	if err != nil {
		return false, err
	}
	indexed := indexAddressable.IndexedAccess(ctx, lookup)
	if indexed == nil {
		return false, nil
	}
	partitions, err := indexed.LookupPartitions(ctx, lookup)
	if err != nil {
		return false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		rows, err := indexed.PartitionRows(ctx, partition)
		if err != nil {
			return false, err
		}
		for {
			row, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return false, err
			}
			if c.predicate == nil {
				_ = rows.Close(ctx)
				return true, nil
			}
			matches, err := c.predicate.matches(ctx, row)
			if err != nil {
				_ = rows.Close(ctx)
				return false, err
			}
			if matches {
				_ = rows.Close(ctx)
				return true, nil
			}
		}
		if err := rows.Close(ctx); err != nil {
			return false, err
		}
	}
}

func (c arbiterIndexCheck) lookup(key sql.Row) (sql.IndexLookup, error) {
	ranges := make(sql.MySQLRange, len(key))
	for i, val := range key {
		if i >= len(c.columnTypes) || c.columnTypes[i] == nil {
			return sql.IndexLookup{}, errors.Errorf("missing type for arbiter index column %d", i)
		}
		ranges[i] = sql.ClosedRangeColumnExpr(val, val, c.columnTypes[i])
	}
	return sql.NewIndexLookup(c.index, sql.MySQLRangeCollection{ranges}, true, false, false, false), nil
}
