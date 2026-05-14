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
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// PostgresForeignKeyInsertHandler preserves GMS foreign-key handling while
// matching PostgreSQL's duplicate-key-before-FK error ordering for INSERT.
type PostgresForeignKeyInsertHandler struct {
	*plan.ForeignKeyHandler
	primaryCheck *postgresForeignKeyPrimaryCheck
	uniqueChecks []postgresForeignKeyUniqueCheck
}

type postgresForeignKeyPrimaryCheck struct {
	columnIndexes []int
	columnTypes   []sql.Type
	name          string
}

type postgresForeignKeyUniqueCheck struct {
	arbiterIndexCheck
	primary bool
}

var _ sql.Node = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.Table = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.InsertableTable = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.ReplaceableTable = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.UpdatableTable = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.DeletableTable = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.TableEditor = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.RowInserter = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.RowUpdater = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.RowDeleter = (*PostgresForeignKeyInsertHandler)(nil)
var _ sql.TableWrapper = (*PostgresForeignKeyInsertHandler)(nil)

// WrapPostgresForeignKeyInsertHandler wraps FK handlers that can pre-check
// simple unique indexes before validating foreign-key references.
func WrapPostgresForeignKeyInsertHandler(ctx *sql.Context, handler *plan.ForeignKeyHandler) (*PostgresForeignKeyInsertHandler, bool, error) {
	primaryCheck := postgresForeignKeyPrimaryCheckForTable(ctx, handler.Table)
	checks, err := postgresForeignKeyUniqueChecks(ctx, handler.Table, handler.Schema(ctx))
	if err != nil {
		return nil, false, err
	}
	if primaryCheck == nil && len(checks) == 0 {
		return nil, false, nil
	}
	return &PostgresForeignKeyInsertHandler{
		ForeignKeyHandler: handler,
		primaryCheck:      primaryCheck,
		uniqueChecks:      checks,
	}, true, nil
}

func postgresForeignKeyPrimaryCheckForTable(ctx *sql.Context, table sql.Table) *postgresForeignKeyPrimaryCheck {
	pkTable, ok := table.(sql.PrimaryKeyTable)
	if !ok {
		return nil
	}
	pkSchema := pkTable.PrimaryKeySchema(ctx)
	if len(pkSchema.PkOrdinals) == 0 {
		return nil
	}
	name := ""
	if commented, ok := table.(sql.CommentedTable); ok {
		name = tablemetadata.PrimaryKeyConstraintName(commented.Comment())
	}
	if strings.TrimSpace(name) == "" {
		name = table.Name() + "_pkey"
	}
	columnTypes := make([]sql.Type, len(pkSchema.PkOrdinals))
	for i, ordinal := range pkSchema.PkOrdinals {
		if ordinal >= 0 && ordinal < len(pkSchema.Schema) {
			columnTypes[i] = pkSchema.Schema[ordinal].Type
		}
	}
	return &postgresForeignKeyPrimaryCheck{
		columnIndexes: pkSchema.PkOrdinals,
		columnTypes:   columnTypes,
		name:          name,
	}
}

func postgresForeignKeyUniqueChecks(ctx *sql.Context, table sql.Table, tableSchema sql.Schema) ([]postgresForeignKeyUniqueCheck, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return nil, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	checks := make([]postgresForeignKeyUniqueCheck, 0)
indexLoop:
	for _, index := range indexes {
		if !indexmetadata.IsUnique(index) {
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
				continue indexLoop
			}
			columnName := column.StorageName
			colIdx := tableSchema.IndexOfColName(columnName)
			if colIdx < 0 {
				return nil, sql.ErrKeyColumnDoesNotExist.New(columnName)
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
				return nil, err
			}
		}
		checks = append(checks, postgresForeignKeyUniqueCheck{
			arbiterIndexCheck: arbiterIndexCheck{
				index:         index,
				columnIndexes: colIndexes,
				columnTypes:   colTypes,
				name:          index.ID(),
				predicate:     predicate,
			},
			primary: indexmetadata.IsPrimaryConstraint(index),
		})
	}
	return checks, nil
}

// WithChildren implements sql.Node and preserves the PostgreSQL wrapper if
// later analyzer rules replace the wrapped child.
func (n *PostgresForeignKeyInsertHandler) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	copied := *n.ForeignKeyHandler
	copied.OriginalNode = children[0]
	return &PostgresForeignKeyInsertHandler{
		ForeignKeyHandler: &copied,
		primaryCheck:      n.primaryCheck,
		uniqueChecks:      n.uniqueChecks,
	}, nil
}

// Inserter implements sql.InsertableTable.
func (n *PostgresForeignKeyInsertHandler) Inserter(*sql.Context) sql.RowInserter {
	return n
}

// Insert implements sql.RowInserter.
func (n *PostgresForeignKeyInsertHandler) Insert(ctx *sql.Context, row sql.Row) error {
	for _, reference := range n.Editor.References {
		if err := reference.CheckReference(ctx, row); err != nil {
			if uniqueErr := n.checkUniqueConflicts(ctx, row); uniqueErr != nil {
				return uniqueErr
			}
			return err
		}
	}
	return n.Editor.Editor.Insert(ctx, row)
}

func (n *PostgresForeignKeyInsertHandler) checkUniqueConflicts(ctx *sql.Context, row sql.Row) error {
	if n.primaryCheck != nil {
		key, hasNull := n.primaryCheck.key(row)
		if !hasNull {
			existing, ok, err := n.primaryCheck.firstMatch(ctx, n.Table, key)
			if err != nil {
				return err
			}
			if ok {
				return sql.NewUniqueKeyErr(
					fmt.Sprintf("duplicate key value violates unique constraint %q", n.primaryCheck.name),
					true,
					existing,
				)
			}
		}
	}
	for _, check := range n.uniqueChecks {
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
			continue
		}
		existing, ok, err := check.firstMatch(ctx, n.Table, key)
		if err != nil {
			return err
		}
		if ok {
			return sql.NewUniqueKeyErr(
				fmt.Sprintf("duplicate key value violates unique constraint %q", check.name),
				check.primary,
				existing,
			)
		}
	}
	return nil
}

func (c postgresForeignKeyPrimaryCheck) key(row sql.Row) (sql.Row, bool) {
	key := make(sql.Row, len(c.columnIndexes))
	for i, colIdx := range c.columnIndexes {
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

func (c postgresForeignKeyPrimaryCheck) firstMatch(ctx *sql.Context, table sql.Table, key sql.Row) (sql.Row, bool, error) {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, false, err
		}
		for {
			matchedRow, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return nil, false, err
			}
			matches, err := c.rowMatchesKey(ctx, matchedRow, key)
			if err != nil {
				_ = rows.Close(ctx)
				return nil, false, err
			}
			if matches {
				_ = rows.Close(ctx)
				return matchedRow, true, nil
			}
		}
		if err := rows.Close(ctx); err != nil {
			return nil, false, err
		}
	}
}

func (c postgresForeignKeyPrimaryCheck) rowMatchesKey(ctx *sql.Context, row sql.Row, key sql.Row) (bool, error) {
	for i, colIdx := range c.columnIndexes {
		if colIdx >= len(row) || i >= len(key) {
			return false, nil
		}
		if i >= len(c.columnTypes) || c.columnTypes[i] == nil {
			return false, nil
		}
		cmp, err := c.columnTypes[i].Compare(ctx, row[colIdx], key[i])
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (c postgresForeignKeyUniqueCheck) firstMatch(ctx *sql.Context, table sql.Table, key sql.Row) (sql.Row, bool, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return nil, false, nil
	}
	lookup, err := c.lookup(key)
	if err != nil {
		return nil, false, err
	}
	indexed := indexAddressable.IndexedAccess(ctx, lookup)
	if indexed == nil {
		return nil, false, nil
	}
	partitions, err := indexed.LookupPartitions(ctx, lookup)
	if err != nil {
		return nil, false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		rows, err := indexed.PartitionRows(ctx, partition)
		if err != nil {
			return nil, false, err
		}
		for {
			matchedRow, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return nil, false, err
			}
			if c.predicate == nil {
				_ = rows.Close(ctx)
				return matchedRow, true, nil
			}
			matches, err := c.predicate.matches(ctx, matchedRow)
			if err != nil {
				_ = rows.Close(ctx)
				return nil, false, err
			}
			if matches {
				_ = rows.Close(ctx)
				return matchedRow, true, nil
			}
		}
		if err := rows.Close(ctx); err != nil {
			return nil, false, err
		}
	}
}
