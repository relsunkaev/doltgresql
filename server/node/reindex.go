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
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
)

// ReindexIndex validates a REINDEX INDEX target. Most Doltgres indexes are
// maintained eagerly, but JSONB GIN can use this path to migrate v1 posting
// sidecars to v2 chunked postings when the v2 storage gate is enabled.
type ReindexIndex struct {
	schema string
	table  string
	index  string
}

var _ sql.ExecSourceRel = (*ReindexIndex)(nil)
var _ vitess.Injectable = (*ReindexIndex)(nil)

// NewReindexIndex returns a new *ReindexIndex.
func NewReindexIndex(schema string, table string, index string) *ReindexIndex {
	return &ReindexIndex{
		schema: schema,
		table:  table,
		index:  index,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, r.schema, r.table, r.index, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf(`index "%s" does not exist`, r.index)
	}
	schemaName, err := core.GetSchemaName(ctx, nil, r.schema)
	if err != nil {
		return nil, err
	}
	if err = reindexJsonbGinIndexIfRequested(ctx, schemaName, located); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) String() string {
	return "REINDEX INDEX"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *ReindexIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *ReindexIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}

func reindexJsonbGinIndexIfRequested(ctx *sql.Context, schemaName string, located *locatedIndex) error {
	metadata, ok := indexmetadata.DecodeComment(located.index.Comment())
	if !ok || metadata.AccessMethod != indexmetadata.AccessMethodGin || metadata.Gin == nil {
		return nil
	}
	currentVersion := indexmetadata.NormalizeGinPostingStorageVersion(metadata.Gin.PostingStorageVersion)
	targetVersion := defaultJsonbGinPostingStorageVersion()
	if currentVersion == targetVersion {
		return nil
	}
	if currentVersion != indexmetadata.GinPostingStorageVersionV1 || targetVersion != indexmetadata.GinPostingStorageVersionV2 {
		return errors.Errorf("REINDEX migration from JSONB GIN posting storage version %d to %d is not supported", currentVersion, targetVersion)
	}
	return rebuildJsonbGinIndexToV2(ctx, schemaName, located, metadata)
}

func rebuildJsonbGinIndexToV2(ctx *sql.Context, schemaName string, located *locatedIndex, metadata indexmetadata.Metadata) error {
	if len(metadata.Columns) != 1 {
		return errors.Errorf("JSONB GIN REINDEX requires exactly one indexed column")
	}
	opClass := indexmetadata.OpClassJsonbOps
	if len(metadata.OpClasses) > 0 && strings.TrimSpace(metadata.OpClasses[0]) != "" {
		opClass = indexmetadata.NormalizeOpClass(metadata.OpClasses[0])
	}
	tableName := located.index.Table()
	indexName := located.index.ID()
	create := NewCreateJsonbGinIndex(false, schemaName, tableName, indexName, metadata.Columns[0], opClass)
	create.postingStorageVersion = indexmetadata.GinPostingStorageVersionV2
	create.postingChunkName = jsonbGinReindexPostingChunkTableName(metadata.Gin.PostingTable, tableName, indexName)

	columnIndex, anchorColumn, err := create.validateTable(ctx, located.table)
	if err != nil {
		return err
	}
	tableCreator, ok := located.db.(sql.TableCreator)
	if !ok {
		return errors.Errorf(`schema "%s" does not support table creation`, schemaName)
	}
	if err = dropTable(ctx, located.db, create.postingChunkName); err != nil && !sql.ErrTableNotFound.Is(err) {
		return err
	}
	if err = create.createPostingChunkTable(ctx, tableCreator); err != nil {
		return err
	}
	cleanupChunkTable := true
	defer func() {
		if cleanupChunkTable {
			_ = dropTable(ctx, located.db, create.postingChunkName)
		}
	}()
	if err = create.backfillPostingChunkTable(ctx, located.db, schemaName, located.table, columnIndex); err != nil {
		return err
	}
	if err = validateJsonbGinV1ToV2Rebuild(ctx, located.db, metadata.Gin.PostingTable, create.postingChunkName); err != nil {
		return err
	}

	nextMetadata := metadata
	nextMetadata.Gin = &indexmetadata.GinMetadata{
		PostingStorageVersion: indexmetadata.GinPostingStorageVersionV2,
		PostingChunkTable:     create.postingChunkName,
	}
	if err = rebuildLocatedIndexWithMetadata(ctx, located, nextMetadata, []sql.IndexColumn{{Name: anchorColumn}}); err != nil {
		return err
	}
	cleanupChunkTable = false
	if strings.TrimSpace(metadata.Gin.PostingTable) != "" {
		if err = dropTable(ctx, located.db, metadata.Gin.PostingTable); err != nil && !sql.ErrTableNotFound.Is(err) {
			return err
		}
	}
	return nil
}

func jsonbGinReindexPostingChunkTableName(postingTableName string, tableName string, indexName string) string {
	postingTableName = strings.TrimSpace(postingTableName)
	if strings.HasSuffix(postingTableName, "_postings") {
		return strings.TrimSuffix(postingTableName, "_postings") + "_posting_chunks"
	}
	return jsonbgin.PostingChunkTableName(tableName, indexName)
}

func rebuildLocatedIndexWithMetadata(ctx *sql.Context, located *locatedIndex, metadata indexmetadata.Metadata, columns []sql.IndexColumn) error {
	constraint := sql.IndexConstraint_None
	if located.index.IsUnique() {
		constraint = sql.IndexConstraint_Unique
		metadata.Constraint = indexmetadata.ConstraintNone
	}
	indexDef := sql.IndexDef{
		Name:       located.index.ID(),
		Columns:    columns,
		Constraint: constraint,
		Storage:    sql.IndexUsing_BTree,
		Comment:    alteredIndexComment(metadata),
	}
	if err := located.alterable.DropIndex(ctx, located.index.ID()); err != nil {
		return err
	}
	return located.alterable.CreateIndex(ctx, indexDef)
}

func validateJsonbGinV1ToV2Rebuild(ctx *sql.Context, db sql.Database, postingTableName string, postingChunkTableName string) error {
	postingTable, err := tableByInsensitiveName(ctx, db, postingTableName)
	if err != nil {
		return err
	}
	postingChunkTable, err := tableByInsensitiveName(ctx, db, postingChunkTableName)
	if err != nil {
		return err
	}
	v1Counts, err := jsonbGinPostingTokenCounts(ctx, postingTable)
	if err != nil {
		return err
	}
	v2Counts, err := jsonbGinPostingChunkTokenCounts(ctx, postingChunkTable)
	if err != nil {
		return err
	}
	if len(v1Counts) != len(v2Counts) {
		return errors.Errorf("JSONB GIN REINDEX validation failed: v1 has %d token(s), v2 has %d", len(v1Counts), len(v2Counts))
	}
	for token, v1Count := range v1Counts {
		if v2Count := v2Counts[token]; v2Count != v1Count {
			return errors.Errorf("JSONB GIN REINDEX validation failed for token %q: v1 has %d row reference(s), v2 has %d", token, v1Count, v2Count)
		}
	}
	return nil
}

func tableByInsensitiveName(ctx *sql.Context, db sql.Database, tableName string) (sql.Table, error) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, errors.Errorf("JSONB GIN REINDEX missing posting storage table")
	}
	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(tableName)
	}
	return table, nil
}

func jsonbGinPostingTokenCounts(ctx *sql.Context, table sql.Table) (map[string]int64, error) {
	counts := make(map[string]int64)
	err := forEachTableRow(ctx, table, func(row sql.Row) error {
		if len(row) < 2 || row[0] == nil || row[1] == nil {
			return errors.Errorf("malformed JSONB GIN posting row")
		}
		token, ok := row[0].(string)
		if !ok {
			return errors.Errorf("unexpected JSONB GIN posting token type %T", row[0])
		}
		counts[token]++
		return nil
	})
	return counts, err
}

func jsonbGinPostingChunkTokenCounts(ctx *sql.Context, table sql.Table) (map[string]int64, error) {
	counts := make(map[string]int64)
	err := forEachTableRow(ctx, table, func(row sql.Row) error {
		if len(row) < 7 || row[0] == nil || row[6] == nil {
			return errors.Errorf("malformed JSONB GIN posting chunk row")
		}
		token, ok := row[0].(string)
		if !ok {
			return errors.Errorf("unexpected JSONB GIN posting chunk token type %T", row[0])
		}
		rowCount, ok, err := postingChunkRowCountMetadata(ctx, row)
		if err != nil {
			return err
		}
		if !ok {
			payload, err := postingChunkPayloadBytes(ctx, row[6])
			if err != nil {
				return err
			}
			chunk, err := jsonbgin.DecodePostingChunk(payload)
			if err != nil {
				return err
			}
			if err = validatePostingChunkRowMetadata(row, chunk); err != nil {
				return err
			}
			rowCount = int64(len(chunk.RowRefs))
		}
		counts[token] += rowCount
		return nil
	})
	return counts, err
}

func forEachTableRow(ctx *sql.Context, table sql.Table, fn func(sql.Row) error) error {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return err
		}
		for {
			row, err := rows.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = rows.Close(ctx)
				return err
			}
			if err = fn(row); err != nil {
				_ = rows.Close(ctx)
				return err
			}
		}
		if err = rows.Close(ctx); err != nil {
			return err
		}
	}
}

// ReindexTable validates a REINDEX TABLE target.
type ReindexTable struct {
	schema string
	table  string
}

var _ sql.ExecSourceRel = (*ReindexTable)(nil)
var _ vitess.Injectable = (*ReindexTable)(nil)

// NewReindexTable returns a new *ReindexTable.
func NewReindexTable(schema string, table string) *ReindexTable {
	return &ReindexTable{
		schema: schema,
		table:  table,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (r *ReindexTable) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (r *ReindexTable) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (r *ReindexTable) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (r *ReindexTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, r.schema)
	if err != nil {
		return nil, err
	}
	db, err := indexDDLDatabase(ctx, schemaName, false)
	if err != nil {
		return nil, err
	}
	if _, ok, err := db.GetTableInsensitive(ctx, r.table); err != nil {
		return nil, err
	} else if !ok {
		return nil, sql.ErrTableNotFound.New(r.table)
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (r *ReindexTable) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (r *ReindexTable) String() string {
	return "REINDEX TABLE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (r *ReindexTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(r, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (r *ReindexTable) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return r, nil
}
