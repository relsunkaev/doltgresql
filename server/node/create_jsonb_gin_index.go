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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	doltschema "github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const jsonbGinPostingTableComment = "Doltgres JSONB GIN posting storage"
const jsonbGinPostingChunkTableComment = "Doltgres JSONB GIN posting chunk storage"
const jsonbGinPostingBackfillChunkRows = 8192
const jsonbGinPostingChunkRowsPerChunk = 256
const jsonbGinPostingStorageVersionEnv = "DOLTGRES_JSONB_GIN_POSTING_STORAGE_VERSION"

// CreateJsonbGinIndex handles CREATE INDEX USING gin for JSONB columns.
type CreateJsonbGinIndex struct {
	ifNotExists              bool
	schema                   string
	tableName                string
	indexName                string
	columnName               string
	opClass                  string
	postingName              string
	postingChunkName         string
	postingStorageVersion    int
	postingChunkRowsPerChunk int
}

var _ sql.ExecSourceRel = (*CreateJsonbGinIndex)(nil)
var _ vitess.Injectable = (*CreateJsonbGinIndex)(nil)

// NewCreateJsonbGinIndex returns a new *CreateJsonbGinIndex.
func NewCreateJsonbGinIndex(ifNotExists bool, schema string, tableName string, indexName string, columnName string, opClass string) *CreateJsonbGinIndex {
	return &CreateJsonbGinIndex{
		ifNotExists:           ifNotExists,
		schema:                schema,
		tableName:             tableName,
		indexName:             indexName,
		columnName:            columnName,
		opClass:               indexmetadata.NormalizeOpClass(opClass),
		postingName:           jsonbgin.PostingTableName(tableName, indexName),
		postingChunkName:      jsonbgin.PostingChunkTableName(tableName, indexName),
		postingStorageVersion: defaultJsonbGinPostingStorageVersion(),
	}
}

func defaultJsonbGinPostingStorageVersion() int {
	if strings.TrimSpace(os.Getenv(jsonbGinPostingStorageVersionEnv)) == "2" {
		return indexmetadata.GinPostingStorageVersionV2
	}
	return indexmetadata.GinPostingStorageVersionV1
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, c.schema)
	if err != nil {
		return nil, err
	}
	db, err := schemaDatabase(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: c.tableName, Schema: schemaName})
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, errors.Errorf(`relation "%s" does not exist`, c.tableName)
	}
	if c.ifNotExists {
		exists, err := indexExists(ctx, table, c.indexName)
		if err != nil {
			return nil, err
		}
		if exists {
			return sql.RowsToRowIter(), nil
		}
	}

	columnIndex, anchorColumn, err := c.validateTable(ctx, table)
	if err != nil {
		return nil, err
	}
	alterable, ok := table.(sql.IndexAlterableTable)
	if !ok {
		return nil, errors.Errorf(`relation "%s" does not support indexes`, c.tableName)
	}
	tableCreator, ok := db.(sql.TableCreator)
	if !ok {
		return nil, errors.Errorf(`schema "%s" does not support table creation`, schemaName)
	}

	metadata := c.indexMetadata()
	if err = alterable.CreateIndex(ctx, sql.IndexDef{
		Name:       c.indexName,
		Comment:    indexmetadata.EncodeComment(metadata),
		Columns:    []sql.IndexColumn{{Name: anchorColumn}},
		Constraint: sql.IndexConstraint_None,
		Storage:    sql.IndexUsing_BTree,
	}); err != nil {
		return nil, err
	}
	if err = c.createPostingStorageTables(ctx, tableCreator, table.Schema(ctx)); err != nil {
		_ = alterable.DropIndex(ctx, c.indexName)
		return nil, err
	}
	postingStorageVersion := indexmetadata.NormalizeGinPostingStorageVersion(c.postingStorageVersion)
	switch postingStorageVersion {
	case indexmetadata.GinPostingStorageVersionV1:
		err = c.backfillPostingTable(ctx, db, schemaName, table, columnIndex)
	case indexmetadata.GinPostingStorageVersionV2:
		err = c.backfillPostingChunkTable(ctx, db, schemaName, table, columnIndex)
	default:
		err = errors.Errorf("unsupported JSONB GIN posting storage version %d", postingStorageVersion)
	}
	if err != nil {
		_ = dropTable(ctx, db, c.postingName)
		_ = dropTable(ctx, db, c.postingChunkName)
		_ = alterable.DropIndex(ctx, c.indexName)
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateJsonbGinIndex) indexMetadata() indexmetadata.Metadata {
	storageVersion := indexmetadata.NormalizeGinPostingStorageVersion(c.postingStorageVersion)
	metadata := indexmetadata.Metadata{
		AccessMethod: indexmetadata.AccessMethodGin,
		Columns:      []string{c.columnName},
		OpClasses:    []string{c.opClass},
		Gin: &indexmetadata.GinMetadata{
			PostingStorageVersion: storageVersion,
		},
	}
	switch storageVersion {
	case indexmetadata.GinPostingStorageVersionV1:
		metadata.Gin.PostingTable = c.postingName
	case indexmetadata.GinPostingStorageVersionV2:
		metadata.Gin.PostingChunkTable = c.postingChunkName
	}
	return metadata
}

func schemaDatabase(ctx *sql.Context, schemaName string) (sql.Database, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.Errorf("database not found")
	}
	schemaDb, ok := db.(sql.SchemaDatabase)
	if !ok {
		return db, nil
	}
	db, ok, err = schemaDb.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf(`schema "%s" does not exist`, schemaName)
	}
	return db, nil
}

func indexExists(ctx *sql.Context, table sql.Table, indexName string) (bool, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return false, err
	}
	for _, index := range indexes {
		if strings.EqualFold(index.ID(), indexName) {
			return true, nil
		}
	}
	return false, nil
}

func (c *CreateJsonbGinIndex) validateTable(ctx *sql.Context, table sql.Table) (int, string, error) {
	if !indexmetadata.IsSupportedGinJsonbOpClass(c.opClass) {
		return -1, "", errors.Errorf("operator class %s is not yet supported for gin indexes", c.opClass)
	}
	sch := table.Schema(ctx)
	columnIndex := sch.IndexOfColName(c.columnName)
	if columnIndex < 0 {
		return -1, "", errors.Errorf(`column "%s" of relation "%s" does not exist`, c.columnName, c.tableName)
	}
	dgType, ok := sch[columnIndex].Type.(*pgtypes.DoltgresType)
	if !ok || dgType.ID != pgtypes.JsonB.ID {
		return -1, "", errors.Errorf(`gin indexes are only supported on jsonb columns`)
	}
	for _, column := range sch {
		if column.PrimaryKey {
			return columnIndex, column.Name, nil
		}
	}
	for _, column := range sch {
		if column.Name != c.columnName {
			return columnIndex, column.Name, nil
		}
	}
	return columnIndex, c.columnName, nil
}

func (c *CreateJsonbGinIndex) createPostingTable(ctx *sql.Context, tableCreator sql.TableCreator, baseSchema sql.Schema) error {
	schema := sql.Schema{
		{
			Name:       "token",
			Source:     c.postingName,
			Type:       pgtypes.Text,
			PrimaryKey: true,
			Nullable:   false,
		},
		{
			Name:       "row_id",
			Source:     c.postingName,
			Type:       pgtypes.Text,
			PrimaryKey: true,
			Nullable:   false,
		},
	}
	for i, column := range baseSchema {
		if !column.PrimaryKey {
			continue
		}
		schema = append(schema, &sql.Column{
			Name:     fmt.Sprintf("pk_%d", i),
			Source:   c.postingName,
			Type:     column.Type,
			Nullable: column.Nullable,
		})
	}
	postingSchema := sql.NewPrimaryKeySchema(schema)
	return tableCreator.CreateTable(ctx, c.postingName, postingSchema, sql.Collation_Default, jsonbGinPostingTableComment)
}

func (c *CreateJsonbGinIndex) createPostingStorageTables(ctx *sql.Context, tableCreator sql.TableCreator, baseSchema sql.Schema) error {
	switch indexmetadata.NormalizeGinPostingStorageVersion(c.postingStorageVersion) {
	case indexmetadata.GinPostingStorageVersionV1:
		return c.createPostingTable(ctx, tableCreator, baseSchema)
	case indexmetadata.GinPostingStorageVersionV2:
		return c.createPostingChunkTable(ctx, tableCreator)
	default:
		return errors.Errorf("unsupported JSONB GIN posting storage version %d", c.postingStorageVersion)
	}
}

func (c *CreateJsonbGinIndex) createPostingChunkTable(ctx *sql.Context, tableCreator sql.TableCreator) error {
	schema := sql.Schema{
		{
			Name:       "token",
			Source:     c.postingChunkName,
			Type:       pgtypes.Text,
			PrimaryKey: true,
			Nullable:   false,
		},
		{
			Name:       "chunk_no",
			Source:     c.postingChunkName,
			Type:       pgtypes.Int64,
			PrimaryKey: true,
			Nullable:   false,
		},
		{
			Name:     "format_version",
			Source:   c.postingChunkName,
			Type:     pgtypes.Int16,
			Nullable: false,
		},
		{
			Name:     "row_count",
			Source:   c.postingChunkName,
			Type:     pgtypes.Int32,
			Nullable: false,
		},
		{
			Name:   "first_row_ref",
			Source: c.postingChunkName,
			Type:   pgtypes.Bytea,
		},
		{
			Name:   "last_row_ref",
			Source: c.postingChunkName,
			Type:   pgtypes.Bytea,
		},
		{
			Name:     "payload",
			Source:   c.postingChunkName,
			Type:     pgtypes.Bytea,
			Nullable: false,
		},
		{
			Name:   "checksum",
			Source: c.postingChunkName,
			Type:   pgtypes.Bytea,
		},
	}
	postingSchema := sql.NewPrimaryKeySchema(schema)
	return tableCreator.CreateTable(ctx, c.postingChunkName, postingSchema, sql.Collation_Default, jsonbGinPostingChunkTableComment)
}

func (c *CreateJsonbGinIndex) backfillPostingTable(ctx *sql.Context, db sql.Database, schemaName string, table sql.Table, columnIndex int) error {
	postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: c.postingName, Schema: schemaName})
	if err != nil {
		return err
	}
	if postingTable == nil {
		return errors.Errorf(`posting table "%s" was not created`, c.postingName)
	}
	postingRows, err := newJsonbGinPostingRowSink(ctx, db, postingTable, jsonbGinPostingBackfillChunkRows)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = postingRows.Close(ctx)
		}
	}()
	completed := false
	defer func() {
		if !completed {
			_ = postingRows.Discard(ctx, errors.New("JSONB GIN backfill failed"))
		}
	}()

	partitions, err := table.Partitions(ctx)
	if err != nil {
		return err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return err
		}
		if err = c.backfillPartition(ctx, table.Schema(ctx), rows, postingRows, columnIndex); err != nil {
			_ = rows.Close(ctx)
			return err
		}
		if err = rows.Close(ctx); err != nil {
			return err
		}
	}
	if err = postingRows.Complete(ctx); err != nil {
		return err
	}
	completed = true
	closed = true
	return postingRows.Close(ctx)
}

func (c *CreateJsonbGinIndex) backfillPostingChunkTable(ctx *sql.Context, db sql.Database, schemaName string, table sql.Table, columnIndex int) error {
	postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: c.postingChunkName, Schema: schemaName})
	if err != nil {
		return err
	}
	if postingTable == nil {
		return errors.Errorf(`posting chunk table "%s" was not created`, c.postingChunkName)
	}
	if _, ok := postingTable.(sql.InsertableTable); !ok {
		return errors.Errorf(`posting chunk table "%s" does not support inserts`, c.postingChunkName)
	}
	store, err := c.buildPostingChunkStoreFromTable(ctx, table, columnIndex)
	if err != nil {
		return err
	}
	sink, err := newJsonbGinPostingChunkRowSink(ctx, db, postingTable)
	if err != nil {
		return err
	}
	completed := false
	defer func() {
		if !completed {
			_ = sink.Discard(ctx, errors.New("JSONB GIN posting chunk backfill failed"))
		}
		_ = sink.Close(ctx)
	}()
	if err = c.writePostingChunkRows(ctx, store, sink); err != nil {
		return err
	}
	if err = sink.Complete(ctx); err != nil {
		return err
	}
	completed = true
	return nil
}

func (c *CreateJsonbGinIndex) buildPostingChunkRowsFromTable(ctx *sql.Context, table sql.Table, columnIndex int) ([]sql.Row, error) {
	store, err := c.buildPostingChunkStoreFromTable(ctx, table, columnIndex)
	if err != nil {
		return nil, err
	}
	return c.materializePostingChunkRows(store)
}

func (c *CreateJsonbGinIndex) buildPostingChunkStoreFromTable(ctx *sql.Context, table sql.Table, columnIndex int) (*jsonbgin.ChunkedPostingStore, error) {
	store := jsonbgin.NewChunkedPostingStore(c.postingChunkRowsPerChunkLimit())
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, err
		}
		if err = c.addPostingChunkRows(ctx, table.Schema(ctx), rows, columnIndex, store); err != nil {
			_ = rows.Close(ctx)
			return nil, err
		}
		if err = rows.Close(ctx); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (c *CreateJsonbGinIndex) buildPostingChunkRows(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, columnIndex int) ([]sql.Row, error) {
	store := jsonbgin.NewChunkedPostingStore(c.postingChunkRowsPerChunkLimit())
	if err := c.addPostingChunkRows(ctx, sch, rows, columnIndex, store); err != nil {
		return nil, err
	}
	return c.materializePostingChunkRows(store)
}

func (c *CreateJsonbGinIndex) addPostingChunkRows(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, columnIndex int, store *jsonbgin.ChunkedPostingStore) error {
	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if row[columnIndex] == nil {
			continue
		}
		rowRef, err := jsonbGinPostingRowReference(ctx, sch, row)
		if err != nil {
			return err
		}
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, row[columnIndex])
		if err != nil {
			return err
		}
		tokens, err := jsonbgin.Extract(doc, c.opClass)
		if err != nil {
			return err
		}
		if err = store.Add(rowRef.Bytes, tokens); err != nil {
			return err
		}
	}
}

func (c *CreateJsonbGinIndex) materializePostingChunkRows(store *jsonbgin.ChunkedPostingStore) ([]sql.Row, error) {
	entries, err := store.ChunkEntries()
	if err != nil {
		return nil, err
	}
	rows := make([]sql.Row, len(entries))
	for i, entry := range entries {
		rows[i] = postingChunkEntryRow(entry)
	}
	return rows, nil
}

func (c *CreateJsonbGinIndex) writePostingChunkRows(ctx *sql.Context, store *jsonbgin.ChunkedPostingStore, sink jsonbGinPostingRowSink) error {
	entries, err := store.ChunkEntries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = sink.Add(ctx, postingChunkEntryRow(entry)); err != nil {
			return err
		}
	}
	return nil
}

func postingChunkEntryRow(entry jsonbgin.PostingChunkEntry) sql.Row {
	chunk := entry.Chunk
	return sql.Row{
		entry.Token,
		entry.ChunkNo,
		int16(chunk.FormatVersion),
		int32(chunk.RowCount),
		chunk.FirstRowRef,
		chunk.LastRowRef,
		chunk.Payload,
		postingChunkChecksumBytes(chunk.Checksum),
	}
}

func (c *CreateJsonbGinIndex) postingChunkRowsPerChunkLimit() int {
	if c.postingChunkRowsPerChunk > 0 {
		return c.postingChunkRowsPerChunk
	}
	return jsonbGinPostingChunkRowsPerChunk
}

func postingChunkChecksumBytes(checksum uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, checksum)
	return buf
}

type jsonbGinPostingRowAppender interface {
	Add(ctx *sql.Context, row sql.Row) error
}

func (c *CreateJsonbGinIndex) backfillPartition(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, postingRows jsonbGinPostingRowAppender, columnIndex int) error {
	for {
		row, err := rows.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if row[columnIndex] == nil {
			continue
		}
		rowID := rowIdentity(sch, row)
		keyValues := primaryKeyRowValues(sch, row)
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, row[columnIndex])
		if err != nil {
			return err
		}
		encodedTokens, err := jsonbgin.ExtractEncoded(doc, c.opClass)
		if err != nil {
			return err
		}
		for _, encodedToken := range encodedTokens {
			postingRow := sql.Row{encodedToken, rowID}
			postingRow = append(postingRow, keyValues...)
			if err = postingRows.Add(ctx, postingRow); err != nil {
				return err
			}
		}
	}
}

type jsonbGinPostingRowSink interface {
	jsonbGinPostingRowAppender
	Complete(ctx *sql.Context) error
	Discard(ctx *sql.Context, err error) error
	Close(ctx *sql.Context) error
}

func newJsonbGinPostingRowSink(ctx *sql.Context, db sql.Database, postingTable sql.Table, maxRows int) (jsonbGinPostingRowSink, error) {
	if sink, ok, err := newJsonbGinBulkPostingRowSink(ctx, db, postingTable, jsonbGinPostingRowLess); ok || err != nil {
		return sink, err
	}
	insertable, ok := postingTable.(sql.InsertableTable)
	if !ok {
		return nil, errors.Errorf(`posting table "%s" does not support inserts`, postingTable.Name())
	}
	inserter := insertable.Inserter(ctx)
	inserter.StatementBegin(ctx)
	return &jsonbGinInserterPostingRowSink{
		inserter: inserter,
		buffer:   newJsonbGinPostingRowBuffer(inserter, maxRows),
	}, nil
}

func newJsonbGinPostingChunkRowSink(ctx *sql.Context, db sql.Database, postingChunkTable sql.Table) (jsonbGinPostingRowSink, error) {
	if sink, ok, err := newJsonbGinBulkPostingRowSink(ctx, db, postingChunkTable, jsonbGinPostingChunkRowLess); ok || err != nil {
		return sink, err
	}
	insertable, ok := postingChunkTable.(sql.InsertableTable)
	if !ok {
		return nil, errors.Errorf(`posting chunk table "%s" does not support inserts`, postingChunkTable.Name())
	}
	inserter := insertable.Inserter(ctx)
	inserter.StatementBegin(ctx)
	return &jsonbGinInserterPostingRowSink{
		inserter: inserter,
		buffer:   newJsonbGinPostingRowBuffer(inserter, jsonbGinPostingBackfillChunkRows),
	}, nil
}

type jsonbGinInserterPostingRowSink struct {
	inserter sql.RowInserter
	buffer   *jsonbGinPostingRowBuffer
}

var _ jsonbGinPostingRowSink = (*jsonbGinInserterPostingRowSink)(nil)

func (s *jsonbGinInserterPostingRowSink) Add(ctx *sql.Context, row sql.Row) error {
	return s.buffer.Add(ctx, row)
}

func (s *jsonbGinInserterPostingRowSink) Complete(ctx *sql.Context) error {
	if err := s.buffer.Flush(ctx); err != nil {
		return err
	}
	return s.inserter.StatementComplete(ctx)
}

func (s *jsonbGinInserterPostingRowSink) Discard(ctx *sql.Context, err error) error {
	return s.inserter.DiscardChanges(ctx, err)
}

func (s *jsonbGinInserterPostingRowSink) Close(ctx *sql.Context) error {
	return s.inserter.Close(ctx)
}

type doltRootDatabase interface {
	GetRoot(ctx *sql.Context) (doltdb.RootValue, error)
	SetRoot(ctx *sql.Context, newRoot doltdb.RootValue) error
}

type doltBackedTable interface {
	DoltTable(ctx *sql.Context) (*doltdb.Table, error)
	TableName() doltdb.TableName
}

type jsonbGinBulkPostingRowSink struct {
	db        doltRootDatabase
	tableName doltdb.TableName
	table     *doltdb.Table
	doltSch   doltschema.Schema
	sqlSch    sql.Schema
	rows      []sql.Row
	less      func(left sql.Row, right sql.Row) bool
}

var _ jsonbGinPostingRowSink = (*jsonbGinBulkPostingRowSink)(nil)

func newJsonbGinBulkPostingRowSink(ctx *sql.Context, db sql.Database, postingTable sql.Table, less func(left sql.Row, right sql.Row) bool) (*jsonbGinBulkPostingRowSink, bool, error) {
	rootDb, ok := db.(doltRootDatabase)
	if !ok {
		return nil, false, nil
	}
	doltTableSource, ok := postingTable.(doltBackedTable)
	if !ok {
		return nil, false, nil
	}
	table, err := doltTableSource.DoltTable(ctx)
	if err != nil {
		return nil, true, err
	}
	doltSch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, true, err
	}
	return &jsonbGinBulkPostingRowSink{
		db:        rootDb,
		tableName: doltTableSource.TableName(),
		table:     table,
		doltSch:   doltSch,
		sqlSch:    postingTable.Schema(ctx),
		less:      less,
	}, true, nil
}

func (s *jsonbGinBulkPostingRowSink) Add(_ *sql.Context, row sql.Row) error {
	s.rows = append(s.rows, append(sql.Row(nil), row...))
	return nil
}

func (s *jsonbGinBulkPostingRowSink) Complete(ctx *sql.Context) error {
	rowData, err := buildSortedPrimaryRowIndex(ctx, s.table.NodeStore(), s.doltSch, s.sqlSch, s.rows, s.less)
	if err != nil {
		return err
	}
	updatedTable, err := s.table.UpdateRows(ctx, rowData)
	if err != nil {
		return err
	}
	root, err := s.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	updatedRoot, err := root.PutTable(ctx, s.tableName, updatedTable)
	if err != nil {
		return err
	}
	clear(s.rows)
	s.rows = nil
	return s.db.SetRoot(ctx, updatedRoot)
}

func (s *jsonbGinBulkPostingRowSink) Discard(_ *sql.Context, _ error) error {
	clear(s.rows)
	s.rows = nil
	return nil
}

func (s *jsonbGinBulkPostingRowSink) Close(_ *sql.Context) error {
	return nil
}

type jsonbGinPostingRowBuffer struct {
	inserter sql.RowInserter
	maxRows  int
	rows     []sql.Row
}

func newJsonbGinPostingRowBuffer(inserter sql.RowInserter, maxRows int) *jsonbGinPostingRowBuffer {
	return &jsonbGinPostingRowBuffer{
		inserter: inserter,
		maxRows:  maxRows,
	}
}

func (b *jsonbGinPostingRowBuffer) Add(ctx *sql.Context, row sql.Row) error {
	b.rows = append(b.rows, row)
	if b.maxRows > 0 && len(b.rows) >= b.maxRows {
		return b.Flush(ctx)
	}
	return nil
}

func (b *jsonbGinPostingRowBuffer) Flush(ctx *sql.Context) error {
	if len(b.rows) == 0 {
		return nil
	}
	sort.Slice(b.rows, func(i, j int) bool {
		return jsonbGinPostingRowLess(b.rows[i], b.rows[j])
	})
	for _, row := range b.rows {
		if err := b.inserter.Insert(ctx, row); err != nil {
			return err
		}
	}
	clear(b.rows)
	b.rows = b.rows[:0]
	return nil
}

func jsonbGinPostingRowLess(left sql.Row, right sql.Row) bool {
	leftToken := jsonbGinPostingSortString(left, 0)
	rightToken := jsonbGinPostingSortString(right, 0)
	if leftToken != rightToken {
		return leftToken < rightToken
	}
	leftRowID := jsonbGinPostingSortString(left, 1)
	rightRowID := jsonbGinPostingSortString(right, 1)
	if leftRowID != rightRowID {
		return leftRowID < rightRowID
	}
	return fmt.Sprint(left) < fmt.Sprint(right)
}

func jsonbGinPostingChunkRowLess(left sql.Row, right sql.Row) bool {
	leftToken := jsonbGinPostingSortString(left, 0)
	rightToken := jsonbGinPostingSortString(right, 0)
	if leftToken != rightToken {
		return leftToken < rightToken
	}
	leftChunkNo := jsonbGinPostingSortInt(left, 1)
	rightChunkNo := jsonbGinPostingSortInt(right, 1)
	if leftChunkNo != rightChunkNo {
		return leftChunkNo < rightChunkNo
	}
	return fmt.Sprint(left) < fmt.Sprint(right)
}

func jsonbGinPostingSortString(row sql.Row, index int) string {
	if index >= len(row) {
		return ""
	}
	value, _ := row[index].(string)
	return value
}

func jsonbGinPostingSortInt(row sql.Row, index int) int64 {
	if index >= len(row) {
		return 0
	}
	value, ok := integralInt64(row[index])
	if !ok {
		return 0
	}
	return value
}

func rowIdentity(sch sql.Schema, row sql.Row) string {
	hash := sha256.New()
	if !writeRowIdentityColumns(hash, sch, row, true) {
		writeRowIdentityColumns(hash, sch, row, false)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func jsonbGinPostingRowReference(ctx *sql.Context, sch sql.Schema, row sql.Row) (jsonbgin.RowReference, error) {
	rowRef, ok, err := jsonbgin.EncodePrimaryKeyRowReference(ctx, sch, row)
	if err == nil && ok {
		return rowRef, nil
	}
	if err != nil && !jsonbgin.IsUnsupportedRowReferenceType(err) {
		return jsonbgin.RowReference{}, err
	}
	return jsonbgin.EncodeOpaqueRowReference(rowIdentity(sch, row))
}

func primaryKeyRowIdentity(sch sql.Schema, row sql.Row) (string, bool) {
	hash := sha256.New()
	if !writeRowIdentityColumns(hash, sch, row, true) {
		return "", false
	}
	return hex.EncodeToString(hash.Sum(nil)), true
}

func writeRowIdentityColumns(hash interface{ Write([]byte) (int, error) }, sch sql.Schema, row sql.Row, primaryOnly bool) bool {
	wrote := false
	for i, value := range row {
		if primaryOnly {
			if i >= len(sch) || !sch[i].PrimaryKey {
				continue
			}
		}
		if wrote {
			_, _ = hash.Write([]byte{0})
		}
		wrote = true
		_, _ = hash.Write([]byte(fmt.Sprintf("%T=%v", value, value)))
	}
	if !wrote && !primaryOnly {
		_, _ = hash.Write([]byte("<empty-row>"))
		wrote = true
	}
	return wrote
}

func dropTable(ctx *sql.Context, db sql.Database, tableName string) error {
	tableDropper, ok := db.(sql.TableDropper)
	if !ok {
		return nil
	}
	return tableDropper.DropTable(ctx, tableName)
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) String() string {
	return "CREATE INDEX USING gin"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateJsonbGinIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateJsonbGinIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
