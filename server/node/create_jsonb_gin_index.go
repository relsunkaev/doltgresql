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
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const jsonbGinPostingChunkTableComment = "Doltgres JSONB GIN posting chunk storage"
const jsonbGinPostingBackfillChunkRows = 8192
const jsonbGinPostingChunkRowsPerChunk = 512
const jsonbGinPostingChunkBuildSpillEntries = 262144
const jsonbGinPostingBuildContextCheckInterval = 1024

// CreateJsonbGinIndex handles CREATE INDEX USING gin for JSONB columns.
type CreateJsonbGinIndex struct {
	ifNotExists                   bool
	concurrently                  bool
	schema                        string
	tableName                     string
	indexName                     string
	columnName                    string
	opClass                       string
	postingChunkName              string
	postingChunkRowsPerChunk      int
	postingChunkBuildSpillEntries int
	postingChunkBuildWorkers      int
	postingChunkBuildTempDir      string
}

var _ sql.ExecSourceRel = (*CreateJsonbGinIndex)(nil)
var _ vitess.Injectable = (*CreateJsonbGinIndex)(nil)

// NewCreateJsonbGinIndex returns a new *CreateJsonbGinIndex.
func NewCreateJsonbGinIndex(ifNotExists bool, concurrently bool, schema string, tableName string, indexName string, columnName string, opClass string) *CreateJsonbGinIndex {
	return &CreateJsonbGinIndex{
		ifNotExists:      ifNotExists,
		concurrently:     concurrently,
		schema:           schema,
		tableName:        tableName,
		indexName:        indexName,
		columnName:       columnName,
		opClass:          indexmetadata.NormalizeOpClass(opClass),
		postingChunkName: jsonbgin.PostingChunkTableName(tableName, indexName),
	}
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
	if err = checkIndexTableOwnership(ctx, doltdb.TableName{Schema: schemaName, Name: c.tableName}); err != nil {
		return nil, err
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
	if c.concurrently {
		metadata.NotReady = true
		metadata.Invalid = true
	}
	if err = alterable.CreateIndex(ctx, sql.IndexDef{
		Name:       c.indexName,
		Comment:    indexmetadata.EncodeComment(metadata),
		Columns:    []sql.IndexColumn{{Name: anchorColumn}},
		Constraint: sql.IndexConstraint_None,
		Storage:    sql.IndexUsing_BTree,
	}); err != nil {
		return nil, err
	}
	if err = c.createPostingStorageTables(ctx, tableCreator); err != nil {
		_ = alterable.DropIndex(ctx, c.indexName)
		return nil, err
	}
	err = c.backfillPostingChunkTable(ctx, db, schemaName, table, columnIndex)
	if err != nil {
		_ = dropTable(ctx, db, c.postingChunkName)
		_ = alterable.DropIndex(ctx, c.indexName)
		return nil, err
	}
	if err = c.finishConcurrentBuild(ctx, schemaName); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateJsonbGinIndex) finishConcurrentBuild(ctx *sql.Context, schemaName string) error {
	if !c.concurrently {
		return nil
	}
	if err := commitInterPhaseTransaction(ctx); err != nil {
		return err
	}
	if testHookBetweenPhases != nil {
		testHookBetweenPhases(ctx)
	}
	finalMetadata := c.indexMetadata()
	if err := flipIndexComment(ctx, schemaName, c.tableName, c.indexName, alteredIndexComment(finalMetadata)); err != nil {
		return err
	}
	return commitInterPhaseTransaction(ctx)
}

func (c *CreateJsonbGinIndex) indexMetadata() indexmetadata.Metadata {
	return indexmetadata.Metadata{
		AccessMethod: indexmetadata.AccessMethodGin,
		Columns:      []string{c.columnName},
		OpClasses:    []string{c.opClass},
		Gin: &indexmetadata.GinMetadata{
			PostingChunkTable: c.postingChunkName,
		},
	}
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
		return -1, "", pgerror.New(pgcode.UndefinedObject, `gin indexes are only supported on jsonb columns`)
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

func (c *CreateJsonbGinIndex) createPostingStorageTables(ctx *sql.Context, tableCreator sql.TableCreator) error {
	return c.createPostingChunkTable(ctx, tableCreator)
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
	sorter, err := c.buildPostingChunkEntrySorterFromTable(ctx, table, columnIndex)
	if err != nil {
		return err
	}
	defer sorter.Close()
	if err = jsonbGinPostingBuildContextErr(ctx); err != nil {
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
	if err = c.writePostingChunkRowsFromEntries(ctx, sorter, sink); err != nil {
		return err
	}
	if err = jsonbGinPostingBuildContextErr(ctx); err != nil {
		return err
	}
	if err = sink.Complete(ctx); err != nil {
		return err
	}
	completed = true
	return nil
}

func (c *CreateJsonbGinIndex) buildPostingChunkRowsFromTable(ctx *sql.Context, table sql.Table, columnIndex int) ([]sql.Row, error) {
	sorter, err := c.buildPostingChunkEntrySorterFromTable(ctx, table, columnIndex)
	if err != nil {
		return nil, err
	}
	defer sorter.Close()
	return c.materializePostingChunkRowsFromEntries(ctx, sorter)
}

func (c *CreateJsonbGinIndex) buildPostingChunkEntrySorterFromTable(ctx *sql.Context, table sql.Table, columnIndex int) (*jsonbGinPostingChunkEntrySorter, error) {
	if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
		return nil, err
	}
	if c.postingChunkBuildWorkerLimit() <= 1 {
		sorter := c.newPostingChunkEntrySorter()
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
				_ = sorter.Close()
				return nil, err
			}
			if err = jsonbGinPostingBuildContextErr(ctx); err != nil {
				_ = sorter.Close()
				return nil, err
			}
			rows, err := table.PartitionRows(ctx, partition)
			if err != nil {
				_ = sorter.Close()
				return nil, err
			}
			if err = c.addPostingChunkEntries(ctx, table.Schema(ctx), rows, columnIndex, sorter); err != nil {
				_ = rows.Close(ctx)
				_ = sorter.Close()
				return nil, err
			}
			if err = rows.Close(ctx); err != nil {
				_ = sorter.Close()
				return nil, err
			}
		}
		return sorter, nil
	}

	sorter := c.newPostingChunkEntrySorter()
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}
	defer partitions.Close(ctx)
	sch := table.Schema(ctx)
	for {
		if err = jsonbGinPostingBuildContextErr(ctx); err != nil {
			_ = sorter.Close()
			return nil, err
		}
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = sorter.Close()
			return nil, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			_ = sorter.Close()
			return nil, err
		}
		partitionSorter, err := c.buildPostingChunkEntrySorterFromRows(ctx, sch, rows, columnIndex)
		if err != nil {
			_ = rows.Close(ctx)
			_ = sorter.Close()
			return nil, err
		}
		if err = sorter.appendRunsFrom(partitionSorter); err != nil {
			_ = partitionSorter.Close()
			_ = rows.Close(ctx)
			_ = sorter.Close()
			return nil, err
		}
		if err = partitionSorter.Close(); err != nil {
			_ = rows.Close(ctx)
			_ = sorter.Close()
			return nil, err
		}
		if err = rows.Close(ctx); err != nil {
			_ = sorter.Close()
			return nil, err
		}
	}
	return sorter, nil
}

func (c *CreateJsonbGinIndex) buildPostingChunkRows(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, columnIndex int) ([]sql.Row, error) {
	sorter, err := c.buildPostingChunkEntrySorterFromRows(ctx, sch, rows, columnIndex)
	if err != nil {
		return nil, err
	}
	defer sorter.Close()
	return c.materializePostingChunkRowsFromEntries(ctx, sorter)
}

func (c *CreateJsonbGinIndex) buildPostingChunkEntrySorterFromRows(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, columnIndex int) (*jsonbGinPostingChunkEntrySorter, error) {
	sorter := c.newPostingChunkEntrySorter()
	if c.postingChunkBuildWorkerLimit() <= 1 {
		if err := c.addPostingChunkEntries(ctx, sch, rows, columnIndex, sorter); err != nil {
			_ = sorter.Close()
			return nil, err
		}
		return sorter, nil
	}
	if err := c.addPostingChunkEntriesParallel(ctx, sch, rows, columnIndex, sorter); err != nil {
		_ = sorter.Close()
		return nil, err
	}
	return sorter, nil
}

func (c *CreateJsonbGinIndex) addPostingChunkEntries(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, columnIndex int, sorter *jsonbGinPostingChunkEntrySorter) error {
	var tokenScratch jsonbgin.EncodedTokenScratch
	for {
		if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
			return err
		}
		row, err := rows.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = c.addPostingChunkEntriesForRow(ctx, sch, row, columnIndex, sorter, &tokenScratch); err != nil {
			return err
		}
	}
}

func (c *CreateJsonbGinIndex) addPostingChunkEntriesParallel(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, columnIndex int, sorter *jsonbGinPostingChunkEntrySorter) error {
	// The base row scan stays serial; workers turn rows into sorted spill runs
	// that the existing chunk writer can merge deterministically.
	if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
		return err
	}
	workerCount := c.postingChunkBuildWorkerLimit()
	jobs := make(chan sql.Row, workerCount*2)
	results := make(chan jsonbGinPostingChunkBuildWorkerResult, workerCount)
	for i := 0; i < workerCount; i++ {
		go c.runPostingChunkBuildWorker(ctx, sch, jobs, columnIndex, results)
	}

	var scanErr error
	for scanErr == nil {
		if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
			scanErr = err
			break
		}
		row, err := rows.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			scanErr = err
			break
		}
		rowCopy := append(sql.Row(nil), row...)
		select {
		case jobs <- rowCopy:
		case <-ctx.Done():
			scanErr = ctx.Err()
		}
	}
	close(jobs)

	var retErr error
	if scanErr != nil {
		retErr = scanErr
	}
	for i := 0; i < workerCount; i++ {
		result := <-results
		if result.err != nil && retErr == nil {
			retErr = result.err
		}
		if result.sorter == nil {
			continue
		}
		if retErr == nil {
			if err := sorter.appendRunsFrom(result.sorter); err != nil {
				retErr = err
			}
		}
		if err := result.sorter.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

type jsonbGinPostingChunkBuildWorkerResult struct {
	sorter *jsonbGinPostingChunkEntrySorter
	err    error
}

func (c *CreateJsonbGinIndex) runPostingChunkBuildWorker(ctx *sql.Context, sch sql.Schema, jobs <-chan sql.Row, columnIndex int, results chan<- jsonbGinPostingChunkBuildWorkerResult) {
	sorter := c.newPostingChunkEntrySorter()
	var tokenScratch jsonbgin.EncodedTokenScratch
	var retErr error
	for {
		var row sql.Row
		var ok bool
		select {
		case <-ctx.Done():
			retErr = ctx.Err()
			ok = false
		case row, ok = <-jobs:
		}
		if !ok {
			break
		}
		if retErr != nil {
			continue
		}
		if err := c.addPostingChunkEntriesForRow(ctx, sch, row, columnIndex, sorter, &tokenScratch); err != nil {
			retErr = err
		}
	}
	if retErr == nil {
		retErr = jsonbGinPostingBuildContextErr(ctx)
	}
	if retErr == nil {
		retErr = sorter.flushRun()
	}
	results <- jsonbGinPostingChunkBuildWorkerResult{sorter: sorter, err: retErr}
}

func (c *CreateJsonbGinIndex) addPostingChunkEntriesForRow(ctx *sql.Context, sch sql.Schema, row sql.Row, columnIndex int, sorter *jsonbGinPostingChunkEntrySorter, tokenScratch *jsonbgin.EncodedTokenScratch) error {
	if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
		return err
	}
	if row[columnIndex] == nil {
		return nil
	}
	rowRef, err := jsonbGinPostingRowReference(ctx, sch, row)
	if err != nil {
		return err
	}
	if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
		return err
	}
	encodedTokens, err := jsonbGinExtractEncodedTokensFromSQLValueWithScratch(ctx, row[columnIndex], c.opClass, tokenScratch)
	if err != nil {
		return err
	}
	return sorter.AddRowTokens(encodedTokens, rowRef.Bytes)
}

func (c *CreateJsonbGinIndex) materializePostingChunkRowsFromEntries(ctx *sql.Context, sorter *jsonbGinPostingChunkEntrySorter) ([]sql.Row, error) {
	collector := &jsonbGinPostingChunkRowCollector{}
	if err := c.writePostingChunkRowsFromEntries(ctx, sorter, collector); err != nil {
		return nil, err
	}
	return collector.rows, nil
}

func (c *CreateJsonbGinIndex) writePostingChunkRowsFromEntries(ctx *sql.Context, sorter *jsonbGinPostingChunkEntrySorter, sink jsonbGinPostingRowAppender) error {
	if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
		return err
	}
	iter, err := sorter.Iterator()
	if err != nil {
		return err
	}
	defer iter.Close()
	rowsPerChunk := c.postingChunkRowsPerChunkLimit()
	var currentToken string
	var chunkNo int64
	var rowRefs [][]byte
	var previous jsonbGinPostingChunkBuildEntry
	havePrevious := false
	flushChunk := func() error {
		if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
			return err
		}
		if len(rowRefs) == 0 {
			return nil
		}
		chunk, err := jsonbgin.EncodePostingChunkForStorage(rowRefs)
		if err != nil {
			return err
		}
		if err = sink.Add(ctx, postingChunkEntryRow(jsonbgin.PostingChunkEntry{
			Token:   currentToken,
			ChunkNo: chunkNo,
			Chunk:   chunk,
		})); err != nil {
			return err
		}
		chunkNo++
		clear(rowRefs)
		rowRefs = rowRefs[:0]
		return nil
	}
	entriesUntilContextCheck := 0
	for {
		if entriesUntilContextCheck <= 0 {
			if err := jsonbGinPostingBuildContextErr(ctx); err != nil {
				return err
			}
			entriesUntilContextCheck = jsonbGinPostingBuildContextCheckInterval
		}
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		entriesUntilContextCheck--
		if havePrevious && previous.token == entry.token && bytes.Equal(previous.rowRef, entry.rowRef) {
			continue
		}
		previous = entry
		havePrevious = true
		if currentToken == "" {
			currentToken = entry.token
		}
		if entry.token != currentToken {
			if err = flushChunk(); err != nil {
				return err
			}
			currentToken = entry.token
			chunkNo = 0
		}
		rowRefs = append(rowRefs, entry.rowRef)
		if len(rowRefs) >= rowsPerChunk {
			if err = flushChunk(); err != nil {
				return err
			}
		}
	}
	return flushChunk()
}

type jsonbGinPostingChunkRowCollector struct {
	rows []sql.Row
}

func (c *jsonbGinPostingChunkRowCollector) Add(_ *sql.Context, row sql.Row) error {
	c.rows = append(c.rows, append(sql.Row(nil), row...))
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

func (c *CreateJsonbGinIndex) postingChunkBuildSpillEntryLimit() int {
	if c.postingChunkBuildSpillEntries > 0 {
		return c.postingChunkBuildSpillEntries
	}
	return jsonbGinPostingChunkBuildSpillEntries
}

func (c *CreateJsonbGinIndex) postingChunkBuildWorkerLimit() int {
	if c.postingChunkBuildWorkers > 0 {
		return c.postingChunkBuildWorkers
	}
	return 1
}

func (c *CreateJsonbGinIndex) newPostingChunkEntrySorter() *jsonbGinPostingChunkEntrySorter {
	return newJsonbGinPostingChunkEntrySorterInDir(c.postingChunkBuildSpillEntryLimit(), c.postingChunkBuildTempDir)
}

func jsonbGinPostingBuildContextErr(ctx *sql.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func postingChunkChecksumBytes(checksum uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, checksum)
	return buf
}

type jsonbGinPostingChunkBuildEntry struct {
	token  string
	rowRef []byte
}

type jsonbGinPostingChunkEntryIterator interface {
	Next() (jsonbGinPostingChunkBuildEntry, error)
	Close() error
}

type jsonbGinPostingChunkEntrySorter struct {
	maxEntries int
	entryCount int
	buckets    map[string][][]byte
	runs       []string
	tempDir    string
}

func newJsonbGinPostingChunkEntrySorter(maxEntries int) *jsonbGinPostingChunkEntrySorter {
	return newJsonbGinPostingChunkEntrySorterInDir(maxEntries, "")
}

func newJsonbGinPostingChunkEntrySorterInDir(maxEntries int, tempDir string) *jsonbGinPostingChunkEntrySorter {
	if maxEntries <= 0 {
		maxEntries = jsonbGinPostingChunkBuildSpillEntries
	}
	return &jsonbGinPostingChunkEntrySorter{
		maxEntries: maxEntries,
		tempDir:    tempDir,
	}
}

func (s *jsonbGinPostingChunkEntrySorter) Add(token string, rowRef []byte) error {
	return s.AddRowTokens([]string{token}, rowRef)
}

func (s *jsonbGinPostingChunkEntrySorter) AddRowTokens(tokens []string, rowRef []byte) error {
	if len(tokens) == 0 {
		return nil
	}
	for _, token := range tokens {
		if token == "" {
			return errors.Errorf("JSONB GIN posting token cannot be empty")
		}
	}
	if len(rowRef) == 0 {
		return errors.Errorf("JSONB GIN posting row reference cannot be empty")
	}
	if s.buckets == nil {
		s.buckets = make(map[string][][]byte)
	}
	rowRefCopy := append([]byte(nil), rowRef...)
	for _, token := range tokens {
		s.buckets[token] = append(s.buckets[token], rowRefCopy)
		s.entryCount++
		if s.entryCount >= s.maxEntries {
			if err := s.flushRun(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *jsonbGinPostingChunkEntrySorter) Iterator() (jsonbGinPostingChunkEntryIterator, error) {
	if len(s.runs) == 0 {
		return newJsonbGinPostingChunkMemoryIterator(s.buckets), nil
	}
	if err := s.flushRun(); err != nil {
		return nil, err
	}
	return newJsonbGinPostingChunkMergeIterator(s.runs)
}

func (s *jsonbGinPostingChunkEntrySorter) Close() error {
	var ret error
	s.clearBuckets()
	s.buckets = nil
	for _, run := range s.runs {
		if err := os.Remove(run); err != nil && !os.IsNotExist(err) && ret == nil {
			ret = err
		}
	}
	clear(s.runs)
	s.runs = nil
	return ret
}

func (s *jsonbGinPostingChunkEntrySorter) appendRunsFrom(other *jsonbGinPostingChunkEntrySorter) error {
	if other == nil {
		return nil
	}
	if err := other.flushRun(); err != nil {
		return err
	}
	s.runs = append(s.runs, other.runs...)
	other.runs = nil
	return nil
}

func (s *jsonbGinPostingChunkEntrySorter) flushRun() error {
	if s.entryCount == 0 {
		return nil
	}
	file, err := os.CreateTemp(s.tempDir, "doltgres-jsonb-gin-posting-chunks-*.run")
	if err != nil {
		return err
	}
	path := file.Name()
	writer := bufio.NewWriter(file)
	tokens := sortedPostingChunkBucketTokens(s.buckets)
	for _, token := range tokens {
		rowRefs := s.buckets[token]
		sortPostingChunkBucketRowRefs(rowRefs)
		var previous []byte
		for _, rowRef := range rowRefs {
			if previous != nil && bytes.Equal(previous, rowRef) {
				continue
			}
			previous = rowRef
			if err = writePostingChunkBuildEntry(writer, jsonbGinPostingChunkBuildEntry{token: token, rowRef: rowRef}); err != nil {
				_ = file.Close()
				_ = os.Remove(path)
				return err
			}
		}
	}
	if err = writer.Flush(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err = file.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	s.runs = append(s.runs, path)
	s.clearBuckets()
	return nil
}

func (s *jsonbGinPostingChunkEntrySorter) clearBuckets() {
	for token, rowRefs := range s.buckets {
		clear(rowRefs)
		delete(s.buckets, token)
	}
	s.entryCount = 0
}

func sortedPostingChunkBucketTokens(buckets map[string][][]byte) []string {
	tokens := make([]string, 0, len(buckets))
	for token := range buckets {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	return tokens
}

func sortPostingChunkBucketRowRefs(rowRefs [][]byte) {
	sort.Slice(rowRefs, func(i, j int) bool {
		return bytes.Compare(rowRefs[i], rowRefs[j]) < 0
	})
}

func jsonbGinPostingChunkBuildEntryLess(left jsonbGinPostingChunkBuildEntry, right jsonbGinPostingChunkBuildEntry) bool {
	if left.token != right.token {
		return left.token < right.token
	}
	return bytes.Compare(left.rowRef, right.rowRef) < 0
}

func writePostingChunkBuildEntry(writer *bufio.Writer, entry jsonbGinPostingChunkBuildEntry) error {
	if len(entry.token) > math.MaxUint32 {
		return errors.Errorf("JSONB GIN posting token is too large: %d bytes", len(entry.token))
	}
	if len(entry.rowRef) > math.MaxUint32 {
		return errors.Errorf("JSONB GIN posting row reference is too large: %d bytes", len(entry.rowRef))
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(entry.token)))
	if _, err := writer.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := writer.WriteString(entry.token); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(entry.rowRef)))
	if _, err := writer.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := writer.Write(entry.rowRef)
	return err
}

func readPostingChunkBuildEntry(reader *bufio.Reader) (jsonbGinPostingChunkBuildEntry, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		return jsonbGinPostingChunkBuildEntry{}, err
	}
	tokenLen := binary.BigEndian.Uint32(lenBuf[:])
	token := make([]byte, tokenLen)
	if _, err := io.ReadFull(reader, token); err != nil {
		return jsonbGinPostingChunkBuildEntry{}, err
	}
	if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
		return jsonbGinPostingChunkBuildEntry{}, err
	}
	rowRefLen := binary.BigEndian.Uint32(lenBuf[:])
	rowRef := make([]byte, rowRefLen)
	if _, err := io.ReadFull(reader, rowRef); err != nil {
		return jsonbGinPostingChunkBuildEntry{}, err
	}
	return jsonbGinPostingChunkBuildEntry{token: string(token), rowRef: rowRef}, nil
}

type jsonbGinPostingChunkMemoryIterator struct {
	buckets        map[string][][]byte
	tokens         []string
	tokenPos       int
	currentToken   string
	currentRowRefs [][]byte
	rowRefPos      int
	previousRowRef []byte
}

func newJsonbGinPostingChunkMemoryIterator(buckets map[string][][]byte) *jsonbGinPostingChunkMemoryIterator {
	return &jsonbGinPostingChunkMemoryIterator{
		buckets: buckets,
		tokens:  sortedPostingChunkBucketTokens(buckets),
	}
}

func (i *jsonbGinPostingChunkMemoryIterator) Next() (jsonbGinPostingChunkBuildEntry, error) {
	for {
		if i.rowRefPos < len(i.currentRowRefs) {
			rowRef := i.currentRowRefs[i.rowRefPos]
			i.rowRefPos++
			if i.previousRowRef != nil && bytes.Equal(i.previousRowRef, rowRef) {
				continue
			}
			i.previousRowRef = rowRef
			return jsonbGinPostingChunkBuildEntry{token: i.currentToken, rowRef: rowRef}, nil
		}
		if i.tokenPos >= len(i.tokens) {
			return jsonbGinPostingChunkBuildEntry{}, io.EOF
		}
		i.currentToken = i.tokens[i.tokenPos]
		i.tokenPos++
		i.currentRowRefs = i.buckets[i.currentToken]
		sortPostingChunkBucketRowRefs(i.currentRowRefs)
		i.rowRefPos = 0
		i.previousRowRef = nil
	}
}

func (i *jsonbGinPostingChunkMemoryIterator) Close() error {
	clear(i.tokens)
	i.tokens = nil
	i.currentRowRefs = nil
	i.previousRowRef = nil
	return nil
}

type jsonbGinPostingChunkRunIterator struct {
	file   *os.File
	reader *bufio.Reader
}

func newJsonbGinPostingChunkRunIterator(path string) (*jsonbGinPostingChunkRunIterator, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &jsonbGinPostingChunkRunIterator{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (i *jsonbGinPostingChunkRunIterator) Next() (jsonbGinPostingChunkBuildEntry, error) {
	return readPostingChunkBuildEntry(i.reader)
}

func (i *jsonbGinPostingChunkRunIterator) Close() error {
	if i.file == nil {
		return nil
	}
	err := i.file.Close()
	i.file = nil
	return err
}

type jsonbGinPostingChunkMergeIterator struct {
	runs []*jsonbGinPostingChunkRunIterator
	heap jsonbGinPostingChunkMergeHeap
}

func newJsonbGinPostingChunkMergeIterator(paths []string) (*jsonbGinPostingChunkMergeIterator, error) {
	iter := &jsonbGinPostingChunkMergeIterator{
		runs: make([]*jsonbGinPostingChunkRunIterator, 0, len(paths)),
	}
	for _, path := range paths {
		run, err := newJsonbGinPostingChunkRunIterator(path)
		if err != nil {
			_ = iter.Close()
			return nil, err
		}
		runIndex := len(iter.runs)
		iter.runs = append(iter.runs, run)
		entry, err := run.Next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			_ = iter.Close()
			return nil, err
		}
		heap.Push(&iter.heap, jsonbGinPostingChunkMergeItem{entry: entry, runIndex: runIndex})
	}
	return iter, nil
}

func (i *jsonbGinPostingChunkMergeIterator) Next() (jsonbGinPostingChunkBuildEntry, error) {
	if i.heap.Len() == 0 {
		return jsonbGinPostingChunkBuildEntry{}, io.EOF
	}
	item := heap.Pop(&i.heap).(jsonbGinPostingChunkMergeItem)
	if next, err := i.runs[item.runIndex].Next(); err == nil {
		heap.Push(&i.heap, jsonbGinPostingChunkMergeItem{entry: next, runIndex: item.runIndex})
	} else if err != io.EOF {
		return jsonbGinPostingChunkBuildEntry{}, err
	}
	return item.entry, nil
}

func (i *jsonbGinPostingChunkMergeIterator) Close() error {
	var ret error
	for _, run := range i.runs {
		if run == nil {
			continue
		}
		if err := run.Close(); err != nil && ret == nil {
			ret = err
		}
	}
	clear(i.runs)
	i.runs = nil
	clear(i.heap)
	i.heap = nil
	return ret
}

type jsonbGinPostingChunkMergeItem struct {
	entry    jsonbGinPostingChunkBuildEntry
	runIndex int
}

type jsonbGinPostingChunkMergeHeap []jsonbGinPostingChunkMergeItem

var _ heap.Interface = (*jsonbGinPostingChunkMergeHeap)(nil)

func (h jsonbGinPostingChunkMergeHeap) Len() int {
	return len(h)
}

func (h jsonbGinPostingChunkMergeHeap) Less(i int, j int) bool {
	return jsonbGinPostingChunkBuildEntryLess(h[i].entry, h[j].entry)
}

func (h jsonbGinPostingChunkMergeHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *jsonbGinPostingChunkMergeHeap) Push(value any) {
	*h = append(*h, value.(jsonbGinPostingChunkMergeItem))
}

func (h *jsonbGinPostingChunkMergeHeap) Pop() any {
	old := *h
	n := len(old)
	value := old[n-1]
	*h = old[:n-1]
	return value
}

type jsonbGinPostingRowAppender interface {
	Add(ctx *sql.Context, row sql.Row) error
}

type jsonbGinPostingRowSink interface {
	jsonbGinPostingRowAppender
	Complete(ctx *sql.Context) error
	Discard(ctx *sql.Context, err error) error
	Close(ctx *sql.Context) error
}

func newJsonbGinPostingChunkRowSink(ctx *sql.Context, db sql.Database, postingChunkTable sql.Table) (jsonbGinPostingRowSink, error) {
	if sink, ok, err := newJsonbGinBulkSortedPostingRowSink(ctx, db, postingChunkTable); ok || err != nil {
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
		buffer:   newJsonbGinPostingRowBuffer(inserter, jsonbGinPostingBackfillChunkRows, jsonbGinPostingChunkRowLess),
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

type jsonbGinBulkSortedPostingRowSink struct {
	db        doltRootDatabase
	tableName doltdb.TableName
	table     *doltdb.Table
	builder   *sortedPrimaryRowIndexBuilder
}

var _ jsonbGinPostingRowSink = (*jsonbGinBulkSortedPostingRowSink)(nil)

func newJsonbGinBulkSortedPostingRowSink(ctx *sql.Context, db sql.Database, postingTable sql.Table) (*jsonbGinBulkSortedPostingRowSink, bool, error) {
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
	builder, err := newSortedPrimaryRowIndexBuilder(ctx, table.NodeStore(), doltSch, postingTable.Schema(ctx))
	if err != nil {
		return nil, true, err
	}
	return &jsonbGinBulkSortedPostingRowSink{
		db:        rootDb,
		tableName: doltTableSource.TableName(),
		table:     table,
		builder:   builder,
	}, true, nil
}

func (s *jsonbGinBulkSortedPostingRowSink) Add(ctx *sql.Context, row sql.Row) error {
	if s.builder == nil {
		return errors.Errorf("sorted JSONB GIN posting sink is closed")
	}
	return s.builder.Add(ctx, row)
}

func (s *jsonbGinBulkSortedPostingRowSink) Complete(ctx *sql.Context) error {
	if s.builder == nil {
		return errors.Errorf("sorted JSONB GIN posting sink is closed")
	}
	rowData, err := s.builder.Complete(ctx)
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
	s.builder = nil
	return s.db.SetRoot(ctx, updatedRoot)
}

func (s *jsonbGinBulkSortedPostingRowSink) Discard(_ *sql.Context, _ error) error {
	s.builder = nil
	return nil
}

func (s *jsonbGinBulkSortedPostingRowSink) Close(_ *sql.Context) error {
	return nil
}

type jsonbGinPostingRowBuffer struct {
	inserter sql.RowInserter
	maxRows  int
	rows     []sql.Row
	less     func(left sql.Row, right sql.Row) bool
}

func newJsonbGinPostingRowBuffer(inserter sql.RowInserter, maxRows int, less func(left sql.Row, right sql.Row) bool) *jsonbGinPostingRowBuffer {
	return &jsonbGinPostingRowBuffer{
		inserter: inserter,
		maxRows:  maxRows,
		less:     less,
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
	if b.less != nil {
		sort.Slice(b.rows, func(i, j int) bool {
			return b.less(b.rows[i], b.rows[j])
		})
	}
	for _, row := range b.rows {
		if err := b.inserter.Insert(ctx, row); err != nil {
			return err
		}
	}
	clear(b.rows)
	b.rows = b.rows[:0]
	return nil
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
