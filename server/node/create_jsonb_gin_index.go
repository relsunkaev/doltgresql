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
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const jsonbGinPostingTableComment = "Doltgres JSONB GIN posting storage"

// CreateJsonbGinIndex handles CREATE INDEX USING gin for JSONB columns.
type CreateJsonbGinIndex struct {
	ifNotExists bool
	schema      string
	tableName   string
	indexName   string
	columnName  string
	opClass     string
	postingName string
}

var _ sql.ExecSourceRel = (*CreateJsonbGinIndex)(nil)
var _ vitess.Injectable = (*CreateJsonbGinIndex)(nil)

// NewCreateJsonbGinIndex returns a new *CreateJsonbGinIndex.
func NewCreateJsonbGinIndex(ifNotExists bool, schema string, tableName string, indexName string, columnName string, opClass string) *CreateJsonbGinIndex {
	return &CreateJsonbGinIndex{
		ifNotExists: ifNotExists,
		schema:      schema,
		tableName:   tableName,
		indexName:   indexName,
		columnName:  columnName,
		opClass:     indexmetadata.NormalizeOpClass(opClass),
		postingName: jsonbgin.PostingTableName(tableName, indexName),
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

	columnIndex, err := c.validateTable(ctx, table)
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

	metadata := indexmetadata.Metadata{
		AccessMethod: indexmetadata.AccessMethodGin,
		OpClasses:    []string{c.opClass},
		Gin: &indexmetadata.GinMetadata{
			PostingTable: c.postingName,
		},
	}
	if err = alterable.CreateIndex(ctx, sql.IndexDef{
		Name:       c.indexName,
		Comment:    indexmetadata.EncodeComment(metadata),
		Columns:    []sql.IndexColumn{{Name: c.columnName}},
		Constraint: sql.IndexConstraint_None,
		Storage:    sql.IndexUsing_BTree,
	}); err != nil {
		return nil, err
	}
	if err = c.createPostingTable(ctx, tableCreator); err != nil {
		_ = alterable.DropIndex(ctx, c.indexName)
		return nil, err
	}
	if err = c.backfillPostingTable(ctx, schemaName, table, columnIndex); err != nil {
		_ = dropTable(ctx, db, c.postingName)
		_ = alterable.DropIndex(ctx, c.indexName)
		return nil, err
	}
	return sql.RowsToRowIter(), nil
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

func (c *CreateJsonbGinIndex) validateTable(ctx *sql.Context, table sql.Table) (int, error) {
	if !indexmetadata.IsSupportedGinJsonbOpClass(c.opClass) {
		return -1, errors.Errorf("operator class %s is not yet supported for gin indexes", c.opClass)
	}
	sch := table.Schema(ctx)
	columnIndex := sch.IndexOfColName(c.columnName)
	if columnIndex < 0 {
		return -1, errors.Errorf(`column "%s" of relation "%s" does not exist`, c.columnName, c.tableName)
	}
	dgType, ok := sch[columnIndex].Type.(*pgtypes.DoltgresType)
	if !ok || dgType.ID != pgtypes.JsonB.ID {
		return -1, errors.Errorf(`gin indexes are only supported on jsonb columns`)
	}
	return columnIndex, nil
}

func (c *CreateJsonbGinIndex) createPostingTable(ctx *sql.Context, tableCreator sql.TableCreator) error {
	postingSchema := sql.NewPrimaryKeySchema(sql.Schema{
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
	})
	return tableCreator.CreateTable(ctx, c.postingName, postingSchema, sql.Collation_Default, jsonbGinPostingTableComment)
}

func (c *CreateJsonbGinIndex) backfillPostingTable(ctx *sql.Context, schemaName string, table sql.Table, columnIndex int) error {
	postingTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: c.postingName, Schema: schemaName})
	if err != nil {
		return err
	}
	if postingTable == nil {
		return errors.Errorf(`posting table "%s" was not created`, c.postingName)
	}
	insertable, ok := postingTable.(sql.InsertableTable)
	if !ok {
		return errors.Errorf(`posting table "%s" does not support inserts`, c.postingName)
	}
	inserter := insertable.Inserter(ctx)
	inserter.StatementBegin(ctx)
	closed := false
	defer func() {
		if !closed {
			_ = inserter.Close(ctx)
		}
	}()
	completed := false
	defer func() {
		if !completed {
			_ = inserter.DiscardChanges(ctx, errors.New("JSONB GIN backfill failed"))
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
		if err = c.backfillPartition(ctx, table.Schema(ctx), rows, inserter, columnIndex); err != nil {
			_ = rows.Close(ctx)
			return err
		}
		if err = rows.Close(ctx); err != nil {
			return err
		}
	}
	if err = inserter.StatementComplete(ctx); err != nil {
		return err
	}
	completed = true
	closed = true
	return inserter.Close(ctx)
}

func (c *CreateJsonbGinIndex) backfillPartition(ctx *sql.Context, sch sql.Schema, rows sql.RowIter, inserter sql.RowInserter, columnIndex int) error {
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
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, row[columnIndex])
		if err != nil {
			return err
		}
		tokens, err := jsonbgin.Extract(doc, c.opClass)
		if err != nil {
			return err
		}
		for _, token := range tokens {
			if err = inserter.Insert(ctx, sql.Row{jsonbgin.EncodeToken(token), rowID}); err != nil {
				return err
			}
		}
	}
}

func rowIdentity(sch sql.Schema, row sql.Row) string {
	hash := sha256.New()
	if !writeRowIdentityColumns(hash, sch, row, true) {
		writeRowIdentityColumns(hash, sch, row, false)
	}
	return hex.EncodeToString(hash.Sum(nil))
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
