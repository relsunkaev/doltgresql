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

package server

import (
	"encoding/binary"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/publications"
	"github.com/dolthub/doltgresql/server/replsource"
)

type replicationChangeAction byte

const (
	replicationChangeInsert replicationChangeAction = 'I'
	replicationChangeUpdate replicationChangeAction = 'U'
	replicationChangeDelete replicationChangeAction = 'D'
)

type replicationChangeCapture struct {
	action            replicationChangeAction
	schema            string
	table             string
	clientReturnsRows bool
	fullRowFieldCount int
	fields            []pgproto3.FieldDescription
	rows              []Row
	rowsAffected      uint64
}

func prepareReplicationChangeCapture(query ConvertedQuery, fullRowColumns []string) (*replicationChangeCapture, bool) {
	capture, ok := replicationChangeCaptureFromStatement(query.AST)
	if !ok {
		return nil, false
	}
	ensureReturningFullRow(query.AST, fullRowColumns)
	return capture, true
}

func replicationChangeCaptureFromStatement(statement vitess.Statement) (*replicationChangeCapture, bool) {
	switch stmt := statement.(type) {
	case *vitess.Insert:
		return &replicationChangeCapture{
			action:            replicationChangeInsert,
			schema:            stmt.Table.SchemaQualifier.String(),
			table:             stmt.Table.Name.String(),
			clientReturnsRows: len(stmt.Returning) > 0,
		}, true
	case *vitess.Update:
		schema, table, ok := tableNameFromTableExprs(stmt.TableExprs)
		if !ok {
			return nil, false
		}
		return &replicationChangeCapture{
			action:            replicationChangeUpdate,
			schema:            schema,
			table:             table,
			clientReturnsRows: len(stmt.Returning) > 0,
		}, true
	case *vitess.Delete:
		schema, table, ok := tableNameFromTableExprs(stmt.TableExprs)
		if !ok {
			return nil, false
		}
		return &replicationChangeCapture{
			action:            replicationChangeDelete,
			schema:            schema,
			table:             table,
			clientReturnsRows: len(stmt.Returning) > 0,
		}, true
	default:
		return nil, false
	}
}

func tableNameFromTableExprs(exprs vitess.TableExprs) (schema string, table string, ok bool) {
	if len(exprs) != 1 {
		return "", "", false
	}
	aliased, ok := exprs[0].(*vitess.AliasedTableExpr)
	if !ok {
		return "", "", false
	}
	tableName, ok := aliased.Expr.(vitess.TableName)
	if !ok {
		return "", "", false
	}
	return tableName.SchemaQualifier.String(), tableName.Name.String(), true
}

func ensureReturningFullRow(statement vitess.Statement, fullRowColumns []string) {
	appendFullRow := func(returning vitess.SelectExprs) vitess.SelectExprs {
		for _, column := range fullRowColumns {
			returning = append(returning, &vitess.AliasedExpr{Expr: vitess.NewColName(column)})
		}
		return returning
	}
	switch stmt := statement.(type) {
	case *vitess.Insert:
		stmt.Returning = appendFullRow(stmt.Returning)
	case *vitess.Update:
		stmt.Returning = appendFullRow(stmt.Returning)
	case *vitess.Delete:
		stmt.Returning = appendFullRow(stmt.Returning)
	}
}

func (capture *replicationChangeCapture) appendResultAndTrimClient(ctx *sql.Context, result *Result) (*Result, error) {
	if capture == nil || result == nil {
		return result, nil
	}
	capture.rowsAffected += result.RowsAffected
	start := 0
	if capture.clientReturnsRows && len(result.Fields) > 0 {
		fullRowFieldCount, err := capture.getFullRowFieldCount(ctx)
		if err != nil {
			return nil, err
		}
		if len(result.Fields) < fullRowFieldCount {
			return nil, errors.Errorf("logical replication result for %s has %d fields, expected at least %d", capture.table, len(result.Fields), fullRowFieldCount)
		}
		start = len(result.Fields) - fullRowFieldCount
	}
	capture.appendResultSlice(result, start, len(result.Fields))
	if capture.clientReturnsRows {
		return resultSlice(result, 0, start), nil
	}
	return result, nil
}

func (capture *replicationChangeCapture) appendResultSlice(result *Result, start int, end int) {
	if capture.fields == nil && len(result.Fields) > 0 {
		capture.fields = append([]pgproto3.FieldDescription(nil), result.Fields[start:end]...)
	}
	for _, row := range result.Rows {
		capture.rows = append(capture.rows, Row{val: append([][]byte(nil), row.val[start:end]...)})
	}
}

func resultSlice(result *Result, start int, end int) *Result {
	if result == nil {
		return nil
	}
	ret := &Result{
		RowsAffected: result.RowsAffected,
	}
	if len(result.Fields) > 0 {
		ret.Fields = append([]pgproto3.FieldDescription(nil), result.Fields[start:end]...)
	}
	if len(result.Rows) > 0 {
		ret.Rows = make([]Row, len(result.Rows))
		for i, row := range result.Rows {
			ret.Rows[i] = Row{val: append([][]byte(nil), row.val[start:end]...)}
		}
	}
	return ret
}

func (capture *replicationChangeCapture) getFullRowFieldCount(ctx *sql.Context) (int, error) {
	if capture.fullRowFieldCount > 0 {
		return capture.fullRowFieldCount, nil
	}
	table, _, err := capture.resolveTable(ctx)
	if err != nil {
		return 0, err
	}
	capture.fullRowFieldCount = len(table.Schema())
	return capture.fullRowFieldCount, nil
}

func (capture *replicationChangeCapture) fullRowColumnNames(ctx *sql.Context) ([]string, error) {
	table, _, err := capture.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(table.Schema()))
	for i, column := range table.Schema() {
		columns[i] = column.Name
	}
	capture.fullRowFieldCount = len(columns)
	return columns, nil
}

func (capture *replicationChangeCapture) resolveTable(ctx *sql.Context) (sql.Table, string, error) {
	schema, err := core.GetSchemaName(ctx, nil, capture.schema)
	if err != nil {
		return nil, "", err
	}
	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: capture.table, Schema: schema})
	if err != nil {
		return nil, "", err
	} else if table == nil {
		return nil, "", errors.Errorf(`table "%s"."%s" was not found for logical replication`, schema, capture.table)
	}
	return table, schema, nil
}

func (capture *replicationChangeCapture) publish(ctx *sql.Context) error {
	if capture == nil {
		return nil
	}
	if len(capture.rows) == 0 {
		if capture.rowsAffected > 0 {
			replsource.AdvanceLSN()
		}
		return nil
	}
	if ctx == nil {
		return nil
	}
	table, schema, err := capture.resolveTable(ctx)
	if err != nil {
		return err
	}
	publicationNames, err := capture.publicationNames(ctx, schema)
	if err != nil || len(publicationNames) == 0 {
		return err
	}

	commitLSN := replsource.AdvanceLSN()
	relationID := id.Cache().ToOID(id.NewTable(schema, capture.table).AsId())
	relation := encodeRelationMessage(relationID, schema, capture.table, table.Schema(), capture.fields)
	messages := []replsource.WALMessage{
		{WALStart: commitLSN, ServerWALEnd: commitLSN, WALData: encodeBeginMessage(commitLSN)},
		{WALStart: commitLSN, ServerWALEnd: commitLSN, WALData: relation},
	}
	for _, row := range capture.rows {
		messages = append(messages, replsource.WALMessage{
			WALStart:     commitLSN,
			ServerWALEnd: commitLSN,
			WALData:      capture.encodeRowMessage(relationID, row),
		})
	}
	messages = append(messages, replsource.WALMessage{
		WALStart:     commitLSN,
		ServerWALEnd: commitLSN,
		WALData:      encodeCommitMessage(commitLSN),
	})
	return replsource.Broadcast(publicationNames, messages)
}

func (capture *replicationChangeCapture) publicationNames(ctx *sql.Context, schema string) ([]string, error) {
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	tableID := id.NewTable(schema, capture.table)
	err = collection.IteratePublications(ctx, func(pub publications.Publication) (stop bool, err error) {
		if !publicationPublishesAction(pub, capture.action) {
			return false, nil
		}
		if pub.AllTables {
			names = append(names, pub.ID.PublicationName())
			return false, nil
		}
		for _, pubSchema := range pub.Schemas {
			if strings.EqualFold(pubSchema, schema) {
				names = append(names, pub.ID.PublicationName())
				return false, nil
			}
		}
		for _, relation := range pub.Tables {
			if relation.Table == tableID {
				names = append(names, pub.ID.PublicationName())
				return false, nil
			}
		}
		return false, nil
	})
	return names, err
}

func publicationPublishesAction(pub publications.Publication, action replicationChangeAction) bool {
	switch action {
	case replicationChangeInsert:
		return pub.PublishInsert
	case replicationChangeUpdate:
		return pub.PublishUpdate
	case replicationChangeDelete:
		return pub.PublishDelete
	default:
		return false
	}
}

func encodeBeginMessage(commitLSN pglogrepl.LSN) []byte {
	data := []byte{byte(pglogrepl.MessageTypeBegin)}
	data = binary.BigEndian.AppendUint64(data, uint64(commitLSN))
	data = binary.BigEndian.AppendUint64(data, uint64(timeToPgTime(time.Now())))
	data = binary.BigEndian.AppendUint32(data, uint32(commitLSN))
	return data
}

func encodeCommitMessage(commitLSN pglogrepl.LSN) []byte {
	data := []byte{byte(pglogrepl.MessageTypeCommit), 0}
	data = binary.BigEndian.AppendUint64(data, uint64(commitLSN))
	data = binary.BigEndian.AppendUint64(data, uint64(commitLSN))
	data = binary.BigEndian.AppendUint64(data, uint64(timeToPgTime(time.Now())))
	return data
}

func encodeRelationMessage(relationID uint32, schema string, table string, tableSchema sql.Schema, fields []pgproto3.FieldDescription) []byte {
	data := []byte{byte(pglogrepl.MessageTypeRelation)}
	data = binary.BigEndian.AppendUint32(data, relationID)
	data = appendCString(data, schema)
	data = appendCString(data, table)
	data = append(data, 'd')
	data = binary.BigEndian.AppendUint16(data, uint16(len(fields)))
	primaryKeys := make(map[string]struct{})
	for _, column := range tableSchema {
		if column.PrimaryKey {
			primaryKeys[strings.ToLower(column.Name)] = struct{}{}
		}
	}
	for _, field := range fields {
		flag := byte(0)
		if _, ok := primaryKeys[strings.ToLower(string(field.Name))]; ok {
			flag = 1
		}
		data = append(data, flag)
		data = appendCString(data, string(field.Name))
		data = binary.BigEndian.AppendUint32(data, field.DataTypeOID)
		data = binary.BigEndian.AppendUint32(data, uint32(field.TypeModifier))
	}
	return data
}

func (capture *replicationChangeCapture) encodeRowMessage(relationID uint32, row Row) []byte {
	switch capture.action {
	case replicationChangeInsert:
		data := []byte{byte(pglogrepl.MessageTypeInsert)}
		data = binary.BigEndian.AppendUint32(data, relationID)
		data = append(data, 'N')
		return appendTupleData(data, row.val)
	case replicationChangeUpdate:
		data := []byte{byte(pglogrepl.MessageTypeUpdate)}
		data = binary.BigEndian.AppendUint32(data, relationID)
		data = append(data, 'N')
		return appendTupleData(data, row.val)
	case replicationChangeDelete:
		data := []byte{byte(pglogrepl.MessageTypeDelete)}
		data = binary.BigEndian.AppendUint32(data, relationID)
		data = append(data, 'O')
		return appendTupleData(data, row.val)
	default:
		return nil
	}
}

func appendTupleData(data []byte, values [][]byte) []byte {
	data = binary.BigEndian.AppendUint16(data, uint16(len(values)))
	for _, value := range values {
		if value == nil {
			data = append(data, byte(pglogrepl.TupleDataTypeNull))
			continue
		}
		data = append(data, byte(pglogrepl.TupleDataTypeText))
		data = binary.BigEndian.AppendUint32(data, uint32(len(value)))
		data = append(data, value...)
	}
	return data
}

func appendCString(data []byte, value string) []byte {
	data = append(data, value...)
	return append(data, 0)
}
