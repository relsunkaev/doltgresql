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
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/publications"
	"github.com/dolthub/doltgresql/server/replicaidentity"
	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type replicationChangeAction byte

var (
	publicationRowFilterIsNotDistinctFromNull = regexp.MustCompile(`(?i)\bis\s+not\s+distinct\s+from\s+null\b`)
	publicationRowFilterIsDistinctFromNull    = regexp.MustCompile(`(?i)\bis\s+distinct\s+from\s+null\b`)
	publicationRowFilterSimpleOperand         = `(?:[a-z_][a-z0-9_]*|true|false|null|[0-9]+(?:\.[0-9]+)?|'(?:''|[^'])*')`
	publicationRowFilterIsNotDistinctFrom     = regexp.MustCompile(`(?i)(` + publicationRowFilterSimpleOperand + `)\s+is\s+not\s+distinct\s+from\s+(` + publicationRowFilterSimpleOperand + `)`)
	publicationRowFilterIsDistinctFrom        = regexp.MustCompile(`(?i)(` + publicationRowFilterSimpleOperand + `)\s+is\s+distinct\s+from\s+(` + publicationRowFilterSimpleOperand + `)`)
	publicationRowFilterTextCast              = regexp.MustCompile(`(?i)(` + publicationRowFilterSimpleOperand + `)\s*::\s*(?:text|varchar|character\s+varying)\b`)
	publicationRowFilterStringAnnotation      = regexp.MustCompile(`(?i)(` + publicationRowFilterSimpleOperand + `)\s*:::\s*string\b`)
	publicationRowFilterIsNotUnknown          = regexp.MustCompile(`(?i)\bis\s+not\s+unknown\b`)
	publicationRowFilterIsUnknown             = regexp.MustCompile(`(?i)\bis\s+unknown\b`)
)

const (
	replicationChangeInsert   replicationChangeAction = 'I'
	replicationChangeUpdate   replicationChangeAction = 'U'
	replicationChangeDelete   replicationChangeAction = 'D'
	replicationChangeTruncate replicationChangeAction = 'T'
)

type replicationChangeCapture struct {
	action            replicationChangeAction
	schema            string
	table             string
	clientReturnsRows bool
	fullRowFieldCount int
	fields            []pgproto3.FieldDescription
	rows              []Row
	oldRows           []Row
	rowsAffected      uint64
}

type publicationChangeTarget struct {
	name          string
	columns       []string
	rowFilter     string
	publishInsert bool
	publishUpdate bool
	publishDelete bool
}

type rowFilterValue struct {
	data    []byte
	null    bool
	typeOID uint32
}

type replicationProjectedRow struct {
	newRow       Row
	oldRow       Row
	action       replicationChangeAction
	forceOldFull bool
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
	case *vitess.DDL:
		if stmt.Action != vitess.TruncateStr {
			return nil, false
		}
		return &replicationChangeCapture{
			action: replicationChangeTruncate,
			schema: stmt.Table.SchemaQualifier.String(),
			table:  stmt.Table.Name.String(),
		}, true
	default:
		return nil, false
	}
}

func tableNameFromTableExprs(exprs vitess.TableExprs) (schema string, table string, ok bool) {
	if len(exprs) != 1 {
		return "", "", false
	}
	return tableNameFromTableExpr(exprs[0])
}

func tableNameFromTableExpr(expr vitess.TableExpr) (schema string, table string, ok bool) {
	aliased, ok := expr.(*vitess.AliasedTableExpr)
	if !ok {
		if join, ok := expr.(*vitess.JoinTableExpr); ok {
			return tableNameFromTableExpr(join.RightExpr)
		}
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
	appendQualifiedFullRow := func(returning vitess.SelectExprs, qualifier string) vitess.SelectExprs {
		for _, column := range fullRowColumns {
			colName := vitess.NewColName(column)
			if qualifier != "" {
				colName.Qualifier = vitess.TableName{Name: vitess.NewTableIdent(qualifier)}
			}
			returning = append(returning, &vitess.AliasedExpr{Expr: colName})
		}
		return returning
	}
	switch stmt := statement.(type) {
	case *vitess.Insert:
		stmt.Returning = appendFullRow(stmt.Returning)
	case *vitess.Update:
		stmt.Returning = appendQualifiedFullRow(stmt.Returning, targetQualifierFromTableExprs(stmt.TableExprs))
	case *vitess.Delete:
		stmt.Returning = appendFullRow(stmt.Returning)
	}
}

func targetQualifierFromTableExprs(exprs vitess.TableExprs) string {
	if len(exprs) != 1 {
		return ""
	}
	return targetQualifierFromTableExpr(exprs[0])
}

func targetQualifierFromTableExpr(expr vitess.TableExpr) string {
	aliased, ok := expr.(*vitess.AliasedTableExpr)
	if !ok {
		if join, ok := expr.(*vitess.JoinTableExpr); ok {
			return targetQualifierFromTableExpr(join.RightExpr)
		}
		return ""
	}
	if !aliased.As.IsEmpty() {
		return aliased.As.String()
	}
	tableName, ok := aliased.Expr.(vitess.TableName)
	if !ok {
		return ""
	}
	return tableName.Name.String()
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

func (capture *replicationChangeCapture) requiresFullRows() bool {
	if capture == nil {
		return false
	}
	return capture.action == replicationChangeInsert || capture.action == replicationChangeUpdate || capture.action == replicationChangeDelete
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
	capture.fullRowFieldCount = len(table.Schema(ctx))
	return capture.fullRowFieldCount, nil
}

func (capture *replicationChangeCapture) fullRowColumnNames(ctx *sql.Context) ([]string, error) {
	table, _, err := capture.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(table.Schema(ctx)))
	for i, column := range table.Schema(ctx) {
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
	return publishReplicationCaptures(ctx, []*replicationChangeCapture{capture})
}

func publishReplicationCaptures(ctx *sql.Context, captures []*replicationChangeCapture) error {
	if len(captures) == 0 {
		return nil
	}
	type publicationMessages struct {
		name     string
		messages []replsource.WALMessage
	}
	messagesByPublication := make(map[string]*publicationMessages)
	var publicationOrder []string
	var commitLSN pglogrepl.LSN
	var beginData []byte
	advanceOnly := false
	for _, capture := range captures {
		if capture == nil {
			continue
		}
		if capture.action == replicationChangeTruncate {
			if ctx == nil {
				continue
			}
			table, schema, err := capture.resolveTable(ctx)
			if err != nil {
				return err
			}
			targets, err := capture.publicationTargets(ctx, schema)
			if err != nil {
				return err
			}
			if len(targets) == 0 {
				continue
			}
			if commitLSN == 0 {
				commitLSN = replsource.AdvanceLSN()
				beginData = encodeBeginMessage(commitLSN)
			}
			relationID := id.Cache().ToOID(id.NewTable(schema, capture.table).AsId())
			fields, err := schemaToFieldDescriptions(ctx, table.Schema(ctx), nil)
			if err != nil {
				return err
			}
			replIdent := replicaidentity.Get(ctx.GetCurrentDatabase(), schema, table.Name())
			keyColumns, err := relationReplicaIdentityColumns(ctx, table, replIdent)
			if err != nil {
				return err
			}
			relation := encodeRelationMessage(relationID, schema, capture.table, table.Schema(ctx), fields, replIdent.Identity.Byte(), keyColumns)
			truncate := encodeTruncateMessage([]uint32{relationID}, 0)
			for _, target := range targets {
				pubMessages := messagesByPublication[target.name]
				if pubMessages == nil {
					pubMessages = &publicationMessages{
						name: target.name,
						messages: []replsource.WALMessage{
							{WALStart: commitLSN, ServerWALEnd: commitLSN, WALData: beginData},
						},
					}
					messagesByPublication[target.name] = pubMessages
					publicationOrder = append(publicationOrder, target.name)
				}
				pubMessages.messages = append(pubMessages.messages,
					replsource.WALMessage{
						WALStart:     commitLSN,
						ServerWALEnd: commitLSN,
						WALData:      relation,
					},
					replsource.WALMessage{
						WALStart:     commitLSN,
						ServerWALEnd: commitLSN,
						WALData:      truncate,
					},
				)
			}
			continue
		}
		if len(capture.rows) == 0 {
			if capture.rowsAffected > 0 {
				advanceOnly = true
			}
			continue
		}
		if ctx == nil {
			continue
		}
		table, schema, err := capture.resolveTable(ctx)
		if err != nil {
			return err
		}
		targets, err := capture.publicationTargets(ctx, schema)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			continue
		}
		relationID := id.Cache().ToOID(id.NewTable(schema, capture.table).AsId())
		for _, target := range targets {
			fields, rows, err := capture.projectRowsForPublication(ctx, target)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				continue
			}
			if commitLSN == 0 {
				commitLSN = replsource.AdvanceLSN()
				beginData = encodeBeginMessage(commitLSN)
			}
			pubMessages := messagesByPublication[target.name]
			if pubMessages == nil {
				pubMessages = &publicationMessages{
					name: target.name,
					messages: []replsource.WALMessage{
						{WALStart: commitLSN, ServerWALEnd: commitLSN, WALData: beginData},
					},
				}
				messagesByPublication[target.name] = pubMessages
				publicationOrder = append(publicationOrder, target.name)
			}
			replIdent := replicaidentity.Get(ctx.GetCurrentDatabase(), schema, table.Name())
			keyColumns, err := relationReplicaIdentityColumns(ctx, table, replIdent)
			if err != nil {
				return err
			}
			relation := encodeRelationMessage(relationID, schema, capture.table, table.Schema(ctx), fields, replIdent.Identity.Byte(), keyColumns)
			pubMessages.messages = append(pubMessages.messages, replsource.WALMessage{
				WALStart:     commitLSN,
				ServerWALEnd: commitLSN,
				WALData:      relation,
			})
			for _, row := range rows {
				pubMessages.messages = append(pubMessages.messages, replsource.WALMessage{
					WALStart:     commitLSN,
					ServerWALEnd: commitLSN,
					WALData:      capture.encodeRowMessage(relationID, row, fields, keyColumns, replIdent.Identity),
				})
			}
		}
	}
	if commitLSN == 0 {
		if advanceOnly {
			replsource.AdvanceLSN()
		}
		return nil
	}
	type broadcastBatch struct {
		publications []string
		messages     []replsource.WALMessage
	}
	commitData := encodeCommitMessage(commitLSN)
	var batches []broadcastBatch
	for _, publication := range publicationOrder {
		pubMessages := messagesByPublication[publication]
		pubMessages.messages = append(pubMessages.messages, replsource.WALMessage{
			WALStart:     commitLSN,
			ServerWALEnd: commitLSN,
			WALData:      commitData,
		})
		matched := false
		for i := range batches {
			if walMessagesEqual(batches[i].messages, pubMessages.messages) {
				batches[i].publications = append(batches[i].publications, pubMessages.name)
				matched = true
				break
			}
		}
		if !matched {
			batches = append(batches, broadcastBatch{
				publications: []string{pubMessages.name},
				messages:     pubMessages.messages,
			})
		}
	}
	for _, batch := range batches {
		if err := replsource.Broadcast(batch.publications, batch.messages); err != nil {
			return err
		}
	}
	return nil
}

func walMessagesEqual(left []replsource.WALMessage, right []replsource.WALMessage) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].WALStart != right[i].WALStart || left[i].ServerWALEnd != right[i].ServerWALEnd {
			return false
		}
		if !bytes.Equal(left[i].WALData, right[i].WALData) {
			return false
		}
	}
	return true
}

func (capture *replicationChangeCapture) publicationTargets(ctx *sql.Context, schema string) ([]publicationChangeTarget, error) {
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var targets []publicationChangeTarget
	tableID := id.NewTable(schema, capture.table)
	err = collection.IteratePublications(ctx, func(pub publications.Publication) (stop bool, err error) {
		if !publicationPublishesCaptureAction(pub, capture.action) {
			return false, nil
		}
		for _, relation := range pub.Tables {
			if relation.Table == tableID {
				targets = append(targets, publicationChangeTargetFor(pub, relation.Columns, relation.RowFilter))
				return false, nil
			}
		}
		if pub.AllTables {
			targets = append(targets, publicationChangeTargetFor(pub, nil, ""))
			return false, nil
		}
		for _, pubSchema := range pub.Schemas {
			if strings.EqualFold(pubSchema, schema) {
				targets = append(targets, publicationChangeTargetFor(pub, nil, ""))
				return false, nil
			}
		}
		return false, nil
	})
	return targets, err
}

func publicationChangeTargetFor(pub publications.Publication, columns []string, rowFilter string) publicationChangeTarget {
	return publicationChangeTarget{
		name:          pub.ID.PublicationName(),
		columns:       columns,
		rowFilter:     rowFilter,
		publishInsert: pub.PublishInsert,
		publishUpdate: pub.PublishUpdate,
		publishDelete: pub.PublishDelete,
	}
}

func (capture *replicationChangeCapture) projectRowsForPublication(ctx *sql.Context, target publicationChangeTarget) ([]pgproto3.FieldDescription, []replicationProjectedRow, error) {
	indexes, err := capture.publicationColumnIndexes(target.columns)
	if err != nil {
		return nil, nil, err
	}
	fields := make([]pgproto3.FieldDescription, len(indexes))
	for i, idx := range indexes {
		fields[i] = capture.fields[idx]
	}
	filterExpr, err := parsePublicationRowFilter(target.rowFilter)
	if err != nil {
		return nil, nil, err
	}
	rows := make([]replicationProjectedRow, 0, len(capture.rows))
	for rowIdx, row := range capture.rows {
		newMatches, err := capture.rowMatchesPublicationFilter(ctx, row, filterExpr)
		if err != nil {
			return nil, nil, err
		}
		projected := Row{val: make([][]byte, len(indexes))}
		for i, idx := range indexes {
			projected.val[i] = append([]byte(nil), row.val[idx]...)
		}
		projectedRow := replicationProjectedRow{
			newRow: projected,
			action: capture.action,
		}
		oldMatches := false
		if rowIdx < len(capture.oldRows) {
			oldMatches, err = capture.rowMatchesPublicationFilter(ctx, capture.oldRows[rowIdx], filterExpr)
			if err != nil {
				return nil, nil, err
			}
			projectedOld := Row{val: make([][]byte, len(indexes))}
			for i, idx := range indexes {
				if idx < len(capture.oldRows[rowIdx].val) {
					projectedOld.val[i] = append([]byte(nil), capture.oldRows[rowIdx].val[idx]...)
				}
			}
			projectedRow.oldRow = projectedOld
		}
		if capture.action == replicationChangeUpdate && rowIdx < len(capture.oldRows) {
			switch {
			case oldMatches && newMatches:
				projectedRow.action = replicationChangeUpdate
			case oldMatches && !newMatches:
				projectedRow.action = replicationChangeDelete
				projectedRow.newRow = projectedRow.oldRow
				projectedRow.oldRow = Row{}
				projectedRow.forceOldFull = true
			case !oldMatches && newMatches:
				projectedRow.action = replicationChangeInsert
				projectedRow.oldRow = Row{}
			default:
				continue
			}
		} else if !newMatches {
			continue
		}
		if !target.publishesAction(projectedRow.action) {
			continue
		}
		rows = append(rows, projectedRow)
	}
	return fields, rows, nil
}

func (target publicationChangeTarget) publishesAction(action replicationChangeAction) bool {
	switch action {
	case replicationChangeInsert:
		return target.publishInsert
	case replicationChangeUpdate:
		return target.publishUpdate
	case replicationChangeDelete:
		return target.publishDelete
	default:
		return false
	}
}

func (capture *replicationChangeCapture) publicationColumnIndexes(columns []string) ([]int, error) {
	if len(columns) == 0 {
		indexes := make([]int, len(capture.fields))
		for i := range capture.fields {
			indexes[i] = i
		}
		return indexes, nil
	}
	indexByName := make(map[string]int, len(capture.fields))
	for i, field := range capture.fields {
		indexByName[strings.ToLower(string(field.Name))] = i
	}
	indexes := make([]int, len(columns))
	for i, column := range columns {
		idx, ok := indexByName[strings.ToLower(column)]
		if !ok {
			return nil, errors.Errorf(`publication column "%s" was not captured for table "%s"`, column, capture.table)
		}
		indexes[i] = idx
	}
	return indexes, nil
}

func parsePublicationRowFilter(rowFilter string) (vitess.Expr, error) {
	rowFilter = strings.TrimSpace(rowFilter)
	if rowFilter == "" {
		return nil, nil
	}
	rowFilter = normalizePublicationRowFilter(rowFilter)
	statement, err := vitess.Parse("SELECT 1 WHERE " + rowFilter)
	if err != nil {
		return nil, err
	}
	selectStatement, ok := statement.(*vitess.Select)
	if !ok || selectStatement.Where == nil {
		return nil, errors.Errorf("publication row filter did not parse as a WHERE expression")
	}
	return selectStatement.Where.Expr, nil
}

func normalizePublicationRowFilter(rowFilter string) string {
	rowFilter = publicationRowFilterIsNotDistinctFromNull.ReplaceAllString(rowFilter, "IS NULL")
	rowFilter = publicationRowFilterIsDistinctFromNull.ReplaceAllString(rowFilter, "IS NOT NULL")
	rowFilter = publicationRowFilterIsNotDistinctFrom.ReplaceAllString(rowFilter, "($1 <=> $2)")
	rowFilter = publicationRowFilterIsDistinctFrom.ReplaceAllString(rowFilter, "NOT ($1 <=> $2)")
	rowFilter = publicationRowFilterTextCast.ReplaceAllString(rowFilter, "$1")
	rowFilter = publicationRowFilterStringAnnotation.ReplaceAllString(rowFilter, "$1")
	rowFilter = normalizePublicationEscapedStringLiterals(rowFilter)
	rowFilter = normalizePublicationTrailingTupleCommas(rowFilter)
	rowFilter = publicationRowFilterIsNotUnknown.ReplaceAllString(rowFilter, "IS NOT NULL")
	rowFilter = publicationRowFilterIsUnknown.ReplaceAllString(rowFilter, "IS NULL")
	return rowFilter
}

func normalizePublicationEscapedStringLiterals(rowFilter string) string {
	var b strings.Builder
	b.Grow(len(rowFilter))
	for i := 0; i < len(rowFilter); {
		if i+1 < len(rowFilter) && (rowFilter[i] == 'e' || rowFilter[i] == 'E') && rowFilter[i+1] == '\'' && (i == 0 || !isPublicationRowFilterIdentPart(rowFilter[i-1])) {
			b.WriteByte('\'')
			i += 2
			for i < len(rowFilter) {
				switch rowFilter[i] {
				case '\\':
					if i+1 >= len(rowFilter) {
						b.WriteByte('\\')
						i++
						continue
					}
					if rowFilter[i+1] == '\'' {
						b.WriteString("''")
					} else {
						b.WriteByte(rowFilter[i+1])
					}
					i += 2
				case '\'':
					if i+1 < len(rowFilter) && rowFilter[i+1] == '\'' {
						b.WriteString("''")
						i += 2
						continue
					}
					b.WriteByte('\'')
					i++
					goto escapedLiteralDone
				default:
					b.WriteByte(rowFilter[i])
					i++
				}
			}
		escapedLiteralDone:
			continue
		}
		b.WriteByte(rowFilter[i])
		i++
	}
	return b.String()
}

func normalizePublicationTrailingTupleCommas(rowFilter string) string {
	var b strings.Builder
	b.Grow(len(rowFilter))
	inString := false
	for i := 0; i < len(rowFilter); {
		if rowFilter[i] == '\'' {
			b.WriteByte(rowFilter[i])
			i++
			if inString && i < len(rowFilter) && rowFilter[i] == '\'' {
				b.WriteByte(rowFilter[i])
				i++
				continue
			}
			inString = !inString
			continue
		}
		if !inString && rowFilter[i] == ',' {
			j := i + 1
			for j < len(rowFilter) && (rowFilter[j] == ' ' || rowFilter[j] == '\t' || rowFilter[j] == '\n' || rowFilter[j] == '\r') {
				j++
			}
			if j < len(rowFilter) && rowFilter[j] == ')' {
				i = j
				continue
			}
		}
		b.WriteByte(rowFilter[i])
		i++
	}
	return b.String()
}

func isPublicationRowFilterIdentPart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

func (capture *replicationChangeCapture) rowMatchesPublicationFilter(ctx *sql.Context, row Row, expr vitess.Expr) (bool, error) {
	if expr == nil {
		return true, nil
	}
	values := make(map[string]rowFilterValue, len(capture.fields))
	for i, field := range capture.fields {
		value := rowFilterValue{}
		if i >= len(row.val) || row.val[i] == nil {
			value.null = true
		} else {
			value.data = row.val[i]
		}
		value.typeOID = field.DataTypeOID
		values[strings.ToLower(string(field.Name))] = value
	}
	return evalPublicationFilterBool(ctx, expr, values)
}

func evalPublicationFilterBool(ctx *sql.Context, expr vitess.Expr, values map[string]rowFilterValue) (bool, error) {
	switch typed := expr.(type) {
	case *vitess.AndExpr:
		left, err := evalPublicationFilterBool(ctx, typed.Left, values)
		if err != nil || !left {
			return left, err
		}
		return evalPublicationFilterBool(ctx, typed.Right, values)
	case *vitess.OrExpr:
		left, err := evalPublicationFilterBool(ctx, typed.Left, values)
		if err != nil || left {
			return left, err
		}
		return evalPublicationFilterBool(ctx, typed.Right, values)
	case *vitess.NotExpr:
		value, err := evalPublicationFilterBool(ctx, typed.Expr, values)
		return !value, err
	case *vitess.ParenExpr:
		return evalPublicationFilterBool(ctx, typed.Expr, values)
	case *vitess.ComparisonExpr:
		return evalPublicationFilterComparison(ctx, typed, values)
	case *vitess.RangeCond:
		return evalPublicationFilterRange(ctx, typed, values)
	case *vitess.IsExpr:
		value, err := evalPublicationFilterScalar(typed.Expr, values)
		if err != nil {
			return false, err
		}
		switch strings.ToLower(typed.Operator) {
		case vitess.IsNullStr:
			return value.null, nil
		case vitess.IsNotNullStr:
			return !value.null, nil
		case vitess.IsTrueStr:
			boolValue, ok := rowFilterValueBool(ctx, value)
			return ok && boolValue, nil
		case vitess.IsNotTrueStr:
			boolValue, ok := rowFilterValueBool(ctx, value)
			return !ok || !boolValue, nil
		case vitess.IsFalseStr:
			boolValue, ok := rowFilterValueBool(ctx, value)
			return ok && !boolValue, nil
		case vitess.IsNotFalseStr:
			boolValue, ok := rowFilterValueBool(ctx, value)
			return !ok || boolValue, nil
		default:
			return false, errors.Errorf("publication row filter operator %q is not supported", typed.Operator)
		}
	case *vitess.ColName, *vitess.SQLVal, vitess.BoolVal, *vitess.NullVal:
		value, err := evalPublicationFilterScalar(expr, values)
		if err != nil {
			return false, err
		}
		boolValue, ok := rowFilterValueBool(ctx, value)
		if !ok {
			return false, nil
		}
		return boolValue, nil
	default:
		return false, errors.Errorf("publication row filter expression %T is not supported", expr)
	}
}

func evalPublicationFilterRange(ctx *sql.Context, expr *vitess.RangeCond, values map[string]rowFilterValue) (bool, error) {
	left, err := evalPublicationFilterScalar(expr.Left, values)
	if err != nil {
		return false, err
	}
	from, err := evalPublicationFilterScalar(expr.From, values)
	if err != nil {
		return false, err
	}
	to, err := evalPublicationFilterScalar(expr.To, values)
	if err != nil {
		return false, err
	}
	lower, ok := rowFilterValuesCompare(ctx, left, from)
	if !ok {
		return false, nil
	}
	upper, ok := rowFilterValuesCompare(ctx, left, to)
	if !ok {
		return false, nil
	}
	between := lower >= 0 && upper <= 0
	switch strings.ToLower(expr.Operator) {
	case vitess.BetweenStr:
		return between, nil
	case vitess.NotBetweenStr:
		return !between, nil
	default:
		return false, errors.Errorf("publication row filter range operator %q is not supported", expr.Operator)
	}
}

func evalPublicationFilterComparison(ctx *sql.Context, expr *vitess.ComparisonExpr, values map[string]rowFilterValue) (bool, error) {
	left, err := evalPublicationFilterScalar(expr.Left, values)
	if err != nil {
		return false, err
	}
	if strings.EqualFold(expr.Operator, vitess.InStr) || strings.EqualFold(expr.Operator, vitess.NotInStr) {
		tuple, ok := expr.Right.(vitess.ValTuple)
		if !ok {
			return false, errors.Errorf("publication row filter IN requires a literal tuple")
		}
		matches := false
		for _, tupleExpr := range tuple {
			right, err := evalPublicationFilterScalar(tupleExpr, values)
			if err != nil {
				return false, err
			}
			if rowFilterValuesEqual(ctx, left, right) {
				matches = true
				break
			}
		}
		if strings.EqualFold(expr.Operator, vitess.NotInStr) {
			return !matches, nil
		}
		return matches, nil
	}
	right, err := evalPublicationFilterScalar(expr.Right, values)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(expr.Operator) {
	case vitess.EqualStr:
		return rowFilterValuesEqual(ctx, left, right), nil
	case vitess.NullSafeEqualStr:
		if left.null || right.null {
			return left.null && right.null, nil
		}
		return rowFilterValuesEqual(ctx, left, right), nil
	case vitess.LikeStr, vitess.NotLikeStr:
		match, ok, err := rowFilterValuesLike(left, right)
		if err != nil || !ok {
			return false, err
		}
		if strings.EqualFold(expr.Operator, vitess.NotLikeStr) {
			return !match, nil
		}
		return match, nil
	case vitess.NotEqualStr, "<>":
		if left.null || right.null {
			return false, nil
		}
		return !rowFilterValuesEqual(ctx, left, right), nil
	case vitess.LessThanStr:
		cmp, ok := rowFilterValuesCompare(ctx, left, right)
		return ok && cmp < 0, nil
	case vitess.LessEqualStr:
		cmp, ok := rowFilterValuesCompare(ctx, left, right)
		return ok && cmp <= 0, nil
	case vitess.GreaterThanStr:
		cmp, ok := rowFilterValuesCompare(ctx, left, right)
		return ok && cmp > 0, nil
	case vitess.GreaterEqualStr:
		cmp, ok := rowFilterValuesCompare(ctx, left, right)
		return ok && cmp >= 0, nil
	default:
		return false, errors.Errorf("publication row filter comparison operator %q is not supported", expr.Operator)
	}
}

func evalPublicationFilterScalar(expr vitess.Expr, values map[string]rowFilterValue) (rowFilterValue, error) {
	switch typed := expr.(type) {
	case *vitess.ColName:
		value, ok := values[strings.ToLower(typed.Name.String())]
		if !ok {
			return rowFilterValue{}, errors.Errorf(`publication row filter references unknown column "%s"`, typed.Name.String())
		}
		return value, nil
	case *vitess.SQLVal:
		return rowFilterValue{data: append([]byte(nil), typed.Val...)}, nil
	case vitess.BoolVal:
		if bool(typed) {
			return rowFilterValue{data: []byte("true"), typeOID: id.Cache().ToOID(pgtypes.Bool.ID.AsId())}, nil
		}
		return rowFilterValue{data: []byte("false"), typeOID: id.Cache().ToOID(pgtypes.Bool.ID.AsId())}, nil
	case *vitess.NullVal:
		return rowFilterValue{null: true}, nil
	case *vitess.ParenExpr:
		return evalPublicationFilterScalar(typed.Expr, values)
	case *vitess.BinaryExpr:
		return evalPublicationFilterBinaryExpr(typed, values)
	case *vitess.UnaryExpr:
		return evalPublicationFilterUnaryExpr(typed, values)
	case *vitess.FuncExpr:
		return evalPublicationFilterFunc(typed, values)
	default:
		return rowFilterValue{}, errors.Errorf("publication row filter scalar expression %T is not supported", expr)
	}
}

func evalPublicationFilterBinaryExpr(expr *vitess.BinaryExpr, values map[string]rowFilterValue) (rowFilterValue, error) {
	left, err := evalPublicationFilterScalar(expr.Left, values)
	if err != nil {
		return rowFilterValue{}, err
	}
	right, err := evalPublicationFilterScalar(expr.Right, values)
	if err != nil {
		return rowFilterValue{}, err
	}
	if left.null || right.null {
		return rowFilterValue{null: true}, nil
	}
	leftNum, ok := parseRowFilterNumeric(left.data)
	if !ok {
		return rowFilterValue{}, errors.Errorf("publication row filter arithmetic operator %q requires numeric operands", expr.Operator)
	}
	rightNum, ok := parseRowFilterNumeric(right.data)
	if !ok {
		return rowFilterValue{}, errors.Errorf("publication row filter arithmetic operator %q requires numeric operands", expr.Operator)
	}
	result := new(big.Rat)
	switch strings.ToLower(expr.Operator) {
	case vitess.PlusStr:
		result.Add(leftNum, rightNum)
	case vitess.MinusStr:
		result.Sub(leftNum, rightNum)
	case vitess.MultStr:
		result.Mul(leftNum, rightNum)
	case vitess.DivStr:
		if rightNum.Sign() == 0 {
			return rowFilterValue{}, errors.New("division by zero in publication row filter")
		}
		result.Quo(leftNum, rightNum)
	default:
		return rowFilterValue{}, errors.Errorf("publication row filter arithmetic operator %q is not supported", expr.Operator)
	}
	return rowFilterValue{data: []byte(result.String())}, nil
}

func evalPublicationFilterUnaryExpr(expr *vitess.UnaryExpr, values map[string]rowFilterValue) (rowFilterValue, error) {
	value, err := evalPublicationFilterScalar(expr.Expr, values)
	if err != nil || value.null {
		return value, err
	}
	num, ok := parseRowFilterNumeric(value.data)
	if !ok {
		return rowFilterValue{}, errors.Errorf("publication row filter unary operator %q requires a numeric operand", expr.Operator)
	}
	switch expr.Operator {
	case vitess.UPlusStr:
		return rowFilterValue{data: []byte(num.String())}, nil
	case vitess.UMinusStr:
		return rowFilterValue{data: []byte(new(big.Rat).Neg(num).String())}, nil
	default:
		return rowFilterValue{}, errors.Errorf("publication row filter unary operator %q is not supported", expr.Operator)
	}
}

func evalPublicationFilterFunc(expr *vitess.FuncExpr, values map[string]rowFilterValue) (rowFilterValue, error) {
	if !expr.Name.EqualString("coalesce") || !expr.Qualifier.IsEmpty() || expr.Distinct || expr.Over != nil {
		return rowFilterValue{}, errors.Errorf("publication row filter function %q is not supported", expr.Name.String())
	}
	if len(expr.Exprs) == 0 {
		return rowFilterValue{}, errors.Errorf("publication row filter COALESCE requires at least one argument")
	}
	for _, selectExpr := range expr.Exprs {
		aliasedExpr, ok := selectExpr.(*vitess.AliasedExpr)
		if !ok {
			return rowFilterValue{}, errors.Errorf("publication row filter COALESCE argument %T is not supported", selectExpr)
		}
		value, err := evalPublicationFilterScalar(aliasedExpr.Expr, values)
		if err != nil {
			return rowFilterValue{}, err
		}
		if !value.null {
			return value, nil
		}
	}
	return rowFilterValue{null: true}, nil
}

func rowFilterValueBool(ctx *sql.Context, value rowFilterValue) (bool, bool) {
	if value.null {
		return false, false
	}
	parsed, err := pgtypes.Bool.IoInput(ctx, strings.TrimSpace(string(value.data)))
	if err != nil {
		return false, false
	}
	boolValue, ok := parsed.(bool)
	return boolValue, ok
}

func rowFilterValuesEqual(ctx *sql.Context, left rowFilterValue, right rowFilterValue) bool {
	if left.null || right.null {
		return false
	}
	if rowFilterUsesTextComparison(left, right) {
		return string(left.data) == string(right.data)
	}
	if cmp, ok := rowFilterTypedCompare(ctx, left, right); ok {
		return cmp == 0
	}
	if leftNum, ok := parseRowFilterNumeric(left.data); ok {
		if rightNum, ok := parseRowFilterNumeric(right.data); ok {
			return leftNum.Cmp(rightNum) == 0
		}
	}
	return string(left.data) == string(right.data)
}

func rowFilterValuesCompare(ctx *sql.Context, left rowFilterValue, right rowFilterValue) (int, bool) {
	if left.null || right.null {
		return 0, false
	}
	if rowFilterUsesTextComparison(left, right) {
		return bytes.Compare(left.data, right.data), true
	}
	if cmp, ok := rowFilterTypedCompare(ctx, left, right); ok {
		return cmp, true
	}
	if leftNum, ok := parseRowFilterNumeric(left.data); ok {
		if rightNum, ok := parseRowFilterNumeric(right.data); ok {
			return leftNum.Cmp(rightNum), true
		}
	}
	return bytes.Compare(left.data, right.data), true
}

func rowFilterValuesLike(left rowFilterValue, right rowFilterValue) (bool, bool, error) {
	if left.null || right.null {
		return false, false, nil
	}
	re, err := regexp.Compile("(?s)" + rowFilterLikePatternToRegex(string(right.data)))
	if err != nil {
		return false, false, err
	}
	return re.Match(left.data), true, nil
}

func rowFilterLikePatternToRegex(pattern string) string {
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			} else {
				b.WriteString(regexp.QuoteMeta(`\`))
			}
		default:
			b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		}
	}
	b.WriteByte('$')
	return b.String()
}

func rowFilterTypedCompare(ctx *sql.Context, left rowFilterValue, right rowFilterValue) (int, bool) {
	doltgresType := rowFilterComparableType(left)
	if doltgresType == nil {
		doltgresType = rowFilterComparableType(right)
	}
	if doltgresType == nil {
		return 0, false
	}
	leftValue, ok := parseRowFilterTypedValue(ctx, doltgresType, left.data)
	if !ok {
		return 0, false
	}
	rightValue, ok := parseRowFilterTypedValue(ctx, doltgresType, right.data)
	if !ok {
		return 0, false
	}
	cmp, err := doltgresType.Compare(ctx, leftValue, rightValue)
	if err != nil {
		return 0, false
	}
	return cmp, true
}

func rowFilterComparableType(value rowFilterValue) *pgtypes.DoltgresType {
	switch value.typeOID {
	case id.Cache().ToOID(pgtypes.Bool.ID.AsId()):
		return pgtypes.Bool
	case id.Cache().ToOID(pgtypes.BpChar.ID.AsId()):
		return pgtypes.BpChar
	case id.Cache().ToOID(pgtypes.Bytea.ID.AsId()):
		return pgtypes.Bytea
	case id.Cache().ToOID(pgtypes.Date.ID.AsId()):
		return pgtypes.Date
	case id.Cache().ToOID(pgtypes.Interval.ID.AsId()):
		return pgtypes.Interval
	case id.Cache().ToOID(pgtypes.JsonB.ID.AsId()):
		return pgtypes.JsonB
	case id.Cache().ToOID(pgtypes.Timestamp.ID.AsId()):
		return pgtypes.Timestamp
	case id.Cache().ToOID(pgtypes.TimestampTZ.ID.AsId()):
		return pgtypes.TimestampTZ
	case id.Cache().ToOID(pgtypes.Time.ID.AsId()):
		return pgtypes.Time
	case id.Cache().ToOID(pgtypes.Uuid.ID.AsId()):
		return pgtypes.Uuid
	default:
		return nil
	}
}

func rowFilterUsesTextComparison(left rowFilterValue, right rowFilterValue) bool {
	return rowFilterIsTextComparable(left) || rowFilterIsTextComparable(right)
}

func rowFilterIsTextComparable(value rowFilterValue) bool {
	switch value.typeOID {
	case id.Cache().ToOID(pgtypes.Text.ID.AsId()), id.Cache().ToOID(pgtypes.VarChar.ID.AsId()):
		return true
	default:
		return false
	}
}

func parseRowFilterTypedValue(ctx *sql.Context, doltgresType *pgtypes.DoltgresType, data []byte) (any, bool) {
	input := strings.TrimSpace(string(data))
	if input == "" {
		return nil, false
	}
	value, err := doltgresType.IoInput(ctx, input)
	if err != nil {
		return nil, false
	}
	return value, true
}

func parseRowFilterNumeric(data []byte) (*big.Rat, bool) {
	value := strings.TrimSpace(string(data))
	if value == "" {
		return nil, false
	}
	rat := new(big.Rat)
	_, ok := rat.SetString(value)
	return rat, ok
}

func publicationPublishesAction(pub publications.Publication, action replicationChangeAction) bool {
	switch action {
	case replicationChangeInsert:
		return pub.PublishInsert
	case replicationChangeUpdate:
		return pub.PublishUpdate
	case replicationChangeDelete:
		return pub.PublishDelete
	case replicationChangeTruncate:
		return pub.PublishTruncate
	default:
		return false
	}
}

func publicationPublishesCaptureAction(pub publications.Publication, action replicationChangeAction) bool {
	if action == replicationChangeUpdate {
		return pub.PublishInsert || pub.PublishUpdate || pub.PublishDelete
	}
	return publicationPublishesAction(pub, action)
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

func encodeTruncateMessage(relationIDs []uint32, option uint8) []byte {
	data := []byte{byte(pglogrepl.MessageTypeTruncate)}
	data = binary.BigEndian.AppendUint32(data, uint32(len(relationIDs)))
	data = append(data, option)
	for _, relationID := range relationIDs {
		data = binary.BigEndian.AppendUint32(data, relationID)
	}
	return data
}

func encodeRelationMessage(relationID uint32, schema string, table string, tableSchema sql.Schema, fields []pgproto3.FieldDescription, replicaIdentity byte, keyColumns map[string]struct{}) []byte {
	data := []byte{byte(pglogrepl.MessageTypeRelation)}
	data = binary.BigEndian.AppendUint32(data, relationID)
	data = appendCString(data, schema)
	data = appendCString(data, table)
	if replicaIdentity == 0 {
		replicaIdentity = replicaidentity.IdentityDefault.Byte()
	}
	data = append(data, replicaIdentity)
	data = binary.BigEndian.AppendUint16(data, uint16(len(fields)))
	if keyColumns == nil {
		keyColumns = make(map[string]struct{})
		for _, column := range tableSchema {
			if column.PrimaryKey {
				keyColumns[strings.ToLower(column.Name)] = struct{}{}
			}
		}
	}
	for _, field := range fields {
		flag := byte(0)
		if _, ok := keyColumns[strings.ToLower(string(field.Name))]; ok {
			flag = 1
		}
		data = append(data, flag)
		data = appendCString(data, string(field.Name))
		data = binary.BigEndian.AppendUint32(data, field.DataTypeOID)
		data = binary.BigEndian.AppendUint32(data, uint32(field.TypeModifier))
	}
	return data
}

func relationReplicaIdentityColumns(ctx *sql.Context, table sql.Table, setting replicaidentity.Setting) (map[string]struct{}, error) {
	switch setting.Identity {
	case replicaidentity.IdentityNothing:
		return map[string]struct{}{}, nil
	case replicaidentity.IdentityFull:
		columns := make(map[string]struct{}, len(table.Schema(ctx)))
		for _, column := range table.Schema(ctx) {
			columns[strings.ToLower(column.Name)] = struct{}{}
		}
		return columns, nil
	case replicaidentity.IdentityUsingIndex:
		indexedTable, ok := table.(sql.IndexAddressable)
		if !ok {
			return map[string]struct{}{}, nil
		}
		indexes, err := indexedTable.GetIndexes(ctx)
		if err != nil {
			return nil, err
		}
		for _, index := range indexes {
			if !strings.EqualFold(logicalReplicationIndexName(index), setting.IndexName) && !strings.EqualFold(index.ID(), setting.IndexName) {
				continue
			}
			columns := make(map[string]struct{}, len(index.Expressions()))
			for _, expr := range index.Expressions() {
				columns[strings.ToLower(logicalReplicationIndexColumnName(expr))] = struct{}{}
			}
			return columns, nil
		}
		return map[string]struct{}{}, nil
	default:
		columns := make(map[string]struct{})
		for _, column := range table.Schema(ctx) {
			if column.PrimaryKey {
				columns[strings.ToLower(column.Name)] = struct{}{}
			}
		}
		return columns, nil
	}
}

func logicalReplicationIndexName(index sql.Index) string {
	if strings.EqualFold(index.ID(), "PRIMARY") {
		return fmt.Sprintf("%s_pkey", index.Table())
	}
	return index.ID()
}

func logicalReplicationIndexColumnName(expr string) string {
	lastDot := strings.LastIndex(expr, ".")
	return expr[lastDot+1:]
}

func (capture *replicationChangeCapture) encodeRowMessage(relationID uint32, row replicationProjectedRow, fields []pgproto3.FieldDescription, keyColumns map[string]struct{}, identity replicaidentity.Identity) []byte {
	action := row.action
	if action == 0 {
		action = capture.action
	}
	switch action {
	case replicationChangeInsert:
		data := []byte{byte(pglogrepl.MessageTypeInsert)}
		data = binary.BigEndian.AppendUint32(data, relationID)
		data = append(data, 'N')
		return appendTupleData(data, row.newRow.val)
	case replicationChangeUpdate:
		data := []byte{byte(pglogrepl.MessageTypeUpdate)}
		data = binary.BigEndian.AppendUint32(data, relationID)
		if identity == replicaidentity.IdentityFull && len(row.oldRow.val) > 0 {
			data = append(data, 'O')
			data = appendTupleData(data, row.oldRow.val)
		} else if len(row.oldRow.val) > 0 && replicaIdentityKeyChanged(row.oldRow.val, row.newRow.val, fields, keyColumns) {
			data = append(data, 'K')
			data = appendTupleData(data, replicaIdentityTuple(row.oldRow.val, fields, keyColumns))
		}
		data = append(data, 'N')
		return appendTupleData(data, row.newRow.val)
	case replicationChangeDelete:
		data := []byte{byte(pglogrepl.MessageTypeDelete)}
		data = binary.BigEndian.AppendUint32(data, relationID)
		if identity == replicaidentity.IdentityFull || row.forceOldFull {
			data = append(data, 'O')
			return appendTupleData(data, row.newRow.val)
		}
		data = append(data, 'K')
		return appendTupleData(data, replicaIdentityTuple(row.newRow.val, fields, keyColumns))
	default:
		return nil
	}
}

func replicaIdentityKeyChanged(oldValues [][]byte, newValues [][]byte, fields []pgproto3.FieldDescription, keyColumns map[string]struct{}) bool {
	for i, field := range fields {
		if _, ok := keyColumns[strings.ToLower(string(field.Name))]; !ok {
			continue
		}
		if i >= len(oldValues) || i >= len(newValues) {
			return true
		}
		if !bytes.Equal(oldValues[i], newValues[i]) {
			return true
		}
	}
	return false
}

func replicaIdentityTuple(values [][]byte, fields []pgproto3.FieldDescription, keyColumns map[string]struct{}) [][]byte {
	if len(keyColumns) == 0 {
		return values
	}
	ret := make([][]byte, 0, len(keyColumns))
	for i, field := range fields {
		if _, ok := keyColumns[strings.ToLower(string(field.Name))]; !ok {
			continue
		}
		if i < len(values) {
			ret = append(ret, values[i])
		} else {
			ret = append(ret, nil)
		}
	}
	if len(ret) == 0 {
		return values
	}
	return ret
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

func wrapReplicationCapturePlan(ctx *sql.Context, boundPlan sql.Node, capture *replicationChangeCapture) (sql.Node, error) {
	if capture == nil || capture.action != replicationChangeUpdate {
		return boundPlan, nil
	}
	wrappedPlan, _, err := transform.NodeWithOpaque(ctx, boundPlan, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		updateNode, ok := node.(*plan.Update)
		if !ok {
			return node, transform.SameTree, nil
		}
		if _, ok = updateNode.Child.(*replicationUpdateCaptureNode); ok {
			return node, transform.SameTree, nil
		}
		newNode, err := updateNode.WithChildren(ctx, &replicationUpdateCaptureNode{
			Source:  updateNode.Child,
			Capture: capture,
		})
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode, transform.NewTree, nil
	})
	return wrappedPlan, err
}

type replicationUpdateCaptureNode struct {
	Source  sql.Node
	Capture *replicationChangeCapture
}

var _ sql.ExecBuilderNode = (*replicationUpdateCaptureNode)(nil)

func (r *replicationUpdateCaptureNode) Children() []sql.Node {
	return []sql.Node{r.Source}
}

func (r *replicationUpdateCaptureNode) IsReadOnly() bool {
	return r.Source.IsReadOnly()
}

func (r *replicationUpdateCaptureNode) Resolved() bool {
	return r.Source.Resolved()
}

func (r *replicationUpdateCaptureNode) Schema(ctx *sql.Context) sql.Schema {
	return r.Source.Schema(ctx)
}

func (r *replicationUpdateCaptureNode) String() string {
	return "REPLICATION UPDATE CAPTURE"
}

func (r *replicationUpdateCaptureNode) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	ret := *r
	ret.Source = children[0]
	return &ret, nil
}

func (r *replicationUpdateCaptureNode) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	iter, err := b.Build(ctx, r.Source, row)
	if err != nil {
		return nil, err
	}
	schema := r.Source.Schema(ctx)
	return &replicationUpdateCaptureIter{
		source:  iter,
		capture: r.Capture,
		schema:  schema[:len(schema)/2],
	}, nil
}

type replicationUpdateCaptureIter struct {
	source  sql.RowIter
	capture *replicationChangeCapture
	schema  sql.Schema
}

var _ sql.RowIter = (*replicationUpdateCaptureIter)(nil)

func (r *replicationUpdateCaptureIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := r.source.Next(ctx)
	if err != nil {
		return row, err
	}
	if r.capture != nil && len(row) >= len(r.schema) {
		oldRow := row[:len(r.schema)]
		outputRow, err := rowToBytes(ctx, r.schema, oldRow, nil)
		if err != nil {
			return nil, err
		}
		r.capture.oldRows = append(r.capture.oldRows, Row{val: outputRow})
	}
	return row, nil
}

func (r *replicationUpdateCaptureIter) Close(ctx *sql.Context) error {
	return r.source.Close(ctx)
}
