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

package hook

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/server/ast"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// BeforeTableAddColumn handles validation that's unique to Doltgres.
func BeforeTableAddColumn(ctx *sql.Context, runner sql.StatementRunner, nodeInterface sql.Node) (sql.Node, error) {
	n, ok := nodeInterface.(*plan.AddColumn)
	if !ok {
		return nil, errors.Errorf("ADD COLUMN pre-hook expected `*plan.AddColumn` but received `%T`", nodeInterface)
	}
	if err := prepareSerialAddColumn(ctx, n); err != nil {
		return nil, err
	}
	var domainType *pgtypes.DoltgresType
	if normalized, dt, err := normalizeAddColumnDomainNullability(ctx, n); err != nil {
		return nil, err
	} else {
		n = normalized
		domainType = dt
	}
	// A NOT NULL column with no default would rewrite existing rows with NULL in
	// PostgreSQL, so non-empty tables must reject it before the schema changes.
	if n.Column().Default == nil && n.Column().Generated == nil {
		if !n.Column().Nullable {
			hasRows, err := tableHasRows(ctx, n.Table)
			if err != nil {
				return nil, err
			}
			if hasRows {
				if domainType != nil {
					return nil, pgtypes.ErrDomainDoesNotAllowNullValues.New(domainType.Name())
				}
				return nil, errors.Errorf(`column "%s" of relation "%s" contains null values`, n.Column().Name, nodeTableName(n.Table))
			}
		}
		return n, nil
	}

	// Grab the table being altered
	doltTable := core.SQLNodeToDoltTable(n.Table)
	if doltTable == nil {
		// If this table isn't a Dolt table then we don't have anything to do
		return n, nil
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return n, nil
	}
	tableName := doltTable.TableName()
	tableAsType := id.NewType(tableName.Schema, tableName.Name)
	allTableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return nil, err
	}

	for _, otherTableName := range allTableNames {
		if doltdb.IsSystemTable(otherTableName) {
			// System tables don't use any table types
			continue
		}
		otherTable, ok, err := root.GetTable(ctx, otherTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("root returned table name `%s` but it could not be found?", otherTableName.String())
		}
		otherTableSch, err := otherTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		for _, otherCol := range otherTableSch.GetAllCols().GetColumns() {
			colType := otherCol.TypeInfo.ToSqlType()
			dgtype, ok := colType.(*pgtypes.DoltgresType)
			if !ok {
				// If this isn't a Doltgres type, then it can't be a table type so we can ignore it
				continue
			}
			if dgtype.ID != tableAsType {
				// This column isn't our table type, so we can ignore it
				continue
			}
			return nil, errors.Errorf(`cannot alter table "%s" because column "%s.%s" uses its row type`,
				tableName.Name, otherTableName.Name, otherCol.Name)
		}
	}
	return n, nil
}

func prepareSerialAddColumn(ctx *sql.Context, n *plan.AddColumn) error {
	col := n.Column()
	doltgresType, ok := col.Type.(*pgtypes.DoltgresType)
	if !ok || !doltgresType.IsSerial {
		return nil
	}

	generatedFromSequence := false
	if col.Generated != nil {
		generatedFromSequence = addColumnGeneratedFromSequence(ctx, col.Generated)
		if !generatedFromSequence {
			return nil
		}
	}

	doltTable := core.SQLNodeToDoltTable(n.Table)
	if doltTable == nil {
		return nil
	}
	tableName := doltTable.TableName()
	schemaName := tableName.Schema
	if schemaName == "" {
		var err error
		schemaName, err = core.GetSchemaName(ctx, n.Database(), "")
		if err != nil {
			return err
		}
	}
	databaseName := databaseNameForSQLDatabase(n.Database())

	sequenceName, err := generateAddColumnSequenceName(ctx, databaseName, schemaName, tableName.Name, col.Name)
	if err != nil {
		return err
	}
	seqName := sequenceDefaultName(ctx, databaseName, schemaName, sequenceName)
	nextVal, foundFunc, err := framework.GetFunction(ctx, "nextval", pgexprs.NewTextLiteral(seqName))
	if err != nil {
		return err
	}
	if !foundFunc {
		return errors.Errorf(`function "nextval" could not be found for SERIAL default`)
	}

	nextValExpr := &sql.ColumnDefaultValue{
		Expr:          nextVal,
		OutType:       pgtypes.Int64,
		Literal:       false,
		ReturnNil:     false,
		Parenthesized: false,
	}
	if generatedFromSequence {
		col.Generated = nextValExpr
	} else {
		col.Default = nextValExpr
	}

	var maxValue int64
	switch doltgresType.Name() {
	case "smallserial":
		col.Type = pgtypes.Int16
		maxValue = math.MaxInt16
	case "serial":
		col.Type = pgtypes.Int32
		maxValue = math.MaxInt32
	case "bigserial":
		col.Type = pgtypes.Int64
		maxValue = math.MaxInt64
	default:
		return errors.Errorf(`type "%s" cannot be serial`, doltgresType.String())
	}

	collection, err := core.GetSequencesCollectionFromContext(ctx, databaseName)
	if err != nil {
		return err
	}
	return collection.CreateSequence(ctx, &sequences.Sequence{
		Id:          id.NewSequence(schemaName, sequenceName),
		DataTypeID:  col.Type.(*pgtypes.DoltgresType).ID,
		Persistence: sequences.Persistence_Permanent,
		Start:       1,
		Current:     1,
		Increment:   1,
		Minimum:     1,
		Maximum:     maxValue,
		Cache:       1,
		Cycle:       false,
		IsAtEnd:     false,
		OwnerTable:  id.NewTable(schemaName, tableName.Name),
		OwnerColumn: col.Name,
	})
}

func addColumnGeneratedFromSequence(ctx *sql.Context, generated *sql.ColumnDefaultValue) bool {
	seenNextVal := false
	seenPlaceholder := false
	transform.InspectExpr(ctx, generated, func(ctx *sql.Context, expr sql.Expression) bool {
		switch e := expr.(type) {
		case *framework.CompiledFunction:
			if strings.EqualFold(e.Name, "nextval") {
				seenNextVal = true
			}
		case *expression.Literal:
			placeholderName := fmt.Sprintf("'%s'", ast.DoltCreateTablePlaceholderSequenceName)
			if e.String() == placeholderName {
				seenPlaceholder = true
			}
		}
		return false
	})
	return seenNextVal || seenPlaceholder
}

func generateAddColumnSequenceName(ctx *sql.Context, databaseName string, schemaName string, tableName string, columnName string) (string, error) {
	baseSequenceName := fmt.Sprintf("%s_%s_seq", tableName, columnName)
	sequenceName := baseSequenceName
	relationType, err := core.GetRelationTypeForDatabase(ctx, databaseName, schemaName, baseSequenceName)
	if err != nil {
		return "", err
	}
	if relationType != core.RelationType_DoesNotExist {
		seqIndex := 1
		for ; seqIndex <= maxSequenceAutoNames; seqIndex++ {
			sequenceName = fmt.Sprintf("%s%d", baseSequenceName, seqIndex)
			relationType, err = core.GetRelationTypeForDatabase(ctx, databaseName, schemaName, sequenceName)
			if err != nil {
				return "", err
			}
			if relationType == core.RelationType_DoesNotExist {
				break
			}
		}
		if seqIndex > maxSequenceAutoNames {
			return "", errors.Errorf("SERIAL sequence name reached max iterations")
		}
	}
	return sequenceName, nil
}

const maxSequenceAutoNames = 10_000

type revisionQualifiedDatabase interface {
	RevisionQualifiedName() string
}

func databaseNameForSQLDatabase(db sql.Database) string {
	if db == nil {
		return ""
	}
	if revisionDb, ok := db.(revisionQualifiedDatabase); ok {
		return revisionDb.RevisionQualifiedName()
	}
	return db.Name()
}

func sequenceDefaultName(ctx *sql.Context, databaseName string, schemaName string, sequenceName string) string {
	if databaseName == "" || databaseName == ctx.GetCurrentDatabase() {
		return doltdb.TableName{Name: sequenceName, Schema: schemaName}.String()
	}
	return quoteIdentifier(databaseName) + "." + quoteIdentifier(schemaName) + "." + quoteIdentifier(sequenceName)
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func tableHasRows(ctx *sql.Context, node any) (bool, error) {
	table, ok := node.(sql.Table)
	if !ok {
		return false, nil
	}
	_, ok, err := firstRow(ctx, table)
	return ok, err
}

func firstColumnValue(ctx *sql.Context, table sql.Table, columnName string) (any, bool, error) {
	columnIndex := table.Schema(ctx).IndexOfColName(columnName)
	if columnIndex < 0 {
		return nil, false, nil
	}
	row, ok, err := firstRow(ctx, table)
	if err != nil || !ok {
		return nil, ok, err
	}
	if columnIndex >= len(row) {
		return nil, false, nil
	}
	return row[columnIndex], true, nil
}

func firstRow(ctx *sql.Context, table sql.Table) (sql.Row, bool, error) {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return nil, false, err
		}
		row, err := rows.Next(ctx)
		closeErr := rows.Close(ctx)
		if err == nil {
			return row, true, closeErr
		}
		if !errors.Is(err, io.EOF) {
			return nil, false, err
		}
		if closeErr != nil {
			return nil, false, closeErr
		}
	}
}

func nodeTableName(node sql.Node) string {
	if table, ok := node.(sql.Table); ok {
		return table.Name()
	}
	return node.String()
}

// AfterTableAddColumn handles updating various table columns, alongside other validation that's unique to Doltgres.
func AfterTableAddColumn(ctx *sql.Context, runner sql.StatementRunner, nodeInterface sql.Node) error {
	n, ok := nodeInterface.(*plan.AddColumn)
	if !ok {
		return errors.Errorf("ADD COLUMN post-hook expected `*plan.AddColumn` but received `%T`", nodeInterface)
	}

	// Grab the table being altered
	doltTable := core.SQLNodeToDoltTable(n.Table)
	if doltTable == nil {
		// If this table isn't a Dolt table then we don't have anything to do
		return nil
	}
	_, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	tableName := doltTable.TableName()
	tableAsType := id.NewType(tableName.Schema, tableName.Name)
	allTableNames, err := root.GetAllTableNames(ctx, false)
	if err != nil {
		return err
	}
	sch := doltTable.Schema(ctx)
	if err = recordColumnMissingValueMetadata(ctx, n); err != nil {
		return err
	}

	for _, otherTableName := range allTableNames {
		if doltdb.IsSystemTable(otherTableName) {
			// System tables don't use any table types
			continue
		}
		otherTable, ok, err := root.GetTable(ctx, otherTableName)
		if err != nil {
			return err
		}
		if !ok {
			return errors.Errorf("root returned table name `%s` but it could not be found?", otherTableName.String())
		}
		otherTableSch, err := otherTable.GetSchema(ctx)
		if err != nil {
			return err
		}
		for _, otherCol := range otherTableSch.GetAllCols().GetColumns() {
			colType := otherCol.TypeInfo.ToSqlType()
			dgtype, ok := colType.(*pgtypes.DoltgresType)
			if !ok {
				// If this isn't a Doltgres type, then it can't be a table type so we can ignore it
				continue
			}
			if dgtype.ID != tableAsType {
				// This column isn't our table type, so we can ignore it
				continue
			}
			// Build the UPDATE statement that we'll run for this table
			rowValues := make([]string, len(sch)+1)
			for i, col := range sch {
				rowValues[i] = fmt.Sprintf(`("%s")."%s"`, otherCol.Name, col.Name)
			}
			rowValues[len(rowValues)-1] = "NULL"
			// The UPDATE changes the values in the table
			updateStr := fmt.Sprintf(`UPDATE "%s"."%s" SET "%s" = ROW(%s)::"%s"."%s" WHERE length("%s"::text) > 0;`,
				otherTableName.Schema, otherTableName.Name, otherCol.Name, strings.Join(rowValues, ","), tableName.Schema, tableName.Name, otherCol.Name)
			// The ALTER updates the type on the schema since it still has the old one
			alterStr := fmt.Sprintf(`ALTER TABLE "%s"."%s" ALTER COLUMN "%s" TYPE "%s"."%s";`,
				otherTableName.Schema, otherTableName.Name, otherCol.Name, tableName.Schema, tableName.Name)
			// We run the statements as though they were interpreted since we're running new statements inside the original
			_, err = sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
				_, rowIter, _, err := runner.QueryWithBindings(subCtx, updateStr, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				_, err = sql.RowIterToRows(subCtx, rowIter)
				if err != nil {
					return nil, err
				}
				_, rowIter, _, err = runner.QueryWithBindings(subCtx, alterStr, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				return sql.RowIterToRows(subCtx, rowIter)
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizeAddColumnDomainNullability(ctx *sql.Context, n *plan.AddColumn) (*plan.AddColumn, *pgtypes.DoltgresType, error) {
	col := n.Column()
	domainType, ok := col.Type.(*pgtypes.DoltgresType)
	if !ok || domainType.TypType != pgtypes.TypeType_Domain {
		return n, nil, nil
	}
	if !domainType.IsResolvedType() {
		typeCollection, err := pgtypes.GetTypesCollectionFromContext(ctx)
		if err != nil {
			return nil, nil, err
		}
		schema, err := core.GetSchemaName(ctx, nil, domainType.ID.SchemaName())
		if err != nil {
			return nil, nil, err
		}
		resolvedType, err := typeCollection.GetType(ctx, id.NewType(schema, domainType.ID.TypeName()))
		if err != nil {
			return nil, nil, err
		}
		if resolvedType == nil {
			return nil, nil, pgtypes.ErrTypeDoesNotExist.New(domainType.ID.TypeName())
		}
		domainType = resolvedType
		col.Type = resolvedType
	}
	col.Nullable = !domainType.NotNull
	targetSchema := n.TargetSchema().Copy()
	for _, targetCol := range targetSchema {
		if targetCol.Name == col.Name {
			targetCol.Type = col.Type
			targetCol.Nullable = col.Nullable
			targetCol.Default = col.Default
			targetCol.Generated = col.Generated
			targetCol.OnUpdate = col.OnUpdate
			break
		}
	}
	updated, err := n.WithTargetSchema(targetSchema)
	if err != nil {
		return nil, nil, err
	}
	return updated.(*plan.AddColumn), domainType, nil
}

func recordColumnMissingValueMetadata(ctx *sql.Context, n *plan.AddColumn) error {
	col := n.Column()
	for _, targetCol := range n.TargetSchema() {
		if targetCol.Name == col.Name {
			col = targetCol
			break
		}
	}
	if col.Default == nil || !col.Default.IsLiteral() {
		return nil
	}
	table, err := alteredTableFromNode(ctx, n.Database(), n.Table)
	if err != nil {
		return err
	}
	value, ok, err := firstColumnValue(ctx, table, col.Name)
	if err != nil {
		return err
	}
	if !ok || value == nil {
		return nil
	}
	missingValue, err := columnMissingValueText(ctx, col, value)
	if err != nil {
		return err
	}
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	alterable, ok := table.(sql.CommentAlterableTable)
	if !ok {
		return sql.ErrAlterTableCommentNotSupported.New(table.Name())
	}
	return alterable.ModifyComment(ctx, tablemetadata.SetColumnMissingValue(commented.Comment(), col.Name, missingValue))
}

func columnMissingValueText(ctx *sql.Context, col *sql.Column, value any) (string, error) {
	if typ, ok := col.Type.(*pgtypes.DoltgresType); ok {
		output, err := typ.IoOutput(ctx, value)
		if err != nil || output != "" {
			return output, err
		}
	}
	output := fmt.Sprint(value)
	if output != "" {
		return output, nil
	}
	defaultText := strings.TrimSpace(col.Default.String())
	if defaultText == "" || strings.EqualFold(defaultText, "NULL") {
		return "", nil
	}
	return strings.Trim(defaultText, "'"), nil
}

func alteredTableFromNode(ctx *sql.Context, db sql.Database, tableNode sql.Node) (sql.Table, error) {
	tableName := alteredTableName(tableNode)
	if tableName == "" {
		return nil, sql.ErrTableNotFound.New(tableName)
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

func alteredTableName(nodeToSearch sql.Node) string {
	nodeStack := []sql.Node{nodeToSearch}
	for len(nodeStack) > 0 {
		node := nodeStack[len(nodeStack)-1]
		nodeStack = nodeStack[:len(nodeStack)-1]
		switch n := node.(type) {
		case *plan.TableAlias:
			if n.UnaryNode != nil {
				nodeStack = append(nodeStack, n.UnaryNode.Child)
				continue
			}
		case *plan.ResolvedTable:
			return n.Table.Name()
		case *plan.UnresolvedTable:
			return n.Name()
		case *plan.IndexedTableAccess:
			return n.Name()
		case sql.TableWrapper:
			return n.Underlying().Name()
		case sql.Table:
			return n.Name()
		}
		nodeStack = append(nodeStack, node.Children()...)
	}
	return ""
}
