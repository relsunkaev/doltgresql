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
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type CreateTableLikeOptions uint8

const (
	CreateTableLikeOptionConstraints CreateTableLikeOptions = 1 << iota
	CreateTableLikeOptionDefaults
	CreateTableLikeOptionIdentity
	CreateTableLikeOptionGenerated
	CreateTableLikeOptionIndexes
)

const CreateTableLikeOptionAll = CreateTableLikeOptionConstraints |
	CreateTableLikeOptionDefaults |
	CreateTableLikeOptionIdentity |
	CreateTableLikeOptionGenerated |
	CreateTableLikeOptionIndexes

const createTableLikeMaxSequenceAutoNames = 10_000

func (o CreateTableLikeOptions) Has(option CreateTableLikeOptions) bool {
	return o&option != 0
}

// CreateTableLike handles PostgreSQL CREATE TABLE ... (LIKE ... [INCLUDING ...]).
type CreateTableLike struct {
	IfNotExists      bool
	Temporary        bool
	DatabaseName     string
	SchemaName       string
	TableName        string
	SourceDatabase   string
	SourceSchemaName string
	SourceTableName  string
	Options          CreateTableLikeOptions
}

var _ sql.ExecBuilderNode = (*CreateTableLike)(nil)
var _ sql.ExecSourceRel = (*CreateTableLike)(nil)
var _ vitess.Injectable = (*CreateTableLike)(nil)

func NewCreateTableLike(
	ifNotExists bool,
	temporary bool,
	databaseName string,
	schemaName string,
	tableName string,
	sourceDatabase string,
	sourceSchemaName string,
	sourceTableName string,
	options CreateTableLikeOptions,
) *CreateTableLike {
	return &CreateTableLike{
		IfNotExists:      ifNotExists,
		Temporary:        temporary,
		DatabaseName:     databaseName,
		SchemaName:       schemaName,
		TableName:        tableName,
		SourceDatabase:   sourceDatabase,
		SourceSchemaName: sourceSchemaName,
		SourceTableName:  sourceTableName,
		Options:          options,
	}
}

func (c *CreateTableLike) Children() []sql.Node { return nil }

func (c *CreateTableLike) IsReadOnly() bool { return false }

func (c *CreateTableLike) Resolved() bool { return true }

func (c *CreateTableLike) Schema(*sql.Context) sql.Schema { return nil }

func (c *CreateTableLike) String() string {
	return fmt.Sprintf("CREATE TABLE %s LIKE %s", c.TableName, c.SourceTableName)
}

func (c *CreateTableLike) WithChildren(*sql.Context, ...sql.Node) (sql.Node, error) {
	return c, nil
}

func (c *CreateTableLike) WithResolvedChildren(_ context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

func (c *CreateTableLike) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	return c.BuildRowIter(ctx, rowexec.NewBuilder(nil, sql.EngineOverrides{}), r)
}

func (c *CreateTableLike) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, c.DatabaseName)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, sql.ErrDatabaseNotFound.New(c.DatabaseName)
	}
	targetSchema, err := core.GetSchemaName(ctx, db, c.SchemaName)
	if err != nil {
		return nil, err
	}
	targetDb, err := databaseForSchema(ctx, db, targetSchema)
	if err != nil {
		return nil, err
	}

	sourceTable, err := core.GetSqlTableFromContext(ctx, c.SourceDatabase, doltdb.TableName{
		Name:   c.SourceTableName,
		Schema: c.SourceSchemaName,
	})
	if err != nil {
		return nil, err
	}
	if sourceTable == nil {
		return nil, sql.ErrTableNotFound.New(c.SourceTableName)
	}

	sourceSchema := createTableLikeSchemaName(ctx, sourceTable, c.SourceSchemaName)
	identitySequences, err := c.sourceIdentitySequences(ctx, sourceSchema)
	if err != nil {
		return nil, err
	}

	schema, pkOrdinals, identityToCreate, err := c.copySourceSchema(ctx, sourceTable, targetSchema, identitySequences)
	if err != nil {
		return nil, err
	}
	idxDefs, err := c.copyIndexDefs(ctx, sourceTable)
	if err != nil {
		return nil, err
	}
	tableSpec := &plan.TableSpec{
		Schema:    sql.NewPrimaryKeySchema(schema, pkOrdinals...),
		IdxDefs:   idxDefs,
		Collation: sourceTable.Collation(),
	}
	createTable := NewCreateTable(
		plan.NewCreateTable(targetDb, c.TableName, c.IfNotExists, c.Temporary, tableSpec),
		nil,
	)

	alreadyExisted := false
	if c.IfNotExists {
		_, alreadyExisted, err = targetDb.GetTableInsensitive(ctx, c.TableName)
		if err != nil {
			return nil, err
		}
	}
	iter, err := createTable.BuildRowIter(ctx, b, r)
	if err != nil {
		return iter, err
	}
	if alreadyExisted {
		if iter != nil {
			if err = iter.Close(ctx); err != nil {
				return nil, err
			}
		}
		return sql.RowsToRowIter(), nil
	}

	for _, sequence := range identityToCreate {
		seqIter, seqErr := sequence.RowIter(ctx, r)
		if seqIter != nil {
			_ = seqIter.Close(ctx)
		}
		if seqErr != nil {
			if iter != nil {
				_ = iter.Close(ctx)
			}
			return nil, seqErr
		}
	}
	if err = c.copyCheckConstraints(ctx, sourceTable, targetDb); err != nil {
		if iter != nil {
			_ = iter.Close(ctx)
		}
		return nil, err
	}
	if iter != nil {
		if err = iter.Close(ctx); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateTableLike) sourceIdentitySequences(ctx *sql.Context, sourceSchema string) (map[string]*sequences.Sequence, error) {
	if !c.Options.Has(CreateTableLikeOptionIdentity) {
		return nil, nil
	}
	databaseName := c.SourceDatabase
	if databaseName == "" {
		databaseName = ctx.GetCurrentDatabase()
	}
	collection, err := core.GetSequencesCollectionFromContext(ctx, databaseName)
	if err != nil {
		return nil, err
	}
	seqs, err := collection.GetSequencesWithTable(ctx, doltdb.TableName{
		Name:   c.SourceTableName,
		Schema: sourceSchema,
	})
	if err != nil {
		return nil, err
	}
	byColumn := make(map[string]*sequences.Sequence, len(seqs))
	for _, seq := range seqs {
		if seq.OwnerColumn != "" {
			byColumn[seq.OwnerColumn] = seq
		}
	}
	return byColumn, nil
}

func (c *CreateTableLike) copySourceSchema(
	ctx *sql.Context,
	sourceTable sql.Table,
	targetSchema string,
	identitySequences map[string]*sequences.Sequence,
) (sql.Schema, []int, []*CreateSequence, error) {
	sourceSchema := sourceTable.Schema(ctx)
	newSchema := make(sql.Schema, 0, len(sourceSchema))
	var pkOrdinals []int
	var identityToCreate []*CreateSequence
	for _, col := range sourceSchema {
		newCol := col.Copy()
		newCol.Source = c.TableName
		newCol.DatabaseSource = c.DatabaseName
		if !c.Options.Has(CreateTableLikeOptionDefaults) {
			newCol.Default = nil
			newCol.OnUpdate = nil
		}
		if !c.Options.Has(CreateTableLikeOptionGenerated) {
			newCol.Generated = nil
			newCol.Virtual = false
		}
		if !c.Options.Has(CreateTableLikeOptionIndexes) {
			newCol.PrimaryKey = false
		}
		if seq := identitySequences[col.Name]; seq != nil {
			newSeq, err := c.copyIdentitySequence(ctx, targetSchema, col.Name, seq, newCol)
			if err != nil {
				return nil, nil, nil, err
			}
			identityToCreate = append(identityToCreate, newSeq)
		} else if !c.Options.Has(CreateTableLikeOptionIdentity) {
			newCol.AutoIncrement = false
			if createTableLikeUsesNextVal(newCol.Default) {
				newCol.Default = nil
			}
			if createTableLikeUsesNextVal(newCol.Generated) {
				newCol.Generated = nil
			}
		}
		if !c.Options.Has(CreateTableLikeOptionIdentity) && !c.Options.Has(CreateTableLikeOptionGenerated) {
			newCol.Extra = ""
		}
		if newCol.PrimaryKey {
			pkOrdinals = append(pkOrdinals, len(newSchema))
		}
		newSchema = append(newSchema, newCol)
	}
	return newSchema, pkOrdinals, identityToCreate, nil
}

func (c *CreateTableLike) copyIdentitySequence(
	ctx *sql.Context,
	targetSchema string,
	columnName string,
	sourceSeq *sequences.Sequence,
	column *sql.Column,
) (*CreateSequence, error) {
	sequenceName, err := c.nextIdentitySequenceName(ctx, targetSchema, columnName)
	if err != nil {
		return nil, err
	}
	nextValName := doltdb.TableName{Name: sequenceName, Schema: targetSchema}.String()
	nextVal, found, err := framework.GetFunction(ctx, "nextval", pgexprs.NewTextLiteral(nextValName))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.Errorf(`function "nextval" could not be found for identity default`)
	}
	outType, ok := column.Type.(*pgtypes.DoltgresType)
	if !ok {
		return nil, errors.Errorf(`identity column "%s" has unexpected type "%s"`, column.Name, column.Type.String())
	}
	nextValExpr := &sql.ColumnDefaultValue{
		Expr:          nextVal,
		OutType:       outType,
		Literal:       false,
		ReturnNil:     false,
		Parenthesized: false,
	}
	if column.Generated != nil {
		column.Generated = nextValExpr
		column.Default = nil
	} else {
		column.Default = nextValExpr
	}
	column.AutoIncrement = false
	return NewCreateSequence(false, c.DatabaseName, targetSchema, false, &sequences.Sequence{
		Id:          id.NewSequence("", sequenceName),
		DataTypeID:  sourceSeq.DataTypeID,
		Persistence: sourceSeq.Persistence,
		Start:       sourceSeq.Start,
		Current:     sourceSeq.Start,
		Increment:   sourceSeq.Increment,
		Minimum:     sourceSeq.Minimum,
		Maximum:     sourceSeq.Maximum,
		Cache:       sourceSeq.Cache,
		Cycle:       sourceSeq.Cycle,
		IsAtEnd:     false,
		IsCalled:    false,
		OwnerTable:  id.NewTable("", c.TableName),
		OwnerColumn: columnName,
	}), nil
}

func (c *CreateTableLike) nextIdentitySequenceName(ctx *sql.Context, schemaName string, columnName string) (string, error) {
	base := fmt.Sprintf("%s_%s_seq", c.TableName, columnName)
	for i := 0; i < createTableLikeMaxSequenceAutoNames; i++ {
		sequenceName := base
		if i > 0 {
			sequenceName = fmt.Sprintf("%s%d", base, i)
		}
		relationType, err := core.GetRelationTypeForDatabase(ctx, c.DatabaseName, schemaName, sequenceName)
		if err != nil {
			return "", err
		}
		if relationType == core.RelationType_DoesNotExist {
			return sequenceName, nil
		}
	}
	return "", errors.Errorf("could not find an available sequence name for column %s", columnName)
}

func (c *CreateTableLike) copyIndexDefs(ctx *sql.Context, sourceTable sql.Table) (sql.IndexDefs, error) {
	if !c.Options.Has(CreateTableLikeOptionIndexes) {
		return nil, nil
	}
	indexed, ok := sourceTable.(sql.IndexAddressableTable)
	if !ok {
		return nil, nil
	}
	indexes, err := indexed.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	defs := make(sql.IndexDefs, 0, len(indexes))
	for _, idx := range indexes {
		if idx.IsGenerated() || idx.ID() == "PRIMARY" {
			continue
		}
		constraint := sql.IndexConstraint_None
		if idx.IsUnique() {
			constraint = sql.IndexConstraint_Unique
		}
		exprs := idx.Expressions()
		columns := make([]sql.IndexColumn, len(exprs))
		for i, expr := range exprs {
			expr = expr[strings.IndexByte(expr, '.')+1:]
			columns[i] = sql.IndexColumn{Name: expr}
		}
		defs = append(defs, &sql.IndexDef{
			Columns:    columns,
			Constraint: constraint,
			Storage:    sql.IndexUsing_Default,
			Comment:    idx.Comment(),
		})
	}
	return defs, nil
}

func (c *CreateTableLike) copyCheckConstraints(ctx *sql.Context, sourceTable sql.Table, targetDb sql.Database) error {
	if !c.Options.Has(CreateTableLikeOptionConstraints) {
		return nil
	}
	sourceChecks, ok := sourceTable.(sql.CheckTable)
	if !ok {
		return nil
	}
	checks, err := sourceChecks.GetChecks(ctx)
	if err != nil {
		return err
	}
	if len(checks) == 0 {
		return nil
	}
	targetTable, ok, err := targetDb.GetTableInsensitive(ctx, c.TableName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(c.TableName)
	}
	checkAlterable, ok := targetTable.(sql.CheckAlterableTable)
	if !ok {
		return plan.ErrNoCheckConstraintSupport.New(c.TableName)
	}
	for _, check := range checks {
		copied := check
		copied.Name = ""
		if err = checkAlterable.CreateCheck(ctx, &copied); err != nil {
			return err
		}
	}
	return nil
}

func createTableLikeSchemaName(ctx *sql.Context, table sql.Table, fallback string) string {
	if schemaTable, ok := table.(sql.DatabaseSchemaTable); ok {
		if schema := schemaTable.DatabaseSchema(); schema != nil {
			return schema.SchemaName()
		}
	}
	if fallback != "" {
		return fallback
	}
	schema, err := core.GetCurrentSchema(ctx)
	if err != nil {
		return "public"
	}
	return schema
}

func createTableLikeUsesNextVal(defaultValue *sql.ColumnDefaultValue) bool {
	return defaultValue != nil && strings.Contains(strings.ToLower(defaultValue.String()), "nextval")
}
