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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/deferrable"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// CreateTypedTable handles CREATE TABLE name OF composite_type.
type CreateTypedTable struct {
	IfNotExists bool
	Temporary   bool

	DatabaseName string
	SchemaName   string
	TableName    string

	TypeSchemaName string
	TypeName       string
	Options        TypedTableOptions
}

var _ sql.ExecSourceRel = (*CreateTypedTable)(nil)
var _ vitess.Injectable = (*CreateTypedTable)(nil)

type TypedTableOptions struct {
	ColumnOptions         []TypedTableColumnOptions
	PrimaryKeyColumns     []string
	UniqueConstraints     []TypedTableUniqueConstraint
	CheckConstraints      []TypedTableCheckConstraint
	ForeignKeyConstraints []TypedTableForeignKeyConstraint
}

type TypedTableColumnOptions struct {
	Name                 string
	NullableSet          bool
	Nullable             bool
	PrimaryKey           bool
	UniqueName           string
	Unique               bool
	HasDefault           bool
	DefaultExpr          sql.Expression
	DefaultLiteral       bool
	DefaultParenthesized bool
	DefaultChildIndex    int
}

type TypedTableUniqueConstraint struct {
	Name    string
	Columns []string
}

type TypedTableCheckConstraint struct {
	Name       string
	Expression string
}

type TypedTableForeignKeyConstraint struct {
	Name               string
	Columns            []string
	ParentDatabaseName string
	ParentSchemaName   string
	ParentTableName    string
	ParentColumns      []string
	OnUpdate           sql.ForeignKeyReferentialAction
	OnDelete           sql.ForeignKeyReferentialAction
}

// NewCreateTypedTable returns a new CREATE TABLE OF execution node.
func NewCreateTypedTable(ifNotExists bool, temporary bool, databaseName string, schemaName string, tableName string, typeSchemaName string, typeName string, options TypedTableOptions) *CreateTypedTable {
	return &CreateTypedTable{
		IfNotExists:    ifNotExists,
		Temporary:      temporary,
		DatabaseName:   databaseName,
		SchemaName:     schemaName,
		TableName:      tableName,
		TypeSchemaName: typeSchemaName,
		TypeName:       typeName,
		Options:        options,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, c.DatabaseName)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, sql.ErrDatabaseNotFound.New(c.DatabaseName)
	}

	schemaName, err := core.GetSchemaName(ctx, db, c.SchemaName)
	if err != nil {
		return nil, err
	}
	db, err = databaseForSchema(ctx, db, schemaName)
	if err != nil {
		return nil, err
	}

	if c.IfNotExists && c.Temporary {
		if temporaryTypedTableExists(ctx, db.Name(), c.TableName) {
			return sql.RowsToRowIter(), nil
		}
	}
	if c.IfNotExists && !c.Temporary {
		_, ok, err := db.GetTableInsensitive(ctx, c.TableName)
		if err != nil {
			return nil, err
		}
		if ok {
			return sql.RowsToRowIter(), nil
		}
	}

	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	compositeType, err := c.resolveType(ctx, typeCollection)
	if err != nil {
		return nil, err
	}
	if compositeType == nil {
		return nil, pgtypes.ErrTypeDoesNotExist.New(c.typeDisplayName())
	}
	if !compositeType.IsCompositeType() || compositeType.IsRecordType() {
		return nil, errors.Errorf(`type "%s" is not a composite type`, c.typeDisplayName())
	}

	tableSchema, err := typedTableSchema(ctx, typeCollection, c.TableName, db.Name(), compositeType, c.Options)
	if err != nil {
		return nil, err
	}

	if c.Temporary {
		if len(c.Options.ForeignKeyConstraints) > 0 {
			return nil, sql.ErrTemporaryTablesForeignKeySupport.New()
		}
		tableCreator, ok := unwrapPrivilegedDatabase(db).(sql.TemporaryTableCreator)
		if !ok {
			return nil, sql.ErrTemporaryTableNotSupported.New()
		}
		err = tableCreator.CreateTemporaryTable(ctx, c.TableName, sql.NewPrimaryKeySchema(tableSchema), plan.GetDatabaseCollation(ctx, db))
		tableExists := sql.ErrTableAlreadyExists.Is(err)
		if tableExists && c.IfNotExists {
			return sql.RowsToRowIter(), nil
		}
		if err != nil {
			return nil, err
		}
		if err = c.applyTemporaryTypedTableOptions(ctx, db); err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(), nil
	}

	tableCreator, ok := unwrapPrivilegedDatabase(db).(sql.TableCreator)
	if !ok {
		return nil, sql.ErrCreateTableNotSupported.New(db.Name())
	}

	comment := tablemetadata.SetOfType("", compositeType.ID)
	err = tableCreator.CreateTable(ctx, c.TableName, sql.NewPrimaryKeySchema(tableSchema), plan.GetDatabaseCollation(ctx, db), comment)
	tableExists := sql.ErrTableAlreadyExists.Is(err)
	if tableExists && c.IfNotExists {
		return sql.RowsToRowIter(), nil
	}
	if err != nil {
		return nil, err
	}
	if err = c.createUniqueConstraints(ctx, db); err != nil {
		return nil, err
	}
	if err = c.createForeignKeyConstraints(ctx, db, schemaName); err != nil {
		return nil, err
	}
	if err = c.createCheckConstraints(ctx, db); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateTypedTable) applyTemporaryTypedTableOptions(ctx *sql.Context, db sql.Database) error {
	constraints := typedTableUniqueConstraints(c.Options)
	checks := c.Options.CheckConstraints
	if len(constraints) == 0 && len(checks) == 0 {
		return nil
	}
	session := dsess.DSessFromSess(ctx.Session)
	table, ok := session.GetTemporaryTable(ctx, db.Name(), c.TableName)
	if !ok {
		return sql.ErrTableNotFound.New(c.TableName)
	}
	session.AddTemporaryTable(ctx, db.Name(), newTypedTableUniqueTempTable(table, constraints, checks))
	return nil
}

func (c *CreateTypedTable) createUniqueConstraints(ctx *sql.Context, db sql.Database) error {
	constraints := typedTableUniqueConstraints(c.Options)
	if len(constraints) == 0 {
		return nil
	}
	table, ok, err := db.GetTableInsensitive(ctx, c.TableName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(c.TableName)
	}
	indexAlterable, ok := table.(sql.IndexAlterableTable)
	if !ok {
		return errors.Errorf("CREATE TABLE OF UNIQUE constraints are not supported by this table")
	}
	for _, constraint := range constraints {
		indexColumns := make([]sql.IndexColumn, len(constraint.Columns))
		for i, column := range constraint.Columns {
			indexColumns[i] = sql.IndexColumn{Name: column}
		}
		if err = indexAlterable.CreateIndex(ctx, sql.IndexDef{
			Name:       constraint.Name,
			Columns:    indexColumns,
			Constraint: sql.IndexConstraint_Unique,
			Storage:    sql.IndexUsing_BTree,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *CreateTypedTable) createForeignKeyConstraints(ctx *sql.Context, db sql.Database, schemaName string) error {
	if len(c.Options.ForeignKeyConstraints) == 0 {
		return nil
	}
	table, ok, err := db.GetTableInsensitive(ctx, c.TableName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrTableNotFound.New(c.TableName)
	}
	foreignKeyTable, ok := typedTableForeignKeyTable(table)
	if !ok {
		return errors.Errorf("CREATE TABLE OF FOREIGN KEY constraints are not supported by this table")
	}
	fkChecks, err := ctx.GetSessionVariable(ctx, "foreign_key_checks")
	if err != nil {
		return err
	}
	generatedNames := make(map[string]struct{}, len(c.Options.ForeignKeyConstraints))
	for _, constraint := range c.Options.ForeignKeyConstraints {
		foreignKey := sql.ForeignKeyConstraint{
			Name:           constraint.Name,
			Database:       db.Name(),
			SchemaName:     schemaName,
			Table:          table.Name(),
			ParentDatabase: db.Name(),
			ParentSchema:   schemaName,
			ParentTable:    constraint.ParentTableName,
			OnUpdate:       constraint.OnUpdate,
			OnDelete:       constraint.OnDelete,
			Columns:        append([]string(nil), constraint.Columns...),
			ParentColumns:  append([]string(nil), constraint.ParentColumns...),
		}
		if foreignKey.Name == "" {
			foreignKey.Name = typedTableForeignKeyName(table.Name(), foreignKey.Columns, generatedNames)
		}
		generatedNames[strings.ToLower(foreignKey.Name)] = struct{}{}

		if constraint.ParentDatabaseName != "" {
			foreignKey.ParentDatabase = constraint.ParentDatabaseName
		}
		parentDb := db
		if constraint.ParentSchemaName != "" {
			rootDb, err := core.GetSqlDatabaseFromContext(ctx, c.DatabaseName)
			if err != nil {
				return err
			}
			parentDb, err = databaseForSchema(ctx, rootDb, constraint.ParentSchemaName)
			if err != nil {
				return err
			}
			if schemaDb, ok := parentDb.(sql.DatabaseSchema); ok {
				foreignKey.ParentSchema = schemaDb.SchemaName()
			} else {
				foreignKey.ParentSchema = constraint.ParentSchemaName
			}
			foreignKey.ParentDatabase = parentDb.Name()
		}

		if err := deferrable.BindForeignKey(ctx, foreignKey); err != nil {
			return err
		}
		if fkChecks.(int8) == 1 {
			parentTable, ok, err := parentDb.GetTableInsensitive(ctx, constraint.ParentTableName)
			if err != nil {
				return err
			}
			if !ok {
				return sql.ErrTableNotFound.New(constraint.ParentTableName)
			}
			parentForeignKeyTable, ok := typedTableForeignKeyTable(parentTable)
			if !ok {
				return errors.Errorf("CREATE TABLE OF FOREIGN KEY constraints are not supported by parent table")
			}
			if err := plan.ResolveForeignKey(ctx, foreignKeyTable, parentForeignKeyTable, foreignKey, true, true, true); err != nil {
				return err
			}
		} else if err := plan.ResolveForeignKey(ctx, foreignKeyTable, nil, foreignKey, true, false, false); err != nil {
			return err
		}
	}
	return nil
}

func (c *CreateTypedTable) createCheckConstraints(ctx *sql.Context, db sql.Database) error {
	if len(c.Options.CheckConstraints) == 0 {
		return nil
	}

	var table sql.Table
	if c.Temporary {
		session := dsess.DSessFromSess(ctx.Session)
		var ok bool
		table, ok = session.GetTemporaryTable(ctx, db.Name(), c.TableName)
		if !ok {
			return sql.ErrTableNotFound.New(c.TableName)
		}
	} else {
		var ok bool
		var err error
		table, ok, err = db.GetTableInsensitive(ctx, c.TableName)
		if err != nil {
			return err
		}
		if !ok {
			return sql.ErrTableNotFound.New(c.TableName)
		}
	}

	checkAlterable, ok := typedTableCheckAlterable(table)
	if !ok {
		return errors.Errorf("CREATE TABLE OF CHECK constraints are not supported by this table")
	}
	for _, constraint := range c.Options.CheckConstraints {
		if err := checkAlterable.CreateCheck(ctx, &sql.CheckDefinition{
			Name:            constraint.Name,
			CheckExpression: constraint.Expression,
			Enforced:        true,
		}); err != nil {
			return err
		}
	}
	return nil
}

func typedTableCheckAlterable(table sql.Table) (sql.CheckAlterableTable, bool) {
	if checkAlterable, ok := table.(sql.CheckAlterableTable); ok {
		return checkAlterable, true
	}
	if table == nil {
		return nil, false
	}
	if checkAlterable, ok := sql.GetUnderlyingTable(table).(sql.CheckAlterableTable); ok {
		return checkAlterable, true
	}
	return nil, false
}

func typedTableForeignKeyTable(table sql.Table) (sql.ForeignKeyTable, bool) {
	if foreignKeyTable, ok := table.(sql.ForeignKeyTable); ok {
		return foreignKeyTable, true
	}
	if table == nil {
		return nil, false
	}
	if foreignKeyTable, ok := sql.GetUnderlyingTable(table).(sql.ForeignKeyTable); ok {
		return foreignKeyTable, true
	}
	return nil, false
}

func typedTableForeignKeyName(tableName string, columns []string, used map[string]struct{}) string {
	baseName := fmt.Sprintf("%s_%s_fkey", tableName, strings.Join(columns, "_"))
	name := baseName
	for i := 0; ; i++ {
		if _, ok := used[strings.ToLower(name)]; !ok {
			return name
		}
		name = fmt.Sprintf("%s%d", baseName, i+1)
	}
}

func typedTableUniqueConstraints(options TypedTableOptions) []TypedTableUniqueConstraint {
	constraints := make([]TypedTableUniqueConstraint, 0, len(options.UniqueConstraints)+len(options.ColumnOptions))
	constraints = append(constraints, options.UniqueConstraints...)
	for _, option := range options.ColumnOptions {
		if option.Unique {
			constraints = append(constraints, TypedTableUniqueConstraint{
				Name:    option.UniqueName,
				Columns: []string{option.Name},
			})
		}
	}
	return constraints
}

func temporaryTypedTableExists(ctx *sql.Context, databaseName string, tableName string) bool {
	session := dsess.DSessFromSess(ctx.Session)
	_, ok := session.GetTemporaryTable(ctx, databaseName, tableName)
	return ok
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) String() string {
	return "CREATE TABLE OF"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateTypedTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (c *CreateTypedTable) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	expectedChildren := 0
	for _, option := range c.Options.ColumnOptions {
		if option.HasDefault {
			expectedChildren++
		}
	}
	if len(children) != expectedChildren {
		return nil, ErrVitessChildCount.New(expectedChildren, len(children))
	}

	options := c.Options
	options.ColumnOptions = append([]TypedTableColumnOptions(nil), c.Options.ColumnOptions...)
	for i := range options.ColumnOptions {
		option := &options.ColumnOptions[i]
		if !option.HasDefault {
			continue
		}
		if option.DefaultChildIndex < 0 || option.DefaultChildIndex >= len(children) {
			return nil, ErrVitessChildCount.New(expectedChildren, len(children))
		}
		expr, ok := children[option.DefaultChildIndex].(sql.Expression)
		if !ok {
			return nil, errors.Errorf("invalid vitess child, expected sql.Expression for Default value but got %T", children[option.DefaultChildIndex])
		}
		option.DefaultExpr = expr
	}

	nct := *c
	nct.Options = options
	return &nct, nil
}

func (c *CreateTypedTable) resolveType(ctx *sql.Context, typeCollection typeResolver) (*pgtypes.DoltgresType, error) {
	if c.TypeSchemaName != "" {
		schemaName, err := core.GetSchemaName(ctx, nil, c.TypeSchemaName)
		if err != nil {
			return nil, err
		}
		return typeCollection.GetType(ctx, id.NewType(schemaName, c.TypeName))
	}

	searchPath, err := core.SearchPath(ctx)
	if err != nil {
		return nil, err
	}
	for _, schemaName := range searchPath {
		typ, err := typeCollection.GetType(ctx, id.NewType(schemaName, c.TypeName))
		if err != nil {
			return nil, err
		}
		if typ != nil {
			return typ, nil
		}
	}
	return nil, nil
}

func (c *CreateTypedTable) typeDisplayName() string {
	if c.TypeSchemaName == "" {
		return c.TypeName
	}
	return c.TypeSchemaName + "." + c.TypeName
}

type typeResolver interface {
	GetType(ctx context.Context, name id.Type) (*pgtypes.DoltgresType, error)
}

func typedTableSchema(ctx *sql.Context, typeCollection typeResolver, tableName string, databaseName string, typ *pgtypes.DoltgresType, options TypedTableOptions) (sql.Schema, error) {
	schema := make(sql.Schema, len(typ.CompositeAttrs))
	for i, attr := range typ.CompositeAttrs {
		attrType, err := typeCollection.GetType(ctx, attr.TypeID)
		if err != nil {
			return nil, err
		}
		if attrType == nil {
			return nil, pgtypes.ErrTypeDoesNotExist.New(attr.TypeID.TypeName())
		}
		schema[i] = &sql.Column{
			Name:           attr.Name,
			Type:           attrType,
			Source:         tableName,
			DatabaseSource: databaseName,
			Nullable:       true,
		}
	}
	if err := applyTypedTableOptions(ctx, schema, typ, options); err != nil {
		return nil, err
	}
	return schema, nil
}

func applyTypedTableOptions(ctx *sql.Context, schema sql.Schema, typ *pgtypes.DoltgresType, options TypedTableOptions) error {
	columnOptions := make(map[string]TypedTableColumnOptions, len(options.ColumnOptions))
	for _, option := range options.ColumnOptions {
		key := strings.ToLower(option.Name)
		if _, ok := columnOptions[key]; ok {
			return errors.Errorf(`column "%s" specified more than once`, option.Name)
		}
		columnOptions[key] = option
	}

	primaryKeyColumns := make([]string, 0, len(options.PrimaryKeyColumns)+1)
	primaryKeyColumns = append(primaryKeyColumns, options.PrimaryKeyColumns...)
	for _, option := range options.ColumnOptions {
		idx := typedTableColumnIndex(schema, option.Name)
		if idx < 0 {
			return errors.Errorf(`column "%s" does not exist in composite type "%s"`, option.Name, typ.ID.TypeName())
		}
		if option.NullableSet {
			schema[idx].Nullable = option.Nullable
		}
		if option.PrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, option.Name)
		}
		if option.HasDefault {
			if option.DefaultExpr == nil {
				return errors.Errorf(`missing default expression for column "%s"`, option.Name)
			}
			defaultValue, err := sql.NewColumnDefaultValue(
				option.DefaultExpr,
				schema[idx].Type,
				option.DefaultLiteral,
				option.DefaultParenthesized,
				schema[idx].Nullable,
			)
			if err != nil {
				return err
			}
			if err = defaultValue.CheckType(ctx); err != nil {
				return err
			}
			schema[idx].Default = defaultValue
		}
	}
	if len(options.PrimaryKeyColumns) > 0 && len(primaryKeyColumns) > len(options.PrimaryKeyColumns) {
		return errors.Errorf("multiple primary keys for table are not allowed")
	}
	if len(options.PrimaryKeyColumns) == 0 && len(primaryKeyColumns) > 1 {
		return errors.Errorf("multiple primary keys for table are not allowed")
	}

	seenPrimary := make(map[string]struct{}, len(primaryKeyColumns))
	for _, columnName := range primaryKeyColumns {
		idx := typedTableColumnIndex(schema, columnName)
		if idx < 0 {
			return errors.Errorf(`column "%s" does not exist in composite type "%s"`, columnName, typ.ID.TypeName())
		}
		key := strings.ToLower(columnName)
		if _, ok := seenPrimary[key]; ok {
			return errors.Errorf(`column "%s" appears twice in primary key constraint`, columnName)
		}
		seenPrimary[key] = struct{}{}
		schema[idx].PrimaryKey = true
		schema[idx].Nullable = false
	}
	seenUniqueNames := make(map[string]struct{}, len(options.UniqueConstraints)+len(options.ColumnOptions))
	for _, unique := range typedTableUniqueConstraints(options) {
		key := strings.ToLower(unique.Name)
		if _, ok := seenUniqueNames[key]; ok {
			return errors.Errorf(`constraint "%s" specified more than once`, unique.Name)
		}
		seenUniqueNames[key] = struct{}{}
		seenUniqueColumns := make(map[string]struct{}, len(unique.Columns))
		for _, columnName := range unique.Columns {
			if typedTableColumnIndex(schema, columnName) < 0 {
				return errors.Errorf(`column "%s" does not exist in composite type "%s"`, columnName, typ.ID.TypeName())
			}
			columnKey := strings.ToLower(columnName)
			if _, ok := seenUniqueColumns[columnKey]; ok {
				return errors.Errorf(`column "%s" appears twice in unique constraint`, columnName)
			}
			seenUniqueColumns[columnKey] = struct{}{}
		}
	}
	return nil
}

func typedTableColumnIndex(schema sql.Schema, name string) int {
	for i, col := range schema {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

func databaseForSchema(ctx *sql.Context, db sql.Database, schemaName string) (sql.Database, error) {
	if schemaDb, ok := db.(sql.DatabaseSchema); ok && schemaDb.SchemaName() == schemaName {
		return db, nil
	}
	schemaDb, ok := db.(sql.SchemaDatabase)
	if !ok {
		return db, nil
	}
	schema, ok, err := schemaDb.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseSchemaNotFound.New(schemaName)
	}
	return schema, nil
}

func unwrapPrivilegedDatabase(db sql.Database) sql.Database {
	if privilegedDb, ok := db.(mysql_db.PrivilegedDatabase); ok {
		return privilegedDb.Unwrap()
	}
	return db
}
