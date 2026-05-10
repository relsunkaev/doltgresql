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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
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
	ColumnOptions     []TypedTableColumnOptions
	PrimaryKeyColumns []string
}

type TypedTableColumnOptions struct {
	Name        string
	NullableSet bool
	Nullable    bool
	PrimaryKey  bool
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
	return sql.RowsToRowIter(), nil
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
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
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
	if err := applyTypedTableOptions(schema, typ, options); err != nil {
		return nil, err
	}
	return schema, nil
}

func applyTypedTableOptions(schema sql.Schema, typ *pgtypes.DoltgresType, options TypedTableOptions) error {
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
