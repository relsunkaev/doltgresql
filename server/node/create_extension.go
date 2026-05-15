// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/extensions"
	"github.com/dolthub/doltgresql/core/extensions/pg_extension"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// CreateExtension implements CREATE EXTENSION.
type CreateExtension struct {
	Name        string
	IfNotExists bool
	SchemaName  string
	Version     string
	Cascade     bool
	Runner      pgexprs.StatementRunner
}

var _ sql.ExecSourceRel = (*CreateExtension)(nil)
var _ sql.Expressioner = (*CreateExtension)(nil)
var _ vitess.Injectable = (*CreateExtension)(nil)

// NewCreateExtension returns a new *CreateExtension.
func NewCreateExtension(name string, ifNotExists bool, schemaName string, version string, cascade bool) *CreateExtension {
	return &CreateExtension{
		Name:        name,
		IfNotExists: ifNotExists,
		SchemaName:  schemaName,
		Version:     version,
		Cascade:     cascade,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateExtension) Children() []sql.Node {
	return nil
}

// Expressions implements the interface sql.Expressioner.
func (c *CreateExtension) Expressions() []sql.Expression {
	return []sql.Expression{c.Runner}
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateExtension) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateExtension) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateExtension) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return nil, err
	}
	if isPreinstalledExtension(c.Name) || extCollection.HasLoadedExtension(ctx, id.NewExtension(c.Name)) {
		if c.IfNotExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`extension "%s" already exists`, c.Name)
	}
	ext, err := extensions.GetExtensionFiles(c.Name)
	if err != nil {
		return nil, err
	}
	if err = c.validateVersion(ext); err != nil {
		return nil, err
	}
	if err = checkExtensionDatabaseCreatePrivilege(ctx); err != nil {
		return nil, err
	}
	targetNamespace, err := c.resolveTargetNamespace(ctx, ext)
	if err != nil {
		return nil, err
	}
	if createExtensionSkipsSQL(c.Name) {
		if err = c.installBuiltinExtensionObjects(ctx, targetNamespace); err != nil {
			return nil, err
		}
		if err = c.addLoadedExtension(ctx, extCollection, ext, targetNamespace); err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(), nil
	}
	restoreSearchPath, err := c.useExtensionSearchPath(ctx, targetNamespace)
	if err != nil {
		return nil, err
	}
	defer restoreSearchPath()
	// The returned files are in their proper order of execution, so we can iterate and execute
	sqlFiles, err := ext.LoadSQLFiles()
	if err != nil {
		return nil, err
	}
	for _, sqlFile := range sqlFiles {
		// Remove echo PSQL control statements
		for {
			echoStartIdx := strings.Index(sqlFile, `\echo`)
			if echoStartIdx == -1 {
				break
			}
			echoEndIdx := strings.Index(sqlFile[echoStartIdx:], "\n")
			if echoEndIdx != -1 {
				// Set the correct absolute position if there is a newline
				echoEndIdx += echoStartIdx
			} else {
				// Set the position at the end of the file if there's no newline (comment appears before EOF)
				echoEndIdx = len(sqlFile)
			}
			sqlFile = strings.Replace(sqlFile, sqlFile[echoStartIdx:echoEndIdx], "", 1)
		}
		statements, err := parser.Parse(sqlFile)
		if err != nil {
			return nil, err
		}
		for _, statement := range statements {
			statementSQL := statement.SQL
			if _, ok := statement.AST.(*tree.CreateFunction); ok {
				statementSQL = strings.ReplaceAll(statementSQL, `'MODULE_PATHNAME'`, fmt.Sprintf(`'%s'`, c.Name))
			}
			_, err = sql.RunInterpreted(ctx, func(subCtx *sql.Context) ([]sql.Row, error) {
				_, rowIter, _, err := c.Runner.Runner.QueryWithBindings(subCtx, statementSQL, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				return sql.RowIterToRows(subCtx, rowIter)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	err = c.addLoadedExtension(ctx, extCollection, ext, targetNamespace)
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func isPreinstalledExtension(name string) bool {
	return strings.EqualFold(name, "plpgsql")
}

func (c *CreateExtension) validateVersion(ext *pg_extension.ExtensionFiles) error {
	if c.Version == "" || c.Version == ext.Control.DefaultVersion.String() {
		return nil
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, `extension "%s" has no installation script nor update path for version "%s"`, c.Name, c.Version)
}

func checkExtensionDatabaseCreatePrivilege(ctx *sql.Context) error {
	var userRole, publicRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
		publicRole = auth.GetRole("public")
	})
	if !userRole.IsValid() {
		return errors.Errorf(`role "%s" does not exist`, ctx.Client().User)
	}
	databaseName := ctx.GetCurrentDatabase()
	roleKey := auth.DatabasePrivilegeKey{Role: userRole.ID(), Name: databaseName}
	publicKey := auth.DatabasePrivilegeKey{Role: publicRole.ID(), Name: databaseName}
	if !auth.HasDatabasePrivilege(roleKey, auth.Privilege_CREATE) && !auth.HasDatabasePrivilege(publicKey, auth.Privilege_CREATE) {
		return errors.Errorf("permission denied for database %s", databaseName)
	}
	return nil
}

func (c *CreateExtension) addLoadedExtension(ctx *sql.Context, extCollection *extensions.Collection, ext *pg_extension.ExtensionFiles, namespace id.Namespace) error {
	owner := ctx.Client().User
	if owner == "" {
		owner = "postgres"
	}
	extID := id.NewExtension(c.Name)
	if err := extCollection.AddLoadedExtension(ctx, extensions.Extension{
		ExtName:       extID,
		Namespace:     namespace,
		Owner:         owner,
		Relocatable:   ext.Control.Relocatable,
		LibIdentifier: extensions.CreateLibraryIdentifier(c.Name, ext.Control.DefaultVersion),
	}); err != nil {
		return err
	}
	setExtensionControlComment(extID, ext.Control.Comment)
	return nil
}

func setExtensionControlComment(extID id.Extension, description string) {
	if description == "" {
		return
	}
	comments.Set(commentObjectKey(extID.AsId(), "pg_extension", 0), &description)
}

func (c *CreateExtension) resolveTargetNamespace(ctx *sql.Context, ext *pg_extension.ExtensionFiles) (id.Namespace, error) {
	schemaName := c.SchemaName
	if !ext.Control.Relocatable && ext.Control.Schema != "" && schemaName != "" && !strings.EqualFold(schemaName, ext.Control.Schema) {
		return id.NullNamespace, errors.Errorf(`extension "%s" must be installed in schema "%s"`, c.Name, ext.Control.Schema)
	}
	if len(schemaName) == 0 {
		schemaName = ext.Control.Schema
	}
	if len(schemaName) == 0 {
		var err error
		schemaName, err = core.GetCurrentSchema(ctx)
		if err != nil {
			return id.NullNamespace, err
		}
	}
	exists, err := schemaExists(ctx, schemaName)
	if err != nil {
		return id.NullNamespace, err
	}
	if !exists {
		return id.NullNamespace, errors.Errorf(`schema "%s" does not exist`, schemaName)
	}
	return id.NewNamespace(schemaName), nil
}

func (c *CreateExtension) installBuiltinExtensionObjects(ctx *sql.Context, namespace id.Namespace) error {
	switch strings.ToLower(c.Name) {
	case "citext":
		return c.installTextCompatibleExtensionType(ctx, namespace, "citext", pgtypes.NewCitextType)
	case "hstore":
		return c.installHstoreExtensionTypes(ctx, namespace)
	case "vector":
		return c.installTextCompatibleExtensionType(ctx, namespace, "vector", pgtypes.NewVectorExtensionType)
	default:
		return nil
	}
}

func (c *CreateExtension) installHstoreExtensionTypes(ctx *sql.Context, namespace id.Namespace) error {
	if err := c.installTextCompatibleExtensionType(ctx, namespace, "hstore", pgtypes.NewHstoreType); err != nil {
		return err
	}
	typesCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	typeID := id.NewType(namespace.SchemaName(), "ghstore")
	arrayID := id.NewType(namespace.SchemaName(), "_ghstore")
	if typesCollection.HasType(ctx, typeID) {
		return nil
	}
	extensionType := pgtypes.NewGhstoreType(arrayID, typeID)
	if err = typesCollection.CreateType(ctx, extensionType); err != nil {
		return err
	}
	if err = typesCollection.CreateType(ctx, pgtypes.CreateArrayTypeFromBaseType(extensionType)); err != nil {
		return err
	}
	return core.MarkTypesCollectionDirty(ctx, "")
}

func (c *CreateExtension) installTextCompatibleExtensionType(ctx *sql.Context, namespace id.Namespace, typeName string, newType func(arrayID, typeID id.Type) *pgtypes.DoltgresType) error {
	typesCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	typeID := id.NewType(namespace.SchemaName(), typeName)
	arrayID := id.NewType(namespace.SchemaName(), "_"+typeName)
	if typesCollection.HasType(ctx, typeID) {
		return nil
	}
	extensionType := newType(arrayID, typeID)
	if err = typesCollection.CreateType(ctx, extensionType); err != nil {
		return err
	}
	if err = typesCollection.CreateType(ctx, pgtypes.CreateArrayTypeFromBaseType(extensionType)); err != nil {
		return err
	}
	return core.MarkTypesCollectionDirty(ctx, "")
}

func (c *CreateExtension) useExtensionSearchPath(ctx *sql.Context, namespace id.Namespace) (func(), error) {
	schemaName := namespace.SchemaName()
	if len(schemaName) == 0 {
		return func() {}, nil
	}
	originalSearchPath, err := ctx.GetSessionVariable(ctx, "search_path")
	if err != nil {
		return nil, err
	}
	if err = ctx.SetSessionVariable(ctx, "search_path", schemaName); err != nil {
		return nil, err
	}
	return func() {
		_ = ctx.SetSessionVariable(ctx, "search_path", originalSearchPath)
	}, nil
}

func schemaExists(ctx *sql.Context, schemaName string) (bool, error) {
	exists := false
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Schema: func(ctx *sql.Context, item functions.ItemSchema) (cont bool, err error) {
			if strings.EqualFold(item.Item.SchemaName(), schemaName) {
				exists = true
				return false, nil
			}
			return true, nil
		},
	})
	return exists, err
}

func createExtensionSkipsSQL(name string) bool {
	switch strings.ToLower(name) {
	case "btree_gist", "citext", "hstore", "pgcrypto", "plpgsql", "uuid-ossp", "vector":
		return true
	default:
		return false
	}
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateExtension) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateExtension) String() string {
	return "CREATE EXTENSION"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateExtension) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithExpressions implements the interface sql.Expressioner.
func (c *CreateExtension) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(expressions), 1)
	}
	newC := *c
	newC.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newC, nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateExtension) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
