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
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// CreatePartialUniqueIndex builds partial unique indexes as non-unique Dolt
// indexes plus Doltgres metadata. Predicate-scoped uniqueness is validated at
// build time here and at write time by PartialUniqueTable.
type CreatePartialUniqueIndex struct {
	ifNotExists  bool
	concurrently bool
	schema       string
	table        string
	indexName    string
	columns      []sql.IndexColumn
	metadata     indexmetadata.Metadata
}

var _ sql.ExecSourceRel = (*CreatePartialUniqueIndex)(nil)
var _ vitess.Injectable = (*CreatePartialUniqueIndex)(nil)

// NewCreatePartialUniqueIndex constructs the partial unique CREATE INDEX node.
func NewCreatePartialUniqueIndex(
	ifNotExists bool,
	concurrently bool,
	schema string,
	table string,
	indexName string,
	columns []sql.IndexColumn,
	metadata indexmetadata.Metadata,
) *CreatePartialUniqueIndex {
	cols := append([]sql.IndexColumn(nil), columns...)
	metadata.Unique = true
	return &CreatePartialUniqueIndex{
		ifNotExists:  ifNotExists,
		concurrently: concurrently,
		schema:       schema,
		table:        table,
		indexName:    indexName,
		columns:      cols,
		metadata:     metadata,
	}
}

// Children implements sql.ExecSourceRel.
func (c *CreatePartialUniqueIndex) Children() []sql.Node { return nil }

// IsReadOnly implements sql.ExecSourceRel.
func (c *CreatePartialUniqueIndex) IsReadOnly() bool { return false }

// Resolved implements sql.ExecSourceRel.
func (c *CreatePartialUniqueIndex) Resolved() bool { return true }

// Schema implements sql.ExecSourceRel.
func (c *CreatePartialUniqueIndex) Schema(*sql.Context) sql.Schema { return nil }

// String implements sql.ExecSourceRel.
func (c *CreatePartialUniqueIndex) String() string { return "CREATE UNIQUE INDEX WHERE" }

// WithChildren implements sql.ExecSourceRel.
func (c *CreatePartialUniqueIndex) WithChildren(_ *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (c *CreatePartialUniqueIndex) WithResolvedChildren(_ context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// RowIter validates existing rows and registers the physical non-unique index.
func (c *CreatePartialUniqueIndex) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, c.schema)
	if err != nil {
		return nil, err
	}
	db, err := indexDDLDatabase(ctx, schemaName, false)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, errors.Errorf(`schema "%s" does not exist`, c.schema)
	}
	table, ok, err := db.GetTableInsensitive(ctx, c.table)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrTableNotFound.New(c.table)
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
	if err = checkIndexTableOwnership(ctx, doltdb.TableName{Schema: schemaName, Name: c.table}); err != nil {
		return nil, err
	}
	alterable, ok := table.(sql.IndexAlterableTable)
	if !ok {
		return nil, errors.Errorf(`relation "%s" does not support index alteration`, c.table)
	}
	tableSchema := table.Schema(ctx)
	metadata := c.metadata
	metadata.Predicate = postgresTextLiteralPredicate(metadata.Predicate, tableSchema)
	check, err := partialUniqueIndexFromColumns(c.indexName, table.Name(), tableSchema, c.columns, metadata)
	if err != nil {
		return nil, err
	}
	if err = validateNoPartialUniqueDuplicates(ctx, scanTableForPartialUniqueCheck(table), check); err != nil {
		return nil, err
	}
	indexMetadata := metadata
	if c.concurrently {
		indexMetadata.NotReady = true
		indexMetadata.Invalid = true
	}
	indexDef := sql.IndexDef{
		Name:       c.indexName,
		Columns:    append([]sql.IndexColumn(nil), c.columns...),
		Constraint: sql.IndexConstraint_None,
		Storage:    sql.IndexUsing_BTree,
		Comment:    indexmetadata.EncodeComment(indexMetadata),
	}
	if err = alterable.CreateIndex(ctx, indexDef); err != nil {
		if c.ifNotExists && sql.ErrDuplicateKey.Is(err) {
			return sql.RowsToRowIter(), nil
		}
		return nil, err
	}
	if err = c.finishConcurrentBuild(ctx, schemaName, metadata); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreatePartialUniqueIndex) finishConcurrentBuild(ctx *sql.Context, schemaName string, metadata indexmetadata.Metadata) error {
	if !c.concurrently {
		return nil
	}
	if err := commitInterPhaseTransaction(ctx); err != nil {
		return err
	}
	if testHookBetweenPhases != nil {
		testHookBetweenPhases(ctx)
	}
	if err := flipIndexComment(ctx, schemaName, c.table, c.indexName, alteredIndexComment(metadata)); err != nil {
		return err
	}
	return commitInterPhaseTransaction(ctx)
}

func postgresTextLiteralPredicate(predicate string, tableSchema sql.Schema) string {
	if predicate == "" {
		return predicate
	}
	for _, column := range tableSchema {
		if !isPostgresTextColumn(column.Type) {
			continue
		}
		predicate = castStringLiteralsComparedToColumn(predicate, column.Name)
	}
	return predicate
}

func isPostgresTextColumn(typ sql.Type) bool {
	if dgType, ok := typ.(*pgtypes.DoltgresType); ok {
		return dgType.ID.TypeName() == "text"
	}
	return strings.EqualFold(typ.String(), "text")
}

func castStringLiteralsComparedToColumn(predicate, columnName string) string {
	quotedName := regexp.QuoteMeta(columnName)
	quotedIdentifier := regexp.QuoteMeta(`"` + strings.ReplaceAll(columnName, `"`, `""`) + `"`)
	identifier := `(?:(?i:\b` + quotedName + `\b)|` + quotedIdentifier + `)`
	stringLiteral := `'(?:''|[^'])*'`
	optionalCast := `(::[[:alpha:]_][[:alnum:]_]*(?:\[\])?)?`
	leftColumn := regexp.MustCompile(`(` + identifier + `\s*=\s*)` + `(` + stringLiteral + `)` + optionalCast)
	predicate = replaceUncastStringLiteral(leftColumn, predicate, 2, func(parts []string) string {
		return parts[1] + parts[2] + "::text"
	})
	rightColumn := regexp.MustCompile(`(` + stringLiteral + `)` + optionalCast + `(\s*=\s*` + identifier + `)`)
	return replaceUncastStringLiteral(rightColumn, predicate, 1, func(parts []string) string {
		return parts[1] + "::text" + parts[3]
	})
}

func replaceUncastStringLiteral(re *regexp.Regexp, predicate string, literalGroup int, replacement func([]string) string) string {
	return re.ReplaceAllStringFunc(predicate, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) <= literalGroup+1 || parts[literalGroup] == "" || parts[literalGroup+1] != "" {
			return match
		}
		return replacement(parts)
	})
}
