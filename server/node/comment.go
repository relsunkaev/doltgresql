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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/settings"
)

type CommentTargetKind string

const (
	CommentTargetTable  CommentTargetKind = "table"
	CommentTargetColumn CommentTargetKind = "column"
)

type Comment struct {
	Kind        CommentTargetKind
	Relation    vitess.TableName
	Column      string
	Description *string
}

var _ vitess.Injectable = Comment{}
var _ sql.ExecSourceRel = Comment{}

func NewCommentOnTable(relation vitess.TableName, description *string) Comment {
	return Comment{Kind: CommentTargetTable, Relation: relation, Description: description}
}

func NewCommentOnColumn(relation vitess.TableName, column string, description *string) Comment {
	return Comment{Kind: CommentTargetColumn, Relation: relation, Column: column, Description: description}
}

func (c Comment) Resolved() bool { return true }

func (c Comment) String() string { return "COMMENT ON " + string(c.Kind) }

func (c Comment) Schema(ctx *sql.Context) sql.Schema { return nil }

func (c Comment) Children() []sql.Node { return nil }

func (c Comment) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

func (c Comment) IsReadOnly() bool { return false }

func (c Comment) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

func (c Comment) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	key, err := c.commentKey(ctx)
	if err != nil {
		return nil, err
	}
	comments.Set(key, c.Description)
	return sql.RowsToRowIter(), nil
}

func (c Comment) commentKey(ctx *sql.Context) (comments.Key, error) {
	relationOID, schema, err := resolveCommentRelation(ctx, c.Relation)
	if err != nil {
		return comments.Key{}, err
	}
	key := comments.Key{
		ObjOID:   id.Cache().ToOID(relationOID),
		ClassOID: comments.PgClassOID(),
		ObjSubID: 0,
	}
	if c.Kind == CommentTargetColumn {
		idx := schema.IndexOfColName(c.Column)
		if idx < 0 {
			return comments.Key{}, errors.Errorf(`column "%s" of relation "%s" does not exist`, c.Column, c.Relation.Name.String())
		}
		key.ObjSubID = int32(idx + 1)
	}
	return key, nil
}

func resolveCommentRelation(ctx *sql.Context, relation vitess.TableName) (id.Id, sql.Schema, error) {
	relationName := relation.Name.String()
	searchSchemas := []string{relation.SchemaQualifier.String()}
	if searchSchemas[0] == "" {
		var err error
		searchSchemas, err = settings.GetCurrentSchemas(ctx)
		if err != nil {
			return id.Null, nil, err
		}
	}
	doltSession, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return id.Null, nil, fmt.Errorf("expected Dolt session")
	}
	database, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return id.Null, nil, err
	}
	schemaDatabase, ok := database.(sql.SchemaDatabase)
	if !ok {
		if schema, ok := database.(sql.DatabaseSchema); ok {
			table, found, err := schema.GetTableInsensitive(ctx, relationName)
			if err != nil {
				return id.Null, nil, err
			}
			if found {
				return id.NewTable(schema.SchemaName(), table.Name()).AsId(), table.Schema(ctx), nil
			}
		}
		return id.Null, nil, fmt.Errorf(`relation "%s" does not exist`, relationName)
	}
	for _, schemaName := range searchSchemas {
		schema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
		if err != nil {
			return id.Null, nil, err
		}
		if !ok {
			continue
		}
		table, found, err := schema.GetTableInsensitive(ctx, relationName)
		if err != nil {
			return id.Null, nil, err
		}
		if !found {
			continue
		}
		return id.NewTable(schema.SchemaName(), table.Name()).AsId(), table.Schema(ctx), nil
	}
	return id.Null, nil, fmt.Errorf(`relation "%s" does not exist`, relationName)
}
