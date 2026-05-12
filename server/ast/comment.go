// Copyright 2024 Dolthub, Inc.
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

package ast

import (
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeComment handles *tree.Comment nodes.
func nodeComment(ctx *Context, stmt *tree.Comment) (vitess.Statement, error) {
	if stmt == nil {
		return nil, nil
	}

	switch obj := stmt.Object.(type) {
	case *tree.CommentOnTable:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTable(tableName, stmt.Comment)}, nil
	case *tree.CommentOnView:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnView(tableName, stmt.Comment)}, nil
	case *tree.CommentOnMaterializedView:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTable(tableName, stmt.Comment)}, nil
	case *tree.CommentOnSequence:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnSequence(tableName, stmt.Comment)}, nil
	case *tree.CommentOnSchema:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnSchema(obj.Name, stmt.Comment)}, nil
	case *tree.CommentOnDatabase:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnDatabase(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnRole:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnRole(obj.Name, stmt.Comment)}, nil
	case *tree.CommentOnExtension:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnExtension(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnAccessMethod:
		accessMethodName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnAccessMethod(accessMethodName.Name.String(), stmt.Comment)}, nil
	case *tree.CommentOnIndex:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnIndex(
			string(obj.Index.Table.SchemaName),
			string(obj.Index.Table.ObjectName),
			string(obj.Index.Index),
			stmt.Comment,
		)}, nil
	case *tree.CommentOnConstraintOnTable:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.Table)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnConstraint(tableName, string(obj.Constraint), stmt.Comment)}, nil
	case *tree.CommentOnTrigger:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.Table)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTrigger(tableName, string(obj.Trigger), stmt.Comment)}, nil
	case *tree.CommentOnPublication:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnPublication(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnSubscription:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnSubscription(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnTextSearchConfiguration:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTextSearchConfiguration(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnTextSearchDictionary:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTextSearchDictionary(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnTextSearchParser:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTextSearchParser(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnTextSearchTemplate:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnTextSearchTemplate(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnFunction:
		routine, err := routineWithParams(ctx, obj.Name, obj.Args)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnFunction(routine, stmt.Comment)}, nil
	case *tree.CommentOnProcedure:
		routine, err := routineWithParams(ctx, obj.Name, obj.Args)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnProcedure(routine, stmt.Comment)}, nil
	case *tree.CommentOnRoutine:
		routine, err := routineWithParams(ctx, obj.Name, obj.Args)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnRoutine(routine, stmt.Comment)}, nil
	case *tree.CommentOnAggregate:
		if err := validateAggArgMode(ctx, obj.AggSig.Args, obj.AggSig.OrderByArgs); err != nil {
			return nil, err
		}
		if len(obj.AggSig.OrderByArgs) > 0 || obj.AggSig.All {
			return NotYetSupportedError("COMMENT ON AGGREGATE ordered-set signature is not yet supported")
		}
		routine, err := routineWithParams(ctx, obj.Name, obj.AggSig.Args)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnFunction(routine, stmt.Comment)}, nil
	case *tree.CommentOnType:
		typeName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnType(typeName, stmt.Comment)}, nil
	case *tree.CommentOnDomain:
		typeName, err := nodeUnresolvedObjectName(ctx, obj.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnType(typeName, stmt.Comment)}, nil
	case *tree.CommentOnLanguage:
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnLanguage(string(obj.Name), stmt.Comment)}, nil
	case *tree.CommentOnColumn:
		tableName, err := nodeUnresolvedObjectName(ctx, obj.ColumnItem.TableName)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{Statement: pgnodes.NewCommentOnColumn(tableName, string(obj.ColumnName), stmt.Comment)}, nil
	default:
		return NewNoOp("COMMENT ON is not yet supported"), nil
	}
}
