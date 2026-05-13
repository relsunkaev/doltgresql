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

package ast

import (
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterView handles ALTER VIEW nodes.
func nodeAlterView(ctx *Context, stmt *tree.AlterView) (sqlparser.Statement, error) {
	if stmt == nil {
		return nil, nil
	}

	if owner, ok := stmt.Cmd.(*tree.AlterViewOwnerTo); ok {
		treeName := stmt.Name.ToTableName()
		viewName, err := nodeTableName(ctx, &treeName)
		if err != nil {
			return nil, err
		}
		return sqlparser.InjectedStatement{
			Statement: pgnodes.NewAlterViewOwner(
				stmt.IfExists,
				viewName.SchemaQualifier.String(),
				viewName.Name.String(),
				owner.Owner,
			),
		}, nil
	}

	if rename, ok := stmt.Cmd.(*tree.AlterViewRenameTo); ok {
		treeName := stmt.Name.ToTableName()
		viewName, err := nodeTableName(ctx, &treeName)
		if err != nil {
			return nil, err
		}
		newName, err := nodeUnresolvedObjectName(ctx, rename.Rename)
		if err != nil {
			return nil, err
		}
		return sqlparser.InjectedStatement{
			Statement: pgnodes.NewAlterViewRename(
				stmt.IfExists,
				viewName.SchemaQualifier.String(),
				viewName.Name.String(),
				newName.SchemaQualifier.String(),
				newName.Name.String(),
			),
		}, nil
	}

	if setSchema, ok := stmt.Cmd.(*tree.AlterViewSetSchema); ok {
		treeName := stmt.Name.ToTableName()
		viewName, err := nodeTableName(ctx, &treeName)
		if err != nil {
			return nil, err
		}
		return sqlparser.InjectedStatement{
			Statement: pgnodes.NewAlterViewSetSchema(
				stmt.IfExists,
				viewName.SchemaQualifier.String(),
				viewName.Name.String(),
				setSchema.Schema,
			),
		}, nil
	}

	if setOption, ok := stmt.Cmd.(*tree.AlterViewSetOption); ok {
		treeName := stmt.Name.ToTableName()
		viewName, err := nodeTableName(ctx, &treeName)
		if err != nil {
			return nil, err
		}
		options := make([]pgnodes.AlterViewOption, len(setOption.Params))
		for i, opt := range setOption.Params {
			options[i] = pgnodes.AlterViewOption{
				Name:     opt.Name,
				CheckOpt: opt.CheckOpt,
				Security: opt.Security,
			}
		}
		return sqlparser.InjectedStatement{
			Statement: pgnodes.NewAlterViewOptions(
				stmt.IfExists,
				viewName.SchemaQualifier.String(),
				viewName.Name.String(),
				setOption.Reset,
				options,
			),
		}, nil
	}

	return NotYetSupportedError("ALTER VIEW is not yet supported")
}
