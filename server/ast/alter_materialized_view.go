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

package ast

import (
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterMaterializedView handles *tree.AlterMaterializedView nodes.
func nodeAlterMaterializedView(ctx *Context, node *tree.AlterMaterializedView) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	if node.Extension != "" {
		return NotYetSupportedError("ALTER MATERIALIZED VIEW DEPENDS ON EXTENSION is not yet supported")
	}
	if len(node.Cmds) != 1 {
		return NotYetSupportedError("ALTER MATERIALIZED VIEW with multiple commands is not yet supported")
	}

	switch cmd := node.Cmds[0].(type) {
	case *tree.AlterTableRenameColumn:
		tableName, err := nodeUnresolvedObjectName(ctx, node.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterMaterializedViewRenameColumn(
				node.Name.Object(),
				node.Name.Schema(),
				bareIdentifier(cmd.Column),
				bareIdentifier(cmd.NewName),
				node.IfExists,
			),
			Children: nil,
			Auth: vitess.AuthInformation{
				AuthType:    auth.AuthType_UPDATE,
				TargetType:  auth.AuthTargetType_TableIdentifiers,
				TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String(), tableName.Name.String()},
			},
		}, nil
	case *tree.AlterTableOwner:
		viewName, err := nodeUnresolvedObjectName(ctx, node.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterMaterializedViewOwner(
				node.IfExists,
				viewName.SchemaQualifier.String(),
				viewName.Name.String(),
				cmd.Owner,
			),
		}, nil
	case *tree.AlterTableSetStorage:
		var relOptions []string
		var resetKeys []string
		var err error
		if cmd.IsReset {
			resetKeys = nodeIndexRelOptionResetKeys(cmd.Params)
		} else {
			relOptions, err = nodeTableRelOptions(cmd.Params)
			if err != nil {
				return nil, err
			}
		}
		viewName, err := nodeUnresolvedObjectName(ctx, node.Name)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterMaterializedViewSetStorage(
				node.IfExists,
				viewName.SchemaQualifier.String(),
				viewName.Name.String(),
				relOptions,
				resetKeys,
			),
		}, nil
	case *tree.AlterTableSetTablespace:
		if !strings.EqualFold(cmd.Tablespace, "pg_default") {
			return nil, errors.Errorf(`tablespace "%s" does not exist`, cmd.Tablespace)
		}
		return NewNoOp(), nil
	case *tree.AlterTableSetAccessMethod:
		if cmd.Method != "" && !strings.EqualFold(cmd.Method, "heap") {
			return nil, errors.Errorf(`access method "%s" does not exist`, cmd.Method)
		}
		return NewNoOp(), nil
	default:
		return NotYetSupportedError("ALTER MATERIALIZED VIEW command is not yet supported")
	}
}
