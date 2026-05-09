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
	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// nodeCreateMaterializedView handles *tree.CreateMaterializedView nodes.
func nodeCreateMaterializedView(ctx *Context, node *tree.CreateMaterializedView) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	if len(node.ColumnNames) > 0 {
		return nil, errors.Errorf("CREATE MATERIALIZED VIEW column names are not yet supported")
	}
	if node.Using != "" {
		return nil, errors.Errorf("CREATE MATERIALIZED VIEW USING is not yet supported")
	}
	if len(node.Params) > 0 {
		return nil, errors.Errorf("CREATE MATERIALIZED VIEW storage parameters are not yet supported")
	}
	if node.Tablespace != "" {
		return nil, errors.Errorf("CREATE MATERIALIZED VIEW TABLESPACE is not yet supported")
	}
	if node.CheckOption != tree.ViewCheckOptionUnspecified {
		return nil, errors.Errorf("CREATE MATERIALIZED VIEW WITH CHECK OPTION is not yet supported")
	}
	if node.WithNoData {
		return nil, errors.Errorf("CREATE MATERIALIZED VIEW WITH NO DATA is not yet supported")
	}

	tableName, err := nodeTableName(ctx, &node.Name)
	if err != nil {
		return nil, err
	}
	selectStmt, err := nodeSelect(ctx, node.AsSource)
	if err != nil {
		return nil, err
	}
	definition := createViewSelectDefinition(ctx, node.AsSource.String())
	return &vitess.DDL{
		Action:      vitess.CreateStr,
		Table:       tableName,
		IfNotExists: node.IfNotExists,
		TableSpec: &vitess.TableSpec{
			TableOpts: []*vitess.TableOption{
				{
					Name:  "comment",
					Value: tablemetadata.SetMaterializedViewDefinition("", definition),
				},
			},
		},
		OptSelect: &vitess.OptSelect{
			Select: selectStmt,
		},
		Auth: vitess.AuthInformation{
			AuthType:    auth.AuthType_CREATE,
			TargetType:  auth.AuthTargetType_SchemaIdentifiers,
			TargetNames: []string{tableName.DbQualifier.String(), tableName.SchemaQualifier.String()},
		},
	}, nil
}
