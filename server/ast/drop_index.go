// Copyright 2023 Dolthub, Inc.
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
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeDropIndex handles *tree.DropIndex nodes.
func nodeDropIndex(ctx *Context, node *tree.DropIndex) (vitess.Statement, error) {
	if node == nil || len(node.IndexList) == 0 {
		return nil, nil
	}
	switch node.DropBehavior {
	case tree.DropDefault, tree.DropRestrict:
		// Default behavior, nothing to do
	case tree.DropCascade:
		return nil, errors.Errorf("CASCADE is not yet supported")
	}
	if node.Concurrently {
		return nil, errors.Errorf("concurrent indexes are not yet supported")
	}
	targets := make([]pgnodes.DropIndexTarget, len(node.IndexList))
	for i, index := range node.IndexList {
		schemaName, tableName, indexName, err := tableIndexNameParts(ctx, index)
		if err != nil {
			return nil, err
		}
		targets[i] = pgnodes.DropIndexTarget{
			Schema: schemaName,
			Table:  tableName,
			Index:  indexName,
		}
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropIndexes(node.IfExists, targets),
	}, nil
}

func tableIndexNameParts(ctx *Context, index *tree.TableIndexName) (schemaName string, tableName string, indexName string, err error) {
	if index == nil {
		return "", "", "", nil
	}
	vitessTableName, err := nodeTableName(ctx, &index.Table)
	if err != nil {
		return "", "", "", err
	}
	return vitessTableName.SchemaQualifier.String(), vitessTableName.Name.String(), string(index.Index), nil
}
