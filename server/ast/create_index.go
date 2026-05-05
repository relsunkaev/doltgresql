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
	"fmt"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreateIndex handles *tree.CreateIndex nodes.
func nodeCreateIndex(ctx *Context, node *tree.CreateIndex) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	if node.Concurrently {
		return nil, errors.Errorf("concurrent index creation is not yet supported")
	}
	accessMethod := indexmetadata.NormalizeAccessMethod(node.Using)
	if accessMethod != indexmetadata.AccessMethodBtree && accessMethod != indexmetadata.AccessMethodGin {
		return nil, errors.Errorf("index method %s is not yet supported", node.Using)
	}
	if node.Predicate != nil {
		return nil, errors.Errorf("WHERE is not yet supported")
	}
	metadata, err := nodeIndexMetadata(node, accessMethod)
	if err != nil {
		return nil, err
	}
	indexDef, err := nodeIndexTableDef(ctx, &tree.IndexTableDef{
		Name:        node.Name,
		Columns:     node.Columns,
		IndexParams: node.IndexParams,
	})
	if err != nil {
		return nil, err
	}
	tableName, err := nodeTableName(ctx, &node.Table)
	if err != nil {
		return nil, err
	}
	if accessMethod == indexmetadata.AccessMethodGin {
		if node.Unique {
			return nil, errors.Errorf("unique gin indexes are not yet supported")
		}
		if len(indexDef.Fields) != 1 {
			return nil, errors.Errorf("multi-column gin indexes are not yet supported")
		}
		if indexDef.Fields[0].Expression != nil || indexDef.Fields[0].Column.IsEmpty() {
			return nil, errors.Errorf("expression gin indexes are not yet supported")
		}
		return vitess.InjectedStatement{
			Statement: pgnodes.NewCreateJsonbGinIndex(
				node.IfNotExists,
				tableName.SchemaQualifier.String(),
				tableName.Name.String(),
				indexDef.Info.Name.String(),
				indexDef.Fields[0].Column.String(),
				metadata.OpClasses[0],
			),
		}, nil
	}
	var indexType string
	if node.Unique {
		indexType = vitess.UniqueStr
	}
	options := indexDef.Options
	var using vitess.ColIdent
	if metadata != nil {
		options = append(options, &vitess.IndexOption{
			Name:  vitess.KeywordString(vitess.COMMENT_KEYWORD),
			Value: vitess.NewStrVal([]byte(indexmetadata.EncodeComment(*metadata))),
		})
		using = vitess.NewColIdent(indexmetadata.AccessMethodBtree)
	}
	return &vitess.AlterTable{
		Table: tableName,
		Statements: []*vitess.DDL{
			{
				Action:      vitess.AlterStr,
				Table:       tableName,
				IfNotExists: node.IfNotExists,
				IndexSpec: &vitess.IndexSpec{
					Action:   vitess.CreateStr,
					FromName: indexDef.Info.Name,
					ToName:   indexDef.Info.Name,
					Type:     indexType,
					Using:    using,
					Fields:   indexDef.Fields,
					Options:  options,
				},
			},
		},
	}, nil
}

func nodeIndexMetadata(node *tree.CreateIndex, accessMethod string) (*indexmetadata.Metadata, error) {
	switch accessMethod {
	case indexmetadata.AccessMethodBtree:
		return nodeBtreeIndexMetadata(node)
	case indexmetadata.AccessMethodGin:
		opClasses := make([]string, len(node.Columns))
		for i, column := range node.Columns {
			opClass := indexmetadata.OpClassJsonbOps
			if column.OpClass != nil {
				if len(column.OpClass.Options) > 0 {
					return nil, errors.Errorf("index operator class options are not yet supported")
				}
				opClass = indexmetadata.NormalizeOpClass(column.OpClass.Name)
			}
			if !indexmetadata.IsSupportedGinJsonbOpClass(opClass) {
				return nil, errors.Errorf("operator class %s is not yet supported for gin indexes", opClass)
			}
			opClasses[i] = opClass
		}
		return &indexmetadata.Metadata{
			AccessMethod: accessMethod,
			OpClasses:    opClasses,
		}, nil
	default:
		return nil, fmt.Errorf("unknown index access method %s", accessMethod)
	}
}

func nodeBtreeIndexMetadata(node *tree.CreateIndex) (*indexmetadata.Metadata, error) {
	sortOptions := make([]indexmetadata.IndexColumnOption, len(node.Columns))
	hasMetadata := false
	for i, column := range node.Columns {
		switch column.Direction {
		case tree.DefaultDirection, tree.Ascending:
		case tree.Descending:
			sortOptions[i].Direction = indexmetadata.SortDirectionDesc
			hasMetadata = true
		default:
			return nil, errors.Errorf("unknown index sorting direction encountered")
		}

		switch column.NullsOrder {
		case tree.DefaultNullsOrder:
		case tree.NullsFirst:
			sortOptions[i].NullsOrder = indexmetadata.NullsOrderFirst
			hasMetadata = true
		case tree.NullsLast:
			return nil, errors.Errorf("NULLS LAST for indexes is not yet supported")
		default:
			return nil, errors.Errorf("unknown NULL ordering for index")
		}
	}
	if !hasMetadata {
		return nil, nil
	}
	return &indexmetadata.Metadata{
		AccessMethod: indexmetadata.AccessMethodBtree,
		SortOptions:  sortOptions,
	}, nil
}
