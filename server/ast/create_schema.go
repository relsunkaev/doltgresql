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

	pgnodes "github.com/dolthub/doltgresql/server/node"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// nodeCreateSchema handles *tree.CreateSchema nodes.
func nodeCreateSchema(ctx *Context, node *tree.CreateSchema) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	// CREATE SCHEMA AUTHORIZATION s1 creates a schema of the same name
	schemaName := node.Schema
	if schemaName == "" {
		schemaName = node.AuthRole
	}
	schemaElements := make([]string, 0, len(node.SchemaElements))
	for _, element := range node.SchemaElements {
		elementSQL, err := createSchemaElementSQL(schemaName, element)
		if err != nil {
			return nil, err
		}
		schemaElements = append(schemaElements, elementSQL)
	}

	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateSchema(schemaName, node.AuthRole, node.IfNotExists, schemaElements),
		Children:  nil,
	}, nil
}

func createSchemaElementSQL(schemaName string, element tree.Statement) (string, error) {
	switch stmt := element.(type) {
	case *tree.CreateTable:
		createTable := *stmt
		if !createTable.Table.ExplicitSchema {
			createTable.Table.SchemaName = tree.Name(schemaName)
			createTable.Table.ExplicitSchema = true
		}
		return tree.AsString(&createTable), nil
	default:
		return "", errors.Errorf("CREATE SCHEMA schema element %T is not yet supported", element)
	}
}
