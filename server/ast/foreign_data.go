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

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

func nodeCreateForeignDataWrapper(ctx *Context, node *tree.CreateForeignDataWrapper) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateForeignDataWrapper(string(node.Name), fdwOptionsToStrings(node.Options)),
		Children:  nil,
	}, nil
}

func nodeAlterForeignDataWrapper(ctx *Context, node *tree.AlterForeignDataWrapper) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewAlterForeignDataWrapper(string(node.Name)),
		Children:  nil,
	}, nil
}

func nodeAlterForeignTable(ctx *Context, node *tree.AlterForeignTable) (vitess.Statement, error) {
	tableName, err := nodeUnresolvedObjectName(ctx, node.Name)
	if err != nil {
		return nil, err
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewAlterForeignTableOptions(
			tableName.SchemaQualifier.String(),
			tableName.Name.String(),
			fdwOptionsToStrings(node.Options),
		),
		Children: nil,
	}, nil
}

func nodeDropForeignDataWrapper(ctx *Context, node *tree.DropForeignDataWrapper) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropForeignDataWrapper(
			node.Names.ToStrings(),
			node.IfExists,
			node.DropBehavior == tree.DropCascade,
		),
		Children: nil,
	}, nil
}

func nodeDropForeignTable(ctx *Context, node *tree.DropForeignTable) (vitess.Statement, error) {
	tables := make([]pgnodes.ForeignTableName, 0, len(node.Names))
	for i := range node.Names {
		tableName, err := nodeTableName(ctx, &node.Names[i])
		if err != nil {
			return nil, err
		}
		tables = append(tables, pgnodes.ForeignTableName{
			Schema: tableName.SchemaQualifier.String(),
			Name:   tableName.Name.String(),
		})
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropForeignTable(
			tables,
			node.IfExists,
			node.DropBehavior == tree.DropCascade,
		),
		Children: nil,
	}, nil
}

func nodeCreateForeignServer(ctx *Context, node *tree.CreateForeignServer) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateForeignServer(
			string(node.Name),
			string(node.Wrapper),
			node.Type,
			node.Version,
			fdwOptionsToStrings(node.Options),
		),
		Children: nil,
	}, nil
}

func nodeAlterForeignServer(ctx *Context, node *tree.AlterForeignServer) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewAlterForeignServer(string(node.Name), node.Version),
		Children:  nil,
	}, nil
}

func nodeDropForeignServer(ctx *Context, node *tree.DropForeignServer) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropForeignServer(
			node.Names.ToStrings(),
			node.IfExists,
			node.DropBehavior == tree.DropCascade,
		),
		Children: nil,
	}, nil
}

func nodeCreateUserMapping(ctx *Context, node *tree.CreateUserMapping) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewCreateUserMapping(node.User, string(node.Server), fdwOptionsToStrings(node.Options)),
		Children:  nil,
	}, nil
}

func nodeAlterUserMapping(ctx *Context, node *tree.AlterUserMapping) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewAlterUserMapping(node.User, string(node.Server)),
		Children:  nil,
	}, nil
}

func nodeDropUserMapping(ctx *Context, node *tree.DropUserMapping) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDropUserMapping(node.User, string(node.Server), node.IfExists),
		Children:  nil,
	}, nil
}

func nodeImportForeignSchema(ctx *Context, node *tree.ImportForeignSchema) (vitess.Statement, error) {
	return vitess.InjectedStatement{
		Statement: pgnodes.NewImportForeignSchema(string(node.Schema), string(node.Server), string(node.Into)),
		Children:  nil,
	}, nil
}

func fdwOptionsToStrings(options []tree.KVOption) []string {
	if len(options) == 0 {
		return nil
	}
	ret := make([]string, 0, len(options))
	for _, option := range options {
		key := strings.TrimSpace(string(option.Key))
		if key == "" {
			continue
		}
		if option.Value == nil {
			ret = append(ret, key)
		} else {
			ret = append(ret, key+"="+tree.AsString(option.Value))
		}
	}
	return ret
}
