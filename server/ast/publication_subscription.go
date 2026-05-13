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
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreatePublication handles *tree.CreatePublication nodes.
func nodeCreatePublication(ctx *Context, node *tree.CreatePublication) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	tables, err := nodePublicationTableSpecs(ctx, node.Targets.Tables)
	if err != nil {
		return nil, err
	}
	options, err := nodeReplicationKVOptions(node.Options)
	if err != nil {
		return nil, err
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.CreatePublication{
			Name:      string(node.Name),
			AllTables: node.Targets.AllTables,
			Tables:    tables,
			Schemas:   append([]string(nil), node.Targets.Schemas...),
			Options:   options,
		},
	}, nil
}

// nodeAlterPublication handles *tree.AlterPublication nodes.
func nodeAlterPublication(ctx *Context, node *tree.AlterPublication) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	tables, err := nodePublicationTableSpecs(ctx, node.Targets.Tables)
	if err != nil {
		return nil, err
	}
	action, err := nodePublicationAlterAction(node.Action)
	if err != nil {
		return nil, err
	}
	options, err := nodeReplicationKVOptions(node.Options)
	if err != nil {
		return nil, err
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.AlterPublication{
			Name:      string(node.Name),
			Action:    action,
			NewName:   string(node.NewName),
			Owner:     node.Owner,
			Tables:    tables,
			Schemas:   append([]string(nil), node.Targets.Schemas...),
			Options:   options,
			AllTables: node.Targets.AllTables,
		},
	}, nil
}

// nodeDropPublication handles *tree.DropPublication nodes.
func nodeDropPublication(ctx *Context, node *tree.DropPublication) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	names := make([]string, len(node.Names))
	for i, name := range node.Names {
		names[i] = string(name)
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.DropPublication{
			Names:    names,
			IfExists: node.IfExists,
			Cascade:  node.DropBehavior == tree.DropCascade,
		},
	}, nil
}

// nodeCreateSubscription handles *tree.CreateSubscription nodes.
func nodeCreateSubscription(ctx *Context, node *tree.CreateSubscription) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	options, err := nodeSubscriptionKVOptions(node.Options)
	if err != nil {
		return nil, err
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.CreateSubscription{
			Name:         string(node.Name),
			ConnInfo:     node.ConnInfo,
			Publications: nodeNameListStrings(node.Publications),
			Options:      options,
		},
	}, nil
}

// nodeAlterSubscription handles *tree.AlterSubscription nodes.
func nodeAlterSubscription(ctx *Context, node *tree.AlterSubscription) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	action, err := nodeSubscriptionAlterAction(node.Action)
	if err != nil {
		return nil, err
	}
	options, err := nodeSubscriptionKVOptions(node.Options)
	if err != nil {
		return nil, err
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.AlterSubscription{
			Name:         string(node.Name),
			Action:       action,
			NewName:      string(node.NewName),
			Owner:        node.Owner,
			ConnInfo:     node.ConnInfo,
			Publications: nodeNameListStrings(node.Publications),
			Options:      options,
		},
	}, nil
}

// nodeDropSubscription handles *tree.DropSubscription nodes.
func nodeDropSubscription(ctx *Context, node *tree.DropSubscription) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.DropSubscription{
			Name:     string(node.Name),
			IfExists: node.IfExists,
			Cascade:  node.DropBehavior == tree.DropCascade,
		},
	}, nil
}

func nodePublicationTableSpecs(ctx *Context, treeTables tree.PublicationTables) ([]pgnodes.PublicationTableSpec, error) {
	if len(treeTables) == 0 {
		return nil, nil
	}
	tables := make([]pgnodes.PublicationTableSpec, len(treeTables))
	for i := range treeTables {
		tableName, err := nodeTableName(ctx, &treeTables[i].Name)
		if err != nil {
			return nil, err
		}
		if len(tableName.DbQualifier.String()) > 0 {
			return nil, errors.New("publication table database qualifiers are not yet supported")
		}
		tables[i] = pgnodes.PublicationTableSpec{
			Schema:    tableName.SchemaQualifier.String(),
			Name:      tableName.Name.String(),
			Columns:   nodeNameListStrings(treeTables[i].Columns),
			RowFilter: nodeExprSQL(treeTables[i].RowFilter),
		}
	}
	return tables, nil
}

func nodePublicationAlterAction(action tree.PublicationAlterAction) (pgnodes.PublicationAlterAction, error) {
	switch action {
	case tree.PublicationAlterAddTables:
		return pgnodes.PublicationAlterAddTables, nil
	case tree.PublicationAlterSetTables:
		return pgnodes.PublicationAlterSetTables, nil
	case tree.PublicationAlterDropTables:
		return pgnodes.PublicationAlterDropTables, nil
	case tree.PublicationAlterAddSchemas:
		return pgnodes.PublicationAlterAddSchemas, nil
	case tree.PublicationAlterSetSchemas:
		return pgnodes.PublicationAlterSetSchemas, nil
	case tree.PublicationAlterDropSchemas:
		return pgnodes.PublicationAlterDropSchemas, nil
	case tree.PublicationAlterSetOptions:
		return pgnodes.PublicationAlterSetOptions, nil
	case tree.PublicationAlterRename:
		return pgnodes.PublicationAlterRename, nil
	case tree.PublicationAlterOwner:
		return pgnodes.PublicationAlterOwner, nil
	default:
		return "", errors.Errorf("unknown ALTER PUBLICATION action: %s", action)
	}
}

func nodeSubscriptionAlterAction(action tree.SubscriptionAlterAction) (pgnodes.SubscriptionAlterAction, error) {
	switch action {
	case tree.SubscriptionAlterConnection:
		return pgnodes.SubscriptionAlterConnection, nil
	case tree.SubscriptionAlterSetPublication:
		return pgnodes.SubscriptionAlterSetPublication, nil
	case tree.SubscriptionAlterAddPublication:
		return pgnodes.SubscriptionAlterAddPublication, nil
	case tree.SubscriptionAlterDropPublication:
		return pgnodes.SubscriptionAlterDropPublication, nil
	case tree.SubscriptionAlterRefresh:
		return pgnodes.SubscriptionAlterRefresh, nil
	case tree.SubscriptionAlterEnable:
		return pgnodes.SubscriptionAlterEnable, nil
	case tree.SubscriptionAlterDisable:
		return pgnodes.SubscriptionAlterDisable, nil
	case tree.SubscriptionAlterSetOptions:
		return pgnodes.SubscriptionAlterSetOptions, nil
	case tree.SubscriptionAlterSkip:
		return pgnodes.SubscriptionAlterSkip, nil
	case tree.SubscriptionAlterRename:
		return pgnodes.SubscriptionAlterRename, nil
	case tree.SubscriptionAlterOwner:
		return pgnodes.SubscriptionAlterOwner, nil
	default:
		return "", errors.Errorf("unknown ALTER SUBSCRIPTION action: %s", action)
	}
}

func nodeReplicationKVOptions(options tree.KVOptions) (map[string]string, error) {
	ret := make(map[string]string, len(options))
	seen := make(map[string]struct{}, len(options))
	for _, option := range options {
		key := strings.ToLower(string(option.Key))
		if _, ok := seen[key]; ok {
			return nil, errors.Errorf("conflicting or redundant options: %s", key)
		}
		seen[key] = struct{}{}
		ret[key] = nodeOptionValue(option.Value)
	}
	return ret, nil
}

func nodeSubscriptionKVOptions(options tree.KVOptions) (map[string]string, error) {
	return nodeReplicationKVOptions(options)
}

func nodeOptionValue(expr tree.Expr) string {
	switch val := expr.(type) {
	case nil:
		return "true"
	case *tree.StrVal:
		return val.RawString()
	case *tree.DString:
		return string(*val)
	case *tree.DBool:
		if bool(*val) {
			return "true"
		}
		return "false"
	default:
		return tree.AsStringWithFlags(expr, tree.FmtBareStrings)
	}
}

func nodeNameListStrings(names tree.NameList) []string {
	if len(names) == 0 {
		return nil
	}
	ret := make([]string, len(names))
	for i, name := range names {
		ret[i] = string(name)
	}
	return ret
}

func nodeExprSQL(expr tree.Expr) string {
	if expr == nil {
		return ""
	}
	return tree.AsStringWithFlags(expr, tree.FmtParsable)
}
