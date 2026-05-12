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
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/config"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterDatabase handles *tree.AlterDatabase nodes.
func nodeAlterDatabase(ctx *Context, node *tree.AlterDatabase) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	name := bareIdentifier(node.Name)
	alterDatabase := &pgnodes.AlterDatabase{Name: name}

	if node.Owner != "" {
		owner := node.Owner
		alterDatabase.Update.Owner = &owner
		return vitess.InjectedStatement{Statement: alterDatabase}, nil
	}
	if len(node.Options) > 0 {
		update, err := databaseMetadataUpdate(node.Options)
		if err != nil {
			return nil, err
		}
		alterDatabase.Update = update
		return vitess.InjectedStatement{Statement: alterDatabase}, nil
	}
	if node.SetVar != nil {
		setName, setValue, err := databaseSetVar(node.SetVar)
		if err != nil {
			return nil, err
		}
		alterDatabase.SetName = setName
		alterDatabase.SetValue = setValue
		return vitess.InjectedStatement{Statement: alterDatabase}, nil
	}
	if node.ResetVar != "" {
		if !config.IsValidPostgresConfigParameter(node.ResetVar) && !config.IsValidDoltConfigParameter(node.ResetVar) {
			return nil, errors.Errorf(`ERROR: unrecognized configuration parameter "%s"`, node.ResetVar)
		}
		alterDatabase.ResetName = node.ResetVar
		return vitess.InjectedStatement{Statement: alterDatabase}, nil
	}
	if node.Tablespace != "" && (config.IsValidPostgresConfigParameter(node.Tablespace) || config.IsValidDoltConfigParameter(node.Tablespace)) {
		alterDatabase.ResetName = node.Tablespace
		return vitess.InjectedStatement{Statement: alterDatabase}, nil
	}
	if node.ResetAll {
		alterDatabase.ResetAll = true
		return vitess.InjectedStatement{Statement: alterDatabase}, nil
	}

	return NotYetSupportedError("ALTER DATABASE is not yet supported")
}

func databaseMetadataUpdate(options []tree.DatabaseOption) (auth.DatabaseMetadataUpdate, error) {
	var update auth.DatabaseMetadataUpdate
	for _, option := range options {
		switch option.Opt {
		case tree.OptAllowConnections:
			value, err := databaseBoolOption(option.Val)
			if err != nil {
				return update, err
			}
			update.AllowConnections = &value
		case tree.OptConnectionLimit:
			value, err := databaseIntOption(option.Val)
			if err != nil {
				return update, err
			}
			update.ConnectionLimit = &value
		case tree.OptIsTemplate:
			value, err := databaseBoolOption(option.Val)
			if err != nil {
				return update, err
			}
			update.IsTemplate = &value
		default:
			return update, errors.Errorf("unknown ALTER DATABASE option %s", option.Opt)
		}
	}
	return update, nil
}

func databaseBoolOption(expr tree.Expr) (bool, error) {
	switch value := expr.(type) {
	case *tree.DBool:
		return bool(*value), nil
	default:
		return false, errors.Errorf("expected boolean database option, found %T", expr)
	}
}

func databaseIntOption(expr tree.Expr) (int32, error) {
	switch value := expr.(type) {
	case *tree.DInt:
		return int32(*value), nil
	case *tree.NumVal:
		intValue, err := value.AsInt64()
		if err != nil {
			return 0, err
		}
		return int32(intValue), nil
	default:
		return 0, errors.Errorf("expected integer database option, found %T", expr)
	}
}

func databaseSetVar(setVar *tree.SetVar) (string, string, error) {
	if setVar.Namespace != "" {
		return "", "", errors.Errorf(`ERROR: unrecognized configuration parameter "%s.%s"`, setVar.Namespace, setVar.Name)
	}
	if !config.IsValidPostgresConfigParameter(setVar.Name) && !config.IsValidDoltConfigParameter(setVar.Name) {
		return "", "", errors.Errorf(`ERROR: unrecognized configuration parameter "%s"`, setVar.Name)
	}
	if len(setVar.Values) == 0 {
		return "", "", errors.Errorf(`ERROR: syntax error at or near ";"'`)
	}
	values := make([]string, len(setVar.Values))
	for i, expr := range setVar.Values {
		values[i] = databaseSetValue(expr)
	}
	return setVar.Name, strings.Join(values, ", "), nil
}

func databaseSetValue(expr tree.Expr) string {
	switch value := expr.(type) {
	case *tree.DString:
		return string(*value)
	case *tree.StrVal:
		return value.RawString()
	default:
		return strings.Trim(tree.AsString(expr), "'")
	}
}
