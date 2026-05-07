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

package functions

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initSetConfig() {
	framework.RegisterFunction(set_config_text_text_boolean)
	framework.RegisterFunction(internal_set_config_local_text_text)
}

// internal_set_config_local_text_text is the two-argument helper the SET
// LOCAL AST conversion calls to avoid having to materialize a boolean
// literal in vitess. Its semantics are exactly set_config(name, value, true).
var internal_set_config_local_text_text = framework.Function2{
	Name:               "__doltgres_set_config_local",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, settingName any, newValue any) (any, error) {
		return setConfigLocal(ctx, settingName, newValue, true)
	},
}

// setConfigLocal performs the work of set_config(name, value, is_local)
// with both forms (the public three-argument and internal two-argument
// helper) sharing one implementation.
func setConfigLocal(ctx *sql.Context, settingName any, newValue any, isLocal bool) (any, error) {
	if settingName == nil {
		return nil, errors.Errorf("NULL value not allowed for configuration setting name")
	}
	if newValue == nil {
		newValue = ""
	}
	name := settingName.(string)
	value := newValue.(string)
	isUserConfig := strings.Contains(name, ".")

	if isLocal {
		if err := SnapshotSessionVarBeforeLocalSet(ctx, name, isUserConfig); err != nil {
			return nil, err
		}
	}

	if isUserConfig {
		if err := ctx.SetUserVariable(ctx, name, value, pgtypes.Text); err != nil {
			return nil, err
		}
	} else {
		if err := ctx.SetSessionVariable(ctx, name, value); err != nil {
			return nil, err
		}
	}

	return value, nil
}

// set_config_text_text_boolean implements the set_config() function
// https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-SET
var set_config_text_text_boolean = framework.Function3{
	Name:               "set_config",
	Return:             pgtypes.Text,
	Parameters:         [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Bool},
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, settingName any, newValue any, isLocal any) (any, error) {
		local, _ := isLocal.(bool)
		return setConfigLocal(ctx, settingName, newValue, local)
	},
}
