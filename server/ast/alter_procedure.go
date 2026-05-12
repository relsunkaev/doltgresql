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

package ast

import (
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeAlterProcedure handles *tree.AlterProcedure nodes.
func nodeAlterProcedure(ctx *Context, node *tree.AlterProcedure) (vitess.Statement, error) {
	options, err := validateRoutineOptions(ctx, node.Options)
	if err != nil {
		return nil, err
	}

	routine, err := routineWithParams(ctx, node.Name, node.Args)
	if err != nil {
		return nil, err
	}
	metadata, hasOptions, err := alterProcedureOptions(options)
	if err != nil {
		return nil, err
	}
	if hasOptions {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterProcedureOptions(routine, metadata),
			Children:  nil,
		}, nil
	}
	if len(node.Options) > 0 {
		return NotYetSupportedError("ALTER PROCEDURE statement is not yet supported")
	}
	if node.Rename != nil {
		newName := node.Rename.ToTableName()
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterProcedureRename(routine, newName.Object()),
			Children:  nil,
		}, nil
	}
	if node.Schema != "" {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterProcedureSetSchema(routine, node.Schema),
			Children:  nil,
		}, nil
	}
	if node.Owner != "" {
		return vitess.InjectedStatement{
			Statement: pgnodes.NewAlterProcedureOwner(routine, node.Owner),
			Children:  nil,
		}, nil
	}

	return NotYetSupportedError("ALTER PROCEDURE statement is not yet supported")
}

func alterProcedureOptions(options map[tree.FunctionOption]tree.RoutineOption) (pgnodes.AlterProcedureOptionMetadata, bool, error) {
	var metadata pgnodes.AlterProcedureOptionMetadata
	hasOptions := false
	if _, ok := options[tree.OptionSet]; ok {
		setConfig, err := routineSetOptions(options)
		if err != nil {
			return metadata, false, err
		}
		metadata.SetConfig = setConfig
		hasOptions = true
	}
	if reset, ok := options[tree.OptionReset]; ok {
		metadata.ResetAllConfig = reset.ResetAll
		if !reset.ResetAll {
			metadata.ResetConfig = []string{reset.ResetParam}
		}
		hasOptions = true
	}
	return metadata, hasOptions, nil
}
