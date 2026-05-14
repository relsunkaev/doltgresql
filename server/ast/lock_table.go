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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

func nodeLockTable(_ *Context, stmt *tree.LockTable) (vitess.Statement, error) {
	targets := make([]pgnodes.RelationLockTarget, 0, len(stmt.Tables))
	for _, table := range stmt.Tables {
		target := pgnodes.RelationLockTarget{
			Database: string(table.CatalogName),
			Schema:   string(table.SchemaName),
			Name:     string(table.ObjectName),
		}
		targets = append(targets, target)
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewLockTable(targets, stmt.Mode, stmt.Nowait),
	}, nil
}
