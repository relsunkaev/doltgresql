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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeSetTransaction handles *tree.SetTransaction nodes.
func nodeSetTransaction(ctx *Context, node *tree.SetTransaction) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}

	return vitess.InjectedStatement{
		Statement: pgnodes.SetTransaction{
			ReadWriteMode:  nodeTransactionReadWriteMode(node.Modes.ReadWriteMode),
			DeferrableMode: nodeTransactionDeferrableMode(node.Modes.Deferrable),
			Isolation:      node.Modes.Isolation != tree.UnspecifiedIsolation,
			Snapshot:       node.Snapshot,
		},
	}, nil
}

func nodeTransactionReadWriteMode(mode tree.ReadWriteMode) pgnodes.TransactionReadWriteMode {
	switch mode {
	case tree.ReadOnly:
		return pgnodes.TransactionReadOnly
	case tree.ReadWrite:
		return pgnodes.TransactionReadWrite
	default:
		return pgnodes.TransactionReadWriteUnspecified
	}
}

func nodeTransactionDeferrableMode(mode tree.DeferrableMode) pgnodes.TransactionDeferrableMode {
	switch mode {
	case tree.Deferrable:
		return pgnodes.TransactionDeferrable
	case tree.NotDeferrable:
		return pgnodes.TransactionNotDeferrable
	default:
		return pgnodes.TransactionDeferrableUnspecified
	}
}
