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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initTxidCurrent registers the functions to the catalog.
func initTxidCurrent() {
	framework.RegisterFunction(txid_current)
}

var txidCurrentState = struct {
	sync.Mutex
	next    int64
	current map[uint32]int64
}{
	next:    1,
	current: make(map[uint32]int64),
}

func BeginSessionTxid(connectionID uint32) {
	txidCurrentState.Lock()
	defer txidCurrentState.Unlock()
	txidCurrentState.current[connectionID] = txidCurrentState.next
	txidCurrentState.next++
}

func EndSessionTxid(connectionID uint32) {
	txidCurrentState.Lock()
	delete(txidCurrentState.current, connectionID)
	txidCurrentState.Unlock()
}

func sessionTxid(connectionID uint32) int64 {
	txidCurrentState.Lock()
	defer txidCurrentState.Unlock()
	if txid, ok := txidCurrentState.current[connectionID]; ok {
		return txid
	}
	txid := txidCurrentState.next
	txidCurrentState.next++
	txidCurrentState.current[connectionID] = txid
	return txid
}

// txid_current returns a stable nonzero identifier for the current transaction.
// Autocommit statements lazily allocate one for the statement and the
// connection layer clears it when the statement finishes.
var txid_current = framework.Function0{
	Name:               "txid_current",
	Return:             pgtypes.Int64,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return sessionTxid(ctx.Session.ID()), nil
	},
}
