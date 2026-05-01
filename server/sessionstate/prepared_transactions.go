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

package sessionstate

import (
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
)

// PreparedTransaction describes a prepared transaction visible in pg_prepared_xacts.
type PreparedTransaction struct {
	TransactionID uint32
	GID           string
	Prepared      time.Time
	Owner         string
	Database      string

	transaction *dsess.DoltTransaction
	workingSet  *doltdb.WorkingSet
}

var preparedTransactions = struct {
	sync.RWMutex
	nextTransactionID uint32
	byGID             map[string]PreparedTransaction
}{
	nextTransactionID: 1,
	byGID:             make(map[string]PreparedTransaction),
}

// PrepareTransaction records the active transaction under gid and rolls the current session back. The stored working
// set remains invisible until CommitPreparedTransaction applies it.
func PrepareTransaction(ctx *sql.Context, gid string) error {
	if gid == "" {
		return errors.Errorf("transaction identifier must not be empty")
	}
	tx := ctx.GetTransaction()
	if tx == nil {
		return errors.Errorf("PREPARE TRANSACTION can only be used in transaction blocks")
	}
	doltTx, ok := tx.(*dsess.DoltTransaction)
	if !ok {
		return errors.Errorf("expected a DoltTransaction")
	}

	sess := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()
	if dbName == "" {
		return errors.Errorf("cannot prepare transaction with no database selected")
	}
	state, ok, err := sess.LookupDbState(ctx, dbName)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseNotFound.New(dbName)
	}
	workingSet := state.WorkingSet()
	if workingSet == nil {
		return errors.Errorf("cannot prepare transaction on detached head")
	}

	owner := sess.Username()
	if owner == "" {
		owner = "postgres"
	}
	prepared := PreparedTransaction{
		GID:         gid,
		Prepared:    time.Now(),
		Owner:       owner,
		Database:    dbName,
		transaction: doltTx,
		workingSet:  workingSet,
	}

	preparedTransactions.Lock()
	if _, ok = preparedTransactions.byGID[gid]; ok {
		preparedTransactions.Unlock()
		return errors.Errorf("prepared transaction with identifier %q already exists", gid)
	}
	prepared.TransactionID = preparedTransactions.nextTransactionID
	preparedTransactions.nextTransactionID++
	preparedTransactions.byGID[gid] = prepared
	preparedTransactions.Unlock()

	if err = sess.Rollback(ctx, tx); err != nil {
		_ = RollbackPreparedTransaction(gid)
		return err
	}
	ctx.SetTransaction(nil)
	ctx.SetIgnoreAutoCommit(false)
	return nil
}

// CommitPreparedTransaction commits and removes the prepared transaction with gid.
func CommitPreparedTransaction(ctx *sql.Context, gid string) error {
	prepared, ok := takePreparedTransaction(gid)
	if !ok {
		return errors.Errorf("prepared transaction with identifier %q does not exist", gid)
	}

	sess := dsess.DSessFromSess(ctx.Session)
	if _, ok, err := sess.LookupDbState(ctx, prepared.Database); err != nil {
		preparedTransactions.Lock()
		preparedTransactions.byGID[gid] = prepared
		preparedTransactions.Unlock()
		return err
	} else if !ok {
		preparedTransactions.Lock()
		preparedTransactions.byGID[gid] = prepared
		preparedTransactions.Unlock()
		return sql.ErrDatabaseNotFound.New(prepared.Database)
	}
	if err := sess.SetWorkingSet(ctx, prepared.Database, prepared.workingSet); err != nil {
		preparedTransactions.Lock()
		preparedTransactions.byGID[gid] = prepared
		preparedTransactions.Unlock()
		return err
	}
	if err := sess.CommitWorkingSet(ctx, prepared.Database, prepared.transaction); err != nil {
		preparedTransactions.Lock()
		preparedTransactions.byGID[gid] = prepared
		preparedTransactions.Unlock()
		return err
	}
	ctx.SetTransaction(nil)
	ctx.SetIgnoreAutoCommit(false)
	return nil
}

// RollbackPreparedTransaction removes the prepared transaction with gid without applying it.
func RollbackPreparedTransaction(gid string) error {
	preparedTransactions.Lock()
	defer preparedTransactions.Unlock()
	if _, ok := preparedTransactions.byGID[gid]; !ok {
		return errors.Errorf("prepared transaction with identifier %q does not exist", gid)
	}
	delete(preparedTransactions.byGID, gid)
	return nil
}

func takePreparedTransaction(gid string) (PreparedTransaction, bool) {
	preparedTransactions.Lock()
	defer preparedTransactions.Unlock()
	prepared, ok := preparedTransactions.byGID[gid]
	if ok {
		delete(preparedTransactions.byGID, gid)
	}
	return prepared, ok
}

// ListPreparedTransactions returns a stable snapshot of prepared transactions. If database is non-empty, only
// prepared transactions for that database are returned.
func ListPreparedTransactions(database string) []PreparedTransaction {
	preparedTransactions.RLock()
	defer preparedTransactions.RUnlock()
	transactions := make([]PreparedTransaction, 0, len(preparedTransactions.byGID))
	for _, transaction := range preparedTransactions.byGID {
		if database == "" || transaction.Database == database {
			transactions = append(transactions, transaction)
		}
	}
	sort.Slice(transactions, func(i, j int) bool {
		return transactions[i].GID < transactions[j].GID
	})
	return transactions
}
