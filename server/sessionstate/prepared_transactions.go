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
	"encoding/hex"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
)

// PreparedTransaction describes a prepared transaction visible in pg_prepared_xacts.
type PreparedTransaction struct {
	TransactionID uint32
	GID           string
	Prepared      time.Time
	Owner         string
	Database      string

	transaction            *dsess.DoltTransaction
	workingSet             *doltdb.WorkingSet
	workingSetName         string
	preparedWorkingSetName string
	transactionRootHash    hash.Hash
	baseWorkingSetHash     hash.Hash
	replication            *PreparedReplicationState
}

type PreparedReplicationState struct {
	Captures []PreparedReplicationCapture
	Advance  bool
}

type PreparedReplicationCapture struct {
	Action       byte
	Schema       string
	Table        string
	Fields       []PreparedReplicationField
	Rows         [][][]byte
	OldRows      [][][]byte
	RowsAffected uint64
}

type PreparedReplicationField struct {
	Name         string
	DataTypeOID  uint32
	TypeModifier int32
}

var preparedTransactions = struct {
	sync.RWMutex
	nextTransactionID uint32
	byGID             map[string]PreparedTransaction
	storageFS         filesys.Filesys
	storagePath       string
}{
	nextTransactionID: 1,
	byGID:             make(map[string]PreparedTransaction),
}

type persistentPreparedTransactionState struct {
	Version           int                             `json:"version"`
	NextTransactionID uint32                          `json:"next_transaction_id"`
	Transactions      []persistentPreparedTransaction `json:"transactions"`
}

type persistentPreparedTransaction struct {
	TransactionID          uint32                    `json:"transaction_id"`
	GID                    string                    `json:"gid"`
	Prepared               time.Time                 `json:"prepared"`
	Owner                  string                    `json:"owner"`
	Database               string                    `json:"database"`
	WorkingSetName         string                    `json:"working_set_name"`
	PreparedWorkingSetName string                    `json:"prepared_working_set_name"`
	TransactionRootHash    string                    `json:"transaction_root_hash"`
	BaseWorkingSetHash     string                    `json:"base_working_set_hash"`
	Replication            *PreparedReplicationState `json:"replication,omitempty"`
}

const preparedTransactionStateVersion = 1

// ConfigurePreparedTransactionStorage loads and persists prepared transaction metadata in the supplied filesystem.
func ConfigurePreparedTransactionStorage(fs filesys.Filesys, storagePath string) error {
	preparedTransactions.Lock()
	defer preparedTransactions.Unlock()
	preparedTransactions.storageFS = fs
	preparedTransactions.storagePath = storagePath
	preparedTransactions.nextTransactionID = 1
	preparedTransactions.byGID = make(map[string]PreparedTransaction)
	if fs == nil || storagePath == "" {
		return nil
	}
	return loadPreparedTransactionsLocked()
}

// PrepareTransaction records the active transaction under gid and rolls the current session back. The stored working
// set remains invisible until CommitPreparedTransaction applies it.
func PrepareTransaction(ctx *sql.Context, gid string, replication *PreparedReplicationState) error {
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
	if err = rejectPrepareWithTemporaryTables(ctx, sess, dbName); err != nil {
		return err
	}
	doltDB, err := doltDBForPreparedTransaction(ctx, sess, dbName)
	if err != nil {
		return err
	}
	transactionRootHash, baseWorkingSetHash, err := baseWorkingSetHashForTransaction(ctx, doltDB, doltTx, dbName, workingSet.Ref())
	if err != nil {
		return err
	}

	owner := ctx.Client().User
	if owner == "" {
		owner = sess.Username()
		if owner == "" {
			owner = "postgres"
		}
	}
	prepared := PreparedTransaction{
		GID:                    gid,
		Prepared:               time.Now(),
		Owner:                  owner,
		Database:               dbName,
		transaction:            doltTx,
		workingSet:             workingSet,
		workingSetName:         workingSet.Ref().GetPath(),
		preparedWorkingSetName: preparedWorkingSetName(gid),
		transactionRootHash:    transactionRootHash,
		baseWorkingSetHash:     baseWorkingSetHash,
		replication:            clonePreparedReplicationState(replication),
	}

	preparedTransactions.Lock()
	if _, ok = preparedTransactions.byGID[gid]; ok {
		preparedTransactions.Unlock()
		return errors.Errorf("prepared transaction with identifier %q already exists", gid)
	}
	if err = writePreparedWorkingSet(ctx, doltDB, prepared, workingSet); err != nil {
		preparedTransactions.Unlock()
		return err
	}
	prepared.TransactionID = preparedTransactions.nextTransactionID
	preparedTransactions.nextTransactionID++
	preparedTransactions.byGID[gid] = prepared
	if err = persistPreparedTransactionsLocked(); err != nil {
		delete(preparedTransactions.byGID, gid)
		preparedTransactions.Unlock()
		_ = deletePreparedWorkingSet(ctx, doltDB, prepared)
		return err
	}
	preparedTransactions.Unlock()

	if err = sess.Rollback(ctx, tx); err != nil {
		_ = RollbackPreparedTransaction(ctx, gid)
		return err
	}
	ctx.SetTransaction(nil)
	ctx.SetIgnoreAutoCommit(false)
	return nil
}

func rejectPrepareWithTemporaryTables(ctx *sql.Context, sess *dsess.DoltSession, dbName string) error {
	tempTables, err := sess.GetAllTemporaryTables(ctx, dbName)
	if err != nil {
		return err
	}
	if len(tempTables) == 0 {
		return nil
	}
	return pgerror.New(pgcode.FeatureNotSupported, "cannot PREPARE a transaction that has operated on temporary objects")
}

// CommitPreparedTransaction commits and removes the prepared transaction with gid.
func CommitPreparedTransaction(ctx *sql.Context, gid string) error {
	prepared, ok := takePreparedTransaction(gid)
	if !ok {
		return errors.Errorf("prepared transaction with identifier %q does not exist", gid)
	}
	if err := checkPreparedTransactionOwner(ctx, prepared); err != nil {
		restorePreparedTransaction(prepared)
		return err
	}

	sess := dsess.DSessFromSess(ctx.Session)
	var err error
	if _, ok, err := sess.LookupDbState(ctx, prepared.Database); err != nil {
		restorePreparedTransaction(prepared)
		return err
	} else if !ok {
		restorePreparedTransaction(prepared)
		return sql.ErrDatabaseNotFound.New(prepared.Database)
	}
	if prepared.transaction != nil && prepared.workingSet != nil {
		if err = sess.SetWorkingSet(ctx, prepared.Database, prepared.workingSet); err != nil {
			restorePreparedTransaction(prepared)
			return err
		}
		if err = sess.CommitWorkingSet(ctx, prepared.Database, prepared.transaction); err != nil {
			restorePreparedTransaction(prepared)
			return err
		}
	} else {
		if err = commitRecoveredPreparedTransaction(ctx, prepared); err != nil {
			restorePreparedTransaction(prepared)
			return err
		}
	}
	if err = forgetPreparedTransaction(prepared); err != nil {
		return err
	}
	if doltDB, dbErr := doltDBForPreparedTransaction(ctx, sess, prepared.Database); dbErr == nil {
		_ = deletePreparedWorkingSet(ctx, doltDB, prepared)
	}
	ctx.SetTransaction(nil)
	ctx.SetIgnoreAutoCommit(false)
	return nil
}

// RollbackPreparedTransaction removes the prepared transaction with gid without applying it.
func RollbackPreparedTransaction(ctx *sql.Context, gid string) error {
	preparedTransactions.Lock()
	prepared, ok := preparedTransactions.byGID[gid]
	if !ok {
		preparedTransactions.Unlock()
		return errors.Errorf("prepared transaction with identifier %q does not exist", gid)
	}
	if err := checkPreparedTransactionOwner(ctx, prepared); err != nil {
		preparedTransactions.Unlock()
		return err
	}
	delete(preparedTransactions.byGID, gid)
	err := persistPreparedTransactionsLocked()
	preparedTransactions.Unlock()
	if err != nil {
		restorePreparedTransaction(prepared)
		return err
	}
	if ctx != nil {
		sess := dsess.DSessFromSess(ctx.Session)
		if doltDB, dbErr := doltDBForPreparedTransaction(ctx, sess, prepared.Database); dbErr == nil {
			_ = deletePreparedWorkingSet(ctx, doltDB, prepared)
		}
	}
	return nil
}

func checkPreparedTransactionOwner(ctx *sql.Context, prepared PreparedTransaction) error {
	if ctx == nil {
		return nil
	}
	user := ctx.Client().User
	if user == "" {
		user = dsess.DSessFromSess(ctx.Session).Username()
		if user == "" {
			user = "postgres"
		}
	}
	if user == prepared.Owner {
		return nil
	}
	var isSuperUser bool
	auth.LockRead(func() {
		role := auth.GetRole(user)
		isSuperUser = role.IsValid() && role.IsSuperUser
	})
	if isSuperUser {
		return nil
	}
	return errors.Errorf("permission denied to finish prepared transaction %q", prepared.GID)
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

func GetPreparedReplication(gid string) (*PreparedReplicationState, bool) {
	preparedTransactions.RLock()
	defer preparedTransactions.RUnlock()
	prepared, ok := preparedTransactions.byGID[gid]
	if !ok || prepared.replication == nil {
		return nil, false
	}
	return clonePreparedReplicationState(prepared.replication), true
}

// ResetPreparedTransactionsForTests clears in-process prepared transaction state.
func ResetPreparedTransactionsForTests() {
	preparedTransactions.Lock()
	defer preparedTransactions.Unlock()
	preparedTransactions.nextTransactionID = 1
	preparedTransactions.byGID = make(map[string]PreparedTransaction)
	preparedTransactions.storageFS = nil
	preparedTransactions.storagePath = ""
}

func clonePreparedReplicationState(state *PreparedReplicationState) *PreparedReplicationState {
	if state == nil {
		return nil
	}
	ret := &PreparedReplicationState{
		Advance: state.Advance,
	}
	if len(state.Captures) > 0 {
		ret.Captures = make([]PreparedReplicationCapture, len(state.Captures))
		for i, capture := range state.Captures {
			ret.Captures[i] = clonePreparedReplicationCapture(capture)
		}
	}
	return ret
}

func clonePreparedReplicationCapture(capture PreparedReplicationCapture) PreparedReplicationCapture {
	ret := PreparedReplicationCapture{
		Action:       capture.Action,
		Schema:       capture.Schema,
		Table:        capture.Table,
		RowsAffected: capture.RowsAffected,
	}
	if len(capture.Fields) > 0 {
		ret.Fields = append([]PreparedReplicationField(nil), capture.Fields...)
	}
	if len(capture.Rows) > 0 {
		ret.Rows = make([][][]byte, len(capture.Rows))
		for i, row := range capture.Rows {
			ret.Rows[i] = make([][]byte, len(row))
			for j, value := range row {
				ret.Rows[i][j] = append([]byte(nil), value...)
			}
		}
	}
	if len(capture.OldRows) > 0 {
		ret.OldRows = make([][][]byte, len(capture.OldRows))
		for i, row := range capture.OldRows {
			ret.OldRows[i] = make([][]byte, len(row))
			for j, value := range row {
				ret.OldRows[i][j] = append([]byte(nil), value...)
			}
		}
	}
	return ret
}

func restorePreparedTransaction(prepared PreparedTransaction) {
	preparedTransactions.Lock()
	defer preparedTransactions.Unlock()
	preparedTransactions.byGID[prepared.GID] = prepared
}

func forgetPreparedTransaction(prepared PreparedTransaction) error {
	preparedTransactions.Lock()
	defer preparedTransactions.Unlock()
	delete(preparedTransactions.byGID, prepared.GID)
	return persistPreparedTransactionsLocked()
}

func doltDBForPreparedTransaction(ctx *sql.Context, sess *dsess.DoltSession, dbName string) (*doltdb.DoltDB, error) {
	sqlDB, ok, err := sess.Provider().SessionDatabase(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(dbName)
	}
	doltDB := sqlDB.DbData().Ddb
	if doltDB == nil {
		return nil, errors.Errorf("database %s is not backed by a Dolt database", dbName)
	}
	return doltDB, nil
}

func baseWorkingSetHashForTransaction(
	ctx *sql.Context,
	doltDB *doltdb.DoltDB,
	tx *dsess.DoltTransaction,
	dbName string,
	workingSetRef ref.WorkingSetRef,
) (hash.Hash, hash.Hash, error) {
	initialRoot, ok := tx.GetInitialRoot(dbName)
	if !ok {
		return hash.Hash{}, hash.Hash{}, errors.Errorf("database %s is unknown to the transaction", dbName)
	}
	baseWorkingSet, err := doltDB.ResolveWorkingSetAtRoot(ctx, workingSetRef, initialRoot)
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}
	baseWorkingSetHash, err := baseWorkingSet.HashOf()
	if err != nil {
		return hash.Hash{}, hash.Hash{}, err
	}
	return initialRoot, baseWorkingSetHash, nil
}

func preparedWorkingSetName(gid string) string {
	return path.Join("preparedTransactions", hex.EncodeToString([]byte(gid)))
}

func writePreparedWorkingSet(ctx *sql.Context, doltDB *doltdb.DoltDB, prepared PreparedTransaction, workingSet *doltdb.WorkingSet) error {
	preparedRef := ref.NewWorkingSetRef(prepared.preparedWorkingSetName)
	var currentHash hash.Hash
	if existing, err := doltDB.ResolveWorkingSet(ctx, preparedRef); err == nil {
		var hashErr error
		currentHash, hashErr = existing.HashOf()
		if hashErr != nil {
			return hashErr
		}
	} else if err != doltdb.ErrWorkingSetNotFound {
		return err
	}
	return doltDB.UpdateWorkingSet(ctx, preparedRef, workingSet, currentHash, doltdb.TodoWorkingSetMeta(), nil)
}

func deletePreparedWorkingSet(ctx *sql.Context, doltDB *doltdb.DoltDB, prepared PreparedTransaction) error {
	if prepared.preparedWorkingSetName == "" {
		return nil
	}
	return doltDB.DeleteWorkingSet(ctx, ref.NewWorkingSetRef(prepared.preparedWorkingSetName))
}

func commitRecoveredPreparedTransaction(ctx *sql.Context, prepared PreparedTransaction) error {
	sess := dsess.DSessFromSess(ctx.Session)
	doltDB, err := doltDBForPreparedTransaction(ctx, sess, prepared.Database)
	if err != nil {
		return err
	}
	state, ok, err := sess.LookupDbState(ctx, prepared.Database)
	if err != nil {
		return err
	}
	if !ok {
		return sql.ErrDatabaseNotFound.New(prepared.Database)
	}
	currentWorkingSet := state.WorkingSet()
	if currentWorkingSet == nil {
		return errors.Errorf("cannot commit prepared transaction %q on detached head", prepared.GID)
	}
	storedCurrentWorkingSet, err := doltDB.ResolveWorkingSet(ctx, currentWorkingSet.Ref())
	if err != nil {
		return err
	}
	existingHash, err := storedCurrentWorkingSet.HashOf()
	if err != nil {
		return err
	}
	startState, err := doltDB.ResolveWorkingSetAtRoot(ctx, currentWorkingSet.Ref(), prepared.transactionRootHash)
	if err != nil {
		return err
	}
	sidecar, err := doltDB.ResolveWorkingSet(ctx, ref.NewWorkingSetRef(prepared.preparedWorkingSetName))
	if err != nil {
		return err
	}
	workingSet := doltdb.EmptyWorkingSet(ref.NewWorkingSetRef(prepared.workingSetName)).
		WithWorkingRoot(sidecar.WorkingRoot()).
		WithStagedRoot(sidecar.StagedRoot())

	if !workingAndStagedEqual(storedCurrentWorkingSet, startState) {
		workingSet, err = mergePreparedWorkingSets(ctx, prepared.Database, state, startState, storedCurrentWorkingSet, workingSet)
		if err != nil {
			return err
		}
	}
	if err = doltDB.UpdateWorkingSet(ctx, workingSet.Ref(), workingSet, existingHash, doltdb.TodoWorkingSetMeta(), nil); err != nil {
		return err
	}
	return sess.SetWorkingSet(ctx, prepared.Database, workingSet)
}

func mergePreparedWorkingSets(
	ctx *sql.Context,
	dbName string,
	state dsess.SessionState,
	startState *doltdb.WorkingSet,
	existingWorkingSet *doltdb.WorkingSet,
	workingSet *doltdb.WorkingSet,
) (*doltdb.WorkingSet, error) {
	tableResolver, err := dsess.GetTableResolver(ctx, dbName)
	if err != nil {
		return nil, err
	}
	if !rootsEqual(existingWorkingSet.WorkingRoot(), workingSet.WorkingRoot()) {
		result, err := merge.MergeRoots(ctx, tableResolver, existingWorkingSet.WorkingRoot(), workingSet.WorkingRoot(), startState.WorkingRoot(), workingSet, startState, state.EditOpts(), merge.MergeOpts{})
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithWorkingRoot(result.Root)
	}
	if !rootsEqual(existingWorkingSet.StagedRoot(), workingSet.StagedRoot()) {
		result, err := merge.MergeRoots(ctx, tableResolver, existingWorkingSet.StagedRoot(), workingSet.StagedRoot(), startState.StagedRoot(), workingSet, startState, state.EditOpts(), merge.MergeOpts{})
		if err != nil {
			return nil, err
		}
		workingSet = workingSet.WithStagedRoot(result.Root)
	}
	return workingSet, nil
}

func workingAndStagedEqual(left *doltdb.WorkingSet, right *doltdb.WorkingSet) bool {
	return rootsEqual(left.WorkingRoot(), right.WorkingRoot()) && rootsEqual(left.StagedRoot(), right.StagedRoot())
}

func rootsEqual(left doltdb.RootValue, right doltdb.RootValue) bool {
	leftHash, leftErr := left.HashOf()
	rightHash, rightErr := right.HashOf()
	return leftErr == nil && rightErr == nil && leftHash == rightHash
}

func loadPreparedTransactionsLocked() error {
	exists, isDir := preparedTransactions.storageFS.Exists(preparedTransactions.storagePath)
	if !exists {
		return nil
	}
	if isDir {
		return errors.Errorf("prepared transaction state path %q is a directory", preparedTransactions.storagePath)
	}
	data, err := preparedTransactions.storageFS.ReadFile(preparedTransactions.storagePath)
	if err != nil {
		return err
	}
	var state persistentPreparedTransactionState
	if err = json.Unmarshal(data, &state); err != nil {
		return err
	}
	maxID := uint32(0)
	for _, stored := range state.Transactions {
		transactionRootHash, ok := hash.MaybeParse(stored.TransactionRootHash)
		if !ok {
			return errors.Errorf("invalid transaction root hash for prepared transaction %q", stored.GID)
		}
		baseHash, ok := hash.MaybeParse(stored.BaseWorkingSetHash)
		if !ok {
			return errors.Errorf("invalid base working set hash for prepared transaction %q", stored.GID)
		}
		prepared := PreparedTransaction{
			TransactionID:          stored.TransactionID,
			GID:                    stored.GID,
			Prepared:               stored.Prepared,
			Owner:                  stored.Owner,
			Database:               stored.Database,
			workingSetName:         stored.WorkingSetName,
			preparedWorkingSetName: stored.PreparedWorkingSetName,
			transactionRootHash:    transactionRootHash,
			baseWorkingSetHash:     baseHash,
			replication:            clonePreparedReplicationState(stored.Replication),
		}
		preparedTransactions.byGID[prepared.GID] = prepared
		if prepared.TransactionID > maxID {
			maxID = prepared.TransactionID
		}
	}
	preparedTransactions.nextTransactionID = state.NextTransactionID
	if preparedTransactions.nextTransactionID <= maxID {
		preparedTransactions.nextTransactionID = maxID + 1
	}
	if preparedTransactions.nextTransactionID == 0 {
		preparedTransactions.nextTransactionID = 1
	}
	return nil
}

func persistPreparedTransactionsLocked() error {
	if preparedTransactions.storageFS == nil || preparedTransactions.storagePath == "" {
		return nil
	}
	if len(preparedTransactions.byGID) == 0 {
		if exists, isDir := preparedTransactions.storageFS.Exists(preparedTransactions.storagePath); exists && !isDir {
			return preparedTransactions.storageFS.DeleteFile(preparedTransactions.storagePath)
		}
		return nil
	}
	dir := filepath.Dir(preparedTransactions.storagePath)
	if dir != "." && dir != "" {
		if err := preparedTransactions.storageFS.MkDirs(dir); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(toPersistentPreparedTransactionStateLocked(), "", "  ")
	if err != nil {
		return err
	}
	return preparedTransactions.storageFS.WriteFile(preparedTransactions.storagePath, data, os.ModePerm)
}

func toPersistentPreparedTransactionStateLocked() persistentPreparedTransactionState {
	state := persistentPreparedTransactionState{
		Version:           preparedTransactionStateVersion,
		NextTransactionID: preparedTransactions.nextTransactionID,
		Transactions:      make([]persistentPreparedTransaction, 0, len(preparedTransactions.byGID)),
	}
	for _, prepared := range preparedTransactions.byGID {
		state.Transactions = append(state.Transactions, persistentPreparedTransaction{
			TransactionID:          prepared.TransactionID,
			GID:                    prepared.GID,
			Prepared:               prepared.Prepared,
			Owner:                  prepared.Owner,
			Database:               prepared.Database,
			WorkingSetName:         prepared.workingSetName,
			PreparedWorkingSetName: prepared.preparedWorkingSetName,
			TransactionRootHash:    prepared.transactionRootHash.String(),
			BaseWorkingSetHash:     prepared.baseWorkingSetHash.String(),
			Replication:            clonePreparedReplicationState(prepared.replication),
		})
	}
	sort.Slice(state.Transactions, func(i, j int) bool {
		return state.Transactions[i].GID < state.Transactions[j].GID
	})
	return state
}
