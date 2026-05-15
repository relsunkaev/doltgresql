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

package pgcatalog

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgLocksName is a constant to the pg_locks name.
const PgLocksName = "pg_locks"

// InitPgLocks handles registration of the pg_locks handler.
func InitPgLocks() {
	tables.AddHandler(PgCatalogName, PgLocksName, PgLocksHandler{})
}

// PgLocksHandler is the handler for the pg_locks table.
type PgLocksHandler struct{}

var _ tables.Handler = PgLocksHandler{}

// Name implements the interface tables.Handler.
func (p PgLocksHandler) Name() string {
	return PgLocksName
}

// RowIter implements the interface tables.Handler.
func (p PgLocksHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	rowLocks := pgnodes.SnapshotRowLocks()
	relationLocks := pgnodes.SnapshotRelationLocks()
	rows := make([]sql.Row, 0, len(rowLocks)+len(relationLocks))
	for _, entry := range relationLocks {
		rows = append(rows, pgLocksRelationLockRow(entry))
	}
	databaseID := id.NewDatabase(ctx.GetCurrentDatabase()).AsId()
	for _, entry := range rowLocks {
		locktype := "tuple"
		mode := "RowExclusiveLock"
		if entry.Kind == pgnodes.RowLockKindTable {
			locktype = "relation"
			mode = "ExclusiveLock"
		}
		relationID := id.Cache().ToInternal(entry.RelationOID)
		if relationID == id.Null {
			relationID = id.NewOID(entry.RelationOID).AsId()
		}
		var waitStart any
		if !entry.Granted {
			waitStart = entry.WaitStart
		}
		rows = append(rows, sql.Row{
			locktype,               // locktype
			databaseID,             // database
			relationID,             // relation
			nil,                    // page
			nil,                    // tuple
			nil,                    // virtualxid
			nil,                    // transactionid
			nil,                    // classid
			nil,                    // objid
			nil,                    // objsubid
			nil,                    // virtualtransaction
			int32(entry.SessionID), // pid
			mode,                   // mode
			entry.Granted,          // granted
			false,                  // fastpath
			waitStart,              // waitstart
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

func pgLocksRelationLockRow(entry pgnodes.RelationLockEntry) sql.Row {
	return sql.Row{
		"relation", // locktype
		id.NewDatabase(entry.Target.Database).AsId(),               // database
		id.NewTable(entry.Target.Schema, entry.Target.Name).AsId(), // relation
		nil,                                 // page
		nil,                                 // tuple
		nil,                                 // virtualxid
		nil,                                 // transactionid
		nil,                                 // classid
		nil,                                 // objid
		nil,                                 // objsubid
		nil,                                 // virtualtransaction
		int32(entry.SessionID),              // pid
		pgLocksRelationModeName(entry.Mode), // mode
		true,                                // granted
		false,                               // fastpath
		nil,                                 // waitstart
	}
}

func pgLocksRelationModeName(mode pgnodes.RelationLockMode) string {
	switch mode {
	case pgnodes.RelationLockAccessShare:
		return "AccessShareLock"
	case pgnodes.RelationLockRowShare:
		return "RowShareLock"
	case pgnodes.RelationLockRowExclusive:
		return "RowExclusiveLock"
	case pgnodes.RelationLockShareUpdateExclusive:
		return "ShareUpdateExclusiveLock"
	case pgnodes.RelationLockShare:
		return "ShareLock"
	case pgnodes.RelationLockShareRowExclusive:
		return "ShareRowExclusiveLock"
	case pgnodes.RelationLockExclusive:
		return "ExclusiveLock"
	default:
		return "AccessExclusiveLock"
	}
}

// Schema implements the interface tables.Handler.
func (p PgLocksHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgLocksSchema,
		PkOrdinals: nil,
	}
}

// pgLocksSchema is the schema for pg_locks.
var pgLocksSchema = sql.Schema{
	{Name: "locktype", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "database", Type: pgtypes.Oid, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "relation", Type: pgtypes.Oid, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "page", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "tuple", Type: pgtypes.Int16, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "virtualxid", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "transactionid", Type: pgtypes.Xid, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "classid", Type: pgtypes.Oid, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "objid", Type: pgtypes.Oid, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "objsubid", Type: pgtypes.Int16, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "virtualtransaction", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "pid", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "mode", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "granted", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "fastpath", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgLocksName},
	{Name: "waitstart", Type: pgtypes.TimestampTZ, Default: nil, Nullable: true, Source: PgLocksName},
}
