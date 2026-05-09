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

package node

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/triggers"
)

type triggerTransitionTable struct {
	name   string
	schema sql.Schema
	rows   []sql.Row
}

var _ sql.Table = (*triggerTransitionTable)(nil)
var _ sql.TemporaryTable = (*triggerTransitionTable)(nil)

func newTriggerTransitionTable(name string, sch sql.Schema, rows []sql.Row) *triggerTransitionTable {
	transitionSchema := make(sql.Schema, len(sch))
	for i, col := range sch {
		colCopy := col.Copy()
		colCopy.Source = name
		colCopy.PrimaryKey = false
		colCopy.AutoIncrement = false
		transitionSchema[i] = colCopy
	}
	transitionRows := make([]sql.Row, len(rows))
	for i, row := range rows {
		transitionRows[i] = cloneRow(row)
	}
	return &triggerTransitionTable{
		name:   name,
		schema: transitionSchema,
		rows:   transitionRows,
	}
}

func (t *triggerTransitionTable) Name() string {
	return t.name
}

func (t *triggerTransitionTable) String() string {
	return t.name
}

func (t *triggerTransitionTable) Schema(*sql.Context) sql.Schema {
	return t.schema
}

func (t *triggerTransitionTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t *triggerTransitionTable) IsTemporary() bool {
	return true
}

func (t *triggerTransitionTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return sql.PartitionsToPartitionIter(triggerTransitionPartition{}), nil
}

func (t *triggerTransitionTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	rows := make([]sql.Row, len(t.rows))
	for i, row := range t.rows {
		rows[i] = cloneRow(row)
	}
	return sql.RowsToRowIter(rows...), nil
}

type triggerTransitionPartition struct{}

func (triggerTransitionPartition) Key() []byte {
	return []byte("transition")
}

func installTransitionTables(
	ctx *sql.Context,
	trigger triggers.Trigger,
	sch sql.Schema,
	oldRows []sql.Row,
	newRows []sql.Row,
) (func() error, error) {
	session := dsess.DSessFromSess(ctx.Session)
	dbName := ctx.GetCurrentDatabase()
	restorers := make([]transitionTableRestorer, 0, 2)
	if len(trigger.OldTransitionName) > 0 {
		restorers = append(restorers, installTransitionTable(ctx, session, dbName, trigger.OldTransitionName, sch, oldRows))
	}
	if len(trigger.NewTransitionName) > 0 {
		restorers = append(restorers, installTransitionTable(ctx, session, dbName, trigger.NewTransitionName, sch, newRows))
	}
	return func() error {
		for i := len(restorers) - 1; i >= 0; i-- {
			restorers[i].restore(ctx, session, dbName)
		}
		return nil
	}, nil
}

type transitionTableRestorer struct {
	name     string
	shadowed sql.Table
	existed  bool
}

func installTransitionTable(
	ctx *sql.Context,
	session *dsess.DoltSession,
	dbName string,
	name string,
	sch sql.Schema,
	rows []sql.Row,
) transitionTableRestorer {
	shadowed, existed := session.GetTemporaryTable(ctx, dbName, name)
	session.AddTemporaryTable(ctx, dbName, newTriggerTransitionTable(name, sch, rows))
	return transitionTableRestorer{
		name:     name,
		shadowed: shadowed,
		existed:  existed,
	}
}

func (r transitionTableRestorer) restore(ctx *sql.Context, session *dsess.DoltSession, dbName string) {
	session.DropTemporaryTable(ctx, dbName, r.name)
	if r.existed {
		session.AddTemporaryTable(ctx, dbName, r.shadowed)
	}
}
