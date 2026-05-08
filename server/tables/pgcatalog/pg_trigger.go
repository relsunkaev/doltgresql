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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgTriggerName is a constant to the pg_trigger name.
const PgTriggerName = "pg_trigger"

// InitPgTrigger handles registration of the pg_trigger handler.
func InitPgTrigger() {
	tables.AddHandler(PgCatalogName, PgTriggerName, PgTriggerHandler{})
}

// PgTriggerHandler is the handler for the pg_trigger table.
type PgTriggerHandler struct{}

var _ tables.Handler = PgTriggerHandler{}

// Name implements the interface tables.Handler.
func (p PgTriggerHandler) Name() string {
	return PgTriggerName
}

// RowIter implements the interface tables.Handler.
func (p PgTriggerHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	collection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}

	var rows []sql.Row
	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			tableID := id.NewTable(schema.Item.SchemaName(), table.Item.Name())
			tableTriggers := collection.GetTriggersForTable(ctx, tableID)
			sort.Slice(tableTriggers, func(i, j int) bool {
				return tableTriggers[i].ID.TriggerName() < tableTriggers[j].ID.TriggerName()
			})
			for _, trigger := range tableTriggers {
				rows = append(rows, pgTriggerToRow(table.Item.Schema(ctx), trigger))
			}
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgTriggerHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgTriggerSchema,
		PkOrdinals: nil,
	}
}

// pgTriggerSchema is the schema for pg_trigger.
var pgTriggerSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgparentid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgfoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgtype", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgenabled", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgisinternal", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgconstrrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgconstrindid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgconstraint", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgdeferrable", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tginitdeferred", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgnargs", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgattr", Type: pgtypes.Int16Array, Default: nil, Nullable: false, Source: PgTriggerName}, // TODO: type int2vector
	{Name: "tgargs", Type: pgtypes.Bytea, Default: nil, Nullable: false, Source: PgTriggerName},
	{Name: "tgqual", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgTriggerName}, // TODO: type pg_node_tree, collation C
	{Name: "tgoldtable", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgTriggerName},
	{Name: "tgnewtable", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgTriggerName},
}

func pgTriggerToRow(schema sql.Schema, trigger triggers.Trigger) sql.Row {
	return sql.Row{
		trigger.ID.AsId(), // oid
		id.NewTable(trigger.ID.SchemaName(), trigger.ID.TableName()).AsId(), // tgrelid
		id.Null,                  // tgparentid
		trigger.ID.TriggerName(), // tgname
		trigger.Function.AsId(),  // tgfoid
		pgTriggerType(trigger),   // tgtype
		"O",                      // tgenabled
		false,                    // tgisinternal
		id.Null,                  // tgconstrrelid
		id.Null,                  // tgconstrindid
		id.Null,                  // tgconstraint
		trigger.Deferrable != triggers.TriggerDeferrable_NotDeferrable,      // tgdeferrable
		trigger.Deferrable == triggers.TriggerDeferrable_DeferrableDeferred, // tginitdeferred
		int16(len(trigger.Arguments)),                                       // tgnargs
		pgTriggerAttrs(schema, trigger),                                     // tgattr
		pgTriggerArgs(trigger.Arguments),                                    // tgargs
		nil,                                                                 // tgqual
		emptyStringAsNil(trigger.OldTransitionName),                         // tgoldtable
		emptyStringAsNil(trigger.NewTransitionName),                         // tgnewtable
	}
}

func pgTriggerType(trigger triggers.Trigger) int16 {
	var triggerType int16
	if trigger.ForEachRow {
		triggerType |= 1
	}
	switch trigger.Timing {
	case triggers.TriggerTiming_Before:
		triggerType |= 2
	case triggers.TriggerTiming_InsteadOf:
		triggerType |= 64
	}
	for _, event := range trigger.Events {
		switch event.Type {
		case triggers.TriggerEventType_Insert:
			triggerType |= 4
		case triggers.TriggerEventType_Delete:
			triggerType |= 8
		case triggers.TriggerEventType_Update:
			triggerType |= 16
		case triggers.TriggerEventType_Truncate:
			triggerType |= 32
		}
	}
	return triggerType
}

func pgTriggerAttrs(schema sql.Schema, trigger triggers.Trigger) []any {
	columnIndexes := make(map[string]int16, len(schema))
	for i, column := range schema {
		columnIndexes[column.Name] = int16(i + 1)
	}
	var attrs []any
	for _, event := range trigger.Events {
		if event.Type != triggers.TriggerEventType_Update {
			continue
		}
		for _, columnName := range event.ColumnNames {
			if idx, ok := columnIndexes[columnName]; ok {
				attrs = append(attrs, idx)
			}
		}
	}
	return attrs
}

func pgTriggerArgs(args []string) []byte {
	if len(args) == 0 {
		return []byte{}
	}
	var encoded []byte
	for _, arg := range args {
		encoded = append(encoded, []byte(arg)...)
		encoded = append(encoded, 0)
	}
	return encoded
}

func emptyStringAsNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
