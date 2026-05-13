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

package information_schema

import (
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	gmsinformation "github.com/dolthub/go-mysql-server/sql/information_schema"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
	"github.com/dolthub/doltgresql/server/functions"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// newTriggersTable creates an information_schema.triggers table with
// PostgreSQL-compatible rows for Doltgres triggers.
func newTriggersTable() *gmsinformation.InformationSchemaTable {
	return &gmsinformation.InformationSchemaTable{
		TableName:   gmsinformation.TriggersTableName,
		TableSchema: triggersSchema,
		Reader:      triggersRowIter,
	}
}

var triggersSchema = sql.Schema{
	{Name: "trigger_catalog", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "trigger_schema", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "trigger_name", Type: sql_identifier, Default: nil, Nullable: false, Source: gmsinformation.TriggersTableName},
	{Name: "event_manipulation", Type: character_data, Default: nil, Nullable: false, Source: gmsinformation.TriggersTableName},
	{Name: "event_object_catalog", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "event_object_schema", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "event_object_table", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "action_order", Type: cardinal_number, Default: nil, Nullable: false, Source: gmsinformation.TriggersTableName},
	{Name: "action_condition", Type: character_data, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "action_statement", Type: character_data, Default: nil, Nullable: false, Source: gmsinformation.TriggersTableName},
	{Name: "action_orientation", Type: character_data, Default: nil, Nullable: false, Source: gmsinformation.TriggersTableName},
	{Name: "action_timing", Type: character_data, Default: nil, Nullable: false, Source: gmsinformation.TriggersTableName},
	{Name: "action_reference_old_table", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "action_reference_new_table", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "action_reference_old_row", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "action_reference_new_row", Type: sql_identifier, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
	{Name: "created", Type: pgtypes.Timestamp, Default: nil, Nullable: true, Source: gmsinformation.TriggersTableName},
}

func triggersRowIter(ctx *sql.Context, cat sql.Catalog) (sql.RowIter, error) {
	collection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}

	var rows []sql.Row
	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			if !relationVisibleToCurrentUser(ctx, schema.Item.SchemaName(), table.Item.Name(), table.Item) {
				return true, nil
			}
			tableID := id.NewTable(schema.Item.SchemaName(), table.Item.Name())
			tableTriggers := collection.GetTriggersForTable(ctx, tableID)
			sort.Slice(tableTriggers, func(i, j int) bool {
				return tableTriggers[i].ID.TriggerName() < tableTriggers[j].ID.TriggerName()
			})
			actionOrder := int32(1)
			for _, trigger := range tableTriggers {
				for _, event := range trigger.Events {
					rows = append(rows, informationSchemaTriggerRow(schema, table, trigger, event, actionOrder))
					actionOrder++
				}
			}
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

func informationSchemaTriggerRow(
	schema functions.ItemSchema,
	table functions.ItemTable,
	trigger triggers.Trigger,
	event triggers.TriggerEvent,
	actionOrder int32,
) sql.Row {
	return sql.Row{
		schema.Item.Name(),                          // trigger_catalog
		schema.Item.SchemaName(),                    // trigger_schema
		trigger.ID.TriggerName(),                    // trigger_name
		triggerEventName(event.Type),                // event_manipulation
		schema.Item.Name(),                          // event_object_catalog
		schema.Item.SchemaName(),                    // event_object_schema
		table.Item.Name(),                           // event_object_table
		actionOrder,                                 // action_order
		nil,                                         // action_condition
		trigger.Definition,                          // action_statement
		triggerOrientation(trigger),                 // action_orientation
		triggerTimingName(trigger.Timing),           // action_timing
		emptyStringAsNil(trigger.OldTransitionName), // action_reference_old_table
		emptyStringAsNil(trigger.NewTransitionName), // action_reference_new_table
		"OLD", // action_reference_old_row
		"NEW", // action_reference_new_row
		nil,   // created
	}
}

func triggerEventName(eventType triggers.TriggerEventType) string {
	switch eventType {
	case triggers.TriggerEventType_Insert:
		return "INSERT"
	case triggers.TriggerEventType_Update:
		return "UPDATE"
	case triggers.TriggerEventType_Delete:
		return "DELETE"
	case triggers.TriggerEventType_Truncate:
		return "TRUNCATE"
	default:
		return ""
	}
}

func triggerTimingName(timing triggers.TriggerTiming) string {
	switch timing {
	case triggers.TriggerTiming_Before:
		return "BEFORE"
	case triggers.TriggerTiming_After:
		return "AFTER"
	case triggers.TriggerTiming_InsteadOf:
		return "INSTEAD OF"
	default:
		return ""
	}
}

func triggerOrientation(trigger triggers.Trigger) string {
	if trigger.ForEachRow {
		return "ROW"
	}
	return "STATEMENT"
}

func emptyStringAsNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
