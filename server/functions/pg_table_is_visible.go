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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgTableIsVisible registers the functions to the catalog.
func initPgTableIsVisible() {
	framework.RegisterFunction(pg_table_is_visible_oid)
}

// pg_table_is_visible_oid represents the PostgreSQL system schema visibility inquiry function.
var pg_table_is_visible_oid = framework.Function1{
	Name:               "pg_table_is_visible",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Oid},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		oidVal := val.(id.Id)
		paths, err := core.SearchPath(ctx)
		if err != nil {
			return false, err
		}

		if oidVal.Section() == id.Section_Table || oidVal.Section() == id.Section_View || oidVal.Section() == id.Section_Sequence {
			return relationIsVisibleInSearchPath(ctx, oidVal, paths)
		}

		lookupPaths := make(map[string]bool)
		for _, path := range paths {
			lookupPaths[path] = true
		}

		var isVisible bool
		err = RunCallback(ctx, oidVal, Callbacks{
			Table: func(ctx *sql.Context, sch ItemSchema, table ItemTable) (cont bool, err error) {
				_, isVisible = lookupPaths[sch.Item.SchemaName()]
				return false, nil
			},
			View: func(ctx *sql.Context, sch ItemSchema, view ItemView) (cont bool, err error) {
				_, isVisible = lookupPaths[sch.Item.SchemaName()]
				return false, nil
			},
			Index: func(ctx *sql.Context, sch ItemSchema, table ItemTable, index ItemIndex) (cont bool, err error) {
				_, isVisible = lookupPaths[sch.Item.SchemaName()]
				return false, nil
			},
			Sequence: func(ctx *sql.Context, sch ItemSchema, sequence ItemSequence) (cont bool, err error) {
				_, isVisible = lookupPaths[sch.Item.SchemaName()]
				return false, nil
			},
			// TODO: This works for all types of relations, including views, materialized views, indexes, sequences and foreign tables.
		})
		if err != nil {
			return false, err
		}
		return isVisible, nil
	},
}

func relationIsVisibleInSearchPath(ctx *sql.Context, oidVal id.Id, paths []string) (bool, error) {
	targetSchema := oidVal.Segment(0)
	relationName := oidVal.Segment(1)
	schemas := make(map[string]sql.DatabaseSchema)
	err := IterateCurrentDatabase(ctx, Callbacks{
		Schema: func(ctx *sql.Context, schema ItemSchema) (cont bool, err error) {
			schemas[schema.Item.SchemaName()] = schema.Item
			return true, nil
		},
	})
	if err != nil {
		return false, err
	}
	for _, path := range paths {
		schema, ok := schemas[path]
		if !ok {
			continue
		}
		if _, ok, err := schema.GetTableInsensitive(ctx, relationName); err != nil {
			return false, err
		} else if ok {
			return path == targetSchema, nil
		}
	}
	return false, nil
}
