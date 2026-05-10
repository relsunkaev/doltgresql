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

package tables

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// Database is a wrapper around Dolt's database object, allowing for functionality specific to Doltgres (such as system
// tables).
type Database struct {
	sqle.Database
}

var _ sql.DatabaseSchema = Database{}

// GetTableInsensitive implements the interface sql.DatabaseSchema.
func (d Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	// Even though this is named "GetTableInsensitive", due to differences in Postgres and MySQL, this should perform an
	// exact search.
	if tableMap, ok := handlers[d.Database.Schema()]; ok {
		if handler, ok := tableMap[tblName]; ok {
			return NewVirtualTable(handler, d.Database), true, nil
		}
	}
	if table, ok, err := d.Database.GetTableInsensitive(ctx, tblName); err != nil || ok {
		return table, ok, err
	}
	sequenceID := id.NewSequence(d.Database.Schema(), tblName)
	collection, err := core.GetSequencesCollectionFromContext(ctx, d.Database.Name())
	if err != nil {
		return nil, false, err
	}
	if collection.HasSequence(ctx, sequenceID) {
		return newSequenceTable(d, sequenceID), true, nil
	}
	return nil, false, nil
}

// GetTableNames implements the interface sql.DatabaseSchema.
func (d Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	tableNames, err := d.Database.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	tableNameSet := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		tableNameSet[tableName] = struct{}{}
	}
	for handlerName := range handlers[d.Database.Schema()] {
		tableNameSet[handlerName] = struct{}{}
	}
	return utils.GetMapKeysSorted(tableNameSet), nil
}

// Name implements the interface sql.DatabaseSchema.
func (d Database) Name() string {
	return d.Database.Name()
}

func (d Database) SchemaName() string {
	return d.Database.SchemaName()
}
