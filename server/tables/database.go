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
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// Database is a wrapper around Dolt's database object, allowing for functionality specific to Doltgres (such as system
// tables).
type Database struct {
	sqle.Database
	nameOverride string
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

	if schemaName := d.Database.Schema(); schemaName != "" && !isTemporarySchemaName(schemaName) {
		session := dsess.DSessFromSess(ctx.Session)
		if table, ok := session.GetTemporaryTable(ctx, d.Database.Name(), tblName); ok {
			session.DropTemporaryTable(ctx, d.Database.Name(), tblName)
			defer session.AddTemporaryTable(ctx, d.Database.Name(), table)
		}
	}

	if table, ok, err := d.Database.GetTableInsensitive(ctx, tblName); err != nil {
		if !sql.ErrTableNotFound.Is(err) {
			return table, ok, err
		}
	} else if ok {
		return table, ok, err
	}
	collection, err := core.GetSequencesCollectionFromContext(ctx, d.Database.Name())
	if err != nil {
		return nil, false, err
	}

	if schemaName := d.Database.Schema(); schemaName != "" {
		sequenceID := id.NewSequence(schemaName, tblName)
		if collection.HasSequence(ctx, sequenceID) {
			return newSequenceTable(d, sequenceID), true, nil
		}
		return nil, false, nil
	}

	searchPath, err := core.SearchPath(ctx)
	if err != nil {
		return nil, false, err
	}
	for _, schemaName := range searchPath {
		sequenceID := id.NewSequence(schemaName, tblName)
		if collection.HasSequence(ctx, sequenceID) {
			schema, ok, err := d.Database.GetSchema(ctx, schemaName)
			if err != nil {
				return nil, false, err
			}
			if ok {
				return newSequenceTable(schema, sequenceID), true, nil
			}
			return newSequenceTable(d, sequenceID), true, nil
		}
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
	lowerTableNameSet := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		tableNameSet[tableName] = struct{}{}
		lowerTableNameSet[strings.ToLower(tableName)] = struct{}{}
	}
	if core.IsContextValid(ctx) {
		_, root, err := core.GetRootFromContext(ctx)
		if err == nil {
			if rootTableNames, err := root.GetTableNames(ctx, d.Database.Schema(), false); err == nil {
				for _, tableName := range rootTableNames {
					if _, ok := tableNameSet[tableName]; ok {
						continue
					}
					if _, ok := lowerTableNameSet[strings.ToLower(tableName)]; ok {
						tableNameSet[tableName] = struct{}{}
					}
				}
			}
		}
	}
	for handlerName := range handlers[d.Database.Schema()] {
		tableNameSet[handlerName] = struct{}{}
	}
	return utils.GetMapKeysSorted(tableNameSet), nil
}

// Name implements the interface sql.DatabaseSchema.
func (d Database) Name() string {
	if d.nameOverride != "" {
		return d.nameOverride
	}
	return d.Database.Name()
}

func (d Database) SchemaName() string {
	return d.Database.SchemaName()
}

func isTemporarySchemaName(schemaName string) bool {
	schemaName = strings.ToLower(schemaName)
	return schemaName == "pg_temp" || strings.HasPrefix(schemaName, "pg_temp_")
}
