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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
)

func relationSchemaDatabase(ctx *sql.Context, db sql.Database) (sql.Database, string, error) {
	schemaName, err := core.GetSchemaName(ctx, db, "")
	if err != nil {
		return nil, "", err
	}
	if schemaName == "" {
		return db, "", nil
	}
	if schemaDb, ok := db.(sql.DatabaseSchema); ok && schemaDb.SchemaName() != "" {
		return db, schemaDb.SchemaName(), nil
	}
	multiSchemaDb, ok := db.(sql.SchemaDatabase)
	if !ok || !multiSchemaDb.SupportsDatabaseSchemas() {
		return db, schemaName, nil
	}
	resolvedSchemaDb, ok, err := multiSchemaDb.GetSchema(ctx, schemaName)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", sql.ErrDatabaseSchemaNotFound.New(schemaName)
	}
	return resolvedSchemaDb, schemaName, nil
}
