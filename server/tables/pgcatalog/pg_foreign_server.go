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
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgForeignServerName is a constant to the pg_foreign_server name.
const PgForeignServerName = "pg_foreign_server"

// InitPgForeignServer handles registration of the pg_foreign_server handler.
func InitPgForeignServer() {
	tables.AddHandler(PgCatalogName, PgForeignServerName, PgForeignServerHandler{})
}

// PgForeignServerHandler is the handler for the pg_foreign_server table.
type PgForeignServerHandler struct{}

var _ tables.Handler = PgForeignServerHandler{}

// Name implements the interface tables.Handler.
func (p PgForeignServerHandler) Name() string {
	return PgForeignServerName
}

// RowIter implements the interface tables.Handler.
func (p PgForeignServerHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	auth.LockRead(func() {
		for _, server := range auth.GetAllForeignServers() {
			owner := catalogOwnerOID()
			if role := auth.GetRole(server.Owner); role.IsValid() {
				owner = id.NewId(id.Section_User, server.Owner)
			}
			rows = append(rows, sql.Row{
				id.NewId(id.Section_ForeignServer, server.Name), // oid
				server.Name, // srvname
				owner,       // srvowner
				id.NewId(id.Section_ForeignDataWrapper, server.Wrapper), // srvfdw
				nullableText(server.Type),                               // srvtype
				nullableText(server.Version),                            // srvversion
				nil,                                                     // srvacl
				server.Options,                                          // srvoptions
				id.NewTable(PgCatalogName, PgForeignServerName).AsId(),
			})
		}
	})
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgForeignServerHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgForeignServerSchema,
		PkOrdinals: nil,
	}
}

// pgForeignServerSchema is the schema for pg_foreign_server.
var pgForeignServerSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignServerName},
	{Name: "srvname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgForeignServerName},
	{Name: "srvowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignServerName},
	{Name: "srvfdw", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignServerName},
	{Name: "srvtype", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgForeignServerName},         // TODO: collation C
	{Name: "srvversion", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgForeignServerName},      // TODO: collation C
	{Name: "srvacl", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgForeignServerName},     // TODO: aclitem[] type
	{Name: "srvoptions", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgForeignServerName}, // TODO: collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignServerName, Hidden: true},
}

func nullableText(text string) any {
	if text == "" {
		return nil
	}
	return text
}
