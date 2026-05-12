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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAuthidName is a constant to the pg_authid name.
const PgAuthidName = "pg_authid"

// InitPgAuthid handles registration of the pg_authid handler.
func InitPgAuthid() {
	tables.AddHandler(PgCatalogName, PgAuthidName, PgAuthidHandler{})
}

// PgAuthidHandler is the handler for the pg_authid table.
type PgAuthidHandler struct{}

var _ tables.Handler = PgAuthidHandler{}

// Name implements the interface tables.Handler.
func (p PgAuthidHandler) Name() string {
	return PgAuthidName
}

// RowIter implements the interface tables.Handler.
func (p PgAuthidHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	roles := auth.GetAllRoles()
	rows := make([]sql.Row, 0, len(roles))
	for _, role := range roles {
		rows = append(rows, sql.Row{
			id.NewId(id.Section_User, role.Name), // oid
			role.Name,                            // rolname
			role.IsSuperUser,                     // rolsuper
			role.InheritPrivileges,               // rolinherit
			role.CanCreateRoles,                  // rolcreaterole
			role.CanCreateDB,                     // rolcreatedb
			role.CanLogin,                        // rolcanlogin
			role.IsReplicationRole,               // rolreplication
			role.CanBypassRowLevelSecurity,       // rolbypassrls
			role.ConnectionLimit,                 // rolconnlimit
			rolePasswordText(role),               // rolpassword
			roleValidUntil(role),                 // rolvaliduntil
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

func rolePasswordText(role auth.Role) any {
	if role.Password == nil {
		return nil
	}
	return role.Password.AsPasswordString()
}

func rolePasswordMask(role auth.Role) any {
	if role.Password == nil {
		return nil
	}
	return "********"
}

func roleValidUntil(role auth.Role) any {
	if role.ValidUntil == nil {
		return nil
	}
	return *role.ValidUntil
}

// Schema implements the interface tables.Handler.
func (p PgAuthidHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAuthidSchema,
		PkOrdinals: nil,
	}
}

// pgAuthidSchema is the schema for pg_authid.
var pgAuthidSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolsuper", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolinherit", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolcreaterole", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolcreatedb", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolcanlogin", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolreplication", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolbypassrls", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolconnlimit", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgAuthidName},
	{Name: "rolpassword", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAuthidName}, // TODO: collation C
	{Name: "rolvaliduntil", Type: pgtypes.TimestampTZ, Default: nil, Nullable: true, Source: PgAuthidName},
}

// pgAuthidRowIter is the sql.RowIter for the pg_authid table.
type pgAuthidRowIter struct {
}

var _ sql.RowIter = (*pgAuthidRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgAuthidRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (iter *pgAuthidRowIter) Close(ctx *sql.Context) error {
	return nil
}
