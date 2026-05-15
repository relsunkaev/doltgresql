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

// PgAuthMembersName is a constant to the pg_auth_members name.
const PgAuthMembersName = "pg_auth_members"

// InitPgAuthMembers handles registration of the pg_auth_members handler.
func InitPgAuthMembers() {
	tables.AddHandler(PgCatalogName, PgAuthMembersName, PgAuthMembersHandler{})
}

// PgAuthMembersHandler is the handler for the pg_auth_members table.
type PgAuthMembersHandler struct{}

var _ tables.Handler = PgAuthMembersHandler{}

// Name implements the interface tables.Handler.
func (p PgAuthMembersHandler) Name() string {
	return PgAuthMembersName
}

// RowIter implements the interface tables.Handler.
func (p PgAuthMembersHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	memberships := auth.GetAllRoleMemberships()
	rows := make([]sql.Row, 0, len(memberships))
	for _, membership := range memberships {
		grantor := membership.Grantor
		if !grantor.IsValid() {
			grantor = membership.Member
		}
		rows = append(rows, sql.Row{
			pgAuthMembersOID(membership),
			id.NewId(id.Section_User, membership.Group.Name),
			id.NewId(id.Section_User, membership.Member.Name),
			pgAuthMembersGrantor(membership, grantor),
			membership.WithAdminOption,
			membership.WithInheritOption,
			membership.WithSetOption,
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgAuthMembersHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAuthMembersSchema,
		PkOrdinals: nil,
	}
}

// pgAuthMembersSchema is the schema for pg_auth_members.
var pgAuthMembersSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAuthMembersName},
	{Name: "roleid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAuthMembersName},
	{Name: "member", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAuthMembersName},
	{Name: "grantor", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAuthMembersName},
	{Name: "admin_option", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthMembersName},
	{Name: "inherit_option", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthMembersName},
	{Name: "set_option", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAuthMembersName},
}

var pgAuthMembersBuiltinOIDs = map[[2]string]uint32{
	{"pg_read_all_settings", "pg_monitor"}: 10226,
	{"pg_read_all_stats", "pg_monitor"}:    10227,
	{"pg_stat_scan_tables", "pg_monitor"}:  10228,
}

func pgAuthMembersOID(membership auth.RoleMembershipInfo) id.Id {
	if oid, ok := pgAuthMembersBuiltinOIDs[[2]string{membership.Group.Name, membership.Member.Name}]; ok {
		return id.NewOID(oid).AsId()
	}
	return id.NewId(id.Section_User, PgAuthMembersName, membership.Group.Name, membership.Member.Name)
}

func pgAuthMembersGrantor(membership auth.RoleMembershipInfo, grantor auth.Role) id.Id {
	if _, ok := pgAuthMembersBuiltinOIDs[[2]string{membership.Group.Name, membership.Member.Name}]; ok {
		return id.NewOID(10).AsId()
	}
	return id.NewId(id.Section_User, grantor.Name)
}
