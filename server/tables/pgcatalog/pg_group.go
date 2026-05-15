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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgGroupName is a constant to the pg_group name.
const PgGroupName = "pg_group"

// InitPgGroup handles registration of the pg_group handler.
func InitPgGroup() {
	tables.AddHandler(PgCatalogName, PgGroupName, PgGroupHandler{})
}

// PgGroupHandler is the handler for the pg_group table.
type PgGroupHandler struct{}

var _ tables.Handler = PgGroupHandler{}

// Name implements the interface tables.Handler.
func (p PgGroupHandler) Name() string {
	return PgGroupName
}

// RowIter implements the interface tables.Handler.
func (p PgGroupHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	membersByGroup := make(map[auth.RoleID][]auth.Role)
	for _, membership := range auth.GetAllRoleMemberships() {
		membersByGroup[membership.Group.ID()] = append(membersByGroup[membership.Group.ID()], membership.Member)
	}
	for groupID := range membersByGroup {
		sort.Slice(membersByGroup[groupID], func(i, j int) bool {
			return membersByGroup[groupID][i].Name < membersByGroup[groupID][j].Name
		})
	}

	roles := auth.GetAllRoles()
	rows := make([]sql.Row, 0, len(roles))
	for _, role := range roles {
		if role.CanLogin || role.Name == "public" {
			continue
		}
		members := membersByGroup[role.ID()]
		memberOIDs := make([]any, len(members))
		for i, member := range members {
			memberOIDs[i] = id.NewId(id.Section_User, member.Name)
		}
		rows = append(rows, sql.Row{
			role.Name,
			id.NewId(id.Section_User, role.Name),
			memberOIDs,
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgGroupHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgGroupSchema,
		PkOrdinals: nil,
	}
}

// pgGroupSchema is the schema for pg_group.
var pgGroupSchema = sql.Schema{
	{Name: "groname", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgGroupName},
	{Name: "grosysid", Type: pgtypes.Oid, Default: nil, Nullable: true, Source: PgGroupName},
	{Name: "grolist", Type: pgtypes.OidArray, Default: nil, Nullable: true, Source: PgGroupName},
}

// pgGroupRowIter is the sql.RowIter for the pg_group table.
type pgGroupRowIter struct {
}

var _ sql.RowIter = (*pgGroupRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgGroupRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (iter *pgGroupRowIter) Close(ctx *sql.Context) error {
	return nil
}
