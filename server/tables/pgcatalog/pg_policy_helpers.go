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

package pgcatalog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/rowsecurity"
)

type rowSecurityPolicyCatalogRow struct {
	schemaName string
	tableName  string
	tableOID   id.Id
	policy     rowsecurity.Policy
}

func rowSecurityPolicyCatalogRows(ctx *sql.Context) ([]rowSecurityPolicyCatalogRow, error) {
	var rows []rowSecurityPolicyCatalogRow
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			schemaName := schema.Item.SchemaName()
			tableName := table.Item.Name()
			state, ok := rowsecurity.Get(ctx.GetCurrentDatabase(), schemaName, tableName)
			if !ok || len(state.Policies) == 0 {
				return true, nil
			}
			for _, policy := range state.Policies {
				rows = append(rows, rowSecurityPolicyCatalogRow{
					schemaName: schemaName,
					tableName:  tableName,
					tableOID:   table.OID.AsId(),
					policy:     policy,
				})
			}
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].schemaName != rows[j].schemaName {
			return rows[i].schemaName < rows[j].schemaName
		}
		if rows[i].tableName != rows[j].tableName {
			return rows[i].tableName < rows[j].tableName
		}
		if rows[i].policy.Name != rows[j].policy.Name {
			return rows[i].policy.Name < rows[j].policy.Name
		}
		return rows[i].policy.Command < rows[j].policy.Command
	})
	return rows, nil
}

func pgPolicyOID(row rowSecurityPolicyCatalogRow) id.Id {
	return id.NewId(id.Section_RowLevelSecurity, row.schemaName, row.tableName, row.policy.Name)
}

func pgPolicyRoleNames(policy rowsecurity.Policy) []any {
	if len(policy.Roles) == 0 {
		return []any{"public"}
	}
	roles := make([]any, 0, len(policy.Roles))
	for _, role := range policy.Roles {
		if strings.TrimSpace(role) == "" {
			continue
		}
		roles = append(roles, role)
	}
	if len(roles) == 0 {
		return []any{"public"}
	}
	return roles
}

func pgPolicyRoleOIDs(policy rowsecurity.Policy) []any {
	names := pgPolicyRoleNames(policy)
	oids := make([]any, 0, len(names))
	for _, name := range names {
		roleName := fmt.Sprint(name)
		if roleName == "public" {
			oids = append(oids, id.NewOID(0).AsId())
			continue
		}
		oids = append(oids, id.NewId(id.Section_User, roleName))
	}
	return oids
}

func pgPolicyCommandName(command string) string {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "select":
		return "SELECT"
	case "insert":
		return "INSERT"
	case "update":
		return "UPDATE"
	case "delete":
		return "DELETE"
	default:
		return "ALL"
	}
}

func pgPolicyCommandChar(command string) string {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "select":
		return "r"
	case "insert":
		return "a"
	case "update":
		return "w"
	case "delete":
		return "d"
	default:
		return "*"
	}
}

func pgPolicyExpression(all bool, column string) any {
	if all {
		return "true"
	}
	if column == "" {
		return nil
	}
	return fmt.Sprintf("(%s = CURRENT_USER)", column)
}
