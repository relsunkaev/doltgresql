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
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgPolicyName is a constant to the pg_policy name.
const PgPolicyName = "pg_policy"

// InitPgPolicy handles registration of the pg_policy handler.
func InitPgPolicy() {
	tables.AddHandler(PgCatalogName, PgPolicyName, PgPolicyHandler{})
}

// PgPolicyHandler is the handler for the pg_policy table.
type PgPolicyHandler struct{}

var _ tables.Handler = PgPolicyHandler{}

// Name implements the interface tables.Handler.
func (p PgPolicyHandler) Name() string {
	return PgPolicyName
}

// RowIter implements the interface tables.Handler.
func (p PgPolicyHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	policies, err := rowSecurityPolicyCatalogRows(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]sql.Row, 0, len(policies))
	for _, policyRow := range policies {
		policy := policyRow.policy
		rows = append(rows, sql.Row{
			pgPolicyOID(policyRow),              // oid
			policy.Name,                         // polname
			policyRow.tableOID,                  // polrelid
			pgPolicyCommandChar(policy.Command), // polcmd
			true,                                // polpermissive
			pgPolicyRoleOIDs(policy),            // polroles
			pgPolicyExpression(policy.UsingAll, policy.UsingColumn), // polqual
			pgPolicyExpression(policy.CheckAll, policy.CheckColumn), // polwithcheck
			id.NewTable(PgCatalogName, PgPolicyName).AsId(),         // tableoid
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgPolicyHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgPolicySchema,
		PkOrdinals: nil,
	}
}

// pgPolicySchema is the schema for pg_policy.
var pgPolicySchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPolicyName},
	{Name: "polname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgPolicyName},
	{Name: "polrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPolicyName},
	{Name: "polcmd", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgPolicyName},
	{Name: "polpermissive", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgPolicyName},
	{Name: "polroles", Type: pgtypes.OidArray, Default: nil, Nullable: false, Source: PgPolicyName},
	{Name: "polqual", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgPolicyName},      // TODO: pg_node_tree type, collation C
	{Name: "polwithcheck", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgPolicyName}, // TODO: pg_node_tree type, collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPolicyName},
}
