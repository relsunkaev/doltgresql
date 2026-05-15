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
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgRewriteName is a constant to the pg_rewrite name.
const PgRewriteName = "pg_rewrite"

// InitPgRewrite handles registration of the pg_rewrite handler.
func InitPgRewrite() {
	tables.AddHandler(PgCatalogName, PgRewriteName, PgRewriteHandler{})
}

// PgRewriteHandler is the handler for the pg_rewrite table.
type PgRewriteHandler struct{}

var _ tables.Handler = PgRewriteHandler{}

// Name implements the interface tables.Handler.
func (p PgRewriteHandler) Name() string {
	return PgRewriteName
}

// RowIter implements the interface tables.Handler.
func (p PgRewriteHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		View: func(ctx *sql.Context, schema functions.ItemSchema, view functions.ItemView) (cont bool, err error) {
			rows = append(rows, pgRewriteViewRow(schema.Item.SchemaName(), view))
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgRewriteHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgRewriteSchema,
		PkOrdinals: nil,
	}
}

// pgRewriteSchema is the schema for pg_rewrite.
var pgRewriteSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRewriteName},
	{Name: "rulename", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgRewriteName},
	{Name: "ev_class", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRewriteName},
	{Name: "ev_type", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgRewriteName},
	{Name: "ev_enabled", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgRewriteName},
	{Name: "is_instead", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgRewriteName},
	{Name: "ev_qual", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgRewriteName},   // TODO: pg_node_tree type, collation C
	{Name: "ev_action", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgRewriteName}, // TODO: pg_node_tree type, collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgRewriteName, Hidden: true},
}

func pgRewriteViewRow(schemaName string, view functions.ItemView) sql.Row {
	return sql.Row{
		id.NewId(id.Section_View, schemaName, view.Item.Name, "_RETURN"), // oid
		"_RETURN",                    // rulename
		view.OID.AsId(),              // ev_class
		"1",                          // ev_type, SELECT
		"O",                          // ev_enabled, enabled
		true,                         // is_instead
		"<>",                         // ev_qual
		viewRewriteAction(view.Item), // ev_action
		id.NewTable(PgCatalogName, PgRewriteName).AsId(), // tableoid
	}
}

func viewRewriteAction(view sql.ViewDefinition) string {
	if view.TextDefinition != "" {
		return view.TextDefinition
	}
	return view.CreateViewStatement
}
