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

// PgForeignDataWrapperName is a constant to the pg_foreign_data_wrapper name.
const PgForeignDataWrapperName = "pg_foreign_data_wrapper"

// InitPgForeignDataWrapper handles registration of the pg_foreign_data_wrapper handler.
func InitPgForeignDataWrapper() {
	tables.AddHandler(PgCatalogName, PgForeignDataWrapperName, PgForeignDataWrapperHandler{})
}

// PgForeignDataWrapperHandler is the handler for the pg_foreign_data_wrapper table.
type PgForeignDataWrapperHandler struct{}

var _ tables.Handler = PgForeignDataWrapperHandler{}

// Name implements the interface tables.Handler.
func (p PgForeignDataWrapperHandler) Name() string {
	return PgForeignDataWrapperName
}

// RowIter implements the interface tables.Handler.
func (p PgForeignDataWrapperHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	auth.LockRead(func() {
		for _, wrapper := range auth.GetAllForeignDataWrappers() {
			owner := catalogOwnerOID()
			if role := auth.GetRole(wrapper.Owner); role.IsValid() {
				owner = id.NewId(id.Section_User, wrapper.Owner)
			}
			rows = append(rows, sql.Row{
				id.NewId(id.Section_ForeignDataWrapper, wrapper.Name), // oid
				wrapper.Name,    // fdwname
				owner,           // fdwowner
				id.Null,         // fdwhandler
				id.Null,         // fdwvalidator
				nil,             // fdwacl
				wrapper.Options, // fdwoptions
				id.NewTable(PgCatalogName, PgForeignDataWrapperName).AsId(),
			})
		}
	})
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgForeignDataWrapperHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgForeignDataWrapperSchema,
		PkOrdinals: nil,
	}
}

// pgForeignDataWrapperSchema is the schema for pg_foreign_data_wrapper.
var pgForeignDataWrapperSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignDataWrapperName},
	{Name: "fdwname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgForeignDataWrapperName},
	{Name: "fdwowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignDataWrapperName},
	{Name: "fdwhandler", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignDataWrapperName},
	{Name: "fdwvalidator", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignDataWrapperName},
	{Name: "fdwacl", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgForeignDataWrapperName},     // TODO: aclitem[] type
	{Name: "fdwoptions", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgForeignDataWrapperName}, // TODO: collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgForeignDataWrapperName, Hidden: true},
}
