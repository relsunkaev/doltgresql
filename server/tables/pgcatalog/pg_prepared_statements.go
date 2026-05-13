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
	"github.com/dolthub/doltgresql/server/sessionstate"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgPreparedStatementsName is a constant to the pg_prepared_statements name.
const PgPreparedStatementsName = "pg_prepared_statements"

// InitPgPreparedStatements handles registration of the pg_prepared_statements handler.
func InitPgPreparedStatements() {
	tables.AddHandler(PgCatalogName, PgPreparedStatementsName, PgPreparedStatementsHandler{})
}

// PgPreparedStatementsHandler is the handler for the pg_prepared_statements table.
type PgPreparedStatementsHandler struct{}

var _ tables.Handler = PgPreparedStatementsHandler{}

// Name implements the interface tables.Handler.
func (p PgPreparedStatementsHandler) Name() string {
	return PgPreparedStatementsName
}

// RowIter implements the interface tables.Handler.
func (p PgPreparedStatementsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if ctx.Session == nil {
		return emptyRowIter()
	}
	return &pgPreparedStatementsRowIter{
		statements: sessionstate.ListPreparedStatements(ctx.Session.ID()),
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgPreparedStatementsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgPreparedStatementsSchema,
		PkOrdinals: nil,
	}
}

// pgPreparedStatementsSchema is the schema for pg_prepared_statements.
var pgPreparedStatementsSchema = sql.Schema{
	{Name: "name", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "statement", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "prepare_time", Type: pgtypes.TimestampTZ, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "parameter_types", Type: pgtypes.RegtypeArray, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "result_types", Type: pgtypes.RegtypeArray, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "from_sql", Type: pgtypes.Bool, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "generic_plans", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
	{Name: "custom_plans", Type: pgtypes.Int64, Default: nil, Nullable: true, Source: PgPreparedStatementsName},
}

// pgPreparedStatementsRowIter is the sql.RowIter for the pg_prepared_statements table.
type pgPreparedStatementsRowIter struct {
	statements []sessionstate.PreparedStatement
	idx        int
}

var _ sql.RowIter = (*pgPreparedStatementsRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgPreparedStatementsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.statements) {
		return nil, io.EOF
	}
	statement := iter.statements[iter.idx]
	iter.idx++
	return sql.Row{
		statement.Name,
		statement.Statement,
		statement.PrepareTime,
		regtypeArrayValues(statement.ParameterOIDs),
		regtypeArrayValues(statement.ResultOIDs),
		statement.FromSQL,
		statement.GenericPlans,
		statement.CustomPlans,
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgPreparedStatementsRowIter) Close(ctx *sql.Context) error {
	return nil
}

func regtypeArrayValues(oids []uint32) []any {
	values := make([]any, len(oids))
	for i, oid := range oids {
		internalID := id.Cache().ToInternal(oid)
		if !internalID.IsValid() {
			internalID = id.NewOID(oid).AsId()
		}
		values[i] = internalID
	}
	return values
}
