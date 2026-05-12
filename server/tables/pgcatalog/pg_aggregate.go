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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAggregateName is a constant to the pg_aggregate name.
const PgAggregateName = "pg_aggregate"

// InitPgAggregate handles registration of the pg_aggregate handler.
func InitPgAggregate() {
	tables.AddHandler(PgCatalogName, PgAggregateName, PgAggregateHandler{})
}

// PgAggregateHandler is the handler for the pg_aggregate table.
type PgAggregateHandler struct{}

var _ tables.Handler = PgAggregateHandler{}

// Name implements the interface tables.Handler.
func (p PgAggregateHandler) Name() string {
	return PgAggregateName
}

// RowIter implements the interface tables.Handler.
func (p PgAggregateHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var rows []sql.Row
	err = funcColl.IterateFunctions(ctx, func(function corefunctions.Function) (stop bool, err error) {
		if function.Aggregate {
			rows = append(rows, pgAggregateRow(function))
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	rows = append(rows, pgAggregateBuiltinRows()...)
	return sql.RowsToRowIter(rows...), nil
}

// Schema implements the interface tables.Handler.
func (p PgAggregateHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAggregateSchema,
		PkOrdinals: nil,
	}
}

// pgAggregateSchema is the schema for pg_aggregate.
var pgAggregateSchema = sql.Schema{
	{Name: "aggfnoid", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName}, // TODO: regproc type
	{Name: "aggkind", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggnumdirectargs", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggtransfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},     // TODO: regproc type
	{Name: "aggfinalfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},     // TODO: regproc type
	{Name: "aggcombinefn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},   // TODO: regproc type
	{Name: "aggserialfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},    // TODO: regproc type
	{Name: "aggdeserialfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},  // TODO: regproc type
	{Name: "aggmtransfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},    // TODO: regproc type
	{Name: "aggminvtransfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName}, // TODO: regproc type
	{Name: "aggmfinalfn", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAggregateName},    // TODO: regproc type
	{Name: "aggfinalextra", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggmfinalextra", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggfinalmodify", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggmfinalmodify", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggsortop", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggtranstype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggtransspace", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggmtranstype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "aggmtransspace", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgAggregateName},
	{Name: "agginitval", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAggregateName},  // TODO: collation C
	{Name: "aggminitval", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgAggregateName}, // TODO: collation C
}

func pgAggregateRow(function corefunctions.Function) sql.Row {
	initCond := any(nil)
	if function.AggregateInitCond != "" {
		initCond = function.AggregateInitCond
	}
	return sql.Row{
		function.ID.FunctionName(),             // aggfnoid
		"n",                                    // aggkind
		int16(0),                               // aggnumdirectargs
		function.AggregateSFunc.FunctionName(), // aggtransfn
		"-",                                    // aggfinalfn
		"-",                                    // aggcombinefn
		"-",                                    // aggserialfn
		"-",                                    // aggdeserialfn
		"-",                                    // aggmtransfn
		"-",                                    // aggminvtransfn
		"-",                                    // aggmfinalfn
		false,                                  // aggfinalextra
		false,                                  // aggmfinalextra
		false,                                  // aggfinalmodify
		false,                                  // aggmfinalmodify
		id.Null,                                // aggsortop
		function.AggregateStateType.AsId(),     // aggtranstype
		int32(0),                               // aggtransspace
		id.Null,                                // aggmtranstype
		int32(0),                               // aggmtransspace
		initCond,                               // agginitval
		nil,                                    // aggminitval
	}
}

func pgAggregateBuiltinRows() []sql.Row {
	names := make([]string, 0, len(framework.AggregateCatalog))
	for name := range framework.AggregateCatalog {
		names = append(names, name)
	}
	sort.Strings(names)

	rows := make([]sql.Row, 0)
	for _, name := range names {
		overloads := append([]framework.AggregateFunctionInterface(nil), framework.AggregateCatalog[name]...)
		sort.Slice(overloads, func(i, j int) bool {
			return string(overloads[i].InternalID()) < string(overloads[j].InternalID())
		})
		for _, overload := range overloads {
			rows = append(rows, pgAggregateBuiltinRow(overload))
		}
	}
	return rows
}

func pgAggregateBuiltinRow(function framework.AggregateFunctionInterface) sql.Row {
	return sql.Row{
		function.GetName(),             // aggfnoid
		"n",                            // aggkind
		int16(0),                       // aggnumdirectargs
		"-",                            // aggtransfn
		"-",                            // aggfinalfn
		"-",                            // aggcombinefn
		"-",                            // aggserialfn
		"-",                            // aggdeserialfn
		"-",                            // aggmtransfn
		"-",                            // aggminvtransfn
		"-",                            // aggmfinalfn
		false,                          // aggfinalextra
		false,                          // aggmfinalextra
		false,                          // aggfinalmodify
		false,                          // aggmfinalmodify
		id.Null,                        // aggsortop
		function.GetReturn().ID.AsId(), // aggtranstype
		int32(0),                       // aggtransspace
		id.Null,                        // aggmtranstype
		int32(0),                       // aggmtransspace
		nil,                            // agginitval
		nil,                            // aggminitval
	}
}
