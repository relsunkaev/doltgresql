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
	coreprocedures "github.com/dolthub/doltgresql/core/procedures"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgProcName is a constant to the pg_proc name.
const PgProcName = "pg_proc"

// InitPgProc handles registration of the pg_proc handler.
func InitPgProc() {
	tables.AddHandler(PgCatalogName, PgProcName, PgProcHandler{})
}

// PgProcHandler is the handler for the pg_proc table.
type PgProcHandler struct{}

var _ tables.Handler = PgProcHandler{}

// Name implements the interface tables.Handler.
func (p PgProcHandler) Name() string {
	return PgProcName
}

// RowIter implements the interface tables.Handler.
func (p PgProcHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	procColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var rows []sql.Row
	err = funcColl.IterateFunctions(ctx, func(function corefunctions.Function) (stop bool, err error) {
		rows = append(rows, pgProcFunctionRow(function))
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	err = procColl.IterateProcedures(ctx, func(procedure coreprocedures.Procedure) (stop bool, err error) {
		rows = append(rows, pgProcProcedureRow(procedure))
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	rows = append(rows, pgProcBuiltinFunctionRows()...)
	return sql.RowsToRowIter(rows...), nil
}

// PkSchema implements the interface tables.Handler.
func (p PgProcHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgProcSchema,
		PkOrdinals: nil,
	}
}

// pgProcSchema is the schema for pg_proc.
var pgProcSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "pronamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "prolang", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "procost", Type: pgtypes.Float32, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "prorows", Type: pgtypes.Float32, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "provariadic", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "prosupport", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgProcName}, // TODO: type regproc
	{Name: "prokind", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "prosecdef", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proleakproof", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proisstrict", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proretset", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "provolatile", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proparallel", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "pronargs", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "pronargdefaults", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "prorettype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proargtypes", Type: pgtypes.Oidvector, Default: nil, Nullable: false, Source: PgProcName},
	{Name: "proallargtypes", Type: pgtypes.OidArray, Default: nil, Nullable: true, Source: PgProcName},
	{Name: "proargmodes", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgProcName}, // TODO: type char[]
	{Name: "proargnames", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgProcName}, // TODO: collation C
	{Name: "proargdefaults", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgProcName},   // TODO: type pg_node_tree, collation C
	{Name: "protrftypes", Type: pgtypes.OidArray, Default: nil, Nullable: true, Source: PgProcName},
	{Name: "prosrc", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgProcName}, // TODO: collation C
	{Name: "probin", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgProcName},
	{Name: "prosqlbody", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgProcName},     // TODO: type pg_node_tree, collation C
	{Name: "proconfig", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgProcName}, // TODO: collation C
	{Name: "proacl", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgProcName},    // TODO: type aclitem[]
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgProcName},
}

func pgProcFunctionRow(function corefunctions.Function) sql.Row {
	owner := function.Owner
	if owner == "" {
		owner = "postgres"
	}
	cost := function.Cost
	if cost == 0 {
		cost = 100
	}
	rows := function.Rows
	if !function.SetOf {
		rows = 0
	} else if rows == 0 {
		rows = 1000
	}
	volatility := function.Volatility
	if volatility == "" {
		volatility = "v"
	}
	parallel := function.Parallel
	if parallel == "" {
		parallel = "u"
	}
	argTypes := make([]any, len(function.ParameterTypes))
	for i, argType := range function.ParameterTypes {
		argTypes[i] = argType.AsId()
	}
	argNames := make([]any, len(function.ParameterNames))
	for i, argName := range function.ParameterNames {
		argNames[i] = argName
	}
	proConfig := []any(nil)
	for name, value := range function.SetConfig {
		proConfig = append(proConfig, name+"="+value)
	}
	proLang := id.NewId(id.Section_FunctionLanguage, "plpgsql")
	if function.SQLDefinition != "" {
		proLang = id.NewId(id.Section_FunctionLanguage, "sql")
	}
	proKind := "f"
	if function.Aggregate {
		proKind = "a"
	}
	return sql.Row{
		function.ID.AsId(),                               // oid
		function.ID.FunctionName(),                       // proname
		id.NewNamespace(function.ID.SchemaName()).AsId(), // pronamespace
		id.NewId(id.Section_User, owner),                 // proowner
		proLang,                                          // prolang
		cost,                                             // procost
		rows,                                             // prorows
		id.Null,                                          // provariadic
		"-",                                              // prosupport
		proKind,                                          // prokind
		function.SecurityDefiner,                         // prosecdef
		function.LeakProof,                               // proleakproof
		function.Strict,                                  // proisstrict
		function.SetOf,                                   // proretset
		volatility,                                       // provolatile
		parallel,                                         // proparallel
		int16(len(function.ParameterTypes)),              // pronargs
		int16(len(function.ParameterDefaults)),           // pronargdefaults
		function.ReturnType.AsId(),                       // prorettype
		argTypes,                                         // proargtypes
		nil,                                              // proallargtypes
		nil,                                              // proargmodes
		argNames,                                         // proargnames
		nil,                                              // proargdefaults
		nil,                                              // protrftypes
		function.GetInnerDefinition(),                    // prosrc
		nil,                                              // probin
		function.SQLDefinition,                           // prosqlbody
		proConfig,                                        // proconfig
		nil,                                              // proacl
		id.NewTable(PgCatalogName, PgProcName).AsId(), // tableoid
	}
}

func pgProcBuiltinFunctionRows() []sql.Row {
	names := make([]string, 0, len(framework.Catalog))
	for name := range framework.Catalog {
		names = append(names, name)
	}
	sort.Strings(names)

	rows := make([]sql.Row, 0)
	for _, name := range names {
		overloads := append([]framework.FunctionInterface(nil), framework.Catalog[name]...)
		sort.Slice(overloads, func(i, j int) bool {
			return string(overloads[i].InternalID()) < string(overloads[j].InternalID())
		})
		for _, overload := range overloads {
			rows = append(rows, pgProcBuiltinFunctionRow(overload))
		}
	}
	return rows
}

func pgProcBuiltinFunctionRow(function framework.FunctionInterface) sql.Row {
	paramTypes := function.GetParameters()
	argTypes := make([]any, len(paramTypes))
	functionIDParamTypes := make([]id.Type, len(paramTypes))
	for i, paramType := range paramTypes {
		argTypes[i] = paramType.ID.AsId()
		functionIDParamTypes[i] = paramType.ID
	}

	rows := float32(0)
	if function.IsSRF() {
		rows = 1000
	}
	volatility := "i"
	if function.NonDeterministic() {
		volatility = "v"
	}
	functionID := id.Function(function.InternalID())
	if !functionID.IsValid() || id.Id(functionID).Section() != id.Section_Function {
		functionID = id.NewFunction("pg_catalog", function.GetName(), functionIDParamTypes...)
	}
	return sql.Row{
		functionID.AsId(),                          // oid
		function.GetName(),                         // proname
		id.NewNamespace("pg_catalog").AsId(),       // pronamespace
		catalogOwnerOID(),                          // proowner
		id.NewId(id.Section_FunctionLanguage, "c"), // prolang
		float32(1),                                 // procost
		rows,                                       // prorows
		id.Null,                                    // provariadic
		"-",                                        // prosupport
		"f",                                        // prokind
		false,                                      // prosecdef
		false,                                      // proleakproof
		function.IsStrict(),                        // proisstrict
		function.IsSRF(),                           // proretset
		volatility,                                 // provolatile
		"s",                                        // proparallel
		int16(len(paramTypes)),                     // pronargs
		int16(0),                                   // pronargdefaults
		function.GetReturn().ID.AsId(),             // prorettype
		argTypes,                                   // proargtypes
		nil,                                        // proallargtypes
		nil,                                        // proargmodes
		nil,                                        // proargnames
		nil,                                        // proargdefaults
		nil,                                        // protrftypes
		"builtin",                                  // prosrc
		nil,                                        // probin
		nil,                                        // prosqlbody
		nil,                                        // proconfig
		nil,                                        // proacl
		id.NewTable(PgCatalogName, PgProcName).AsId(), // tableoid
	}
}

func pgProcProcedureRow(procedure coreprocedures.Procedure) sql.Row {
	owner := procedure.Owner
	if owner == "" {
		owner = "postgres"
	}
	argTypes := make([]any, 0, len(procedure.ParameterTypes))
	argNames := make([]any, len(procedure.ParameterNames))
	for i, argName := range procedure.ParameterNames {
		argNames[i] = argName
	}
	for i, argType := range procedure.ParameterTypes {
		if i < len(procedure.ParameterModes) && procedure.ParameterModes[i] == coreprocedures.ParameterMode_OUT {
			continue
		}
		argTypes = append(argTypes, argType.AsId())
	}
	proConfig := []any(nil)
	for name, value := range procedure.SetConfig {
		proConfig = append(proConfig, name+"="+value)
	}
	proLang := id.NewId(id.Section_FunctionLanguage, "plpgsql")
	if procedure.SQLDefinition != "" {
		proLang = id.NewId(id.Section_FunctionLanguage, "sql")
	}
	return sql.Row{
		procedure.ID.AsId(),                               // oid
		procedure.ID.ProcedureName(),                      // proname
		id.NewNamespace(procedure.ID.SchemaName()).AsId(), // pronamespace
		id.NewId(id.Section_User, owner),                  // proowner
		proLang,                                           // prolang
		float32(100),                                      // procost
		float32(0),                                        // prorows
		id.Null,                                           // provariadic
		"-",                                               // prosupport
		"p",                                               // prokind
		false,                                             // prosecdef
		false,                                             // proleakproof
		false,                                             // proisstrict
		false,                                             // proretset
		"v",                                               // provolatile
		"u",                                               // proparallel
		int16(len(argTypes)),                              // pronargs
		int16(len(procedure.ParameterDefaults)),           // pronargdefaults
		pgtypes.Void.ID.AsId(),                            // prorettype
		argTypes,                                          // proargtypes
		nil,                                               // proallargtypes
		nil,                                               // proargmodes
		argNames,                                          // proargnames
		nil,                                               // proargdefaults
		nil,                                               // protrftypes
		procedure.Definition,                              // prosrc
		nil,                                               // probin
		procedure.SQLDefinition,                           // prosqlbody
		proConfig,                                         // proconfig
		nil,                                               // proacl
		id.NewTable(PgCatalogName, PgProcName).AsId(), // tableoid
	}
}
