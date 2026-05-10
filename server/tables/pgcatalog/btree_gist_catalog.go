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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
)

const (
	accessMethodGist       = "gist"
	btreeGistExtensionName = "btree_gist"
)

type btreeGistCatalogType struct {
	opclass            string
	typeName           string
	procs              []btreeGistProc
	distanceSortFamily string
}

type btreeGistProc struct {
	procNum int16
	proc    string
}

var btreeGistCatalogTypes = []btreeGistCatalogType{
	{opclass: "gist_oid_ops", typeName: "oid", procs: btreeGistFixedProcs("oid", btreeGistProc{procNum: 8, proc: "gbt_oid_distance"}, btreeGistProc{procNum: 9, proc: "gbt_oid_fetch"}), distanceSortFamily: "oid_ops"},
	{opclass: "gist_int2_ops", typeName: "int2", procs: btreeGistFixedProcs("int2", btreeGistProc{procNum: 8, proc: "gbt_int2_distance"}, btreeGistProc{procNum: 9, proc: "gbt_int2_fetch"}), distanceSortFamily: "integer_ops"},
	{opclass: "gist_int4_ops", typeName: "int4", procs: btreeGistFixedProcs("int4", btreeGistProc{procNum: 8, proc: "gbt_int4_distance"}, btreeGistProc{procNum: 9, proc: "gbt_int4_fetch"}), distanceSortFamily: "integer_ops"},
	{opclass: "gist_int8_ops", typeName: "int8", procs: btreeGistFixedProcs("int8", btreeGistProc{procNum: 8, proc: "gbt_int8_distance"}, btreeGistProc{procNum: 9, proc: "gbt_int8_fetch"}), distanceSortFamily: "integer_ops"},
	{opclass: "gist_float4_ops", typeName: "float4", procs: btreeGistFixedProcs("float4", btreeGistProc{procNum: 8, proc: "gbt_float4_distance"}, btreeGistProc{procNum: 9, proc: "gbt_float4_fetch"}), distanceSortFamily: "float_ops"},
	{opclass: "gist_float8_ops", typeName: "float8", procs: btreeGistFixedProcs("float8", btreeGistProc{procNum: 8, proc: "gbt_float8_distance"}, btreeGistProc{procNum: 9, proc: "gbt_float8_fetch"}), distanceSortFamily: "float_ops"},
	{opclass: "gist_timestamp_ops", typeName: "timestamp", procs: btreeGistProcs("gbt_ts_consistent", "gbt_ts_union", "gbt_ts_compress", "gbt_decompress", "gbt_ts_penalty", "gbt_ts_picksplit", "gbt_ts_same", btreeGistProc{procNum: 8, proc: "gbt_ts_distance"}, btreeGistProc{procNum: 9, proc: "gbt_ts_fetch"}), distanceSortFamily: "interval_ops"},
	{opclass: "gist_timestamptz_ops", typeName: "timestamptz", procs: btreeGistProcs("gbt_tstz_consistent", "gbt_ts_union", "gbt_tstz_compress", "gbt_decompress", "gbt_ts_penalty", "gbt_ts_picksplit", "gbt_ts_same", btreeGistProc{procNum: 8, proc: "gbt_tstz_distance"}, btreeGistProc{procNum: 9, proc: "gbt_ts_fetch"}), distanceSortFamily: "interval_ops"},
	{opclass: "gist_time_ops", typeName: "time", procs: btreeGistFixedProcs("time", btreeGistProc{procNum: 8, proc: "gbt_time_distance"}, btreeGistProc{procNum: 9, proc: "gbt_time_fetch"}), distanceSortFamily: "interval_ops"},
	{opclass: "gist_timetz_ops", typeName: "timetz", procs: btreeGistProcs("gbt_timetz_consistent", "gbt_time_union", "gbt_timetz_compress", "gbt_decompress", "gbt_time_penalty", "gbt_time_picksplit", "gbt_time_same")},
	{opclass: "gist_date_ops", typeName: "date", procs: btreeGistFixedProcs("date", btreeGistProc{procNum: 8, proc: "gbt_date_distance"}, btreeGistProc{procNum: 9, proc: "gbt_date_fetch"}), distanceSortFamily: "integer_ops"},
	{opclass: "gist_interval_ops", typeName: "interval", procs: btreeGistProcs("gbt_intv_consistent", "gbt_intv_union", "gbt_intv_compress", "gbt_intv_decompress", "gbt_intv_penalty", "gbt_intv_picksplit", "gbt_intv_same", btreeGistProc{procNum: 8, proc: "gbt_intv_distance"}, btreeGistProc{procNum: 9, proc: "gbt_intv_fetch"}), distanceSortFamily: "interval_ops"},
	{opclass: "gist_text_ops", typeName: "text", procs: btreeGistVarProcs("text", btreeGistProc{procNum: 9, proc: "gbt_var_fetch"})},
	{opclass: "gist_bpchar_ops", typeName: "bpchar", procs: btreeGistProcs("gbt_bpchar_consistent", "gbt_text_union", "gbt_bpchar_compress", "gbt_var_decompress", "gbt_text_penalty", "gbt_text_picksplit", "gbt_text_same", btreeGistProc{procNum: 9, proc: "gbt_var_fetch"})},
	{opclass: "gist_bytea_ops", typeName: "bytea", procs: btreeGistVarProcs("bytea", btreeGistProc{procNum: 9, proc: "gbt_var_fetch"})},
	{opclass: "gist_numeric_ops", typeName: "numeric", procs: btreeGistVarProcs("numeric", btreeGistProc{procNum: 9, proc: "gbt_var_fetch"})},
	{opclass: "gist_bit_ops", typeName: "bit", procs: btreeGistVarProcs("bit", btreeGistProc{procNum: 9, proc: "gbt_var_fetch"})},
	{opclass: "gist_vbit_ops", typeName: "varbit", procs: btreeGistVarProcs("bit", btreeGistProc{procNum: 9, proc: "gbt_var_fetch"})},
	{opclass: "gist_uuid_ops", typeName: "uuid", procs: btreeGistFixedProcs("uuid", btreeGistProc{procNum: 9, proc: "gbt_uuid_fetch"})},
	{opclass: "gist_enum_ops", typeName: "anyenum", procs: btreeGistFixedProcs("enum", btreeGistProc{procNum: 9, proc: "gbt_enum_fetch"})},
	{opclass: "gist_bool_ops", typeName: "bool", procs: btreeGistFixedProcs("bool", btreeGistProc{procNum: 9, proc: "gbt_bool_fetch"})},
}

func appendBtreeGistOpclasses(ctx *sql.Context, opclasses []opclass) ([]opclass, error) {
	namespace, ok, err := btreeGistNamespace(ctx)
	if err != nil || !ok {
		return opclasses, err
	}
	next := make([]opclass, 0, len(opclasses)+len(btreeGistCatalogTypes))
	next = append(next, opclasses...)
	for _, typ := range btreeGistCatalogTypes {
		next = append(next, opclass{
			oid:       pgCatalogOpclassID(accessMethodGist, typ.opclass),
			opcmethod: id.NewAccessMethod(accessMethodGist).AsId(),
			opcname:   typ.opclass,
			namespace: namespace,
			family:    btreeGistOpfamilyID(typ.opclass),
			intype:    pgCatalogTypeID(typ.typeName),
			keytype:   zeroOID(),
			isDefault: true,
		})
	}
	return next, nil
}

func appendBtreeGistOpfamilies(ctx *sql.Context, opfamilies []opfamily) ([]opfamily, error) {
	namespace, ok, err := btreeGistNamespace(ctx)
	if err != nil || !ok {
		return opfamilies, err
	}
	next := make([]opfamily, 0, len(opfamilies)+len(btreeGistCatalogTypes))
	next = append(next, opfamilies...)
	for _, typ := range btreeGistCatalogTypes {
		next = append(next, opfamily{
			oid:       btreeGistOpfamilyID(typ.opclass),
			opfmethod: id.NewAccessMethod(accessMethodGist).AsId(),
			opfname:   typ.opclass,
			namespace: namespace,
		})
	}
	return next, nil
}

func appendBtreeGistAmops(ctx *sql.Context, amops []amop) ([]amop, error) {
	_, ok, err := btreeGistNamespace(ctx)
	if err != nil || !ok {
		return amops, err
	}
	next := make([]amop, 0, len(amops)+len(btreeGistCatalogTypes)*7)
	next = append(next, amops...)
	for _, typ := range btreeGistCatalogTypes {
		for _, operator := range btreeComparisonOperators {
			next = append(next, newBtreeGistAmop(typ, operator.name, operator.strategy, "", zeroOID()))
		}
		next = append(next, newBtreeGistAmop(typ, "<>", 6, "", zeroOID()))
		if typ.distanceSortFamily != "" {
			next = append(next, newBtreeGistAmop(typ, "<->", 15, "o", btreeOpfamilyID(typ.distanceSortFamily)))
		}
	}
	return next, nil
}

func appendBtreeGistAmprocs(ctx *sql.Context, amprocs []amproc) ([]amproc, error) {
	_, ok, err := btreeGistNamespace(ctx)
	if err != nil || !ok {
		return amprocs, err
	}
	next := make([]amproc, 0, len(amprocs)+len(btreeGistCatalogTypes)*9)
	next = append(next, amprocs...)
	for _, typ := range btreeGistCatalogTypes {
		for _, proc := range typ.procs {
			next = append(next, amproc{
				oid:       btreeGistAmprocID(typ.opclass, typ.typeName, proc.procNum),
				family:    btreeGistOpfamilyID(typ.opclass),
				leftType:  pgCatalogTypeID(typ.typeName),
				rightType: pgCatalogTypeID(typ.typeName),
				procNum:   proc.procNum,
				proc:      proc.proc,
			})
		}
	}
	return next, nil
}

func btreeGistNamespace(ctx *sql.Context) (id.Id, bool, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return id.Null, false, err
	}
	ext, err := extCollection.GetLoadedExtension(ctx, id.NewExtension(btreeGistExtensionName))
	if err != nil {
		return id.Null, false, err
	}
	if !ext.ExtName.IsValid() {
		return id.Null, false, nil
	}
	namespace := ext.Namespace
	if !namespace.IsValid() {
		namespace = id.NewNamespace("public")
	}
	return namespace.AsId(), true, nil
}

func btreeGistFixedProcs(prefix string, extra ...btreeGistProc) []btreeGistProc {
	return btreeGistProcs(
		"gbt_"+prefix+"_consistent",
		"gbt_"+prefix+"_union",
		"gbt_"+prefix+"_compress",
		"gbt_decompress",
		"gbt_"+prefix+"_penalty",
		"gbt_"+prefix+"_picksplit",
		"gbt_"+prefix+"_same",
		extra...,
	)
}

func btreeGistVarProcs(prefix string, extra ...btreeGistProc) []btreeGistProc {
	return btreeGistProcs(
		"gbt_"+prefix+"_consistent",
		"gbt_"+prefix+"_union",
		"gbt_"+prefix+"_compress",
		"gbt_var_decompress",
		"gbt_"+prefix+"_penalty",
		"gbt_"+prefix+"_picksplit",
		"gbt_"+prefix+"_same",
		extra...,
	)
}

func btreeGistProcs(consistent, union, compress, decompress, penalty, picksplit, same string, extra ...btreeGistProc) []btreeGistProc {
	procs := []btreeGistProc{
		{procNum: 1, proc: consistent},
		{procNum: 2, proc: union},
		{procNum: 3, proc: compress},
		{procNum: 4, proc: decompress},
		{procNum: 5, proc: penalty},
		{procNum: 6, proc: picksplit},
		{procNum: 7, proc: same},
	}
	return append(procs, extra...)
}

func btreeGistOpfamilyID(name string) id.Id {
	return id.NewId(id.Section_OperatorFamily, accessMethodGist, name)
}

func newBtreeGistAmop(typ btreeGistCatalogType, operator string, strategy int16, purpose string, sortFamily id.Id) amop {
	return amop{
		oid:        btreeGistAmopID(typ.opclass, typ.typeName, strategy),
		family:     btreeGistOpfamilyID(typ.opclass),
		leftType:   pgCatalogTypeID(typ.typeName),
		rightType:  pgCatalogTypeID(typ.typeName),
		strategy:   strategy,
		operator:   pgCatalogOperatorID(operator, typ.typeName, typ.typeName),
		method:     id.NewAccessMethod(accessMethodGist).AsId(),
		purpose:    purpose,
		sortFamily: sortFamily,
	}
}

func btreeGistAmopID(opfamily string, typeName string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"btree_gist_amop",
		opfamily,
		typeName,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func btreeGistAmprocID(opfamily string, typeName string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"btree_gist_amproc",
		opfamily,
		typeName,
		strconv.FormatInt(int64(procNum), 10),
	)
}
