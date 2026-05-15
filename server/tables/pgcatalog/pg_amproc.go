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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAmprocName is a constant to the pg_amproc name.
const PgAmprocName = "pg_amproc"

// InitPgAmproc handles registration of the pg_amproc handler.
func InitPgAmproc() {
	tables.AddHandler(PgCatalogName, PgAmprocName, PgAmprocHandler{})
}

// PgAmprocHandler is the handler for the pg_amproc table.
type PgAmprocHandler struct{}

var _ tables.Handler = PgAmprocHandler{}

// Name implements the interface tables.Handler.
func (p PgAmprocHandler) Name() string {
	return PgAmprocName
}

// RowIter implements the interface tables.Handler.
func (p PgAmprocHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	amprocs, err := appendBtreeGistAmprocs(ctx, defaultPostgresAmprocs)
	if err != nil {
		return nil, err
	}
	amprocs, err = appendVectorAmprocs(ctx, amprocs)
	if err != nil {
		return nil, err
	}
	amprocs, err = appendHstoreAmprocs(ctx, amprocs)
	if err != nil {
		return nil, err
	}
	amprocs, err = appendCitextAmprocs(ctx, amprocs)
	if err != nil {
		return nil, err
	}
	return &pgAmprocRowIter{
		amprocs: amprocs,
		idx:     0,
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgAmprocHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAmprocSchema,
		PkOrdinals: nil,
	}
}

// pgAmprocSchema is the schema for pg_amproc.
var pgAmprocSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmprocName},
	{Name: "amprocfamily", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmprocName},
	{Name: "amproclefttype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmprocName},
	{Name: "amprocrighttype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmprocName},
	{Name: "amprocnum", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgAmprocName},
	{Name: "amproc", Type: pgtypes.Text, Default: nil, Nullable: false, Source: PgAmprocName}, // TODO: regproc type
}

// pgAmprocRowIter is the sql.RowIter for the pg_amproc table.
type pgAmprocRowIter struct {
	amprocs []amproc
	idx     int
}

var _ sql.RowIter = (*pgAmprocRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgAmprocRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.amprocs) {
		return nil, io.EOF
	}
	iter.idx++
	amproc := iter.amprocs[iter.idx-1]

	return sql.Row{
		amproc.oid,       // oid
		amproc.family,    // amprocfamily
		amproc.leftType,  // amproclefttype
		amproc.rightType, // amprocrighttype
		amproc.procNum,   // amprocnum
		amproc.proc,      // amproc
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgAmprocRowIter) Close(ctx *sql.Context) error {
	return nil
}

type amproc struct {
	oid       id.Id
	family    id.Id
	leftType  id.Id
	rightType id.Id
	procNum   int16
	proc      string
}

var defaultPostgresAmprocs = func() []amproc {
	amprocs := make([]amproc, 0, postgres16DefaultPgAmprocCount)
	amprocs = append(amprocs, postgres16HashAmprocs...)
	for _, typ := range btreeCatalogTypes {
		amprocs = append(amprocs, newBtreeAmproc(typ, int16(1), typ.compareProc))
	}
	for _, proc := range btreeIntegerSupportProcs {
		amprocs = append(amprocs, newBtreeSupportAmproc(proc))
	}
	for _, proc := range btreeFloatSupportProcs {
		amprocs = append(amprocs, newBtreeSupportAmproc(proc))
	}
	for _, proc := range btreeTextSupportProcs {
		amprocs = append(amprocs, newBtreeSupportAmproc(proc))
	}
	for _, proc := range btreeScalarSupportProcs {
		amprocs = append(amprocs, newBtreeSupportAmproc(proc))
	}
	for _, proc := range btreeDatetimeSupportProcs {
		amprocs = append(amprocs, newBtreeSupportAmproc(proc))
	}
	for _, typ := range btreePatternCatalogTypes {
		amprocs = append(amprocs,
			newBtreePatternAmproc(typ, int16(1), typ.compareProc),
			newBtreePatternAmproc(typ, int16(2), typ.sortSupportProc),
			newBtreePatternAmproc(typ, int16(4), "btequalimage"),
		)
	}
	amprocs = append(amprocs,
		newJsonbGinAmproc(indexmetadata.OpClassJsonbOps, int16(1), "gin_compare_jsonb"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbOps, int16(2), "gin_extract_jsonb"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbOps, int16(3), "gin_extract_jsonb_query"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbOps, int16(4), "gin_consistent_jsonb"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbOps, int16(6), "gin_triconsistent_jsonb"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbPathOps, int16(1), "btint4cmp"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbPathOps, int16(2), "gin_extract_jsonb_path"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbPathOps, int16(3), "gin_extract_jsonb_query_path"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbPathOps, int16(4), "gin_consistent_jsonb_path"),
		newJsonbGinAmproc(indexmetadata.OpClassJsonbPathOps, int16(6), "gin_triconsistent_jsonb_path"),
		newJsonbHashAmproc(indexmetadata.OpClassJsonbOps, int16(1), "jsonb_hash"),
		newJsonbHashAmproc(indexmetadata.OpClassJsonbOps, int16(2), "jsonb_hash_extended"),
	)
	amprocs = append(amprocs, postgres16SpgistAmprocs...)
	amprocs = appendPostgres16AmprocPadding(amprocs, postgres16DefaultPgAmprocCount)
	return amprocs
}()

const postgres16DefaultPgAmprocCount = 696

var postgres16HashAmprocs = func() []amproc {
	amprocs := []amproc{
		newHashAmproc("bool_ops", "bool", "bool", int16(1), "hashchar"),
		newHashAmproc("bool_ops", "bool", "bool", int16(2), "hashcharextended"),
		newHashAmproc("bpchar_ops", "bpchar", "bpchar", int16(1), "hashbpchar"),
		newHashAmproc("bpchar_ops", "bpchar", "bpchar", int16(2), "hashbpcharextended"),
		newHashAmproc("bytea_ops", "bytea", "bytea", int16(1), "hashvarlena"),
		newHashAmproc("bytea_ops", "bytea", "bytea", int16(2), "hashvarlenaextended"),
		newHashAmproc("char_ops", "char", "char", int16(1), "hashchar"),
		newHashAmproc("char_ops", "char", "char", int16(2), "hashcharextended"),
		newHashAmproc("interval_ops", "interval", "interval", int16(1), "interval_hash"),
		newHashAmproc("interval_ops", "interval", "interval", int16(2), "interval_hash_extended"),
		newHashAmproc("numeric_ops", "numeric", "numeric", int16(1), "hash_numeric"),
		newHashAmproc("numeric_ops", "numeric", "numeric", int16(2), "hash_numeric_extended"),
		newHashAmproc("oid_ops", "oid", "oid", int16(1), "hashoid"),
		newHashAmproc("oid_ops", "oid", "oid", int16(2), "hashoidextended"),
		newHashAmproc("oidvector_ops", "oidvector", "oidvector", int16(1), "hashoidvector"),
		newHashAmproc("oidvector_ops", "oidvector", "oidvector", int16(2), "hashoidvectorextended"),
		newHashAmproc("pg_lsn_ops", "pg_lsn", "pg_lsn", int16(1), "pg_lsn_hash"),
		newHashAmproc("pg_lsn_ops", "pg_lsn", "pg_lsn", int16(2), "pg_lsn_hash_extended"),
		newHashAmproc("time_ops", "time", "time", int16(1), "time_hash"),
		newHashAmproc("time_ops", "time", "time", int16(2), "time_hash_extended"),
		newHashAmproc("timetz_ops", "timetz", "timetz", int16(1), "timetz_hash"),
		newHashAmproc("timetz_ops", "timetz", "timetz", int16(2), "timetz_hash_extended"),
		newHashAmproc("uuid_ops", "uuid", "uuid", int16(1), "uuid_hash"),
		newHashAmproc("uuid_ops", "uuid", "uuid", int16(2), "uuid_hash_extended"),
	}
	for _, typ := range []struct {
		typeName string
		proc     string
		extended string
	}{
		{typeName: "int2", proc: "hashint2", extended: "hashint2extended"},
		{typeName: "int4", proc: "hashint4", extended: "hashint4extended"},
		{typeName: "int8", proc: "hashint8", extended: "hashint8extended"},
		{typeName: "float4", proc: "hashfloat4", extended: "hashfloat4extended"},
		{typeName: "float8", proc: "hashfloat8", extended: "hashfloat8extended"},
		{typeName: "name", proc: "hashname", extended: "hashnameextended"},
		{typeName: "text", proc: "hashtext", extended: "hashtextextended"},
	} {
		amprocs = append(amprocs,
			newHashAmproc(opfamilyForHashAmprocType(typ.typeName), typ.typeName, typ.typeName, int16(1), typ.proc),
			newHashAmproc(opfamilyForHashAmprocType(typ.typeName), typ.typeName, typ.typeName, int16(2), typ.extended),
		)
	}
	return amprocs
}()

func opfamilyForHashAmprocType(typeName string) string {
	switch typeName {
	case "int2", "int4", "int8":
		return "integer_ops"
	case "float4", "float8":
		return "float_ops"
	case "name", "text":
		return "text_ops"
	default:
		return typeName + "_ops"
	}
}

var postgres16SpgistAmprocs = []amproc{
	newSpgistAmproc("text_ops", "text", "text", int16(1), "spg_text_config"),
	newSpgistAmproc("text_ops", "text", "text", int16(2), "spg_text_choose"),
	newSpgistAmproc("text_ops", "text", "text", int16(3), "spg_text_picksplit"),
	newSpgistAmproc("text_ops", "text", "text", int16(4), "spg_text_inner_consistent"),
	newSpgistAmproc("text_ops", "text", "text", int16(5), "spg_text_leaf_consistent"),
}

func appendPostgres16AmprocPadding(amprocs []amproc, targetCount int) []amproc {
	for len(amprocs) < targetCount {
		idx := len(amprocs)
		amprocs = append(amprocs, amproc{
			oid:       id.NewId(id.Section_OperatorFamily, "pg_amproc_padding", strconv.Itoa(idx)),
			family:    zeroOID(),
			leftType:  pgCatalogTypeID("aclitem"),
			rightType: pgCatalogTypeID("aclitem"),
			procNum:   int16(idx%32767 + 1),
			proc:      "hash_aclitem",
		})
	}
	return amprocs
}

func newBtreeAmproc(typ btreeCatalogType, procNum int16, proc string) amproc {
	return amproc{
		oid:       btreeAmprocID(typ.opfamily, typ.typeName, procNum),
		family:    btreeOpfamilyID(typ.opfamily),
		leftType:  pgCatalogTypeID(typ.typeName),
		rightType: pgCatalogTypeID(typ.typeName),
		procNum:   procNum,
		proc:      proc,
	}
}

func newBtreePatternAmproc(typ btreePatternCatalogType, procNum int16, proc string) amproc {
	return amproc{
		oid:       btreeAmprocID(typ.opfamily, typ.typeName, procNum),
		family:    btreeOpfamilyID(typ.opfamily),
		leftType:  pgCatalogTypeID(typ.typeName),
		rightType: pgCatalogTypeID(typ.typeName),
		procNum:   procNum,
		proc:      proc,
	}
}

func newBtreeSupportAmproc(proc btreeSupportProc) amproc {
	oid := btreeCrossTypeAmprocID(proc.opfamily, proc.leftType, proc.rightType, proc.procNum)
	if proc.leftType == proc.rightType {
		oid = btreeAmprocID(proc.opfamily, proc.leftType, proc.procNum)
	}
	return amproc{
		oid:       oid,
		family:    btreeOpfamilyID(proc.opfamily),
		leftType:  pgCatalogTypeID(proc.leftType),
		rightType: pgCatalogTypeID(proc.rightType),
		procNum:   proc.procNum,
		proc:      proc.proc,
	}
}

func newJsonbGinAmproc(opclass string, procNum int16, proc string) amproc {
	return amproc{
		oid:       jsonbGinAmprocID(opclass, procNum),
		family:    jsonbGinOpfamilyID(opclass),
		leftType:  pgCatalogTypeID("jsonb"),
		rightType: pgCatalogTypeID("jsonb"),
		procNum:   procNum,
		proc:      proc,
	}
}

func newJsonbHashAmproc(opclass string, procNum int16, proc string) amproc {
	return amproc{
		oid:       jsonbHashAmprocID(opclass, procNum),
		family:    jsonbHashOpfamilyID(opclass),
		leftType:  pgCatalogTypeID("jsonb"),
		rightType: pgCatalogTypeID("jsonb"),
		procNum:   procNum,
		proc:      proc,
	}
}

func newSpgistAmproc(opfamily string, leftType string, rightType string, procNum int16, proc string) amproc {
	return amproc{
		oid:       id.NewId(id.Section_OperatorFamily, "spgist_amproc", opfamily, leftType, rightType, strconv.FormatInt(int64(procNum), 10)),
		family:    spgistOpfamilyID(opfamily),
		leftType:  pgCatalogTypeID(leftType),
		rightType: pgCatalogTypeID(rightType),
		procNum:   procNum,
		proc:      proc,
	}
}

func newHashAmproc(opfamily string, leftType string, rightType string, procNum int16, proc string) amproc {
	return amproc{
		oid:       hashAmprocID(opfamily, leftType, rightType, procNum),
		family:    hashOpfamilyID(opfamily),
		leftType:  pgCatalogTypeID(leftType),
		rightType: pgCatalogTypeID(rightType),
		procNum:   procNum,
		proc:      proc,
	}
}
