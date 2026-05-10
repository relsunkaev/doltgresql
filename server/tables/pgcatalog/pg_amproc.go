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
	amprocs := make([]amproc, 0, len(btreeCatalogTypes)+10)
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
	)
	return amprocs
}()

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
