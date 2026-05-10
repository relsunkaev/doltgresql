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

// PgOpfamilyName is a constant to the pg_opfamily name.
const PgOpfamilyName = "pg_opfamily"

// InitPgOpfamily handles registration of the pg_opfamily handler.
func InitPgOpfamily() {
	tables.AddHandler(PgCatalogName, PgOpfamilyName, PgOpfamilyHandler{})
}

// PgOpfamilyHandler is the handler for the pg_opfamily table.
type PgOpfamilyHandler struct{}

var _ tables.Handler = PgOpfamilyHandler{}

// Name implements the interface tables.Handler.
func (p PgOpfamilyHandler) Name() string {
	return PgOpfamilyName
}

// RowIter implements the interface tables.Handler.
func (p PgOpfamilyHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	opfamilies, err := appendBtreeGistOpfamilies(ctx, defaultPostgresOpfamilies)
	if err != nil {
		return nil, err
	}
	opfamilies, err = appendVectorOpfamilies(ctx, opfamilies)
	if err != nil {
		return nil, err
	}
	opfamilies, err = appendHstoreOpfamilies(ctx, opfamilies)
	if err != nil {
		return nil, err
	}
	opfamilies, err = appendCitextOpfamilies(ctx, opfamilies)
	if err != nil {
		return nil, err
	}
	return &pgOpfamilyRowIter{
		opfamilies: opfamilies,
		idx:        0,
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgOpfamilyHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgOpfamilySchema,
		PkOrdinals: nil,
	}
}

// pgOpfamilySchema is the schema for pg_opfamily.
var pgOpfamilySchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpfamilyName},
	{Name: "opfmethod", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpfamilyName},
	{Name: "opfname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgOpfamilyName},
	{Name: "opfnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpfamilyName},
	{Name: "opfowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpfamilyName},
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpfamilyName},
}

// pgOpfamilyRowIter is the sql.RowIter for the pg_opfamily table.
type pgOpfamilyRowIter struct {
	opfamilies []opfamily
	idx        int
}

var _ sql.RowIter = (*pgOpfamilyRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgOpfamilyRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.opfamilies) {
		return nil, io.EOF
	}
	iter.idx++
	opfamily := iter.opfamilies[iter.idx-1]

	return sql.Row{
		opfamily.oid,                          // oid
		opfamily.opfmethod,                    // opfmethod
		opfamily.opfname,                      // opfname
		opfamily.namespace,                    // opfnamespace
		id.NewId(id.Section_User, "postgres"), // opfowner
		id.NewTable(PgCatalogName, PgOpfamilyName).AsId(), // tableoid
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgOpfamilyRowIter) Close(ctx *sql.Context) error {
	return nil
}

type opfamily struct {
	oid       id.Id
	opfmethod id.Id
	opfname   string
	namespace id.Id
}

var defaultPostgresOpfamilies = []opfamily{
	newBtreeOpfamily("bit_ops"),
	newBtreeOpfamily("bool_ops"),
	newBtreeOpfamily("integer_ops"),
	newBtreeOpfamily("float_ops"),
	newBtreeOpfamily("numeric_ops"),
	newBtreeOpfamily("char_ops"),
	newBtreeOpfamily("text_ops"),
	newBtreeOpfamily("bpchar_ops"),
	newBtreeOpfamily("bytea_ops"),
	newBtreeOpfamily(indexmetadata.OpClassTextPatternOps),
	newBtreeOpfamily(indexmetadata.OpClassBpcharPatternOps),
	newBtreeOpfamily("datetime_ops"),
	newBtreeOpfamily("interval_ops"),
	newBtreeOpfamily(indexmetadata.OpClassJsonbOps),
	newBtreeOpfamily("oid_ops"),
	newBtreeOpfamily("oidvector_ops"),
	newBtreeOpfamily("pg_lsn_ops"),
	newBtreeOpfamily("time_ops"),
	newBtreeOpfamily("timetz_ops"),
	newBtreeOpfamily("uuid_ops"),
	newBtreeOpfamily("varbit_ops"),
	newJsonbGinOpfamily(indexmetadata.OpClassJsonbOps),
	newJsonbGinOpfamily(indexmetadata.OpClassJsonbPathOps),
}

func btreeOpfamilyID(name string) id.Id {
	return id.NewId(id.Section_OperatorFamily, indexmetadata.AccessMethodBtree, name)
}

func newBtreeOpfamily(name string) opfamily {
	return opfamily{
		oid:       btreeOpfamilyID(name),
		opfmethod: id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
		opfname:   name,
		namespace: pgCatalogNamespaceID(),
	}
}

func newJsonbGinOpfamily(name string) opfamily {
	return opfamily{
		oid:       jsonbGinOpfamilyID(name),
		opfmethod: id.NewAccessMethod(indexmetadata.AccessMethodGin).AsId(),
		opfname:   name,
		namespace: pgCatalogNamespaceID(),
	}
}
