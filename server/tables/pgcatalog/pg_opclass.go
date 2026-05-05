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

// PgOpclassName is a constant to the pg_opclass name.
const PgOpclassName = "pg_opclass"

// InitPgOpclass handles registration of the pg_opclass handler.
func InitPgOpclass() {
	tables.AddHandler(PgCatalogName, PgOpclassName, PgOpclassHandler{})
}

// PgOpclassHandler is the handler for the pg_opclass table.
type PgOpclassHandler struct{}

var _ tables.Handler = PgOpclassHandler{}

// Name implements the interface tables.Handler.
func (p PgOpclassHandler) Name() string {
	return PgOpclassName
}

// RowIter implements the interface tables.Handler.
func (p PgOpclassHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &pgOpclassRowIter{
		opclasses: defaultPostgresOpclasses,
		idx:       0,
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgOpclassHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgOpclassSchema,
		PkOrdinals: nil,
	}
}

// pgOpclassSchema is the schema for pg_opclass.
var pgOpclassSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcmethod", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcfamily", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcintype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opcdefault", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgOpclassName},
	{Name: "opckeytype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName},
}

// pgOpclassRowIter is the sql.RowIter for the pg_opclass table.
type pgOpclassRowIter struct {
	opclasses []opclass
	idx       int
}

var _ sql.RowIter = (*pgOpclassRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgOpclassRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.opclasses) {
		return nil, io.EOF
	}
	iter.idx++
	opclass := iter.opclasses[iter.idx-1]

	return sql.Row{
		opclass.oid,       // oid
		opclass.opcmethod, // opcmethod
		opclass.opcname,   // opcname
		opclass.namespace, // opcnamespace
		id.Null,           // opcowner
		opclass.family,    // opcfamily
		opclass.intype,    // opcintype
		opclass.isDefault, // opcdefault
		id.Null,           // opckeytype
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgOpclassRowIter) Close(ctx *sql.Context) error {
	return nil
}

type opclass struct {
	oid       id.Id
	opcmethod id.Id
	opcname   string
	namespace id.Id
	family    id.Id
	intype    id.Id
	isDefault bool
}

var defaultPostgresOpclasses = []opclass{
	newJsonbGinOpclass(indexmetadata.OpClassJsonbOps, true),
	newJsonbGinOpclass(indexmetadata.OpClassJsonbPathOps, false),
}

func newJsonbGinOpclass(name string, isDefault bool) opclass {
	return opclass{
		oid:       id.NewId(id.Section_OperatorClass, name),
		opcmethod: id.NewAccessMethod(indexmetadata.AccessMethodGin).AsId(),
		opcname:   name,
		namespace: id.NewNamespace("pg_catalog").AsId(),
		family:    id.NewId(id.Section_OperatorFamily, name),
		intype:    id.NewType("pg_catalog", "jsonb").AsId(),
		isDefault: isDefault,
	}
}
