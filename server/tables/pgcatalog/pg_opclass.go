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
	opclasses, err := appendBtreeGistOpclasses(ctx, defaultPostgresOpclasses)
	if err != nil {
		return nil, err
	}
	opclasses, err = appendVectorOpclasses(ctx, opclasses)
	if err != nil {
		return nil, err
	}
	opclasses, err = appendHstoreOpclasses(ctx, opclasses)
	if err != nil {
		return nil, err
	}
	opclasses, err = appendCitextOpclasses(ctx, opclasses)
	if err != nil {
		return nil, err
	}
	return &pgOpclassRowIter{
		opclasses: opclasses,
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
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOpclassName, Hidden: true},
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
		opclass.oid,                           // oid
		opclass.opcmethod,                     // opcmethod
		opclass.opcname,                       // opcname
		opclass.namespace,                     // opcnamespace
		id.NewId(id.Section_User, "postgres"), // opcowner
		opclass.family,                        // opcfamily
		opclass.intype,                        // opcintype
		opclass.isDefault,                     // opcdefault
		opclass.keytype,                       // opckeytype
		id.NewTable(PgCatalogName, PgOpclassName).AsId(), // tableoid
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
	keytype   id.Id
	isDefault bool
}

var defaultPostgresOpclasses = []opclass{
	newBtreeOpclass("bit_ops", "bit", "bit_ops"),
	newBtreeOpclass("bool_ops", "bool", "bool_ops"),
	newBtreeOpclass("int2_ops", "int2", "integer_ops"),
	newBtreeOpclass("int4_ops", "int4", "integer_ops"),
	newBtreeOpclass("int8_ops", "int8", "integer_ops"),
	newHashOpclass("int4_ops", "int4", "integer_ops"),
	newBtreeOpclass("float4_ops", "float4", "float_ops"),
	newBtreeOpclass("float8_ops", "float8", "float_ops"),
	newBtreeOpclass("numeric_ops", "numeric", "numeric_ops"),
	newBtreeOpclass("char_ops", "char", "char_ops"),
	newBtreeOpclassWithKeyType("name_ops", "name", "text_ops", "cstring"),
	newBtreeOpclass("text_ops", "text", "text_ops"),
	newBtreeOpclass("varchar_ops", "text", "text_ops"),
	newBtreeOpclass("bpchar_ops", "bpchar", "bpchar_ops"),
	newBtreeOpclass("bytea_ops", "bytea", "bytea_ops"),
	newBtreeOpclassWithDefault(indexmetadata.OpClassTextPatternOps, "text", "text_pattern_ops", false),
	newBtreeOpclassWithDefault(indexmetadata.OpClassVarcharPatternOps, "text", "text_pattern_ops", false),
	newBtreeOpclassWithDefault(indexmetadata.OpClassBpcharPatternOps, "bpchar", "bpchar_pattern_ops", false),
	newBtreeOpclass("date_ops", "date", "datetime_ops"),
	newBtreeOpclass("interval_ops", "interval", "interval_ops"),
	newBtreeOpclass(indexmetadata.OpClassJsonbOps, "jsonb", "jsonb_ops"),
	newBtreeOpclass("oid_ops", "oid", "oid_ops"),
	newBtreeOpclass("oidvector_ops", "oidvector", "oidvector_ops"),
	newBtreeOpclass("pg_lsn_ops", "pg_lsn", "pg_lsn_ops"),
	newBtreeOpclass("time_ops", "time", "time_ops"),
	newBtreeOpclass("timestamp_ops", "timestamp", "datetime_ops"),
	newBtreeOpclass("timestamptz_ops", "timestamptz", "datetime_ops"),
	newBtreeOpclass("timetz_ops", "timetz", "timetz_ops"),
	newBtreeOpclass("uuid_ops", "uuid", "uuid_ops"),
	newBtreeOpclass("varbit_ops", "varbit", "varbit_ops"),
	newJsonbGinOpclass(indexmetadata.OpClassJsonbOps, pgCatalogTypeID("text"), true),
	newJsonbGinOpclass(indexmetadata.OpClassJsonbPathOps, pgCatalogTypeID("int4"), false),
	newJsonbHashOpclass(indexmetadata.OpClassJsonbOps),
}

func newBtreeOpclass(name string, typeName string, family string) opclass {
	return newBtreeOpclassWithDefault(name, typeName, family, true)
}

func newBtreeOpclassWithDefault(name string, typeName string, family string, isDefault bool) opclass {
	return newBtreeOpclassWithKeyTypeAndDefault(name, typeName, family, "", isDefault)
}

func newBtreeOpclassWithKeyType(name string, typeName string, family string, keyType string) opclass {
	return newBtreeOpclassWithKeyTypeAndDefault(name, typeName, family, keyType, true)
}

func newBtreeOpclassWithKeyTypeAndDefault(name string, typeName string, family string, keyType string, isDefault bool) opclass {
	keyTypeID := zeroOID()
	if keyType != "" {
		keyTypeID = pgCatalogTypeID(keyType)
	}
	return opclass{
		oid:       pgCatalogOpclassID(indexmetadata.AccessMethodBtree, name),
		opcmethod: id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
		opcname:   name,
		namespace: pgCatalogNamespaceID(),
		family:    btreeOpfamilyID(family),
		intype:    pgCatalogTypeID(typeName),
		keytype:   keyTypeID,
		isDefault: isDefault,
	}
}

func newJsonbGinOpclass(name string, keytype id.Id, isDefault bool) opclass {
	return opclass{
		oid:       pgCatalogOpclassID(indexmetadata.AccessMethodGin, name),
		opcmethod: id.NewAccessMethod(indexmetadata.AccessMethodGin).AsId(),
		opcname:   name,
		namespace: pgCatalogNamespaceID(),
		family:    jsonbGinOpfamilyID(name),
		intype:    pgCatalogTypeID("jsonb"),
		keytype:   keytype,
		isDefault: isDefault,
	}
}

func newJsonbHashOpclass(name string) opclass {
	return newHashOpclass(name, "jsonb", name)
}

func newHashOpclass(name string, typeName string, family string) opclass {
	return opclass{
		oid:       pgCatalogOpclassID(accessMethodHash, name),
		opcmethod: id.NewAccessMethod(accessMethodHash).AsId(),
		opcname:   name,
		namespace: pgCatalogNamespaceID(),
		family:    hashOpfamilyID(family),
		intype:    pgCatalogTypeID(typeName),
		keytype:   zeroOID(),
		isDefault: true,
	}
}
