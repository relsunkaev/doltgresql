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
	"reflect"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/replicaidentity"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgIndexName is a constant to the pg_index name.
const PgIndexName = "pg_index"

// InitPgIndex handles registration of the pg_index handler.
func InitPgIndex() {
	tables.AddHandler(PgCatalogName, PgIndexName, PgIndexHandler{})
}

// PgIndexHandler is the handler for the pg_index table.
type PgIndexHandler struct{}

var _ tables.Handler = PgIndexHandler{}
var _ tables.IndexedTableHandler = PgIndexHandler{}

// Name implements the interface tables.Handler.
func (p PgIndexHandler) Name() string {
	return PgIndexName
}

// RowIter implements the interface tables.Handler.
func (p PgIndexHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	// Use cached data from this session if it exists
	pgCatalogCache, err := getPgCatalogCache(ctx)
	if err != nil {
		return nil, err
	}

	if pgCatalogCache.pgIndexes == nil {
		err = cachePgIndexes(ctx, pgCatalogCache)
		if err != nil {
			return nil, err
		}
	}

	if indexIdxPart, ok := partition.(inMemIndexPartition); ok {
		return &inMemIndexScanIter[*pgIndex]{
			lookup:         indexIdxPart.lookup,
			rangeConverter: p,
			btreeAccess:    pgCatalogCache.pgIndexes,
			rowConverter:   pgIndexToRow,
		}, nil
	}

	return &pgIndexTableScanIter{
		indexCache: pgCatalogCache.pgIndexes,
		idx:        0,
	}, nil
}

// PkSchema implements the interface tables.Handler.
func (p PgIndexHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgIndexSchema,
		PkOrdinals: nil,
	}
}

// Indexes implements tables.IndexedTableHandler.
func (p PgIndexHandler) Indexes() ([]sql.Index, error) {
	return []sql.Index{
		pgCatalogInMemIndex{
			name:        "pg_index_indexrelid_index",
			tblName:     "pg_index",
			dbName:      "pg_catalog",
			uniq:        true,
			columnExprs: []sql.ColumnExpressionType{{Expression: "pg_index.indexrelid", Type: pgtypes.Oid}},
		},
		pgCatalogInMemIndex{
			name:        "pg_index_indrelid_index",
			tblName:     "pg_index",
			dbName:      "pg_catalog",
			uniq:        false,
			columnExprs: []sql.ColumnExpressionType{{Expression: "pg_index.indrelid", Type: pgtypes.Oid}},
		},
	}, nil
}

// LookupPartitions implements tables.IndexedTableHandler.
func (p PgIndexHandler) LookupPartitions(context *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return &inMemIndexPartIter{
		part: inMemIndexPartition{
			idxName: lookup.Index.(pgCatalogInMemIndex).name,
			lookup:  lookup,
		},
	}, nil
}

// getIndexScanRange implements the interface RangeConverter.
func (p PgIndexHandler) getIndexScanRange(rng sql.Range, index sql.Index) (*pgIndex, bool, *pgIndex, bool) {
	var gte, lt *pgIndex
	var hasLowerBound, hasUpperBound bool

	switch index.(pgCatalogInMemIndex).name {
	case "pg_index_indexrelid_index":
		msrng := rng.(sql.MySQLRange)
		oidRng := msrng[0]
		if oidRng.HasLowerBound() {
			lb := sql.GetMySQLRangeCutKey(oidRng.LowerBound)
			if lb != nil {
				lowerRangeCutKey := lb.(id.Id)
				gte = &pgIndex{
					indexOidNative: idToOid(lowerRangeCutKey),
				}
				hasLowerBound = true
			}
		}
		if oidRng.HasUpperBound() {
			ub := sql.GetMySQLRangeCutKey(oidRng.UpperBound)
			if ub != nil {
				upperRangeCutKey := ub.(id.Id)
				lt = &pgIndex{
					indexOidNative: idToOid(upperRangeCutKey) + 1,
				}
				hasUpperBound = true
			}
		}

	case "pg_index_indrelid_index":
		msrng := rng.(sql.MySQLRange)
		oidRng := msrng[0]
		if oidRng.HasLowerBound() {
			lb := sql.GetMySQLRangeCutKey(oidRng.LowerBound)
			if lb != nil {
				lowerRangeCutKey := lb.(id.Id)
				gte = &pgIndex{
					tableOidNative: idToOid(lowerRangeCutKey),
				}
				hasLowerBound = true
			}
		}
		if oidRng.HasUpperBound() {
			ub := sql.GetMySQLRangeCutKey(oidRng.UpperBound)
			if ub != nil {
				upperRangeCutKey := ub.(id.Id)
				lt = &pgIndex{
					tableOidNative: idToOid(upperRangeCutKey) + 1,
				}
				hasUpperBound = true
			}
		}
	default:
		panic("unknown index name: " + index.(pgCatalogInMemIndex).name)
	}

	return gte, hasLowerBound, lt, hasUpperBound
}

// pgIndexSchema is the schema for pg_index.
var pgIndexSchema = sql.Schema{
	{Name: "indexrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indnatts", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indnkeyatts", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisunique", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indnullsnotdistinct", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisprimary", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisexclusion", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indimmediate", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisclustered", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisvalid", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indcheckxmin", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisready", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indislive", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indisreplident", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indkey", Type: pgtypes.Int16vector, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indcollation", Type: pgtypes.Oidvector, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indclass", Type: pgtypes.Oidvector, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indoption", Type: pgtypes.Int16vector, Default: nil, Nullable: false, Source: PgIndexName},
	{Name: "indexprs", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgIndexName}, // TODO: type pg_node_tree, collation C
	{Name: "indpred", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgIndexName},  // TODO: type pg_node_tree, collation C
}

// pgIndex represents a row in the pg_index table.
// We store oids in their native format as well so that we can do range scans on them.
type pgIndex struct {
	index          sql.Index
	indexName      string
	schemaName     string
	indexOid       id.Id
	indexOidNative uint32
	tableOid       id.Id
	tableOidNative uint32
	indnatts       int16
	indnkeyatts    int16
	indisunique    bool
	indisprimary   bool
	indisreplident bool
	indkey         []any
	indcollation   []any
	indclass       []any
	indexprs       any
	indpred        any
	indexdef       string
}

// lessIndexOid is a sort function for pgIndex based on indexrelid.
func lessIndexOid(a, b *pgIndex) bool {
	return a.indexOidNative < b.indexOidNative
}

// lessIndrelid is a sort function for pgIndex based on indrelid.
func lessIndrelid(a, b []*pgIndex) bool {
	return a[0].tableOidNative < b[0].tableOidNative
}

// pgIndexTableScanIter is the sql.RowIter for the pg_index table.
type pgIndexTableScanIter struct {
	indexCache *pgIndexCache
	idx        int
}

var _ sql.RowIter = (*pgIndexTableScanIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgIndexTableScanIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.indexCache.indexes) {
		return nil, io.EOF
	}
	iter.idx++
	index := iter.indexCache.indexes[iter.idx-1]

	return pgIndexToRow(index), nil
}

// Close implements the interface sql.RowIter.
func (iter *pgIndexTableScanIter) Close(ctx *sql.Context) error {
	return nil
}

// pgIndexToRow converts a pgIndex to a sql.Row.
func pgIndexToRow(index *pgIndex) sql.Row {
	indcollation := index.indcollation
	if indcollation == nil {
		indcollation = []any{}
	}
	indclass := index.indclass
	if indclass == nil {
		indclass = []any{}
	}
	indoption := indexmetadata.IndOptionValues(index.index.Comment(), int(index.indnkeyatts))
	return sql.Row{
		index.indexOid,         // indexrelid
		index.tableOid,         // indrelid
		index.indnatts,         // indnatts
		index.indnkeyatts,      // indnkeyatts
		index.index.IsUnique(), // indisunique
		false,                  // indnullsnotdistinct
		index.indisprimary,     // indisprimary
		false,                  // indisexclusion
		index.index.IsUnique(), // indimmediate
		false,                  // indisclustered
		true,                   // indisvalid
		false,                  // indcheckxmin
		true,                   // indisready
		true,                   // indislive
		index.indisreplident,   // indisreplident
		index.indkey,           // indkey
		indcollation,           // indcollation
		indclass,               // indclass
		indoption,              // indoption
		index.indexprs,         // indexprs
		index.indpred,          // indpred
	}
}

// cachePgIndexes caches the pg_index data for the current database in the session.
func cachePgIndexes(ctx *sql.Context, pgCatalogCache *pgCatalogCache) error {
	var indexes []*pgIndex
	indexOidIdx := NewUniqueInMemIndexStorage[*pgIndex](lessIndexOid)
	indrelidIdx := NewNonUniqueInMemIndexStorage[*pgIndex](lessIndrelid)

	tableSchemas := make(map[id.Id]sql.Schema)
	tableNames := make(map[id.Id]string)

	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			if tableSchemas[table.OID.AsId()] == nil {
				tableSchemas[table.OID.AsId()] = table.Item.Schema(ctx)
			}
			if tableNames[table.OID.AsId()] == "" {
				tableNames[table.OID.AsId()] = table.Item.Name()
			}

			s := tableSchemas[table.OID.AsId()]
			logicalColumns := indexmetadata.LogicalColumns(index.Item, s)
			indexColumns := make([]string, len(logicalColumns))
			includeColumns := indexmetadata.IncludeColumns(index.Item.Comment())
			indKey := make([]any, 0, len(logicalColumns)+len(includeColumns))
			for i, col := range logicalColumns {
				indexColumns[i] = col.StorageName
				if col.Expression {
					indKey = append(indKey, int16(0))
					continue
				}
				indKey = append(indKey, visibleAttributeNumber(s, col.StorageName))
			}
			for _, includeColumn := range includeColumns {
				indKey = append(indKey, visibleAttributeNumber(s, includeColumn))
			}
			indexExpressions := indexmetadata.ExpressionDefinitions(index.Item, s)
			var indexprs any
			if len(indexExpressions) > 0 {
				indexprs = strings.Join(indexExpressions, ", ")
			}
			var indpred any
			if predicate := indexmetadata.Predicate(index.Item.Comment()); predicate != "" {
				indpred = predicate
			}
			indClass := indexOpClassIds(index.Item, s, indexColumns)
			indCollation := indexCollationIds(index.Item, s, indexColumns)

			pgIdx := &pgIndex{
				index:          index.Item,
				indexName:      formatIndexNameForTable(index.Item, table.Item),
				schemaName:     schema.Item.SchemaName(),
				indexOid:       index.OID.AsId(),
				indexOidNative: id.Cache().ToOID(index.OID.AsId()),
				tableOid:       table.OID.AsId(),
				tableOidNative: id.Cache().ToOID(table.OID.AsId()),
				indkey:         indKey,
				indnatts:       int16(len(indKey)),
				indnkeyatts:    int16(len(indexColumns)),
				indisunique:    index.Item.IsUnique(),
				indisprimary:   strings.ToLower(index.Item.ID()) == "primary",
				indcollation:   indCollation,
				indclass:       indClass,
				indexprs:       indexprs,
				indpred:        indpred,
				indexdef:       indexmetadata.DefinitionForTable(index.Item, schema.Item.SchemaName(), table.Item, s),
			}
			replicaIdent := replicaidentity.Get(ctx.GetCurrentDatabase(), schema.Item.SchemaName(), table.Item.Name())
			pgIdx.indisreplident = replicaIdent.Identity == replicaidentity.IdentityUsingIndex &&
				strings.EqualFold(replicaIdent.IndexName, pgIdx.indexName)

			indexOidIdx.Add(pgIdx)
			indrelidIdx.Add(pgIdx)
			indexes = append(indexes, pgIdx)
			return true, nil
		},
	})
	if err != nil {
		return err
	}

	pgCatalogCache.pgIndexes = &pgIndexCache{
		indexes:     indexes,
		tableNames:  tableNames,
		indexOidIdx: indexOidIdx,
		indrelidIdx: indrelidIdx,
	}

	return nil
}

func visibleAttributeNumber(schema sql.Schema, colName string) int16 {
	attnum := int16(0)
	for _, col := range schema {
		if col.HiddenSystem {
			continue
		}
		attnum++
		if strings.EqualFold(col.Name, colName) {
			return attnum
		}
	}
	return 0
}

func opClassIds(opClasses []string) []any {
	ids := make([]any, len(opClasses))
	for i, opClass := range opClasses {
		ids[i] = pgCatalogOpclassID(indexmetadata.AccessMethodBtree, opClass)
	}
	return ids
}

func indexOpClassIds(index sql.Index, schema sql.Schema, indexColumns []string) []any {
	accessMethod := indexmetadata.AccessMethod(index.IndexType(), index.Comment())
	opClasses := indexmetadata.OpClasses(index.Comment())
	if len(opClasses) > 0 {
		return indexOpClassIdsWithDefaults(accessMethod, schema, indexColumns, opClasses)
	}
	if accessMethod != indexmetadata.AccessMethodBtree {
		return nil
	}

	ids := make([]any, len(indexColumns))
	for i, colName := range indexColumns {
		colIdx := schema.IndexOfColName(colName)
		if colIdx < 0 {
			return nil
		}
		opClass, ok := defaultBtreeOpClassForType(schema[colIdx].Type)
		if !ok {
			return nil
		}
		ids[i] = pgCatalogOpclassID(indexmetadata.AccessMethodBtree, opClass)
	}
	return ids
}

func indexOpClassIdsWithDefaults(accessMethod string, schema sql.Schema, indexColumns []string, opClasses []string) []any {
	ids := make([]any, len(indexColumns))
	for i, colName := range indexColumns {
		if i < len(opClasses) && opClasses[i] != "" {
			ids[i] = pgCatalogOpclassID(accessMethod, opClasses[i])
			continue
		}
		if accessMethod != indexmetadata.AccessMethodBtree {
			return nil
		}
		colIdx := schema.IndexOfColName(colName)
		if colIdx < 0 {
			return nil
		}
		opClass, ok := defaultBtreeOpClassForType(schema[colIdx].Type)
		if !ok {
			return nil
		}
		ids[i] = pgCatalogOpclassID(indexmetadata.AccessMethodBtree, opClass)
	}
	return ids
}

func indexCollationIds(index sql.Index, schema sql.Schema, indexColumns []string) []any {
	collations := indexmetadata.Collations(index.Comment())
	ids := make([]any, len(indexColumns))
	for i, colName := range indexColumns {
		if i < len(collations) && collations[i] != "" {
			ids[i] = id.NewCollation("pg_catalog", collations[i]).AsId()
			continue
		}
		colIdx := schema.IndexOfColName(colName)
		if colIdx < 0 {
			return nil
		}
		ids[i] = collationIDForType(schema[colIdx].Type)
	}
	return ids
}

func collationIDForType(typ sql.Type) id.Id {
	doltgresType, ok := doltgresType(typ)
	if !ok {
		return id.Null
	}
	return collationIDForDoltgresType(doltgresType)
}

func collationIDForDoltgresType(doltgresType *pgtypes.DoltgresType) id.Id {
	if doltgresType == nil || doltgresType.TypCollation == id.NullCollation {
		return id.Null
	}
	return doltgresType.TypCollation.AsId()
}

func defaultBtreeOpClassForType(typ sql.Type) (string, bool) {
	typeName, ok := doltgresTypeName(typ)
	if !ok {
		return "", false
	}

	switch typeName {
	case "bit":
		return "bit_ops", true
	case "bool":
		return "bool_ops", true
	case "int2":
		return "int2_ops", true
	case "int4":
		return "int4_ops", true
	case "int8":
		return "int8_ops", true
	case "float4":
		return "float4_ops", true
	case "float8":
		return "float8_ops", true
	case "numeric":
		return "numeric_ops", true
	case "char":
		return "char_ops", true
	case "name":
		return "name_ops", true
	case "text":
		return "text_ops", true
	case "varchar":
		return "varchar_ops", true
	case "bpchar":
		return "bpchar_ops", true
	case "bytea":
		return "bytea_ops", true
	case "date":
		return "date_ops", true
	case "interval":
		return "interval_ops", true
	case "jsonb":
		return "jsonb_ops", true
	case "oid":
		return "oid_ops", true
	case "oidvector":
		return "oidvector_ops", true
	case "pg_lsn":
		return "pg_lsn_ops", true
	case "time":
		return "time_ops", true
	case "timestamp":
		return "timestamp_ops", true
	case "timestamptz":
		return "timestamptz_ops", true
	case "timetz":
		return "timetz_ops", true
	case "uuid":
		return "uuid_ops", true
	case "varbit":
		return "varbit_ops", true
	default:
		return "", false
	}
}

func doltgresTypeName(typ sql.Type) (string, bool) {
	doltgresType, ok := doltgresType(typ)
	if !ok {
		return "", false
	}
	return doltgresType.ID.TypeName(), true
}

func doltgresType(typ sql.Type) (*pgtypes.DoltgresType, bool) {
	if typ == nil || isNilType(typ) {
		return nil, false
	}
	if typ, ok := typ.(*pgtypes.DoltgresType); ok {
		return typ, true
	}

	doltgresType, ok := doltgresTypeFromGmsType(typ)
	if !ok {
		return nil, false
	}
	return doltgresType, true
}

func doltgresTypeFromGmsType(typ sql.Type) (doltgresType *pgtypes.DoltgresType, ok bool) {
	defer func() {
		if recover() != nil {
			doltgresType = nil
			ok = false
		}
	}()
	// Some compatibility schemas still expose wrapper sql.Type values with unresolved inner types.
	// For indclass derivation, a failed conversion only means no default opclass is known.
	doltgresType, err := pgtypes.FromGmsTypeToDoltgresType(typ)
	return doltgresType, err == nil && doltgresType != nil
}

func isNilType(typ sql.Type) bool {
	value := reflect.ValueOf(typ)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
