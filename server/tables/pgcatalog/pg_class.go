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
	"fmt"
	"io"
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/core/triggers"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/replicaidentity"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgClassName is a constant to the pg_class name.
const PgClassName = "pg_class"

// InitPgClass handles registration of the pg_class handler.
func InitPgClass() {
	tables.AddHandler(PgCatalogName, PgClassName, PgClassHandler{})
}

// PgClassHandler is the handler for the pg_class table.
type PgClassHandler struct{}

var _ tables.Handler = PgClassHandler{}
var _ tables.IndexedTableHandler = PgClassHandler{}

// Name implements the interface tables.Handler.
func (p PgClassHandler) Name() string {
	return PgClassName
}

// RowIter implements the interface tables.Handler.
func (p PgClassHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	// Use cached data from this session if it exists
	pgCatalogCache, err := getPgCatalogCache(ctx)
	if err != nil {
		return nil, err
	}

	if pgCatalogCache.pgClasses == nil {
		err = cachePgClasses(ctx, pgCatalogCache)
		if err != nil {
			return nil, err
		}
	}

	if classIdxPart, ok := partition.(inMemIndexPartition); ok {
		return &inMemIndexScanIter[*pgClass]{
			lookup:         classIdxPart.lookup,
			rangeConverter: p,
			btreeAccess:    pgCatalogCache.pgClasses,
			rowConverter:   pgClassToRow,
		}, nil
	}

	return &pgClassTableScanIter{
		classCache: pgCatalogCache.pgClasses,
		idx:        0,
	}, nil
}

// cachePgClasses caches the pg_class data for the current database in the session.
func cachePgClasses(ctx *sql.Context, pgCatalogCache *pgCatalogCache) error {
	var classes []*pgClass
	tableHasIndexes := make(map[uint32]struct{})
	tableHasTriggers := make(map[uint32]struct{})
	nameIdx := NewUniqueInMemIndexStorage[*pgClass](lessName)
	oidIdx := NewUniqueInMemIndexStorage[*pgClass](lessOid)

	triggerCollection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	err = triggerCollection.IterateTriggers(ctx, func(trigger triggers.Trigger) (stop bool, err error) {
		tableID := id.NewTable(trigger.ID.SchemaName(), trigger.ID.TableName())
		tableHasTriggers[id.Cache().ToOID(tableID.AsId())] = struct{}{}
		return false, nil
	})
	if err != nil {
		return err
	}

	err = functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Index: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, index functions.ItemIndex) (cont bool, err error) {
			tableHasIndexes[id.Cache().ToOID(table.OID.AsId())] = struct{}{}
			schemaOid := schema.OID
			class := &pgClass{
				oid:             index.OID.AsId(),
				oidNative:       id.Cache().ToOID(index.OID.AsId()),
				name:            formatIndexNameForTable(index.Item, table.Item),
				hasIndexes:      false,
				kind:            "i",
				schemaOid:       schemaOid.AsId(),
				schemaOidNative: id.Cache().ToOID(schemaOid.AsId()),
				relType:         id.Null,
				relam:           id.NewAccessMethod(indexmetadata.AccessMethod(index.Item.IndexType(), index.Item.Comment())).AsId(),
				reloptions:      pgClassRelOptions(index.Item.Comment()),
			}
			nameIdx.Add(class)
			oidIdx.Add(class)
			classes = append(classes, class)
			return true, nil
		},
		ForeignKey: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable, foreignKey functions.ItemForeignKey) (cont bool, err error) {
			tableHasTriggers[id.Cache().ToOID(table.OID.AsId())] = struct{}{}

			parentSchema := foreignKey.Item.ParentSchema
			if parentSchema == "" {
				parentSchema = schema.Item.SchemaName()
			}
			parentTableID := id.NewTable(parentSchema, foreignKey.Item.ParentTable)
			tableHasTriggers[id.Cache().ToOID(parentTableID.AsId())] = struct{}{}
			return true, nil
		},
		Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
			_, hasIndexes := tableHasIndexes[id.Cache().ToOID(table.OID.AsId())]
			_, hasTriggers := tableHasTriggers[id.Cache().ToOID(table.OID.AsId())]
			kind := "r"
			if isMaterializedViewTable(table.Item) {
				kind = "m"
			}
			relOfType := id.Null
			comment := tableComment(table.Item)
			if typeID, ok := tablemetadata.OfType(comment); ok {
				relOfType = typeID.AsId()
			}
			relPersistence := tablemetadata.RelPersistence(comment)
			if relPersistence == "" {
				relPersistence = "p"
			}
			class := &pgClass{
				oid:             table.OID.AsId(),
				oidNative:       id.Cache().ToOID(table.OID.AsId()),
				name:            table.Item.Name(),
				schemaName:      schema.Item.SchemaName(),
				hasIndexes:      hasIndexes,
				hasTriggers:     hasTriggers,
				kind:            kind,
				schemaOid:       schema.OID.AsId(),
				schemaOidNative: id.Cache().ToOID(schema.OID.AsId()),
				replicaIdentity: replicaidentity.Get(ctx.GetCurrentDatabase(), schema.Item.SchemaName(), table.Item.Name()).Identity.String(),
				relType:         id.NewType(table.OID.SchemaName(), table.OID.SchemaName()).AsId(),
				relOfType:       relOfType,
				reloptions:      pgClassRelOptions(comment),
				relpersistence:  relPersistence,
			}
			nameIdx.Add(class)
			oidIdx.Add(class)
			classes = append(classes, class)
			return true, nil
		},
		View: func(ctx *sql.Context, schema functions.ItemSchema, view functions.ItemView) (cont bool, err error) {
			class := &pgClass{
				oid:             view.OID.AsId(),
				oidNative:       id.Cache().ToOID(view.OID.AsId()),
				name:            view.Item.Name,
				schemaName:      schema.Item.SchemaName(),
				hasIndexes:      false,
				kind:            "v",
				schemaOid:       schema.OID.AsId(),
				schemaOidNative: id.Cache().ToOID(schema.OID.AsId()),
				relType:         id.NewType(view.OID.SchemaName(), view.OID.SchemaName()).AsId(),
			}
			nameIdx.Add(class)
			oidIdx.Add(class)
			classes = append(classes, class)
			return true, nil
		},
		Sequence: func(ctx *sql.Context, schema functions.ItemSchema, sequence functions.ItemSequence) (cont bool, err error) {
			relPersistence := "p"
			if sequence.Item.Persistence == sequences.Persistence_Unlogged {
				relPersistence = "u"
			} else if sequence.Item.Persistence == sequences.Persistence_Temporary {
				relPersistence = "t"
			}
			class := &pgClass{
				oid:             sequence.OID.AsId(),
				oidNative:       id.Cache().ToOID(sequence.OID.AsId()),
				name:            sequence.Item.Id.SequenceName(),
				schemaName:      schema.Item.SchemaName(),
				hasIndexes:      false,
				kind:            "S",
				schemaOid:       schema.OID.AsId(),
				schemaOidNative: id.Cache().ToOID(schema.OID.AsId()),
				relType:         id.Null,
				relpersistence:  relPersistence,
			}
			nameIdx.Add(class)
			oidIdx.Add(class)
			classes = append(classes, class)
			return true, nil
		},
	})
	if err != nil {
		return err
	}
	pgCatalogSchemaID := id.NewNamespace(PgCatalogName).AsId()
	for _, handler := range tables.HandlersForSchema(PgCatalogName) {
		relationID := id.NewTable(PgCatalogName, handler.Name()).AsId()
		class := &pgClass{
			oid:             relationID,
			oidNative:       id.Cache().ToOID(relationID),
			name:            handler.Name(),
			schemaName:      PgCatalogName,
			hasIndexes:      false,
			kind:            "r",
			schemaOid:       pgCatalogSchemaID,
			schemaOidNative: id.Cache().ToOID(pgCatalogSchemaID),
			relType:         id.Null,
		}
		nameIdx.Add(class)
		oidIdx.Add(class)
		classes = append(classes, class)
	}

	pgCatalogCache.pgClasses = &pgClassCache{
		classes: classes,
		nameIdx: nameIdx,
		oidIdx:  oidIdx,
	}

	return nil
}

// formatIndexName returns the name of an index for display
func formatIndexName(idx sql.Index) string {
	return indexmetadata.DisplayName(idx)
	// TODO: Unnamed indexes should have below format
	// return fmt.Sprintf("%s_%s_key", idx.Table(), idx.ID())
}

// formatIndexNameForTable returns the name of an index for display using
// table-level Doltgres metadata when the native index cannot carry it.
func formatIndexNameForTable(idx sql.Index, table sql.Table) string {
	return indexmetadata.DisplayNameForTable(idx, table)
}

// getIndexScanRange implements the interface RangeConverter.
func (p PgClassHandler) getIndexScanRange(rng sql.Range, index sql.Index) (*pgClass, bool, *pgClass, bool) {
	var gte, lt *pgClass
	var hasLowerBound, hasUpperBound bool

	switch index.(pgCatalogInMemIndex).name {
	case "pg_class_oid_index":
		msrng := rng.(sql.MySQLRange)
		oidRng := msrng[0]
		if oidRng.HasLowerBound() {
			lb := sql.GetMySQLRangeCutKey(oidRng.LowerBound)
			if lb != nil {
				lowerRangeCutKey := lb.(id.Id)
				gte = &pgClass{
					oidNative: idToOid(lowerRangeCutKey),
				}
				hasLowerBound = true
			}
		}

		if oidRng.HasUpperBound() {
			ub := sql.GetMySQLRangeCutKey(oidRng.UpperBound)
			if ub != nil {
				upperRangeCutKey := ub.(id.Id)
				lt = &pgClass{
					oidNative: idToOid(upperRangeCutKey) + 1,
				}
				hasUpperBound = true
			}
		}

	case "pg_class_relname_nsp_index":
		msrng := rng.(sql.MySQLRange)
		relNameRange := msrng[0]
		schemaOidRange := msrng[1]
		var relnameLower, relnameUpper string
		schemaOidLower := uint32(0)
		schemaOidUpper := uint32(math.MaxUint32)
		schemaOidUpperSet := false

		if relNameRange.HasLowerBound() {
			lb := sql.GetMySQLRangeCutKey(relNameRange.LowerBound)
			if lb != nil {
				relnameLower = lb.(string)
				hasLowerBound = true
			}
		}
		if relNameRange.HasUpperBound() {
			ub := sql.GetMySQLRangeCutKey(relNameRange.UpperBound)
			if ub != nil {
				relnameUpper = ub.(string)
				hasUpperBound = true
			}
		}

		if schemaOidRange.HasLowerBound() {
			lb := sql.GetMySQLRangeCutKey(schemaOidRange.LowerBound)
			if lb != nil {
				lowerRangeCutKey := lb.(id.Id)
				schemaOidLower = idToOid(lowerRangeCutKey)
			}
		}
		if schemaOidRange.HasUpperBound() {
			ub := sql.GetMySQLRangeCutKey(schemaOidRange.UpperBound)
			if ub != nil {
				upperRangeCutKey := ub.(id.Id)
				schemaOidUpper = idToOid(upperRangeCutKey)
				schemaOidUpperSet = true
			}
		}

		if relNameRange.HasLowerBound() || schemaOidRange.HasLowerBound() {
			gte = &pgClass{
				name:            relnameLower,
				schemaOidNative: schemaOidLower,
			}
		}

		if relNameRange.HasUpperBound() || schemaOidRange.HasUpperBound() {
			// our less-than upper bound depends on whether we have a prefix match or both fields were set
			if !schemaOidUpperSet {
				relnameUpper = fmt.Sprintf("%s%o", relnameUpper, rune(0))
			} else {
				schemaOidUpper = schemaOidUpper + 1
			}
			lt = &pgClass{
				name:            relnameUpper,
				schemaOidNative: schemaOidUpper,
			}
		}
	default:
		panic("unknown index name: " + index.(pgCatalogInMemIndex).name)
	}

	return gte, hasLowerBound, lt, hasUpperBound
}

// idToOid converts an id.Id to its native uint32 OID representation. The type conversion process during index
// building will produce one of two values for comparison against an OID column: either a known OID value, which
// will be an Id of the appropriate type (Table, Namespace, etc), or an unknown value, which will be an oid.Oid.
func idToOid(i id.Id) uint32 {
	switch i.Section() {
	case id.Section_OID:
		return id.Oid(i).OID()
	default:
		return id.Cache().ToOID(i)
	}
}

// PkSchema implements the interface tables.Handler.
func (p PgClassHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgClassSchema,
		PkOrdinals: nil,
	}
}

// Indexes implements tables.IndexedTableHandler.
func (p PgClassHandler) Indexes() ([]sql.Index, error) {
	return []sql.Index{
		pgCatalogInMemIndex{
			name:        "pg_class_oid_index",
			tblName:     "pg_class",
			dbName:      "pg_catalog",
			uniq:        true,
			columnExprs: []sql.ColumnExpressionType{{Expression: "pg_class.oid", Type: pgtypes.Oid}},
		},
		pgCatalogInMemIndex{
			name:    "pg_class_relname_nsp_index",
			tblName: "pg_class",
			dbName:  "pg_catalog",
			uniq:    true,
			columnExprs: []sql.ColumnExpressionType{
				{Expression: "pg_class.relname", Type: pgtypes.Name},
				{Expression: "pg_class.relnamespace", Type: pgtypes.Oid},
			},
		},
	}, nil
}

// LookupPartitions implements tables.IndexedTableHandler.
func (p PgClassHandler) LookupPartitions(context *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return &inMemIndexPartIter{
		part: inMemIndexPartition{
			idxName: lookup.Index.(pgCatalogInMemIndex).name,
			lookup:  lookup,
		},
	}, nil
}

// pgClassSchema is the schema for pg_class.
var pgClassSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "reltype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "reloftype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relam", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relfilenode", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "reltablespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relpages", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "reltuples", Type: pgtypes.Float32, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relallvisible", Type: pgtypes.Int32, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "reltoastrelid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relhasindex", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relisshared", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relpersistence", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relkind", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relnatts", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relchecks", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relhasrules", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relhastriggers", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relhassubclass", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relrowsecurity", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relforcerowsecurity", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relispopulated", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relreplident", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relispartition", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relrewrite", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relfrozenxid", Type: pgtypes.Xid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relminmxid", Type: pgtypes.Xid, Default: nil, Nullable: false, Source: PgClassName},
	{Name: "relacl", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgClassName},     // TODO: type aclitem[]
	{Name: "reloptions", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgClassName}, // TODO: collation C
	{Name: "relpartbound", Type: pgtypes.Text, Default: nil, Nullable: true, Source: PgClassName},    // TODO: type pg_node_tree, collation C
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgClassName},
}

// pgClass represents a row in the pg_class table.
// We store oids in their native format as well so that we can do range scans on them.
type pgClass struct {
	oid             id.Id
	oidNative       uint32
	name            string
	schemaName      string
	schemaOid       id.Id
	schemaOidNative uint32
	hasIndexes      bool
	hasTriggers     bool
	replicaIdentity string
	kind            string // r = ordinary table, i = index, S = sequence, t = TOAST table, v = view, m = materialized view, c = composite type, f = foreign table, p = partitioned table, I = partitioned index
	relType         id.Id
	relOfType       id.Id
	relam           id.Id
	reloptions      []any
	relpersistence  string
}

// lessOid is a sort function for pgClass based on oid.
func lessOid(a, b *pgClass) bool {
	return a.oidNative < b.oidNative
}

// lessName is a sort function for pgClass based on name, then schemaOid.
func lessName(a, b *pgClass) bool {
	if a.name == b.name {
		return a.schemaOidNative < b.schemaOidNative
	}
	return a.name < b.name
}

// pgClassTableScanIter is the sql.RowIter for the pg_class table.
type pgClassTableScanIter struct {
	classCache *pgClassCache
	idx        int
}

var _ sql.RowIter = (*pgClassTableScanIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgClassTableScanIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.classCache.classes) {
		return nil, io.EOF
	}
	iter.idx++
	class := iter.classCache.classes[iter.idx-1]

	return pgClassToRow(class), nil
}

func pgClassToRow(class *pgClass) sql.Row {
	replicaIdentity := class.replicaIdentity
	if replicaIdentity == "" {
		replicaIdentity = replicaidentity.IdentityDefault.String()
	}
	var reloptions any
	if len(class.reloptions) > 0 {
		reloptions = class.reloptions
	}
	relPersistence := class.relpersistence
	if relPersistence == "" {
		relPersistence = "p"
	}

	// TODO: this is temporary definition of 'relam' field
	var relam = id.Null
	if class.kind == "i" {
		if class.relam.IsValid() {
			relam = class.relam
		} else {
			relam = id.NewAccessMethod("btree").AsId()
		}
	} else if class.kind == "r" || class.kind == "m" || class.kind == "t" {
		relam = id.NewAccessMethod("heap").AsId()
	}

	var relacl any
	switch class.kind {
	case "S":
		relacl = aclTextArray(auth.SequenceACLItems(class.schemaName, class.name))
	case "r", "m", "v":
		relacl = aclTextArray(auth.TableACLItems(class.schemaName, class.name))
	}

	// TODO: Fill in the rest of the pg_class columns
	return sql.Row{
		class.oid,                             // oid
		class.name,                            // relname
		class.schemaOid,                       // relnamespace
		class.relType,                         // reltype
		class.relOfType,                       // reloftype
		id.NewId(id.Section_User, "postgres"), // relowner
		relam,                                 // relam
		id.Null,                               // relfilenode
		id.Null,                               // reltablespace
		int32(0),                              // relpages
		float32(0),                            // reltuples
		int32(0),                              // relallvisible
		id.Null,                               // reltoastrelid
		class.hasIndexes,                      // relhasindex
		false,                                 // relisshared
		relPersistence,                        // relpersistence
		class.kind,                            // relkind
		int16(0),                              // relnatts
		int16(0),                              // relchecks
		false,                                 // relhasrules
		class.hasTriggers,                     // relhastriggers
		false,                                 // relhassubclass
		false,                                 // relrowsecurity
		false,                                 // relforcerowsecurity
		true,                                  // relispopulated
		replicaIdentity,                       // relreplident
		false,                                 // relispartition
		id.Null,                               // relrewrite
		uint32(0),                             // relfrozenxid
		uint32(0),                             // relminmxid
		relacl,                                // relacl
		reloptions,                            // reloptions
		nil,                                   // relpartbound
		id.NewTable(PgCatalogName, PgClassName).AsId(), // tableoid
	}
}

func pgClassRelOptions(comment string) []any {
	relOptions := indexmetadata.RelOptions(comment)
	if len(relOptions) == 0 {
		relOptions = tablemetadata.RelOptions(comment)
	}
	if len(relOptions) == 0 {
		return nil
	}
	values := make([]any, len(relOptions))
	for i, option := range relOptions {
		values[i] = option
	}
	return values
}

// Close implements the interface sql.RowIter.
func (iter *pgClassTableScanIter) Close(ctx *sql.Context) error {
	return nil
}
