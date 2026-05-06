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

package node

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

type BtreePlannerBoundaryTable struct {
	underlying sql.Table
}

var _ sql.Table = (*BtreePlannerBoundaryTable)(nil)
var _ sql.DatabaseSchemaTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexAddressableTable = (*BtreePlannerBoundaryTable)(nil)
var _ sql.IndexedTable = (*BtreePlannerBoundaryTable)(nil)

func WrapBtreePlannerBoundaryTable(ctx *sql.Context, table sql.Table) (sql.Table, bool, error) {
	if _, ok := table.(*BtreePlannerBoundaryTable); ok {
		return table, false, nil
	}
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return table, false, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return table, false, err
	}
	for _, index := range indexes {
		if unsafeBtreePlannerIndex(index) {
			return &BtreePlannerBoundaryTable{underlying: table}, true, nil
		}
	}
	return table, false, nil
}

func (t *BtreePlannerBoundaryTable) Name() string {
	return t.underlying.Name()
}

func (t *BtreePlannerBoundaryTable) String() string {
	return t.underlying.String()
}

func (t *BtreePlannerBoundaryTable) Schema(ctx *sql.Context) sql.Schema {
	return t.underlying.Schema(ctx)
}

func (t *BtreePlannerBoundaryTable) Collation() sql.CollationID {
	return t.underlying.Collation()
}

func (t *BtreePlannerBoundaryTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return t.underlying.Partitions(ctx)
}

func (t *BtreePlannerBoundaryTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return t.underlying.PartitionRows(ctx, partition)
}

func (t *BtreePlannerBoundaryTable) DatabaseSchema() sql.DatabaseSchema {
	if schemaTable, ok := t.underlying.(sql.DatabaseSchemaTable); ok {
		return schemaTable.DatabaseSchema()
	}
	return nil
}

func (t *BtreePlannerBoundaryTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.IndexedAccess(ctx, lookup)
	}
	return nil
}

func (t *BtreePlannerBoundaryTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexAddressable, ok := t.underlying.(sql.IndexAddressable)
	if !ok {
		return nil, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]sql.Index, 0, len(indexes))
	for _, index := range indexes {
		if !unsafeBtreePlannerIndex(index) {
			filtered = append(filtered, index)
		}
	}
	return filtered, nil
}

func (t *BtreePlannerBoundaryTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	if indexedTable, ok := t.underlying.(sql.IndexedTable); ok {
		return indexedTable.LookupPartitions(ctx, lookup)
	}
	return nil, errors.Errorf("table %s is not indexed", t.Name())
}

func (t *BtreePlannerBoundaryTable) PreciseMatch() bool {
	if indexAddressable, ok := t.underlying.(sql.IndexAddressable); ok {
		return indexAddressable.PreciseMatch()
	}
	return false
}

func unsafeBtreePlannerIndex(index sql.Index) bool {
	metadata, ok := indexmetadata.DecodeComment(index.Comment())
	if !ok {
		return false
	}
	if indexmetadata.AccessMethod(index.IndexType(), index.Comment()) != indexmetadata.AccessMethodBtree {
		return false
	}
	for _, collation := range metadata.Collations {
		if strings.TrimSpace(collation) != "" {
			return true
		}
	}
	for _, opClass := range metadata.OpClasses {
		switch indexmetadata.NormalizeOpClass(opClass) {
		case indexmetadata.OpClassTextPatternOps, indexmetadata.OpClassVarcharPatternOps, indexmetadata.OpClassBpcharPatternOps:
			return true
		}
	}
	return false
}
