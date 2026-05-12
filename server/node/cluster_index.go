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
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// ClusterIndex handles PostgreSQL's CLUSTER index ON table form. Dolt storage
// is already index-addressable, so this records the PostgreSQL catalog flag
// without rewriting table rows.
type ClusterIndex struct {
	schema string
	table  string
	index  string
}

var _ sql.ExecSourceRel = (*ClusterIndex)(nil)
var _ vitess.Injectable = (*ClusterIndex)(nil)

// NewClusterIndex returns a new *ClusterIndex.
func NewClusterIndex(schema string, table string, index string) *ClusterIndex {
	return &ClusterIndex{
		schema: schema,
		table:  table,
		index:  index,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	located, ok, err := locateIndex(ctx, c.schema, c.table, c.index, false)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrIndexNotFound.New(c.index)
	}

	indexAddressable, ok := located.table.(sql.IndexAddressable)
	if !ok {
		return nil, sql.ErrIndexNotFound.New(c.index)
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		target := indexNameMatches(index, located.table, c.index)
		if !target && !indexmetadata.IsClustered(index.Comment()) {
			continue
		}

		metadata, ok := indexmetadata.DecodeComment(index.Comment())
		if !ok {
			metadata = indexmetadata.Metadata{
				AccessMethod: indexmetadata.AccessMethod(index.IndexType(), index.Comment()),
			}
		}
		if metadata.Clustered == target {
			continue
		}
		metadata.Clustered = target

		indexName := indexmetadata.DisplayNameForTable(index, located.table)
		if indexName == "" {
			indexName = index.ID()
		}
		if err := flipIndexComment(ctx, c.schema, c.table, indexName, alteredIndexComment(metadata)); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) String() string {
	return "CLUSTER"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *ClusterIndex) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *ClusterIndex) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
