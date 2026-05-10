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

package tables

import (
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type sequenceTable struct {
	schema   sql.DatabaseSchema
	sequence id.Sequence
}

var _ sql.DatabaseSchemaTable = (*sequenceTable)(nil)
var _ sql.PrimaryKeyTable = (*sequenceTable)(nil)
var _ sql.Table = (*sequenceTable)(nil)

func newSequenceTable(schema sql.DatabaseSchema, sequence id.Sequence) *sequenceTable {
	return &sequenceTable{
		schema:   schema,
		sequence: sequence,
	}
}

func (tbl *sequenceTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (tbl *sequenceTable) DatabaseSchema() sql.DatabaseSchema {
	return tbl.schema
}

func (tbl *sequenceTable) Name() string {
	return tbl.sequence.SequenceName()
}

func (tbl *sequenceTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	collection, err := core.GetSequencesCollectionFromContext(ctx, tbl.schema.Name())
	if err != nil {
		return nil, err
	}
	sequence, err := collection.GetSequence(ctx, tbl.sequence)
	if err != nil {
		return nil, err
	}
	if sequence == nil {
		return nil, errors.Errorf(`relation "%s" does not exist`, tbl.sequence.SequenceName())
	}
	return &sequenceRowIter{
		sequence: sequence,
	}, nil
}

func (tbl *sequenceTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{}, nil
}

func (tbl *sequenceTable) PrimaryKeySchema(ctx *sql.Context) sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema: sql.Schema{
			{Name: "last_value", Type: pgtypes.Int64, Default: nil, Nullable: false, Source: tbl.Name()},
			{Name: "log_cnt", Type: pgtypes.Int64, Default: nil, Nullable: false, Source: tbl.Name()},
			{Name: "is_called", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: tbl.Name()},
		},
		PkOrdinals: nil,
	}
}

func (tbl *sequenceTable) Schema(ctx *sql.Context) sql.Schema {
	return tbl.PrimaryKeySchema(ctx).Schema
}

func (tbl *sequenceTable) String() string {
	return tbl.Name()
}

type sequenceRowIter struct {
	sequence *sequences.Sequence
	used     bool
}

var _ sql.RowIter = (*sequenceRowIter)(nil)

func (iter *sequenceRowIter) Close(ctx *sql.Context) error {
	return nil
}

func (iter *sequenceRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.used {
		return nil, io.EOF
	}
	iter.used = true
	return sql.Row{
		sequenceLastValue(iter.sequence),
		int64(0),
		iter.sequence.IsCalled,
	}, nil
}

func sequenceLastValue(sequence *sequences.Sequence) int64 {
	if !sequence.IsCalled || sequence.IsAtEnd {
		return sequence.Current
	}
	return sequence.Current - sequence.Increment
}
