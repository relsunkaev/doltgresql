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
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	doltschema "github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
)

var sortedPrimaryRowBuildPool = pool.NewBuffPool()

func buildSortedPrimaryRowIndex(
	ctx context.Context,
	ns tree.NodeStore,
	doltSch doltschema.Schema,
	sqlSch sql.Schema,
	rows []sql.Row,
	rowLess func(left sql.Row, right sql.Row) bool,
) (durable.Index, error) {
	sortedRows := append([]sql.Row(nil), rows...)
	sort.Slice(sortedRows, func(i, j int) bool {
		return rowLess(sortedRows[i], sortedRows[j])
	})

	keyMap, err := ordinalMappingForColumns(sqlSch, doltSch.GetPKCols())
	if err != nil {
		return nil, err
	}
	valMap, err := ordinalMappingForColumns(sqlSch, doltSch.GetNonPKCols())
	if err != nil {
		return nil, err
	}
	keyDesc, valueDesc := doltSch.GetMapDescriptors(ns)
	builder := sortedPrimaryRowTupleBuilder{
		ns:       ns,
		keyBld:   val.NewTupleBuilder(keyDesc, ns),
		valueBld: val.NewTupleBuilder(valueDesc, ns),
		keyMap:   keyMap,
		valueMap: valMap,
	}
	tuples := make([]sortedPrimaryRowTuple, 0, len(sortedRows))
	for _, row := range sortedRows {
		keyTuple, valueTuple, err := builder.tuplesFromRow(ctx, row)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, sortedPrimaryRowTuple{key: keyTuple, value: valueTuple})
	}
	rowMap, err := prolly.NewMapFromTupleIter(ctx, ns, keyDesc, valueDesc, &sortedPrimaryRowTupleIter{tuples: tuples})
	if err != nil {
		return nil, err
	}
	return durable.IndexFromProllyMap(rowMap), nil
}

func ordinalMappingForColumns(sqlSch sql.Schema, columns *doltschema.ColCollection) (val.OrdinalMapping, error) {
	mapping := make(val.OrdinalMapping, columns.StoredSize())
	for to := range mapping {
		column := columns.GetByStoredIndex(to)
		from := sqlSch.IndexOfColName(column.Name)
		if from < 0 {
			return nil, errors.Errorf(`column "%s" is missing from SQL schema`, column.Name)
		}
		mapping[to] = from
	}
	return mapping, nil
}

type sortedPrimaryRowTupleBuilder struct {
	ns       tree.NodeStore
	keyBld   *val.TupleBuilder
	valueBld *val.TupleBuilder
	keyMap   val.OrdinalMapping
	valueMap val.OrdinalMapping
}

func (b sortedPrimaryRowTupleBuilder) tuplesFromRow(ctx context.Context, row sql.Row) (val.Tuple, val.Tuple, error) {
	keyTuple, err := b.tupleFromRow(ctx, b.keyBld, b.keyMap, row, true)
	if err != nil {
		return nil, nil, err
	}
	valueTuple, err := b.tupleFromRow(ctx, b.valueBld, b.valueMap, row, false)
	if err != nil {
		return nil, nil, err
	}
	return keyTuple, valueTuple, nil
}

func (b sortedPrimaryRowTupleBuilder) tupleFromRow(ctx context.Context, tupleBuilder *val.TupleBuilder, mapping val.OrdinalMapping, row sql.Row, permissive bool) (val.Tuple, error) {
	for to := range mapping {
		from := mapping.MapOrdinal(to)
		if from < 0 || from >= len(row) {
			return nil, errors.Errorf("row has %d columns, cannot read ordinal %d", len(row), from)
		}
		if err := tree.PutField(ctx, b.ns, tupleBuilder, to, row[from]); err != nil {
			return nil, err
		}
	}
	if permissive {
		return tupleBuilder.BuildPermissive(sortedPrimaryRowBuildPool)
	}
	return tupleBuilder.Build(sortedPrimaryRowBuildPool)
}

type sortedPrimaryRowTuple struct {
	key   val.Tuple
	value val.Tuple
}

type sortedPrimaryRowTupleIter struct {
	tuples []sortedPrimaryRowTuple
	pos    int
}

var _ prolly.TupleIter = (*sortedPrimaryRowTupleIter)(nil)

func (i *sortedPrimaryRowTupleIter) Next(context.Context) (val.Tuple, val.Tuple) {
	if i.pos >= len(i.tuples) {
		return nil, nil
	}
	tuple := i.tuples[i.pos]
	i.pos++
	return tuple.key, tuple.value
}
