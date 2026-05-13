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

package core

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/go-mysql-server/sql"
)

const doltSchemaViewFragmentType = "view"

type viewRootObject struct {
	name                doltdb.TableName
	fragment            string
	createViewStatement string
}

var _ doltdb.RootObject = viewRootObject{}

func (v viewRootObject) HashOf(ctx context.Context) (hash.Hash, error) {
	serialized, err := v.serialize(ctx)
	if err != nil {
		return hash.Hash{}, err
	}
	return hash.Of(serialized), nil
}

func (v viewRootObject) Name() doltdb.TableName {
	return v.name
}

func (v viewRootObject) serialize(context.Context) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(v.name.Schema)
	buf.WriteByte(0)
	buf.WriteString(v.name.Name)
	buf.WriteByte(0)
	buf.WriteString(v.fragment)
	buf.WriteByte(0)
	buf.WriteString(v.createViewStatement)
	return buf.Bytes(), nil
}

func (root *RootValue) getViewRootObject(ctx context.Context, name doltdb.TableName) (doltdb.RootObject, bool, error) {
	var matched viewRootObject
	err := root.iterViewRootObjects(ctx, func(candidateName doltdb.TableName, rootObj doltdb.RootObject) (bool, error) {
		if rootObjectTableNameMatches(candidateName, name) {
			matched = rootObj.(viewRootObject)
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return nil, false, err
	}
	if matched.name.Name == "" {
		return nil, false, nil
	}
	return matched, true, nil
}

func (root *RootValue) iterViewRootObjects(ctx context.Context, cb func(name doltdb.TableName, table doltdb.RootObject) (stop bool, err error)) error {
	schemas, err := schemaNames(ctx, root)
	if err != nil {
		return err
	}
	for _, schemaName := range schemas {
		views, err := root.viewRootObjectsInSchema(ctx, schemaName)
		if err != nil {
			return err
		}
		for _, view := range views {
			stop, err := cb(view.Name(), view)
			if err != nil || stop {
				return err
			}
		}
	}
	return nil
}

func (root *RootValue) viewRootObjectsInSchema(ctx context.Context, schemaName string) ([]viewRootObject, error) {
	table, ok, err := root.GetTable(ctx, doltdb.TableName{Name: doltdb.SchemasTableName, Schema: schemaName})
	if err != nil || !ok {
		return nil, err
	}
	tableSchema, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	typeIndex := doltSchemaColumnIndex(tableSchema.GetAllCols().GetColumns(), doltdb.SchemasTablesTypeCol)
	nameIndex := doltSchemaColumnIndex(tableSchema.GetAllCols().GetColumns(), doltdb.SchemasTablesNameCol)
	fragmentIndex := doltSchemaColumnIndex(tableSchema.GetAllCols().GetColumns(), doltdb.SchemasTablesFragmentCol)
	if typeIndex < 0 || nameIndex < 0 || fragmentIndex < 0 {
		return nil, nil
	}

	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := durable.ProllyMapFromIndex(rowData)
	if err != nil {
		return nil, err
	}
	iter, err := rows.IterAll(ctx)
	if err != nil {
		return nil, err
	}
	rowIter := index.NewProllyRowIterForMap(tableSchema, rows, iter, nil)
	defer rowIter.Close(sqlContext(ctx))

	var views []viewRootObject
	for {
		row, err := rowIter.Next(sqlContext(ctx))
		if err == io.EOF {
			return views, nil
		}
		if err != nil {
			return nil, err
		}
		fragType, ok := row[typeIndex].(string)
		if !ok || !strings.EqualFold(fragType, doltSchemaViewFragmentType) {
			continue
		}
		name, ok := row[nameIndex].(string)
		if !ok {
			continue
		}
		fragment, ok := row[fragmentIndex].(string)
		if !ok {
			continue
		}
		logicalName := DecodePhysicalViewName(name)
		views = append(views, viewRootObject{
			name: doltdb.TableName{
				Name:   logicalName,
				Schema: schemaName,
			},
			fragment:            fragment,
			createViewStatement: fragment,
		})
	}
}

func sqlContext(ctx context.Context) *sql.Context {
	if sqlCtx, ok := ctx.(*sql.Context); ok {
		return sqlCtx
	}
	return sql.NewContext(ctx)
}

func doltSchemaColumnIndex(columns []schema.Column, name string) int {
	for i, column := range columns {
		if strings.EqualFold(column.Name, name) {
			return i
		}
	}
	return -1
}
