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

package pgcatalog

import (
	"slices"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/publications"
	"github.com/dolthub/doltgresql/server/functions"
)

type pgPublicationTableRow struct {
	pubName   string
	schema    string
	table     string
	attNames  []string
	rowFilter string
}

func allPublications(ctx *sql.Context) ([]publications.Publication, error) {
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var pubs []publications.Publication
	err = collection.IteratePublications(ctx, func(pub publications.Publication) (stop bool, err error) {
		pubs = append(pubs, pub)
		return false, nil
	})
	return pubs, err
}

func publicationTableRows(ctx *sql.Context, pub publications.Publication) ([]pgPublicationTableRow, error) {
	var rows []pgPublicationTableRow
	seen := make(map[string]struct{})
	add := func(schema string, table string, columns []string, rowFilter string) error {
		key := strings.ToLower(schema) + "\x00" + strings.ToLower(table)
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		attNames, err := publicationAttNames(ctx, schema, table, columns)
		if err != nil {
			return err
		}
		rows = append(rows, pgPublicationTableRow{
			pubName:   pub.ID.PublicationName(),
			schema:    schema,
			table:     table,
			attNames:  attNames,
			rowFilter: rowFilter,
		})
		return nil
	}
	for _, relation := range pub.Tables {
		if err := add(relation.Table.SchemaName(), relation.Table.TableName(), relation.Columns, relation.RowFilter); err != nil {
			return nil, err
		}
	}
	if pub.AllTables || len(pub.Schemas) > 0 {
		publishAllSchemas := pub.AllTables
		schemaSet := make(map[string]struct{}, len(pub.Schemas))
		for _, schema := range pub.Schemas {
			schemaSet[strings.ToLower(schema)] = struct{}{}
		}
		err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
			Table: func(ctx *sql.Context, schema functions.ItemSchema, table functions.ItemTable) (cont bool, err error) {
				schemaName := schema.Item.SchemaName()
				if !publishAllSchemas {
					if _, ok := schemaSet[strings.ToLower(schemaName)]; !ok {
						return true, nil
					}
				}
				if err = add(schemaName, table.Item.Name(), nil, ""); err != nil {
					return false, err
				}
				return true, nil
			},
		})
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(rows, func(a, b pgPublicationTableRow) int {
		if cmp := strings.Compare(a.schema, b.schema); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.table, b.table)
	})
	return rows, nil
}

func publicationAttNames(ctx *sql.Context, schema string, table string, columns []string) ([]string, error) {
	if len(columns) > 0 {
		return slices.Clone(columns), nil
	}
	sqlTable, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: table, Schema: schema})
	if err != nil || sqlTable == nil {
		return nil, err
	}
	attNames := make([]string, 0, len(sqlTable.Schema()))
	for _, column := range sqlTable.Schema() {
		attNames = append(attNames, column.Name)
	}
	return attNames, nil
}

func publicationAttNums(ctx *sql.Context, relation publications.PublicationRelation) ([]any, error) {
	if len(relation.Columns) == 0 {
		return nil, nil
	}
	table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: relation.Table.TableName(), Schema: relation.Table.SchemaName()})
	if err != nil || table == nil {
		return nil, err
	}
	attNums := make([]any, 0, len(relation.Columns))
	for _, wanted := range relation.Columns {
		found := false
		for i, column := range table.Schema() {
			if column.Name == wanted || strings.EqualFold(column.Name, wanted) {
				attNums = append(attNums, int16(i+1))
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
	}
	return attNums, nil
}

func stringSliceToAny(values []string) []any {
	if values == nil {
		return nil
	}
	ret := make([]any, len(values))
	for i, value := range values {
		ret[i] = value
	}
	return ret
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func catalogOwnerOID() id.Id {
	return id.NewOID(10).AsId()
}
