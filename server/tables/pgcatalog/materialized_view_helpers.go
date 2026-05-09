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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/tablemetadata"
)

func tableComment(table sql.Table) string {
	commented, ok := table.(sql.CommentedTable)
	if !ok {
		return ""
	}
	return commented.Comment()
}

func isMaterializedViewTable(table sql.Table) bool {
	return tablemetadata.IsMaterializedView(tableComment(table))
}

func materializedViewDefinition(table sql.Table) string {
	return tablemetadata.MaterializedViewDefinition(tableComment(table))
}

func materializedViewPopulated(table sql.Table) bool {
	return tablemetadata.IsMaterializedViewPopulated(tableComment(table))
}

func tableHasIndexes(ctx *sql.Context, table sql.Table) (bool, error) {
	indexed, ok := table.(sql.IndexAddressable)
	if !ok {
		return false, nil
	}
	indexes, err := indexed.GetIndexes(ctx)
	if err != nil {
		return false, err
	}
	return len(indexes) > 0, nil
}
