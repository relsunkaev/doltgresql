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

package indexmetadata

import (
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/go-mysql-server/sql"
)

// DisplayName returns the PostgreSQL-facing name for index.
func DisplayName(idx sql.Index) string {
	if strings.EqualFold(idx.ID(), "PRIMARY") {
		return fmt.Sprintf("%s_pkey", idx.Table())
	}

	switch idx.(type) {
	case *index.BranchNameIndex, *index.CommitIndex:
		return fmt.Sprintf("%s_%s_key", idx.Table(), idx.ID())
	}

	return idx.ID()
}

// Definition returns a PostgreSQL CREATE INDEX definition for index.
func Definition(index sql.Index, schema string) string {
	unique := ""
	if index.IsUnique() {
		unique = " UNIQUE"
	}
	return fmt.Sprintf("CREATE%s INDEX %s ON %s.%s USING %s (%s)",
		unique,
		DisplayName(index),
		schema,
		index.Table(),
		AccessMethod(index.IndexType(), index.Comment()),
		strings.Join(ColumnDefinitions(index), ", "),
	)
}

// ColumnDefinitions returns PostgreSQL-facing indexed column definitions,
// including any opclass metadata preserved by Doltgres.
func ColumnDefinitions(index sql.Index) []string {
	cols := Columns(index.Comment())
	if len(cols) == 0 {
		exprs := index.Expressions()
		cols = make([]string, len(exprs))
		for i, expr := range exprs {
			cols[i] = unqualifiedIndexExpression(expr)
		}
	}

	opClasses := OpClasses(index.Comment())
	sortOptions := SortOptions(index.Comment())
	for i := range cols {
		if i < len(opClasses) && opClasses[i] != "" {
			cols[i] += " " + opClasses[i]
		}
		if i < len(sortOptions) {
			if optionDef := columnOptionDefinition(sortOptions[i]); optionDef != "" {
				cols[i] += " " + optionDef
			}
		}
	}
	return cols
}

func unqualifiedIndexExpression(expr string) string {
	if lastDot := strings.LastIndex(expr, "."); lastDot >= 0 {
		expr = expr[lastDot+1:]
	}
	return strings.Trim(expr, "`\"")
}

func columnOptionDefinition(option IndexColumnOption) string {
	var parts []string
	if option.Direction == SortDirectionDesc {
		parts = append(parts, "DESC")
	}
	if option.NullsOrder == NullsOrderFirst && option.Direction != SortDirectionDesc {
		parts = append(parts, "NULLS FIRST")
	}
	return strings.Join(parts, " ")
}
