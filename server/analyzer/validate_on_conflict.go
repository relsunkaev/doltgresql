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

package analyzer

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

// ValidateOnConflictArbiter prevents PostgreSQL targeted ON CONFLICT clauses
// from falling through to MySQL's broader ON DUPLICATE KEY / INSERT IGNORE
// behavior when multiple unique indexes could be the source of the conflict.
func ValidateOnConflictArbiter(ctx *sql.Context, _ *gmsanalyzer.Analyzer, node sql.Node, _ *plan.Scope, _ gmsanalyzer.RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	insert, ok := node.(*plan.InsertInto)
	if !ok {
		return node, transform.SameTree, nil
	}
	hasOnDuplicateUpdates := insert.OnDupExprs != nil && insert.OnDupExprs.HasUpdates()
	if !hasOnDuplicateUpdates && !insert.Ignore {
		return node, transform.SameTree, nil
	}

	conflict, ok := onConflictClauseForInsert(ctx.Query(), nodeName(insert.Destination))
	if !ok || conflict == nil || len(conflict.Columns) == 0 {
		return node, transform.SameTree, nil
	}
	if err := validateConflictTargetMatchesOnlyUniqueIndex(ctx, insert.Destination, conflict.Columns); err != nil {
		return nil, transform.NewTree, err
	}
	return node, transform.SameTree, nil
}

func nodeName(node sql.Node) string {
	nameable, ok := node.(sql.Nameable)
	if !ok {
		return ""
	}
	return nameable.Name()
}

func onConflictClauseForInsert(query string, tableName string) (*tree.OnConflict, bool) {
	if query == "" {
		return nil, false
	}
	statements, err := parser.Parse(query)
	if err != nil {
		return nil, false
	}
	for _, statement := range statements {
		insert, ok := statement.AST.(*tree.Insert)
		if !ok || insert.OnConflict == nil {
			continue
		}
		insertTableName, ok := insertTableObjectName(insert.Table)
		if !ok || tableName == "" || strings.EqualFold(insertTableName, tableName) {
			return insert.OnConflict, true
		}
	}
	return nil, false
}

func insertTableObjectName(table tree.TableExpr) (string, bool) {
	switch table := table.(type) {
	case *tree.TableName:
		return string(table.ObjectName), true
	case *tree.AliasedTableExpr:
		tableName, ok := table.Expr.(*tree.TableName)
		if !ok {
			return "", false
		}
		return string(tableName.ObjectName), true
	default:
		return "", false
	}
}

func validateConflictTargetMatchesOnlyUniqueIndex(ctx *sql.Context, destination sql.Node, targetColumns tree.NameList) error {
	table, err := plan.GetInsertable(destination)
	if err != nil {
		return err
	}
	indexedTable, ok := table.(sql.IndexAddressable)
	if !ok {
		return errors.Errorf("there is no unique or exclusion constraint matching the ON CONFLICT specification")
	}
	indexes, err := indexedTable.GetIndexes(ctx)
	if err != nil {
		return err
	}

	schema := table.Schema(ctx)
	uniqueIndexCount := 0
	matchingUniqueIndexCount := 0
	for _, index := range indexes {
		if !index.IsUnique() {
			continue
		}
		uniqueIndexCount++
		if uniqueIndexMatchesConflictTarget(index, schema, targetColumns) {
			matchingUniqueIndexCount++
		}
	}
	if matchingUniqueIndexCount == 0 {
		return errors.Errorf("there is no unique or exclusion constraint matching the ON CONFLICT specification")
	}
	if uniqueIndexCount > 1 {
		return errors.Errorf("ON CONFLICT with a conflict target is not yet supported on tables with multiple unique indexes")
	}
	return nil
}

func uniqueIndexMatchesConflictTarget(index sql.Index, schema sql.Schema, targetColumns tree.NameList) bool {
	logicalColumns := indexmetadata.LogicalColumns(index, schema)
	if len(logicalColumns) != len(targetColumns) {
		return false
	}

	indexColumnCounts := make(map[string]int, len(logicalColumns))
	for _, column := range logicalColumns {
		if column.Expression {
			return false
		}
		indexColumnCounts[strings.ToLower(column.Definition)]++
	}
	for _, targetColumn := range targetColumns {
		name := strings.ToLower(string(targetColumn))
		if indexColumnCounts[name] == 0 {
			return false
		}
		indexColumnCounts[name]--
	}
	return true
}
