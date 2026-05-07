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
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// ValidateOnConflictArbiter prevents PostgreSQL targeted ON CONFLICT clauses
// from falling through to MySQL's broader ON DUPLICATE KEY / INSERT IGNORE
// behavior when multiple unique indexes could be the source of the conflict.
//
// For tables with a single unique index, the GMS path matches PG semantics
// and the rule is a pure validator. For tables with multiple unique indexes,
// the rule allows targeted DO UPDATE by wrapping each ON DUP expression with
// an OnConflictTargetGuard so a conflict against a non-target unique index
// raises instead of silently firing the update. DO NOTHING on multi-unique
// tables remains rejected because INSERT IGNORE swallows the non-target
// unique violation; that case still requires the explicit pre-check path.
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
	if !ok || conflict == nil || (len(conflict.Columns) == 0 && conflict.Constraint == "") {
		return node, transform.SameTree, nil
	}
	target, err := resolveConflictTarget(ctx, insert.Destination, conflict.Columns, string(conflict.Constraint))
	if err != nil {
		return nil, transform.NewTree, err
	}
	if !target.multipleUniques {
		return node, transform.SameTree, nil
	}
	if !hasOnDuplicateUpdates {
		// DO NOTHING on a multi-unique table routes through INSERT
		// IGNORE in GMS, which would silently swallow a violation of
		// any unique index — including a non-target one — making the
		// upsert incorrect under PG semantics. Wrap the destination
		// with a pre-check inserter that raises (as a non-Unique-
		// KeyError) on a non-target unique conflict; target conflicts
		// still flow through to the underlying INSERT IGNORE path.
		wrapped, err := wrapDestinationForArbiterPreCheck(ctx, insert, target)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return wrapped, transform.NewTree, nil
	}

	wrapped, err := wrapOnDupForTargetGuard(ctx, insert, target)
	if err != nil {
		return nil, transform.NewTree, err
	}
	return wrapped, transform.NewTree, nil
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

// conflictTarget summarizes the resolved targeted unique index for an
// ON CONFLICT clause: the column indexes (in destination schema order),
// the destination schema length, the constraint name to embed in error
// messages, and whether the destination has more than one unique index
// (the case that needs runtime guarding).
type conflictTarget struct {
	targetIndexes      []int
	targetColumnNames  []string
	schemaLen          int
	constraintName     string
	multipleUniques    bool
}

func joinNameList(names []string) string {
	if len(names) == 0 {
		return ""
	}
	out := names[0]
	for _, n := range names[1:] {
		out += ", " + n
	}
	return out
}

func resolveConflictTarget(ctx *sql.Context, destination sql.Node, targetColumns tree.NameList, constraintName string) (conflictTarget, error) {
	table, err := plan.GetInsertable(destination)
	if err != nil {
		return conflictTarget{}, err
	}
	indexedTable, ok := table.(sql.IndexAddressable)
	if !ok {
		return conflictTarget{}, errors.Errorf("there is no unique or exclusion constraint matching the ON CONFLICT specification")
	}
	indexes, err := indexedTable.GetIndexes(ctx)
	if err != nil {
		return conflictTarget{}, err
	}
	schema := table.Schema(ctx)
	uniqueIndexCount := 0
	var matchingIndex sql.Index
	for _, index := range indexes {
		if !index.IsUnique() {
			continue
		}
		uniqueIndexCount++
		if matchingIndex != nil {
			continue
		}
		if constraintName != "" {
			if uniqueIndexMatchesConstraintName(index, constraintName) {
				matchingIndex = index
			}
			continue
		}
		if uniqueIndexMatchesConflictTarget(index, schema, targetColumns) {
			matchingIndex = index
		}
	}
	if matchingIndex == nil {
		if constraintName != "" {
			return conflictTarget{}, errors.Errorf(
				"constraint %q for table does not exist", constraintName)
		}
		return conflictTarget{}, errors.Errorf("there is no unique or exclusion constraint matching the ON CONFLICT specification")
	}
	indexes2 := indexmetadata.LogicalColumns(matchingIndex, schema)
	targetIndexes := make([]int, 0, len(indexes2))
	targetNames := make([]string, 0, len(indexes2))
	for _, column := range indexes2 {
		idx := schema.IndexOfColName(column.StorageName)
		if idx < 0 {
			return conflictTarget{}, errors.Errorf("ON CONFLICT target column %q does not exist", column.StorageName)
		}
		targetIndexes = append(targetIndexes, idx)
		targetNames = append(targetNames, column.StorageName)
	}
	return conflictTarget{
		targetIndexes:     targetIndexes,
		targetColumnNames: targetNames,
		schemaLen:         len(schema),
		constraintName:    matchingIndex.ID(),
		multipleUniques:   uniqueIndexCount > 1,
	}, nil
}

// uniqueIndexMatchesConstraintName returns whether the named index
// is the one targeted by an `ON CONFLICT ON CONSTRAINT name` clause.
// PG users address indexes by the constraint name produced by
// `CREATE TABLE ... PRIMARY KEY` (e.g. `t_pkey`), whereas GMS
// reports the primary key as id "PRIMARY". Translate both forms so
// either spelling resolves.
func uniqueIndexMatchesConstraintName(index sql.Index, constraintName string) bool {
	if strings.EqualFold(index.ID(), constraintName) {
		return true
	}
	// PostgreSQL's auto-generated primary-key constraint name is
	// `<table>_pkey`; GMS reports the same index as "PRIMARY".
	if strings.EqualFold(index.ID(), "PRIMARY") &&
		strings.EqualFold(strings.TrimSuffix(constraintName, "_pkey"), index.Table()) {
		return true
	}
	return false
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

// wrapOnDupForTargetGuard returns an updated InsertInto whose
// ON DUP expressions have their RHS wrapped with the
// OnConflictTargetGuard so a conflict on a non-target unique index
// raises rather than silently firing the update.
// wrapDestinationForArbiterPreCheck wraps the InsertInto's
// destination table with an OnConflictDoNothingArbiterTable so that
// a non-target unique-index conflict raises (as a non-Unique-Key
// error that GMS's INSERT IGNORE handler does not swallow). The
// target unique still flows through to the underlying inserter
// where the IGNORE swallow correctly suppresses the user's chosen
// duplicate.
func wrapDestinationForArbiterPreCheck(ctx *sql.Context, insert *plan.InsertInto, target conflictTarget) (sql.Node, error) {
	targetSet := map[string]struct{}{target.constraintName: {}}
	destination, same, err := pgtransform.NodeWithOpaque(ctx, insert.Destination, func(ctx *sql.Context, n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		resolved, ok := n.(*plan.ResolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		wrapped, wasWrapped, err := pgnodes.WrapOnConflictDoNothingArbiterTable(ctx, resolved.Table, targetSet)
		if err != nil || !wasWrapped {
			return n, transform.SameTree, err
		}
		newNode, err := resolved.ReplaceTable(ctx, wrapped)
		if err != nil {
			return nil, transform.NewTree, err
		}
		return newNode.(sql.Node), transform.NewTree, nil
	})
	if err != nil {
		return nil, err
	}
	if same == transform.SameTree {
		return insert, nil
	}
	out, err := insert.WithChildren(ctx, destination)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func wrapOnDupForTargetGuard(ctx *sql.Context, insert *plan.InsertInto, target conflictTarget) (sql.Node, error) {
	if insert.OnDupExprs == nil {
		return insert, nil
	}
	exprs := insert.OnDupExprs.AllExpressions()
	if len(exprs) == 0 {
		return insert, nil
	}
	newExprs := make([]sql.Expression, len(exprs))
	for i, e := range exprs {
		setField, ok := e.(*expression.SetField)
		if !ok {
			newExprs[i] = e
			continue
		}
		guarded := pgexprs.NewOnConflictTargetGuard(
			setField.RightChild, target.targetIndexes, target.schemaLen, target.constraintName)
		replaced, err := setField.WithChildren(ctx, setField.LeftChild, guarded)
		if err != nil {
			return nil, err
		}
		newExprs[i] = replaced
	}
	updated, err := insert.OnDupExprs.WithExpressions(newExprs)
	if err != nil {
		return nil, err
	}
	out := *insert
	out.OnDupExprs = updated
	return &out, nil
}
