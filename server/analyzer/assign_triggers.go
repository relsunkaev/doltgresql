// Copyright 2024 Dolthub, Inc.
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
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

// AssignTriggers assigns triggers wherever they're needed.
func AssignTriggers(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	replicaRole, err := sessionReplicationRoleIsReplica(ctx)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if replicaRole {
		return node, transform.SameTree, nil
	}
	return pgtransform.NodeWithOpaque(ctx, node, func(ctx *sql.Context, node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := node.(type) {
		case *plan.DeleteFrom, *plan.InsertInto, *plan.Truncate, *plan.Update:
			triggerInfo, err := getTriggerInformation(ctx, node)
			if err != nil {
				return nil, transform.NewTree, err
			}
			if triggerInfo.isEmpty() {
				return node, transform.SameTree, nil
			}
			newNode := node
			operation := getTriggerOperation(node)
			if len(triggerInfo.beforeStatement) > 0 {
				executionNode := &pgnodes.TriggerExecution{
					Timing:    triggers.TriggerTiming_Before,
					Statement: true,
					Operation: operation,
					Triggers:  triggerInfo.beforeStatement,
					Split:     pgnodes.TriggerExecutionRowHandling_None,
					Return:    pgnodes.TriggerExecutionRowHandling_None,
					Sch:       triggerInfo.sch,
					Source:    getTriggerSource(newNode),
					Runner:    pgexprs.StatementRunner{Runner: a.Runner},
				}
				if _, ok := newNode.(*plan.Truncate); ok {
					executionNode.Source = newNode
					newNode = executionNode
				} else {
					newNode, err = nodeWithTriggers(ctx, newNode, executionNode)
					if err != nil {
						return nil, transform.NewTree, err
					}
				}
			}
			if len(triggerInfo.beforeRow) > 0 {
				handling := getTriggerRowHandling(node)
				var insertDefaultProjections []sql.Expression
				if insert, ok := node.(*plan.InsertInto); ok {
					insertDefaultProjections, err = insertTriggerDefaultProjections(ctx, insert, insert.Destination.Schema(ctx))
					if err != nil {
						return nil, transform.NewTree, err
					}
				}
				newNode, err = nodeWithTriggers(ctx, newNode, &pgnodes.TriggerExecution{
					Timing:                   triggers.TriggerTiming_Before,
					Operation:                operation,
					Triggers:                 triggerInfo.beforeRow,
					Split:                    handling,
					Return:                   handling,
					Sch:                      triggerInfo.sch,
					Source:                   getTriggerSource(newNode),
					Runner:                   pgexprs.StatementRunner{Runner: a.Runner},
					InsertDefaultProjections: insertDefaultProjections,
				})
				if err != nil {
					return nil, transform.NewTree, err
				}
			}
			if len(triggerInfo.afterRow) > 0 {
				newNode = &pgnodes.TriggerExecution{
					Timing:    triggers.TriggerTiming_After,
					Operation: operation,
					Triggers:  triggerInfo.afterRow,
					Split:     getTriggerRowHandling(node),
					Return:    pgnodes.TriggerExecutionRowHandling_None,
					Sch:       triggerInfo.sch,
					Source:    newNode,
					Runner:    pgexprs.StatementRunner{Runner: a.Runner},
				}
			}
			if len(triggerInfo.afterStatement) > 0 {
				newNode = &pgnodes.TriggerExecution{
					Timing:    triggers.TriggerTiming_After,
					Statement: true,
					Operation: operation,
					Triggers:  triggerInfo.afterStatement,
					Split:     getTriggerRowHandling(node),
					Return:    pgnodes.TriggerExecutionRowHandling_None,
					Sch:       triggerInfo.sch,
					Source:    newNode,
					Runner:    pgexprs.StatementRunner{Runner: a.Runner},
				}
			}
			return newNode, transform.NewTree, nil
		default:
			return node, transform.SameTree, nil
		}
	})
}

type triggerInformation struct {
	sch             sql.Schema
	beforeStatement []triggers.Trigger
	beforeRow       []triggers.Trigger
	afterRow        []triggers.Trigger
	afterStatement  []triggers.Trigger
}

func (ti triggerInformation) isEmpty() bool {
	return len(ti.beforeStatement) == 0 &&
		len(ti.beforeRow) == 0 &&
		len(ti.afterRow) == 0 &&
		len(ti.afterStatement) == 0
}

// getTriggerInformation loads information that is common for the different trigger types.
func getTriggerInformation(ctx *sql.Context, node sql.Node) (triggerInformation, error) {
	var tbl sql.Table
	var err error
	switch node := node.(type) {
	case *plan.DeleteFrom:
		tbl, err = plan.GetDeletable(node.Child)
		if err != nil {
			return triggerInformation{}, err
		}
	case *plan.InsertInto:
		tbl, err = plan.GetInsertable(node.Destination)
		if err != nil {
			return triggerInformation{}, err
		}
	case *plan.Truncate:
		tbl, err = plan.GetTruncatable(node.Child)
		if err != nil {
			return triggerInformation{}, err
		}
	case *plan.Update:
		// TODO: If there is a JoinNode in here, then don't bother calling GetUpdatable, because
		//       it doesn't currently return a type that can be used to query trigger information.
		//       We need to rework the plan.GetUpdatable() API to support returning multiple
		//       update targets and to return types that are compatible with the interfaces
		//       Doltgres needs in order to populate trigger information.
		if hasJoinNode(node) {
			return triggerInformation{}, nil
		}

		tbl, err = plan.GetUpdatable(node.Child)
		if err != nil {
			return triggerInformation{}, err
		}
	default:
		return triggerInformation{}, nil
	}

	dbName := ctx.GetCurrentDatabase()
	// TODO: some dolt tables don't implement this interface, so we use the current db for now
	// An alternative would be to get the resolved table and use the db there.
	schTbl, ok := tbl.(sql.DatabaseSchemaTable)
	if ok {
		dbName = schTbl.DatabaseSchema().Name()
	}

	trigCollection, err := core.GetTriggersCollectionFromContext(ctx, dbName)
	if err != nil {
		return triggerInformation{}, err
	}

	tblID, ok, _ := id.GetFromTable(ctx, tbl)
	if !ok {
		return triggerInformation{}, nil
	}
	allTrigs := trigCollection.GetTriggersForTable(ctx, tblID)
	info := triggerInformation{
		sch: tbl.Schema(ctx),
	}
	// Return early if there are no triggers for the table
	if len(allTrigs) == 0 {
		return info, nil
	}
	// Trigger order is determined by the name
	sort.Slice(allTrigs, func(i, j int) bool {
		return allTrigs[i].ID.TriggerName() < allTrigs[j].ID.TriggerName()
	})
	for _, trig := range allTrigs {
		matchesEventType := false
		for _, event := range trig.Events {
			switch node.(type) {
			case *plan.DeleteFrom:
				if event.Type == triggers.TriggerEventType_Delete {
					matchesEventType = true
				}
			case *plan.InsertInto:
				if event.Type == triggers.TriggerEventType_Insert {
					matchesEventType = true
				}
			case *plan.Truncate:
				if event.Type == triggers.TriggerEventType_Truncate {
					matchesEventType = true
				}
			case *plan.Update:
				if event.Type == triggers.TriggerEventType_Update {
					matchesEventType = true
				}
			}
		}
		if !matchesEventType {
			continue
		}
		switch trig.Timing {
		case triggers.TriggerTiming_Before:
			if trig.ForEachRow {
				info.beforeRow = append(info.beforeRow, trig)
			} else {
				info.beforeStatement = append(info.beforeStatement, trig)
			}
		case triggers.TriggerTiming_After:
			if trig.ForEachRow {
				info.afterRow = append(info.afterRow, trig)
			} else {
				info.afterStatement = append(info.afterStatement, trig)
			}
		default:
			return triggerInformation{}, fmt.Errorf("trigger timing has not yet been implemented")
		}
	}
	return info, nil
}

// hasJoinNode returns true if |node| or any child is a JoinNode.
func hasJoinNode(node sql.Node) bool {
	updateJoinFound := false
	transform.Inspect(node, func(n sql.Node) bool {
		if _, ok := n.(*plan.JoinNode); ok {
			updateJoinFound = true
		}
		return !updateJoinFound
	})
	return updateJoinFound
}

// getTriggerSource returns the trigger's source node.
func getTriggerSource(node sql.Node) sql.Node {
	switch node := node.(type) {
	case *plan.DeleteFrom:
		return node.Child
	case *plan.InsertInto:
		return node.Source
	case *plan.Truncate:
		return node.Child
	case *plan.Update:
		return node.Child
	default:
		return node
	}
}

// getTriggerRowHandling returns the trigger's row handling type (based on how GMS passes rows in the intermediate
// steps).
func getTriggerRowHandling(node sql.Node) pgnodes.TriggerExecutionRowHandling {
	switch node.(type) {
	case *plan.DeleteFrom:
		return pgnodes.TriggerExecutionRowHandling_Old
	case *plan.InsertInto:
		return pgnodes.TriggerExecutionRowHandling_New
	case *plan.Truncate:
		return pgnodes.TriggerExecutionRowHandling_None
	case *plan.Update:
		return pgnodes.TriggerExecutionRowHandling_OldNew
	default:
		return pgnodes.TriggerExecutionRowHandling_None
	}
}

func getTriggerOperation(node sql.Node) string {
	switch node.(type) {
	case *plan.DeleteFrom:
		return "DELETE"
	case *plan.InsertInto:
		return "INSERT"
	case *plan.Truncate:
		return "TRUNCATE"
	case *plan.Update:
		return "UPDATE"
	default:
		return ""
	}
}

// nodeWithTriggers calls the appropriate WithX function depending on the node type.
func nodeWithTriggers(ctx *sql.Context, node sql.Node, executionNode *pgnodes.TriggerExecution) (sql.Node, error) {
	switch node := node.(type) {
	case *plan.DeleteFrom:
		return node.WithChildren(ctx, executionNode)
	case *plan.InsertInto:
		newNode := node.WithSource(executionNode)
		if executionNode.Return == pgnodes.TriggerExecutionRowHandling_New {
			newNode = newNode.WithColumnNames(schemaColumnNames(executionNode.Sch))
		}
		return newNode, nil
	case *plan.Truncate:
		return node.WithChildren(ctx, executionNode)
	case *plan.Update:
		return node.WithChildren(ctx, executionNode)
	default:
		return nil, fmt.Errorf("unknown node for triggers")
	}
}

func schemaColumnNames(schema sql.Schema) []string {
	columnNames := make([]string, len(schema))
	for i, column := range schema {
		columnNames[i] = column.Name
	}
	return columnNames
}

func insertTriggerDefaultProjections(ctx *sql.Context, insert *plan.InsertInto, schema sql.Schema) ([]sql.Expression, error) {
	columnNames := insert.ColumnNames
	if len(columnNames) == 0 {
		columnNames = schemaColumnNames(schema)
	}

	projections := make([]sql.Expression, len(schema))
	colNameToIdx := make(map[string]int, len(schema))
	for i, c := range schema {
		colNameToIdx[strings.ToLower(c.Name)] = i
		if c.Source != "" {
			colNameToIdx[fmt.Sprintf("%s.%s", strings.ToLower(c.Source), strings.ToLower(c.Name))] = i
		}
	}

	for i, col := range schema {
		colIdx := findInsertColumnIndex(col.Name, columnNames)
		if colIdx != -1 {
			projections[i] = expression.NewGetField(colIdx, col.Type, col.Name, col.Nullable)
			continue
		}

		defaultExpr := col.Default
		if defaultExpr == nil {
			defaultExpr = col.Generated
		}
		if defaultExpr == nil {
			projections[i] = expression.NewLiteral(nil, types.Null)
			continue
		}

		def, _, err := transform.Expr(ctx, defaultExpr, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.GetField:
				key := strings.ToLower(e.Name())
				if e.Table() != "" {
					key = fmt.Sprintf("%s.%s", strings.ToLower(e.Table()), key)
				}
				idx, ok := colNameToIdx[key]
				if !ok {
					return nil, transform.SameTree, fmt.Errorf("field not found: %s", e.String())
				}
				return e.WithIndex(idx), transform.NewTree, nil
			default:
				return e, transform.SameTree, nil
			}
		})
		if err != nil {
			return nil, err
		}
		projections[i] = def
	}

	return projections, nil
}

func findInsertColumnIndex(colName string, columnNames []string) int {
	for i, name := range columnNames {
		if strings.EqualFold(name, colName) {
			return i
		}
	}
	return -1
}
