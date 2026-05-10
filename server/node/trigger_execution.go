// Copyright 2025 Dolthub, Inc.
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
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/rowexec"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/plpgsql"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// TriggerExecutionRowHandling states how to interpret the source row, or how to return the resulting row.
type TriggerExecutionRowHandling uint8

const (
	TriggerExecutionRowHandling_None TriggerExecutionRowHandling = iota
	TriggerExecutionRowHandling_Old
	TriggerExecutionRowHandling_OldNew
	TriggerExecutionRowHandling_NewOld
	TriggerExecutionRowHandling_New
)

// TriggerExecution handles the execution of a set of triggers on a table.
type TriggerExecution struct {
	Timing                   triggers.TriggerTiming
	Statement                bool
	Operation                string
	Triggers                 []triggers.Trigger
	Split                    TriggerExecutionRowHandling // How the source row should be split
	Return                   TriggerExecutionRowHandling // How the returned rows should be combined
	Sch                      sql.Schema
	Source                   sql.Node
	Runner                   pgexprs.StatementRunner
	InsertDefaultProjections []sql.Expression
}

var _ sql.ExecBuilderNode = (*TriggerExecution)(nil)
var _ sql.Expressioner = (*TriggerExecution)(nil)

func (te *TriggerExecution) Children() []sql.Node {
	return []sql.Node{te.Source}
}

// Expressions implements the interface sql.Expressioner.
func (te *TriggerExecution) Expressions() []sql.Expression {
	return []sql.Expression{te.Runner}
}

// IsReadOnly implements the interface sql.ExecBuilderNode.
func (te *TriggerExecution) IsReadOnly() bool {
	return te.Source.IsReadOnly()
}

// Resolved implements the interface sql.ExecBuilderNode.
func (te *TriggerExecution) Resolved() bool {
	return te.Source.Resolved()
}

// BuildRowIter implements the interface sql.ExecBuilderNode.
func (te *TriggerExecution) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, r sql.Row) (sql.RowIter, error) {
	sourceIter, err := b.Build(ctx, te.Source, r)
	if err != nil {
		return nil, err
	}
	// If there are no triggers, then we'll just return the source iter
	if len(te.Triggers) == 0 {
		return sourceIter, nil
	}
	trigFuncs := make([]framework.InterpretedFunction, len(te.Triggers))
	whens := make([]framework.InterpretedFunction, len(te.Triggers))
	for i, trig := range te.Triggers {
		trigFuncs[i], err = te.loadTriggerFunction(ctx, trig)
		if err != nil {
			return nil, err
		}
		// If we have a WHEN expression, then we need to build a "function" to execute the expression
		if len(trig.When) > 0 {
			whens[i] = framework.InterpretedFunction{
				ID:         trigFuncs[i].ID, // Assign the same ID just so we have a valid one for later
				ReturnType: pgtypes.Bool,
				Statements: trig.When,
			}
		}
	}

	return &triggerExecutionIter{
		triggers:                 te.Triggers,
		functions:                trigFuncs,
		whens:                    whens,
		statement:                te.Statement,
		split:                    te.Split,
		treturn:                  te.Return,
		runner:                   te.Runner.Runner,
		sch:                      te.Sch,
		source:                   sourceIter,
		tgOp:                     te.Operation,
		timing:                   te.Timing,
		insertDefaultProjections: te.InsertDefaultProjections,
	}, nil
}

// Schema implements the interface sql.ExecBuilderNode.
func (te *TriggerExecution) Schema(ctx *sql.Context) sql.Schema {
	switch te.Return {
	case TriggerExecutionRowHandling_Old, TriggerExecutionRowHandling_New:
		return te.Sch
	case TriggerExecutionRowHandling_OldNew, TriggerExecutionRowHandling_NewOld:
		sch := make(sql.Schema, 0, len(te.Sch)*2)
		sch = append(sch, te.Sch...)
		sch = append(sch, te.Sch...)
		return sch
	default:
		return te.Source.Schema(ctx)
	}
}

// String implements the interface sql.ExecBuilderNode.
func (te *TriggerExecution) String() string {
	return "TRIGGER EXECUTION"
}

// WithChildren implements the interface sql.ExecBuilderNode.
func (te *TriggerExecution) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(te, len(children), 1)
	}
	newTe := *te
	newTe.Source = children[0]
	return &newTe, nil
}

// WithExpressions implements the interface sql.Expressioner.
func (te *TriggerExecution) WithExpressions(ctx *sql.Context, expressions ...sql.Expression) (sql.Node, error) {
	if len(expressions) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(te, len(expressions), 1)
	}
	newTe := *te
	newTe.Runner = expressions[0].(pgexprs.StatementRunner)
	return &newTe, nil
}

// loadTriggerFunction loads the given trigger's framework.InterpretedFunction.
func (te *TriggerExecution) loadTriggerFunction(ctx *sql.Context, trigger triggers.Trigger) (framework.InterpretedFunction, error) {
	function, err := loadFunction(ctx, nil, trigger.Function)
	if err != nil {
		return framework.InterpretedFunction{}, err
	}
	if !function.ID.IsValid() {
		return framework.InterpretedFunction{}, errors.Errorf("function %s() does not exist", trigger.Function.FunctionName())
	}
	if function.ReturnType != pgtypes.Trigger.ID {
		return framework.InterpretedFunction{}, errors.Errorf(`function %s must return type trigger`, function.ID.FunctionName())
	}
	return framework.InterpretedFunction{
		ID:                 function.ID,
		ReturnType:         pgtypes.Trigger,
		ParameterNames:     nil,
		ParameterTypes:     nil,
		Variadic:           function.Variadic,
		IsNonDeterministic: function.IsNonDeterministic,
		Strict:             function.Strict,
		Statements:         function.Operations,
	}, nil
}

// triggerExecutionIter is the iterator for TriggerExecution.
type triggerExecutionIter struct {
	triggers                 []triggers.Trigger
	functions                []framework.InterpretedFunction
	whens                    []framework.InterpretedFunction
	statement                bool
	statementFired           bool
	split                    TriggerExecutionRowHandling
	treturn                  TriggerExecutionRowHandling
	runner                   sql.StatementRunner
	sch                      sql.Schema
	source                   sql.RowIter
	tgOp                     string
	timing                   triggers.TriggerTiming
	insertDefaultProjections []sql.Expression
	sourceClosed             bool
	oldRows                  []sql.Row
	newRows                  []sql.Row
	pendingRows              []sql.Row
	pendingRowIdx            int
	afterRowsDrained         bool
}

var _ sql.RowIter = (*triggerExecutionIter)(nil)

// Next implements the interface sql.RowIter.
func (t *triggerExecutionIter) Next(ctx *sql.Context) (sql.Row, error) {
	if t.statement {
		return t.nextStatement(ctx)
	}
	if t.hasAfterRowTransitionTables() {
		return t.nextAfterRowWithTransitionTables(ctx)
	}

	nextRow, err := t.source.Next(ctx)
	if err != nil {
		return nextRow, err
	}
	return t.fireRowTriggers(ctx, nextRow)
}

func (t *triggerExecutionIter) nextAfterRowWithTransitionTables(ctx *sql.Context) (sql.Row, error) {
	if !t.afterRowsDrained {
		for {
			nextRow, err := t.source.Next(ctx)
			if err == nil {
				t.collectTransitionRows(nextRow)
				t.pendingRows = append(t.pendingRows, cloneRow(nextRow))
				continue
			}
			if err != io.EOF {
				return nextRow, err
			}
			t.afterRowsDrained = true
			if closeErr := t.closeSource(ctx); closeErr != nil {
				return nil, closeErr
			}
			break
		}
	}
	if t.pendingRowIdx >= len(t.pendingRows) {
		return nil, io.EOF
	}
	nextRow := t.pendingRows[t.pendingRowIdx]
	t.pendingRowIdx++
	return t.fireRowTriggers(ctx, nextRow)
}

func (t *triggerExecutionIter) fireRowTriggers(ctx *sql.Context, nextRow sql.Row) (sql.Row, error) {
	var err error
	oldRow, newRow := splitTriggerRow(t.split, t.sch, nextRow)
	if len(t.insertDefaultProjections) > 0 {
		newRow, err = rowexec.ProjectRow(ctx, t.insertDefaultProjections, newRow)
		if err != nil {
			return nil, err
		}
	}

	for funcIdx, trigger := range t.triggers {
		function := t.functions[funcIdx]
		triggerVars := t.triggerVars(trigger, "ROW")
		restore, err := t.installRowTransitionTables(ctx, trigger)
		if err != nil {
			return nil, err
		}
		if t.whens[funcIdx].ID.IsValid() {
			whenValue, err := plpgsql.TriggerCall(ctx, t.whens[funcIdx], t.runner, t.sch, oldRow, newRow, triggerVars)
			if err != nil {
				restoreErr := restore()
				if restoreErr != nil {
					return nil, restoreErr
				}
				if strings.Contains(err.Error(), "no valid cast for return value") {
					// TODO: this error should technically be caught during parsing, but interpreted functions don't
					//  have the ability to determine types during parsing yet (also applies to the same error below)
					return nil, fmt.Errorf("argument of WHEN must be type boolean")
				}
				return nil, err
			}
			whenBool, ok := whenValue.(bool)
			if !ok {
				restoreErr := restore()
				if restoreErr != nil {
					return nil, restoreErr
				}
				return nil, fmt.Errorf("argument of WHEN must be type boolean")
			}
			if !whenBool {
				if err = restore(); err != nil {
					return nil, err
				}
				continue
			}
		}

		returnedValue, err := plpgsql.TriggerCall(ctx, function, t.runner, t.sch, oldRow, newRow, triggerVars)
		restoreErr := restore()
		if err != nil {
			return nil, err
		}
		if restoreErr != nil {
			return nil, restoreErr
		}

		if returnedValue == nil {
			// a returned value of NULL on a BEFORE trigger means to not modify the row, so we return a signal error
			if t.timing == triggers.TriggerTiming_Before {
				return nil, sql.ErrRowEditCanceled.New()
			} else {
				return nextRow, nil
			}
		}
		var ok bool
		returnedRow, ok := returnedValue.(sql.Row)
		if !ok {
			return nil, fmt.Errorf("invalid trigger return value")
		}
		switch t.split {
		case TriggerExecutionRowHandling_Old:
			oldRow = returnedRow
		case TriggerExecutionRowHandling_OldNew, TriggerExecutionRowHandling_NewOld, TriggerExecutionRowHandling_New:
			newRow = returnedRow
		}
	}
	switch t.treturn {
	case TriggerExecutionRowHandling_Old:
		return oldRow, nil
	case TriggerExecutionRowHandling_OldNew:
		retRow := make(sql.Row, len(nextRow))
		copy(retRow, oldRow)
		copy(retRow[len(oldRow):], newRow)
		return retRow, nil
	case TriggerExecutionRowHandling_NewOld:
		retRow := make(sql.Row, len(nextRow))
		copy(retRow, newRow)
		copy(retRow[len(newRow):], oldRow)
		return retRow, nil
	case TriggerExecutionRowHandling_New:
		return newRow, nil
	default:
		return nextRow, nil
	}
}

func (t *triggerExecutionIter) hasAfterRowTransitionTables() bool {
	if t.timing != triggers.TriggerTiming_After {
		return false
	}
	for _, trigger := range t.triggers {
		if len(trigger.OldTransitionName) > 0 || len(trigger.NewTransitionName) > 0 {
			return true
		}
	}
	return false
}

func (t *triggerExecutionIter) installRowTransitionTables(ctx *sql.Context, trigger triggers.Trigger) (func() error, error) {
	if len(trigger.OldTransitionName) == 0 && len(trigger.NewTransitionName) == 0 {
		return func() error { return nil }, nil
	}
	return installTransitionTables(ctx, trigger, t.sch, t.oldRows, t.newRows)
}

func (t *triggerExecutionIter) nextStatement(ctx *sql.Context) (sql.Row, error) {
	if t.timing == triggers.TriggerTiming_Before && !t.statementFired {
		t.statementFired = true
		if err := t.fireStatementTriggers(ctx); err != nil {
			return nil, err
		}
	}

	nextRow, err := t.source.Next(ctx)
	if err == nil {
		t.collectTransitionRows(nextRow)
		return nextRow, nil
	}
	if err != io.EOF {
		return nextRow, err
	}
	if t.timing == triggers.TriggerTiming_After && !t.statementFired {
		t.statementFired = true
		if closeErr := t.closeSource(ctx); closeErr != nil {
			return nil, closeErr
		}
		if fireErr := t.fireStatementTriggers(ctx); fireErr != nil {
			return nil, fireErr
		}
	}
	return nextRow, err
}

func (t *triggerExecutionIter) collectTransitionRows(row sql.Row) {
	if len(row) == 0 {
		return
	}
	oldRow, newRow := splitTriggerRow(t.split, t.sch, row)
	if oldRow != nil {
		t.oldRows = append(t.oldRows, cloneRow(oldRow))
	}
	if newRow != nil {
		t.newRows = append(t.newRows, cloneRow(newRow))
	}
}

func (t *triggerExecutionIter) fireStatementTriggers(ctx *sql.Context) error {
	for funcIdx, trigger := range t.triggers {
		triggerVars := t.triggerVars(trigger, "STATEMENT")
		if t.whens[funcIdx].ID.IsValid() {
			whenValue, err := plpgsql.TriggerCall(ctx, t.whens[funcIdx], t.runner, t.sch, nil, nil, triggerVars)
			if err != nil {
				if strings.Contains(err.Error(), "no valid cast for return value") {
					return fmt.Errorf("argument of WHEN must be type boolean")
				}
				return err
			}
			whenBool, ok := whenValue.(bool)
			if !ok {
				return fmt.Errorf("argument of WHEN must be type boolean")
			}
			if !whenBool {
				continue
			}
		}

		restore, err := installTransitionTables(ctx, trigger, t.sch, t.oldRows, t.newRows)
		if err != nil {
			return err
		}
		_, callErr := plpgsql.TriggerCall(ctx, t.functions[funcIdx], t.runner, t.sch, nil, nil, triggerVars)
		restoreErr := restore()
		if callErr != nil {
			return callErr
		}
		if restoreErr != nil {
			return restoreErr
		}
	}
	return nil
}

func (t *triggerExecutionIter) triggerVars(trigger triggers.Trigger, level string) map[string]any {
	triggerVars := make(map[string]any)
	triggerVars["TG_NAME"] = trigger.ID.TriggerName()
	triggerVars["TG_WHEN"] = triggerTimingString(t.timing)
	triggerVars["TG_LEVEL"] = level
	if t.tgOp != "" {
		triggerVars["TG_OP"] = t.tgOp
	}
	triggerVars["TG_RELID"] = triggerTableID(trigger).AsId()
	triggerVars["TG_RELNAME"] = trigger.ID.TableName()
	triggerVars["TG_TABLE_NAME"] = trigger.ID.TableName()
	triggerVars["TG_TABLE_SCHEMA"] = trigger.ID.SchemaName()
	triggerVars["TG_NARGS"] = int32(len(trigger.Arguments))
	return triggerVars
}

func triggerTimingString(timing triggers.TriggerTiming) string {
	switch timing {
	case triggers.TriggerTiming_Before:
		return "BEFORE"
	case triggers.TriggerTiming_After:
		return "AFTER"
	case triggers.TriggerTiming_InsteadOf:
		return "INSTEAD OF"
	default:
		return ""
	}
}

func triggerTableID(trigger triggers.Trigger) id.Table {
	return id.NewTable(trigger.ID.SchemaName(), trigger.ID.TableName())
}

func splitTriggerRow(handling TriggerExecutionRowHandling, sch sql.Schema, row sql.Row) (oldRow sql.Row, newRow sql.Row) {
	switch handling {
	case TriggerExecutionRowHandling_Old:
		oldRow = row
	case TriggerExecutionRowHandling_OldNew:
		oldRow = row[:len(sch)]
		newRow = row[len(sch):]
	case TriggerExecutionRowHandling_NewOld:
		newRow = row[:len(sch)]
		oldRow = row[len(sch):]
	case TriggerExecutionRowHandling_New:
		newRow = row
	}
	return oldRow, newRow
}

func cloneRow(row sql.Row) sql.Row {
	if row == nil {
		return nil
	}
	cloned := make(sql.Row, len(row))
	copy(cloned, row)
	return cloned
}

// Close implements the interface sql.RowIter.
func (t *triggerExecutionIter) Close(ctx *sql.Context) error {
	return t.closeSource(ctx)
}

func (t *triggerExecutionIter) closeSource(ctx *sql.Context) error {
	if t.sourceClosed {
		return nil
	}
	t.sourceClosed = true
	return t.source.Close(ctx)
}
