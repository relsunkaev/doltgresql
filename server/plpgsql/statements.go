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

package plpgsql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/cockroachdb/errors"
	pg_query "github.com/dolthub/pg_query_go/v6"
)

// Statement represents a PL/pgSQL statement.
type Statement interface {
	// OperationSize reports the number of operations that the statement will convert to.
	OperationSize() int32
	// AppendOperations adds the statement to the operation slice.
	AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error
}

// Assignment represents an assignment statement.
type Assignment struct {
	VariableName  string
	Expression    string
	VariableIndex int32 // TODO: figure out what this is used for, probably to get around shadowed variables?
	LineNumber    int32
}

var _ Statement = Assignment{}

// OperationSize implements the interface Statement.
func (Assignment) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Assignment) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	expression, referencedVariables, err := substituteVariableReferences(stmt.Expression, stack)
	if err != nil {
		return err
	}

	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_Assign,
		PrimaryData:   "SELECT " + expression + ";",
		SecondaryData: referencedVariables,
		Target:        stmt.VariableName,
		Options:       diagnosticStatementOptions(stmt.LineNumber, "assignment", ""),
	})
	return nil
}

// Block contains a collection of statements, alongside the variables that were declared for the block. Only the
// top-level block will contain parameter variables.
type Block struct {
	TriggerNew int32 // When non-zero, indicates that the NEW record exists for use with triggers
	TriggerOld int32 // When non-zero, indicates that the OLD record exists for use with triggers
	Variables  []Variable
	Records    []Record
	Body       []Statement
	Label      string
	IsLoop     bool
}

var _ Statement = Block{}

// OperationSize implements the interface Statement.
func (stmt Block) OperationSize() int32 {
	total := int32(2) // We start with 2 since we'll have ScopeBegin and ScopeEnd
	for _, variable := range stmt.Variables {
		if !variable.IsParameter {
			total++
		}
	}
	for _, record := range stmt.Records {
		if record.Name != "" {
			total++
		}
	}
	for _, innerStmt := range stmt.Body {
		total += innerStmt.OperationSize()
	}
	return total
}

// AppendOperations implements the interface Statement.
func (stmt Block) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	stack.PushScope()
	stack.SetLabel(stmt.Label) // If the label is empty, then this won't change anything
	var loop string
	if stmt.IsLoop {
		loop = "_"
		// All loops need a label, so we'll make an anonymous one if an explicit one hasn't been given
		if len(stmt.Label) == 0 {
			stack.SetAnonymousLabel()
			stmt.Label = stack.GetCurrentLabel()
		}
	}
	*ops = append(*ops, InterpreterOperation{
		OpCode:      OpCode_ScopeBegin,
		PrimaryData: stmt.Label,
		Target:      loop,
	})
	for _, variable := range stmt.Variables {
		op := InterpreterOperation{
			OpCode:      OpCode_Declare,
			PrimaryData: variable.Type,
			Target:      variable.Name,
		}
		var val any
		if variable.Default != "" {
			op.SecondaryData = []string{variable.Default}
			val = variable.Default
		}
		if !variable.IsParameter {
			*ops = append(*ops, op)
		}
		stack.NewVariableWithValue(variable.Name, nil, val)
	}
	for _, record := range stmt.Records {
		var fakeSch sql.Schema
		for _, fieldName := range record.Fields {
			fakeSch = append(fakeSch, &sql.Column{Name: fieldName})
		}
		if record.Name != "" {
			*ops = append(*ops, InterpreterOperation{
				OpCode:      OpCode_Declare,
				PrimaryData: "record",
				Target:      record.Name,
			})
		}
		stack.NewRecord(record.Name, fakeSch, nil)
	}
	for _, innerStmt := range stmt.Body {
		if err := innerStmt.AppendOperations(ops, stack); err != nil {
			return err
		}
	}
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_ScopeEnd,
	})
	stack.PopScope()
	return nil
}

// ExecuteSQL represents a standard SQL statement's execution (including the INTO syntax).
type ExecuteSQL struct {
	Statement  string
	Target     string
	LineNumber int32
}

var _ Statement = ExecuteSQL{}

// OperationSize implements the interface Statement.
func (ExecuteSQL) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt ExecuteSQL) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	statementStr, referencedVariables, err := substituteVariableReferences(stmt.Statement, stack)
	if err != nil {
		return err
	}
	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_Execute,
		PrimaryData:   statementStr,
		SecondaryData: referencedVariables,
		Target:        stmt.Target,
		Options:       diagnosticStatementOptions(stmt.LineNumber, "SQL statement", stmt.Statement),
	})
	return nil
}

// DynamicExecute represents a dynamic SQL statement's execution.
type DynamicExecute struct {
	Query      string
	Params     []string
	Target     string
	Strict     bool
	LineNumber int32
}

var _ Statement = DynamicExecute{}

// OperationSize implements the interface Statement.
func (DynamicExecute) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt DynamicExecute) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	queryStr, referencedVariables, err := substituteVariableReferences(stmt.Query, stack)
	if err != nil {
		return err
	}
	params := make([]string, 0, len(referencedVariables)+len(stmt.Params))
	params = append(params, referencedVariables...)
	params = append(params, stmt.Params...)
	options := diagnosticStatementOptions(stmt.LineNumber, "EXECUTE", stmt.Query)
	if options == nil {
		options = make(map[string]string)
	}
	options["dynamic"] = "true"
	options["queryBindingCount"] = strconv.Itoa(len(referencedVariables))
	options["strict"] = strconv.FormatBool(stmt.Strict)
	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_Execute,
		PrimaryData:   queryStr,
		SecondaryData: params,
		Target:        stmt.Target,
		Options:       options,
	})
	return nil
}

// GetDiagnosticsItem represents one GET DIAGNOSTICS assignment.
type GetDiagnosticsItem struct {
	Target string
	Kind   string
}

// GetDiagnostics represents a GET DIAGNOSTICS statement.
type GetDiagnostics struct {
	LineNumber int32
	Items      []GetDiagnosticsItem
	Stacked    bool
}

var _ Statement = GetDiagnostics{}

// OperationSize implements the interface Statement.
func (stmt GetDiagnostics) OperationSize() int32 {
	return int32(len(stmt.Items))
}

// AppendOperations implements the interface Statement.
func (stmt GetDiagnostics) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	for _, item := range stmt.Items {
		*ops = append(*ops, InterpreterOperation{
			OpCode:      OpCode_Get,
			PrimaryData: strings.ToUpper(item.Kind),
			Target:      item.Target,
			Options: map[string]string{
				diagnosticOptionLineNumber: strconv.Itoa(int(stmt.LineNumber)),
				"stacked":                  strconv.FormatBool(stmt.Stacked),
			},
		})
	}
	return nil
}

// ExceptionHandler represents one EXCEPTION WHEN branch.
type ExceptionHandler struct {
	Conditions []string
	Body       []Statement
}

// ExceptionBlock represents a block body protected by EXCEPTION handlers.
type ExceptionBlock struct {
	Body     []Statement
	Handlers []ExceptionHandler
}

var _ Statement = ExceptionBlock{}

// OperationSize implements the interface Statement.
func (stmt ExceptionBlock) OperationSize() int32 {
	total := int32(1)
	for _, innerStmt := range stmt.Body {
		total += innerStmt.OperationSize()
	}
	for _, handler := range stmt.Handlers {
		for _, innerStmt := range handler.Body {
			total += innerStmt.OperationSize()
		}
	}
	return total
}

// AppendOperations implements the interface Statement.
func (stmt ExceptionBlock) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	if len(stmt.Handlers) == 0 {
		return errors.New("PL/pgSQL exception block requires at least one handler")
	}
	markerIndex := len(*ops)
	*ops = append(*ops, InterpreterOperation{
		OpCode: OpCode_Exception,
		Options: map[string]string{
			"handlerCount": strconv.Itoa(len(stmt.Handlers)),
		},
	})
	for _, innerStmt := range stmt.Body {
		if err := innerStmt.AppendOperations(ops, stack); err != nil {
			return err
		}
	}
	bodyEnd := len(*ops)
	for i, handler := range stmt.Handlers {
		handlerStart := len(*ops)
		conditionKey := fmt.Sprintf("handlerConditions.%d", i)
		startKey := fmt.Sprintf("handlerStart.%d", i)
		endKey := fmt.Sprintf("handlerEnd.%d", i)
		(*ops)[markerIndex].Options[conditionKey] = strings.Join(handler.Conditions, ",")
		(*ops)[markerIndex].Options[startKey] = strconv.Itoa(handlerStart)
		if i == 0 {
			(*ops)[markerIndex].Options["handlerConditions"] = (*ops)[markerIndex].Options[conditionKey]
			(*ops)[markerIndex].Options["handlerStart"] = (*ops)[markerIndex].Options[startKey]
		}
		for _, innerStmt := range handler.Body {
			if err := innerStmt.AppendOperations(ops, stack); err != nil {
				return err
			}
		}
		handlerEnd := len(*ops)
		(*ops)[markerIndex].Options[endKey] = strconv.Itoa(handlerEnd)
		if i == 0 {
			(*ops)[markerIndex].Options["handlerEnd"] = (*ops)[markerIndex].Options[endKey]
		}
	}
	handlerEnd := len(*ops)
	(*ops)[markerIndex].Index = bodyEnd
	(*ops)[markerIndex].Options["handlerEnd"] = strconv.Itoa(handlerEnd)
	return nil
}

// ForQueryInit executes a SQL query and stores the result set in a named cursor on the stack.
// It is the first operation emitted for a FOR record IN query LOOP statement.
type ForQueryInit struct {
	CursorName string
	Query      string
}

var _ Statement = ForQueryInit{}

// OperationSize implements the interface Statement.
func (ForQueryInit) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt ForQueryInit) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	queryStr, referencedVariables, err := substituteVariableReferences(stmt.Query, stack)
	if err != nil {
		return err
	}
	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_ForQueryInit,
		PrimaryData:   queryStr,
		SecondaryData: referencedVariables,
		Target:        stmt.CursorName,
	})
	return nil
}

// ForQueryNext fetches the next row from a named cursor and assigns it to a record variable.
// When the cursor is exhausted it jumps forward by GotoOffset (like an If), exiting the loop.
type ForQueryNext struct {
	CursorName string
	RecordVar  string
	GotoOffset int32
}

var _ Statement = ForQueryNext{}

// OperationSize implements the interface Statement.
func (ForQueryNext) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt ForQueryNext) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	*ops = append(*ops, InterpreterOperation{
		OpCode:      OpCode_ForQueryNext,
		PrimaryData: stmt.CursorName,
		Target:      stmt.RecordVar,
		Index:       len(*ops) + int(stmt.GotoOffset),
	})
	return nil
}

// Goto jumps to the counter at the given offset.
type Goto struct {
	Offset         int32
	Label          string
	NearestScopeOp bool
}

var _ Statement = Goto{}

// OperationSize implements the interface Statement.
func (Goto) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Goto) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	if len(stmt.Label) > 0 {
		*ops = append(*ops, InterpreterOperation{
			OpCode:      OpCode_Goto,
			PrimaryData: stmt.Label,
			Index:       int(stmt.Offset),
		})
	} else if stmt.NearestScopeOp {
		label := stack.GetCurrentLabel()
		if len(label) == 0 {
			if stmt.Offset > 0 {
				return errors.New("EXIT cannot be used outside a loop, unless it has a label")
			} else {
				return errors.New("CONTINUE cannot be used outside a loop")
			}
		}
		*ops = append(*ops, InterpreterOperation{
			OpCode:      OpCode_Goto,
			PrimaryData: label,
			Index:       int(stmt.Offset),
		})
	} else {
		*ops = append(*ops, InterpreterOperation{
			OpCode: OpCode_Goto,
			Index:  len(*ops) + int(stmt.Offset),
		})
	}
	return nil
}

// If represents an IF condition, alongside its Goto offset if the condition is true.
type If struct {
	Condition  string
	GotoOffset int32
}

var _ Statement = If{}

// OperationSize implements the interface Statement.
func (If) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt If) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	condition, referencedVariables, err := substituteVariableReferences(stmt.Condition, stack)
	if err != nil {
		return err
	}

	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_If,
		PrimaryData:   "SELECT " + condition + ";",
		SecondaryData: referencedVariables,
		Index:         len(*ops) + int(stmt.GotoOffset),
	})
	return nil
}

// Perform represents a PERFORM statement.
type Perform struct {
	Statement  string
	LineNumber int32
}

var _ Statement = Perform{}

// OperationSize implements the interface Statement.
func (Perform) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Perform) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	statementStr, referencedVariables, err := substituteVariableReferences(stmt.Statement, stack)
	if err != nil {
		return err
	}

	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_Perform,
		PrimaryData:   statementStr,
		SecondaryData: referencedVariables,
		Options:       diagnosticStatementOptions(stmt.LineNumber, "PERFORM", stmt.Statement),
	})
	return nil
}

// Raise represents a RAISE statement
type Raise struct {
	Level      string
	Message    string
	Params     []string
	Options    map[string]string
	LineNumber int32
}

var _ Statement = Raise{}

// OperationSize implements the interface Statement.
func (r Raise) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (r Raise) AppendOperations(ops *[]InterpreterOperation, _ *InterpreterStack) error {
	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_Raise,
		PrimaryData:   r.Level,
		SecondaryData: append([]string{r.Message}, r.Params...),
		Options:       mergeDiagnosticStatementOptions(r.Options, r.LineNumber, "RAISE", ""),
	})
	return nil
}

// Record represents a record (along with known fields for future access). These are exclusively found within Block.
type Record struct {
	Name   string
	Fields []string
}

// ReturnQuery represents a RETURN QUERY statement.
type ReturnQuery struct {
	Query string
}

var _ Statement = ReturnQuery{}

// OperationSize implements the interface Statement.
func (r ReturnQuery) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (r ReturnQuery) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	query, referencedVariables, err := substituteVariableReferences(r.Query, stack)
	if err != nil {
		return err
	}

	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_ReturnQuery,
		PrimaryData:   query,
		SecondaryData: referencedVariables,
	})
	return nil
}

// Return represents a RETURN statement.
type Return struct {
	Expression string
}

var _ Statement = Return{}

// OperationSize implements the interface Statement.
func (Return) OperationSize() int32 {
	return 1
}

// AppendOperations implements the interface Statement.
func (stmt Return) AppendOperations(ops *[]InterpreterOperation, stack *InterpreterStack) error {
	expression, referencedVariables, err := substituteVariableReferences(stmt.Expression, stack)
	if err != nil {
		return err
	}
	if len(expression) > 0 {
		expression = "SELECT " + expression + ";"
	}
	*ops = append(*ops, InterpreterOperation{
		OpCode:        OpCode_Return,
		PrimaryData:   expression,
		SecondaryData: referencedVariables,
	})
	return nil
}

// Variable represents a variable. These are exclusively found within Block.
type Variable struct {
	Name        string
	Type        string
	IsParameter bool
	Default     string
}

// OperationSizeForStatements returns the sum of OperationSize for every statement.
func OperationSizeForStatements(stmts []Statement) int32 {
	total := int32(0)
	for _, stmt := range stmts {
		total += stmt.OperationSize()
	}
	return total
}

func diagnosticStatementOptions(lineNumber int32, action, statement string) map[string]string {
	options := make(map[string]string)
	if lineNumber > 0 {
		options[diagnosticOptionLineNumber] = strconv.Itoa(int(lineNumber))
	}
	if strings.TrimSpace(action) != "" {
		options[diagnosticOptionAction] = strings.TrimSpace(action)
	}
	if strings.TrimSpace(statement) != "" {
		options[diagnosticOptionStatement] = strings.TrimSpace(statement)
	}
	if len(options) == 0 {
		return nil
	}
	return options
}

func mergeDiagnosticStatementOptions(options map[string]string, lineNumber int32, action, statement string) map[string]string {
	diagnosticOptions := diagnosticStatementOptions(lineNumber, action, statement)
	if len(diagnosticOptions) == 0 {
		return options
	}
	if options == nil {
		options = make(map[string]string, len(diagnosticOptions))
	}
	for key, value := range diagnosticOptions {
		options[key] = value
	}
	return options
}

// substituteVariableReferences parses the specified |expression| and replaces
// any token that matches a variable name in the |stack| with "$N", where N
// indicates which variable in the returned |referenceVars| slice is used.
func substituteVariableReferences(expression string, stack *InterpreterStack) (newExpression string, referencedVars []string, err error) {
	scanResult, err := pg_query.Scan(expression)
	if err != nil {
		return "", nil, err
	}

	varMap := stack.ListVariables()
	for i := 0; i < len(scanResult.Tokens); i++ {
		token := scanResult.Tokens[i]
		substring := expression[token.Start:token.End]
		// varMap lowercases everything, so we'll lowercase our substring to enable case-insensitivity
		if _, ok := varMap[strings.ToLower(substring)]; ok {
			// If there's a '.', then we'll assume this is accessing a record's field (`NEW.val1` for example)
			for i+2 < len(scanResult.Tokens) && scanResult.Tokens[i+1].Token == '.' {
				nextFieldSubstring := expression[scanResult.Tokens[i+2].Start:scanResult.Tokens[i+2].End]
				substring += "." + nextFieldSubstring
				i += 2
			}
			// Variables cannot have a '(' after their name as that would classify them as functions, so we have to
			// explicitly check for that. This is because variables and functions can share names, for example:
			// SELECT COUNT(*) INTO count FROM table_name;
			if i+1 >= len(scanResult.Tokens) || scanResult.Tokens[i+1].Token != '(' {
				referencedVars = append(referencedVars, substring)
				newExpression += fmt.Sprintf("$%d ", len(referencedVars))
			} else {
				newExpression += substring + " "
			}
		} else if _, ok := triggerSpecialVariables[substring]; ok {
			referencedVars = append(referencedVars, substring)
			newExpression += fmt.Sprintf("$%d ", len(referencedVars))
		} else {
			newExpression += substring + " "
		}
	}

	return newExpression, referencedVars, nil
}
