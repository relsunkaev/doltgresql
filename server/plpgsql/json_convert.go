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
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

type jsonConversionContext struct {
	datumNames            map[int32]string
	datumRows             map[int32][]field
	cursorQueries         map[int32]string
	cursorArgNames        map[int32][]string
	returnExpressions     []string
	returnIndex           *int
	returnNextExpressions []string
	returnNextIndex       *int
}

func newJSONConversionContext(datums []datum, source string) jsonConversionContext {
	returnIndex := 0
	returnNextIndex := 0
	conv := jsonConversionContext{
		datumNames:            make(map[int32]string, len(datums)),
		datumRows:             make(map[int32][]field),
		cursorQueries:         make(map[int32]string),
		cursorArgNames:        make(map[int32][]string),
		returnExpressions:     extractReturnExpressions(source),
		returnIndex:           &returnIndex,
		returnNextExpressions: extractReturnNextExpressions(source),
		returnNextIndex:       &returnNextIndex,
	}
	for i, d := range datums {
		datumNumber := int32(i)
		switch {
		case d.Record != nil:
			if d.Record.RefName != "" {
				conv.datumNames[datumNumber] = d.Record.RefName
				conv.datumNames[d.Record.DatumNumber] = d.Record.RefName
			}
		case d.Row != nil:
			if d.Row.RefName != "" {
				conv.datumNames[datumNumber] = d.Row.RefName
			}
			conv.datumRows[datumNumber] = d.Row.Fields
		case d.Variable != nil:
			if d.Variable.RefName != "" {
				conv.datumNames[datumNumber] = d.Variable.RefName
			}
			if d.Variable.CursorExplicitExpression != nil && d.Variable.CursorExplicitExpression.Expression.Query != "" {
				conv.cursorQueries[datumNumber] = d.Variable.CursorExplicitExpression.Expression.Query
			}
		}
	}
	for i, d := range datums {
		if d.Variable == nil || d.Variable.CursorExplicitExpression == nil || d.Variable.CursorExplicitArgRow < 0 {
			continue
		}
		fields := conv.datumRows[d.Variable.CursorExplicitArgRow]
		argNames := make([]string, 0, len(fields))
		for _, cursorArg := range fields {
			name := cursorArg.Name
			if name == "" {
				name, _ = conv.datumName(cursorArg.VariableNumber)
			}
			if name != "" {
				argNames = append(argNames, name)
			}
		}
		if len(argNames) > 0 {
			conv.cursorArgNames[int32(i)] = argNames
		}
	}
	return conv
}

var returnExpressionRegex = regexp.MustCompile(`(?i)\breturn(?:\s+([^;]*))?;`)
var returnNextExpressionRegex = regexp.MustCompile(`(?i)\breturn\s+next(?:\s+([^;]*))?;`)

func extractReturnExpressions(source string) []string {
	matches := returnExpressionRegex.FindAllStringSubmatch(source, -1)
	expressions := make([]string, 0, len(matches))
	for _, match := range matches {
		var expression string
		if len(match) >= 2 {
			expression = strings.TrimSpace(match[1])
		}
		lowerExpression := strings.ToLower(expression)
		if lowerExpression == "next" || strings.HasPrefix(lowerExpression, "next ") ||
			lowerExpression == "query" || strings.HasPrefix(lowerExpression, "query ") {
			continue
		}
		expressions = append(expressions, expression)
	}
	return expressions
}

func extractReturnNextExpressions(source string) []string {
	matches := returnNextExpressionRegex.FindAllStringSubmatch(source, -1)
	expressions := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			expressions = append(expressions, "")
			continue
		}
		expressions = append(expressions, strings.TrimSpace(match[1]))
	}
	return expressions
}

func (conv jsonConversionContext) nextReturnNextExpression() string {
	if conv.returnNextIndex == nil || *conv.returnNextIndex >= len(conv.returnNextExpressions) {
		return ""
	}
	expression := conv.returnNextExpressions[*conv.returnNextIndex]
	*conv.returnNextIndex = *conv.returnNextIndex + 1
	return expression
}

func (conv jsonConversionContext) nextReturnExpression() string {
	if conv.returnIndex == nil || *conv.returnIndex >= len(conv.returnExpressions) {
		return ""
	}
	expression := conv.returnExpressions[*conv.returnIndex]
	*conv.returnIndex = *conv.returnIndex + 1
	return expression
}

func (conv jsonConversionContext) datumName(datumNumber int32) (string, bool) {
	name, ok := conv.datumNames[datumNumber]
	return name, ok
}

func (conv jsonConversionContext) cursorQuery(datumNumber int32) (string, bool) {
	query, ok := conv.cursorQueries[datumNumber]
	return query, ok
}

func (conv jsonConversionContext) cursorArgs(datumNumber int32) []string {
	return conv.cursorArgNames[datumNumber]
}

// jsonConvert handles the conversion from the JSON format into a format that is easier to work with.
func jsonConvert(jsonBlock plpgSQL_block, source string) (Block, error) {
	conv := newJSONConversionContext(jsonBlock.Datums, source)
	block := Block{
		TriggerNew: jsonBlock.NewVariableNumber,
		TriggerOld: jsonBlock.OldVariableNumber,
		Label:      jsonBlock.Action.StmtBlock.Label,
	}
	lowestRecordNumber := int32(2147483647)
	// We do a first loop to determine the offset for the first record
	for _, v := range jsonBlock.Datums {
		switch {
		case v.Record != nil:
			if v.Record.DatumNumber < lowestRecordNumber {
				lowestRecordNumber = v.Record.DatumNumber
			}
		}
	}
	offset := int32(0) - lowestRecordNumber
	// Then we do a second loop that actually adds all of the datums to the block
	for _, v := range jsonBlock.Datums {
		switch {
		case v.Record != nil:
			// TODO: support normal record types
			datumNumber := v.Record.DatumNumber + offset
			if int(datumNumber) >= len(block.Records) {
				oldRecords := block.Records
				block.Records = make([]Record, datumNumber+1)
				copy(block.Records, oldRecords)
			}

			if v.Record.DatumNumber > 0 {
				block.Records[datumNumber].Name = v.Record.RefName
			}
		case v.RecordField != nil:
			recordParentNumber := v.RecordField.RecordParentNumber + offset
			if int(recordParentNumber) >= len(block.Records) {
				return Block{}, errors.New("invalid record parent number")
			}
			block.Records[recordParentNumber].Fields = append(
				block.Records[recordParentNumber].Fields, v.RecordField.FieldName)
		case v.Row != nil:
		case v.Variable != nil:
			block.Variables = append(block.Variables, Variable{
				Name:        v.Variable.RefName,
				Type:        strings.ToLower(v.Variable.Type.Type.Name),
				IsParameter: v.Variable.LineNumber == 0,
				NotNull:     v.Variable.NotNull,
				Default:     v.Variable.Default.Var.Query,
			})
		default:
			return Block{}, errors.Errorf("unhandled datum type: %T", v)
		}
	}
	var err error
	block.Body, err = conv.convertBlockBody(jsonBlock.Action.StmtBlock)
	if err != nil {
		return Block{}, err
	}
	return block, nil
}

func (conv jsonConversionContext) convertBlockBody(stmt plpgSQL_stmt_block) ([]Statement, error) {
	body, err := conv.convertStatements(stmt.Body)
	if err != nil {
		return nil, err
	}
	if stmt.Exceptions == nil {
		return body, nil
	}
	handlers := make([]ExceptionHandler, 0, len(stmt.Exceptions.ExceptionBlock.ExceptionList))
	for _, exceptionStmt := range stmt.Exceptions.ExceptionBlock.ExceptionList {
		handlerBody, err := conv.convertStatements(exceptionStmt.Exception.Action)
		if err != nil {
			return nil, err
		}
		conditions := make([]string, 0, len(exceptionStmt.Exception.Conditions))
		for _, condition := range exceptionStmt.Exception.Conditions {
			conditions = append(conditions, strings.ToLower(condition.Condition.ConditionName))
		}
		handlers = append(handlers, ExceptionHandler{
			Conditions: conditions,
			Body:       handlerBody,
		})
	}
	return []Statement{ExceptionBlock{
		Body:     body,
		Handlers: handlers,
	}}, nil
}

// convertStatement converts a statement in JSON form to the output form.
func (conv jsonConversionContext) convertStatement(stmt statement) (Statement, error) {
	switch {
	case stmt.Assignment != nil:
		return stmt.Assignment.Convert()
	case stmt.Assert != nil:
		return stmt.Assert.Convert(), nil
	case stmt.Block != nil:
		stmts, err := conv.convertBlockBody(*stmt.Block)
		if err != nil {
			return Block{}, err
		}
		return Block{
			Body: stmts,
		}, nil
	case stmt.Call != nil:
		return stmt.Call.Convert()
	case stmt.Case != nil:
		return stmt.Case.Convert(conv)
	case stmt.Commit != nil:
		return stmt.Commit.Convert(), nil
	case stmt.DynExec != nil:
		return stmt.DynExec.Convert()
	case stmt.DynForSLoop != nil:
		return stmt.DynForSLoop.Convert(conv)
	case stmt.ExecSQL != nil:
		return stmt.ExecSQL.Convert()
	case stmt.Exit != nil:
		return stmt.Exit.Convert(), nil
	case stmt.ForILoop != nil:
		return stmt.ForILoop.Convert(conv)
	case stmt.ForEachLoop != nil:
		return stmt.ForEachLoop.Convert(conv)
	case stmt.ForSLoop != nil:
		return stmt.ForSLoop.Convert(conv)
	case stmt.GetDiag != nil:
		return stmt.GetDiag.Convert(conv)
	case stmt.If != nil:
		return stmt.If.Convert(conv)
	case stmt.Loop != nil:
		return stmt.Loop.Convert(conv)
	case stmt.Open != nil:
		return stmt.Open.Convert(conv)
	case stmt.Perform != nil:
		return stmt.Perform.Convert(), nil
	case stmt.Raise != nil:
		return stmt.Raise.Convert(), nil
	case stmt.Return != nil:
		return stmt.Return.Convert(conv.nextReturnExpression()), nil
	case stmt.ReturnNext != nil:
		return stmt.ReturnNext.Convert(conv.nextReturnNextExpression()), nil
	case stmt.ReturnQuery != nil:
		return stmt.ReturnQuery.Convert(), nil
	case stmt.Rollback != nil:
		return stmt.Rollback.Convert(), nil
	case stmt.Fetch != nil:
		return stmt.Fetch.Convert(conv)
	case stmt.Close != nil:
		return stmt.Close.Convert(conv)
	case stmt.While != nil:
		return stmt.While.Convert(conv)
	default:
		return Block{}, errors.Errorf("unhandled statement type: %T", stmt)
	}
}

// convertStatements converts a collection of statements in JSON form to their output form.
func (conv jsonConversionContext) convertStatements(stmts []statement) ([]Statement, error) {
	newStmts := make([]Statement, len(stmts))
	for i, stmt := range stmts {
		var err error
		newStmts[i], err = conv.convertStatement(stmt)
		if err != nil {
			return nil, err
		}
	}
	return newStmts, nil
}
