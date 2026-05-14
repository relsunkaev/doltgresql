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
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	corefunctions "github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/triggers"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	pgfunctions "github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/tablemetadata"
)

// RoutineWithParams represent a function or a procedure with schema name, routine name and its parameters.
type RoutineWithParams struct {
	SchemaName  string
	RoutineName string
	Args        []RoutineParam
}

// DropFunction implements DROP FUNCTION.
type DropFunction struct {
	RoutinesWithArgs []*RoutineWithParams
	IfExists         bool
	Cascade          bool
}

var _ sql.ExecSourceRel = (*DropFunction)(nil)
var _ vitess.Injectable = (*DropFunction)(nil)

// NewDropFunction returns a new *DropFunction.
func NewDropFunction(ifExists bool, routinesWithArgs []*RoutineWithParams, cascade bool) *DropFunction {
	return &DropFunction{
		IfExists:         ifExists,
		RoutinesWithArgs: routinesWithArgs,
		Cascade:          cascade,
	}
}

// Resolved implements the interface sql.ExecSourceRel.
func (d *DropFunction) Resolved() bool {
	return true
}

// String implements the interface sql.ExecSourceRel.
func (d *DropFunction) String() string {
	return "DROP FUNCTION"
}

// Schema implements the interface sql.ExecSourceRel.
func (d *DropFunction) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// Children implements the interface sql.ExecSourceRel.
func (d *DropFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.ExecSourceRel.
func (d *DropFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (d *DropFunction) IsReadOnly() bool {
	return false
}

// RowIter implements the interface sql.ExecSourceRel.
func (d *DropFunction) RowIter(ctx *sql.Context, r sql.Row) (iter sql.RowIter, err error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	for _, routineWithArgs := range d.RoutinesWithArgs {
		err = dropFunction(ctx, funcColl, routineWithArgs, d.IfExists, d.Cascade)
		if err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(), nil
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (d *DropFunction) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

func dropFunction(ctx *sql.Context, funcColl *corefunctions.Collection, fn *RoutineWithParams, ifExists bool, cascade bool) error {
	funcId, err := resolveFunctionID(ctx, funcColl, fn)
	if err != nil {
		return err
	}
	funcExists := funcColl.HasFunction(ctx, funcId)
	if !funcExists && ifExists {
		return nil
	}
	if funcExists {
		function, err := funcColl.GetFunction(ctx, funcId)
		if err != nil {
			return err
		}
		if err = checkFunctionOwnership(ctx, function); err != nil {
			return errors.Wrap(err, "permission denied")
		}
	}

	dependentTriggers, err := dependentTriggersForFunction(ctx, funcId)
	if err != nil {
		return err
	}
	if len(dependentTriggers) > 0 && !cascade {
		return pgerror.Newf(pgcode.DependentObjectsStillExist, "cannot drop function %s because other objects depend on it", funcId.FunctionName())
	}
	hasMetadataDependencies, err := metadataDependsOnFunction(ctx, funcId)
	if err != nil {
		return err
	}
	if hasMetadataDependencies && !cascade {
		return pgerror.Newf(pgcode.DependentObjectsStillExist, "cannot drop function %s because other objects depend on it", funcId.FunctionName())
	}
	if hasMetadataDependencies {
		return pgerror.Newf(pgcode.FeatureNotSupported, "DROP FUNCTION with CASCADE is not supported for this dependent object")
	}
	if len(dependentTriggers) > 0 {
		trigColl, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
		if err != nil {
			return err
		}
		if err = trigColl.DropTrigger(ctx, dependentTriggers...); err != nil {
			return err
		}
		for _, trigID := range dependentTriggers {
			comments.RemoveObject(trigID.AsId(), "pg_trigger")
		}
	}

	if err = funcColl.DropFunction(ctx, funcId); err != nil {
		return err
	}
	clearFunctionComment(funcId)
	var persistErr error
	auth.LockWrite(func() {
		auth.RemoveAllRoutinePrivileges(funcId.SchemaName(), funcId.FunctionName())
		persistErr = auth.PersistChanges()
	})
	return persistErr
}

func dependentTriggersForFunction(ctx *sql.Context, funcId id.Function) ([]id.Trigger, error) {
	trigColl, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	var dependentTriggers []id.Trigger
	if err = trigColl.IterateTriggers(ctx, func(trigger triggers.Trigger) (stop bool, err error) {
		if trigger.Function == funcId {
			dependentTriggers = append(dependentTriggers, trigger.ID)
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	return dependentTriggers, nil
}

func metadataDependsOnFunction(ctx *sql.Context, funcId id.Function) (bool, error) {
	found := false
	err := pgfunctions.IterateCurrentDatabase(ctx, pgfunctions.Callbacks{
		ColumnDefault: func(ctx *sql.Context, schema pgfunctions.ItemSchema, table pgfunctions.ItemTable, colDefault pgfunctions.ItemColumnDefault) (cont bool, err error) {
			col := colDefault.Item.Column
			if col.Default != nil && sqlTextReferencesFunction(col.Default.String(), funcId) {
				found = true
				return false, nil
			}
			if col.Generated != nil && sqlTextReferencesFunction(col.Generated.String(), funcId) {
				found = true
				return false, nil
			}
			return true, nil
		},
		Index: func(ctx *sql.Context, schema pgfunctions.ItemSchema, table pgfunctions.ItemTable, index pgfunctions.ItemIndex) (cont bool, err error) {
			for _, expr := range index.Item.Expressions() {
				if sqlTextReferencesFunction(expr, funcId) {
					found = true
					return false, nil
				}
			}
			return true, nil
		},
		Table: func(ctx *sql.Context, schema pgfunctions.ItemSchema, table pgfunctions.ItemTable) (cont bool, err error) {
			if !tablemetadata.IsMaterializedView(tableComment(table.Item)) {
				return true, nil
			}
			if sqlTextReferencesFunction(tablemetadata.MaterializedViewDefinition(tableComment(table.Item)), funcId) {
				found = true
				return false, nil
			}
			return true, nil
		},
		View: func(ctx *sql.Context, schema pgfunctions.ItemSchema, view pgfunctions.ItemView) (cont bool, err error) {
			if sqlTextReferencesFunction(viewDefinitionSQL(view.Item), funcId) {
				found = true
				return false, nil
			}
			return true, nil
		},
	})
	return found, err
}

func sqlTextReferencesFunction(sqlText string, funcId id.Function) bool {
	for idx := 0; idx < len(sqlText); {
		next, skipped := skipDropFunctionSQLIgnored(sqlText, idx)
		if skipped {
			idx = next
			continue
		}
		first, firstEnd, ok := readDropFunctionSQLIdentifier(sqlText, idx)
		if !ok {
			idx++
			continue
		}
		afterFirst := skipDropFunctionSQLWhitespace(sqlText, firstEnd)
		if afterFirst < len(sqlText) && sqlText[afterFirst] == '.' {
			second, secondEnd, ok := readDropFunctionSQLIdentifier(sqlText, afterFirst+1)
			if ok && functionIdentifierMatches(first, second, funcId) {
				afterSecond := skipDropFunctionSQLWhitespace(sqlText, secondEnd)
				if afterSecond < len(sqlText) && sqlText[afterSecond] == '(' {
					return true
				}
			}
			if ok {
				idx = secondEnd
			} else {
				idx = afterFirst + 1
			}
			continue
		}
		if strings.EqualFold(first, funcId.FunctionName()) && afterFirst < len(sqlText) && sqlText[afterFirst] == '(' {
			return true
		}
		idx = firstEnd
	}
	return false
}

func functionIdentifierMatches(schemaName string, functionName string, funcId id.Function) bool {
	return strings.EqualFold(schemaName, funcId.SchemaName()) && strings.EqualFold(functionName, funcId.FunctionName())
}

func readDropFunctionSQLIdentifier(sqlText string, idx int) (string, int, bool) {
	if idx >= len(sqlText) {
		return "", idx, false
	}
	if sqlText[idx] == '"' {
		var builder strings.Builder
		idx++
		for idx < len(sqlText) {
			if sqlText[idx] == '"' {
				idx++
				if idx < len(sqlText) && sqlText[idx] == '"' {
					builder.WriteByte('"')
					idx++
					continue
				}
				return builder.String(), idx, true
			}
			builder.WriteByte(sqlText[idx])
			idx++
		}
		return "", idx, false
	}
	if !isDropFunctionIdentifierStart(sqlText[idx]) {
		return "", idx, false
	}
	start := idx
	idx++
	for idx < len(sqlText) && isDropFunctionIdentifierPart(sqlText[idx]) {
		idx++
	}
	return sqlText[start:idx], idx, true
}

func skipDropFunctionSQLIgnored(sqlText string, idx int) (int, bool) {
	if idx >= len(sqlText) {
		return idx, false
	}
	switch {
	case sqlText[idx] == '\'':
		return skipDropFunctionSingleQuoted(sqlText, idx), true
	case sqlText[idx] == '-' && idx+1 < len(sqlText) && sqlText[idx+1] == '-':
		idx += 2
		for idx < len(sqlText) && sqlText[idx] != '\n' {
			idx++
		}
		return idx, true
	case sqlText[idx] == '/' && idx+1 < len(sqlText) && sqlText[idx+1] == '*':
		idx += 2
		for idx+1 < len(sqlText) {
			if sqlText[idx] == '*' && sqlText[idx+1] == '/' {
				return idx + 2, true
			}
			idx++
		}
		return len(sqlText), true
	case sqlText[idx] == '$':
		if next, ok := skipDropFunctionDollarQuoted(sqlText, idx); ok {
			return next, true
		}
	}
	return idx, false
}

func skipDropFunctionSingleQuoted(sqlText string, idx int) int {
	idx++
	for idx < len(sqlText) {
		if sqlText[idx] == '\'' {
			idx++
			if idx < len(sqlText) && sqlText[idx] == '\'' {
				idx++
				continue
			}
			return idx
		}
		idx++
	}
	return idx
}

func skipDropFunctionDollarQuoted(sqlText string, idx int) (int, bool) {
	endTag := idx + 1
	for endTag < len(sqlText) && isDropFunctionIdentifierPart(sqlText[endTag]) {
		endTag++
	}
	if endTag >= len(sqlText) || sqlText[endTag] != '$' {
		return idx, false
	}
	tag := sqlText[idx : endTag+1]
	if next := strings.Index(sqlText[endTag+1:], tag); next >= 0 {
		return endTag + 1 + next + len(tag), true
	}
	return len(sqlText), true
}

func skipDropFunctionSQLWhitespace(sqlText string, idx int) int {
	for idx < len(sqlText) {
		switch sqlText[idx] {
		case ' ', '\t', '\n', '\r', '\f':
			idx++
		default:
			return idx
		}
	}
	return idx
}

func isDropFunctionIdentifierStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isDropFunctionIdentifierPart(ch byte) bool {
	return isDropFunctionIdentifierStart(ch) || (ch >= '0' && ch <= '9') || ch == '$'
}

func clearFunctionComment(funcId id.Function) {
	comments.Set(comments.Key{
		ObjOID:   id.Cache().ToOID(funcId.AsId()),
		ClassOID: comments.ClassOID("pg_proc"),
		ObjSubID: 0,
	}, nil)
}

func resolveFunctionID(ctx *sql.Context, funcColl *corefunctions.Collection, fn *RoutineWithParams) (id.Function, error) {
	// TODO: provide db
	schema, err := core.GetSchemaName(ctx, nil, fn.SchemaName)
	if err != nil {
		return id.NullFunction, err
	}
	var funcId = id.NewFunction(schema, fn.RoutineName)
	if len(fn.Args) == 0 {
		funcs, err := funcColl.GetFunctionOverloads(ctx, funcId)
		if err != nil {
			return id.NullFunction, err
		}
		if len(funcs) == 1 {
			funcId = funcs[0].ID
		} else if len(funcs) > 1 {
			return id.NullFunction, pgerror.Newf(pgcode.AmbiguousFunction, `function name "%s" is not unique`, fn.RoutineName)
		}
	} else {
		var argTypes = make([]id.Type, len(fn.Args))
		for i, arg := range fn.Args {
			argTypes[i] = arg.Type.ID
		}
		funcId = id.NewFunction(schema, fn.RoutineName, argTypes...)
	}
	return funcId, nil
}
