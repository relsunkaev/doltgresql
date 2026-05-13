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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
)

func preserveCreateViewFunctionBindings(ctx *sql.Context, createView *plan.CreateView) (sql.Node, transform.TreeIdentity, error) {
	if createView == nil || createView.Definition == nil {
		return createView, transform.SameTree, nil
	}
	definition := createView.Definition
	if childDefinition, ok := createView.Child.(*plan.SubqueryAlias); ok {
		definition = childDefinition
	}
	bindings, err := collectResolvedViewFunctionBindings(ctx, definition)
	if err != nil {
		return nil, transform.SameTree, err
	}
	if len(bindings) == 0 {
		return createView, transform.SameTree, nil
	}
	textDefinition, textChanged := rewriteResolvedFunctionBindings(definition.TextDefinition, bindings)
	createViewString, createChanged := rewriteResolvedFunctionBindings(createView.CreateViewString, bindings)
	if !textChanged && !createChanged && definition == createView.Definition {
		return createView, transform.SameTree, nil
	}

	newDefinition := *definition
	if textChanged {
		newDefinition.TextDefinition = textDefinition
	}
	newCreateView := *createView
	newCreateView.Definition = &newDefinition
	newCreateView.Child = &newDefinition
	if createChanged {
		newCreateView.CreateViewString = createViewString
	}
	return &newCreateView, transform.NewTree, nil
}

func collectResolvedViewFunctionBindings(ctx *sql.Context, node sql.Node) (map[string]string, error) {
	bindings := make(map[string]string)
	conflicts := make(map[string]struct{})
	_, _, err := pgtransform.NodeExprsWithOpaque(ctx, node, func(ctx *sql.Context, expr sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch fn := expr.(type) {
		case *framework.CompiledFunction:
			addResolvedViewFunctionBinding(fn.FunctionName(), fn.ResolvedRoutine, bindings, conflicts)
		case *framework.CompiledAggregateFunction:
			addResolvedViewFunctionBinding(fn.FunctionName(), fn.ResolvedRoutine, bindings, conflicts)
		}
		return expr, transform.SameTree, nil
	})
	return bindings, err
}

func addResolvedViewFunctionBinding(functionName string, resolvedRoutine func() (id.Id, string, bool), bindings map[string]string, conflicts map[string]struct{}) {
	routineID, _, ok := resolvedRoutine()
	if !ok || !routineID.IsValid() || routineID.Section() != id.Section_Function {
		return
	}
	functionID := id.Function(routineID)
	schemaName := functionID.SchemaName()
	if schemaName == "" || strings.EqualFold(schemaName, "pg_catalog") {
		return
	}
	resolvedName := functionID.FunctionName()
	if resolvedName == "" || !isSQLIdentifierStart(resolvedName[0]) {
		return
	}
	key := strings.ToLower(functionName)
	if key == "" || !isSQLIdentifierStart(key[0]) {
		return
	}
	qualifiedName := quoteViewFunctionIdentifier(schemaName) + "." + quoteViewFunctionIdentifier(resolvedName)
	if existing, ok := bindings[key]; ok && existing != qualifiedName {
		delete(bindings, key)
		conflicts[key] = struct{}{}
		return
	}
	if _, ok := conflicts[key]; ok {
		return
	}
	bindings[key] = qualifiedName
}

func rewriteResolvedFunctionBindings(sqlText string, bindings map[string]string) (string, bool) {
	if sqlText == "" || len(bindings) == 0 {
		return sqlText, false
	}
	var builder strings.Builder
	last := 0
	changed := false
	for idx := 0; idx < len(sqlText); {
		switch {
		case sqlText[idx] == '\'':
			idx = skipSingleQuotedSQLString(sqlText, idx)
			continue
		case sqlText[idx] == '"':
			idx = skipDoubleQuotedSQLIdentifier(sqlText, idx)
			continue
		case sqlText[idx] == '-' && idx+1 < len(sqlText) && sqlText[idx+1] == '-':
			idx = skipSQLLineComment(sqlText, idx)
			continue
		case sqlText[idx] == '/' && idx+1 < len(sqlText) && sqlText[idx+1] == '*':
			idx = skipSQLBlockComment(sqlText, idx)
			continue
		case sqlText[idx] == '$':
			if nextIdx, ok := skipDollarQuotedSQLString(sqlText, idx); ok {
				idx = nextIdx
				continue
			}
		case isSQLIdentifierStart(sqlText[idx]):
			start := idx
			idx++
			for idx < len(sqlText) && isSQLIdentifierPart(sqlText[idx]) {
				idx++
			}
			identifier := sqlText[start:idx]
			qualifiedName, ok := bindings[strings.ToLower(identifier)]
			if !ok || previousNonWhitespaceByte(sqlText, start) == '.' {
				continue
			}
			afterIdentifier := skipSQLWhitespace(sqlText, idx)
			if afterIdentifier >= len(sqlText) || sqlText[afterIdentifier] != '(' {
				continue
			}
			if !changed {
				builder.Grow(len(sqlText) + len(qualifiedName))
			}
			builder.WriteString(sqlText[last:start])
			builder.WriteString(qualifiedName)
			last = idx
			changed = true
			continue
		}
		idx++
	}
	if !changed {
		return sqlText, false
	}
	builder.WriteString(sqlText[last:])
	return builder.String(), true
}

func quoteViewFunctionIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func skipSingleQuotedSQLString(sqlText string, idx int) int {
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

func skipDoubleQuotedSQLIdentifier(sqlText string, idx int) int {
	idx++
	for idx < len(sqlText) {
		if sqlText[idx] == '"' {
			idx++
			if idx < len(sqlText) && sqlText[idx] == '"' {
				idx++
				continue
			}
			return idx
		}
		idx++
	}
	return idx
}

func skipSQLLineComment(sqlText string, idx int) int {
	idx += 2
	for idx < len(sqlText) && sqlText[idx] != '\n' {
		idx++
	}
	return idx
}

func skipSQLBlockComment(sqlText string, idx int) int {
	idx += 2
	for idx+1 < len(sqlText) {
		if sqlText[idx] == '*' && sqlText[idx+1] == '/' {
			return idx + 2
		}
		idx++
	}
	return len(sqlText)
}

func skipDollarQuotedSQLString(sqlText string, idx int) (int, bool) {
	delimiterEnd := idx + 1
	for delimiterEnd < len(sqlText) && isSQLIdentifierPart(sqlText[delimiterEnd]) {
		delimiterEnd++
	}
	if delimiterEnd >= len(sqlText) || sqlText[delimiterEnd] != '$' {
		return idx, false
	}
	delimiter := sqlText[idx : delimiterEnd+1]
	bodyStart := delimiterEnd + 1
	bodyEnd := strings.Index(sqlText[bodyStart:], delimiter)
	if bodyEnd < 0 {
		return len(sqlText), true
	}
	return bodyStart + bodyEnd + len(delimiter), true
}

func skipSQLWhitespace(sqlText string, idx int) int {
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

func previousNonWhitespaceByte(sqlText string, idx int) byte {
	for idx--; idx >= 0; idx-- {
		switch sqlText[idx] {
		case ' ', '\t', '\n', '\r', '\f':
			continue
		default:
			return sqlText[idx]
		}
	}
	return 0
}

func isSQLIdentifierStart(ch byte) bool {
	return ch == '_' ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z')
}

func isSQLIdentifierPart(ch byte) bool {
	return isSQLIdentifierStart(ch) ||
		ch == '$' ||
		(ch >= '0' && ch <= '9')
}
