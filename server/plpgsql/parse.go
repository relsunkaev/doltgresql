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
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	pg_query "github.com/dolthub/pg_query_go/v6"
)

// Parse parses the given CREATE FUNCTION string (which must be the entire string, not just the body) into a Block
// containing the contents of the body.
func Parse(fullCreateFunctionString string) ([]InterpreterOperation, error) {
	var functions []function
	rewrittenCreateFunctionString := rewriteRefcursorParameterOpen(fullCreateFunctionString)
	parsedBody, err := pg_query.ParsePlPgSqlToJSON(rewrittenCreateFunctionString)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(parsedBody), &functions)
	if err != nil {
		return nil, err
	}
	if len(functions) != 1 {
		return nil, errors.New("CREATE FUNCTION parsed multiple blocks")
	}
	block, err := jsonConvert(functions[0].Function, fullCreateFunctionString)
	if err != nil {
		return nil, err
	}
	if aliases := extractAliasDeclarations(fullCreateFunctionString, block.Variables); len(aliases) > 0 {
		aliasStatements := make([]Statement, len(aliases))
		for i, alias := range aliases {
			aliasStatements[i] = alias
		}
		block.Body = append(aliasStatements, block.Body...)
	}
	ops := make([]InterpreterOperation, 0, len(block.Body)+len(block.Variables))
	stack := NewInterpreterStack(nil)
	if err = block.AppendOperations(&ops, &stack); err != nil {
		return nil, err
	}
	if err = reconcileLabels(ops); err != nil {
		return nil, err
	}
	return ops, nil
}

var createFunctionArgsRegex = regexp.MustCompile(`(?is)\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+[^()]+\((.*?)\)`)
var refcursorParamRegex = regexp.MustCompile(`(?is)^\s*(?:IN|OUT|INOUT|VARIADIC\s+)?([A-Za-z_][A-Za-z0-9_$]*)\s+(?:pg_catalog\.)?refcursor\b`)
var plpgsqlBodyStartRegex = regexp.MustCompile(`(?is)(AS\s+\$[^$]*\$\s*)(DECLARE\b|BEGIN\b)`)

func rewriteRefcursorParameterOpen(source string) string {
	matches := createFunctionArgsRegex.FindStringSubmatch(source)
	if len(matches) < 2 {
		return source
	}
	refcursorParams := extractRefcursorParams(matches[1])
	if len(refcursorParams) == 0 {
		return source
	}
	rewritten := source
	var declarations []string
	for _, param := range refcursorParams {
		generatedName := "__doltgres_refcursor_" + param
		openRegex := regexp.MustCompile(`(?is)\bOPEN\s+` + regexp.QuoteMeta(param) + `\s+FOR\b`)
		if !openRegex.MatchString(rewritten) {
			continue
		}
		rewritten = openRegex.ReplaceAllString(rewritten, "OPEN "+generatedName+" FOR")
		declarations = append(declarations, generatedName+" refcursor := "+param+";")
	}
	if len(declarations) == 0 {
		return source
	}
	return injectRefcursorDeclarations(rewritten, declarations)
}

func extractRefcursorParams(args string) []string {
	parts := strings.Split(args, ",")
	params := make([]string, 0, len(parts))
	for _, part := range parts {
		matches := refcursorParamRegex.FindStringSubmatch(part)
		if len(matches) >= 2 {
			params = append(params, matches[1])
		}
	}
	return params
}

func injectRefcursorDeclarations(source string, declarations []string) string {
	indexes := plpgsqlBodyStartRegex.FindStringSubmatchIndex(source)
	if len(indexes) < 6 {
		return source
	}
	prefixEnd := indexes[3]
	keywordStart := indexes[4]
	keywordEnd := indexes[5]
	keyword := source[keywordStart:keywordEnd]
	declarationText := strings.Join(declarations, "\n") + "\n"
	if strings.EqualFold(keyword, "DECLARE") {
		return source[:keywordEnd] + "\n" + declarationText + source[keywordEnd:]
	}
	return source[:prefixEnd] + "DECLARE\n" + declarationText + source[keywordStart:]
}

var plpgsqlAliasDeclarationRegex = regexp.MustCompile(`(?i)\b([A-Za-z_][A-Za-z0-9_$]*)\s+alias\s+for\s+(\$[0-9]+|[A-Za-z_][A-Za-z0-9_$]*)\s*;`)

func extractAliasDeclarations(src string, variables []Variable) []Alias {
	declareSection := extractTopLevelDeclareSection(src)
	if declareSection == "" {
		return nil
	}
	matches := plpgsqlAliasDeclarationRegex.FindAllStringSubmatch(declareSection, -1)
	if len(matches) == 0 {
		return nil
	}
	aliases := make([]Alias, 0, len(matches))
	for _, match := range matches {
		target := resolveAliasTarget(match[2], variables)
		if target == "" {
			continue
		}
		aliases = append(aliases, Alias{
			Name:   match[1],
			Target: target,
		})
	}
	return aliases
}

func extractTopLevelDeclareSection(src string) string {
	lower := strings.ToLower(src)
	declareIdx := strings.Index(lower, "declare")
	if declareIdx == -1 {
		return ""
	}
	beginIdx := strings.Index(lower[declareIdx:], "begin")
	if beginIdx == -1 {
		return ""
	}
	return src[declareIdx : declareIdx+beginIdx]
}

func resolveAliasTarget(target string, variables []Variable) string {
	if !strings.HasPrefix(target, "$") {
		return target
	}
	ordinal, err := strconv.Atoi(strings.TrimPrefix(target, "$"))
	if err != nil || ordinal <= 0 {
		return ""
	}
	paramIndex := 0
	for _, variable := range variables {
		if variable.IsParameter && !strings.EqualFold(variable.Name, "found") {
			paramIndex++
			if paramIndex == ordinal {
				return variable.Name
			}
		}
	}
	return ""
}
