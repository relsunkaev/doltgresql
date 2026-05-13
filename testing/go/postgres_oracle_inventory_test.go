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

package _go

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type scriptAssertionOracleRecord struct {
	ID     string
	Source string
}

func TestScriptAssertionOracleInventory(t *testing.T) {
	manifest := loadPostgresOracleManifest(t)
	validatePostgresOracleManifest(t, manifest)

	assertions := scanScriptTestExpectationAssertions(t, manifest.Inventory.AssertionFields)
	require.Greater(t, len(assertions), 10000)

	assertionsBySource := map[string][]scriptAssertionOracleRecord{}
	for _, assertion := range assertions {
		assertionsBySource[assertion.Source] = append(assertionsBySource[assertion.Source], assertion)
	}

	postgresSources := postgresScriptTestOracleSources(manifest)
	require.NotEmpty(t, postgresSources)
	for source := range postgresSources {
		require.NotEmpty(t, assertionsBySource[source], "Postgres oracle source %s must point at a ScriptTest assertion", source)
	}
}

func scanScriptTestExpectationAssertions(t testing.TB, assertionFields []string) []scriptAssertionOracleRecord {
	t.Helper()
	require.NotEmpty(t, assertionFields)
	fieldSet := map[string]struct{}{}
	for _, field := range assertionFields {
		fieldSet[field] = struct{}{}
	}

	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	sort.Strings(files)

	records := make([]scriptAssertionOracleRecord, 0)
	for _, file := range files {
		if strings.HasPrefix(file, "postgres_oracle_") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		require.NoError(t, err)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			source := fmt.Sprintf("testing/go/%s:%s", file, fn.Name.Name)
			ordinal := 0
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				lit, ok := node.(*ast.CompositeLit)
				if !ok || !compositeHasExpectationField(lit, fieldSet) {
					return true
				}
				ordinal++
				records = append(records, scriptAssertionOracleRecord{
					ID:     fmt.Sprintf("%s#%04d", source, ordinal),
					Source: source,
				})
				return true
			})
		}
	}
	return records
}

func postgresScriptTestOracleSources(manifest postgresOracleManifest) map[string]struct{} {
	sources := map[string]struct{}{}
	for _, entry := range manifest.Entries {
		if entry.Oracle != "postgres" {
			continue
		}
		sourceFile, _, ok := strings.Cut(entry.Source, ":")
		if !ok || !strings.HasPrefix(sourceFile, "testing/go/") || strings.HasPrefix(filepath.Base(sourceFile), "postgres_oracle_") {
			continue
		}
		sources[entry.Source] = struct{}{}
	}
	return sources
}
