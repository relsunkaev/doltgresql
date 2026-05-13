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
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const scriptAssertionOracleInventoryPath = "testdata/script_assertion_oracle_inventory.json"

type scriptAssertionOracleInventoryFile struct {
	Version                int                 `json:"version"`
	Scope                  string              `json:"scope"`
	CanonicalPostgresMajor int                 `json:"canonicalPostgresMajor"`
	NormalizationProfile   string              `json:"normalizationProfile"`
	AssertionFields        []string            `json:"assertionFields"`
	Classifications        map[string][]string `json:"classifications"`
}

type scriptAssertionOracleRecord struct {
	ID     string
	Source string
}

func TestScriptAssertionOracleInventory(t *testing.T) {
	manifest := loadPostgresOracleManifest(t)
	validatePostgresOracleManifest(t, manifest)
	actualAssertions := scanScriptTestExpectationAssertions(t, manifest.Inventory.AssertionFields)

	if os.Getenv("DOLTGRES_UPDATE_SCRIPT_ASSERTION_ORACLE_INVENTORY") != "" {
		inventory := buildScriptAssertionOracleInventory(t, manifest, actualAssertions)
		writeScriptAssertionOracleInventory(t, inventory)
		return
	}

	inventory := loadScriptAssertionOracleInventory(t)
	validateScriptAssertionOracleInventory(t, manifest, inventory, actualAssertions)
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

func loadScriptAssertionOracleInventory(t testing.TB) scriptAssertionOracleInventoryFile {
	t.Helper()
	data, err := os.ReadFile(scriptAssertionOracleInventoryPath)
	require.NoError(t, err)
	var inventory scriptAssertionOracleInventoryFile
	require.NoError(t, json.Unmarshal(data, &inventory))
	return inventory
}

func tryLoadScriptAssertionOracleInventory(t testing.TB) (scriptAssertionOracleInventoryFile, bool) {
	t.Helper()
	data, err := os.ReadFile(scriptAssertionOracleInventoryPath)
	if os.IsNotExist(err) {
		return scriptAssertionOracleInventoryFile{}, false
	}
	require.NoError(t, err)
	var inventory scriptAssertionOracleInventoryFile
	require.NoError(t, json.Unmarshal(data, &inventory))
	return inventory, true
}

func buildScriptAssertionOracleInventory(
	t testing.TB,
	manifest postgresOracleManifest,
	actualAssertions []scriptAssertionOracleRecord,
) scriptAssertionOracleInventoryFile {
	t.Helper()
	existing, hasExisting := tryLoadScriptAssertionOracleInventory(t)
	existingClassifications := classificationByAssertionID(t, existing)

	defaultOracle := ""
	if !hasExisting {
		defaultOracle = manifest.DefaultOracle
	}
	if explicitDefault := os.Getenv("DOLTGRES_SCRIPT_ASSERTION_DEFAULT_ORACLE"); explicitDefault != "" {
		require.Contains(t, []string{"postgres", "internal"}, explicitDefault)
		defaultOracle = explicitDefault
	}

	postgresSources := postgresScriptTestOracleSources(manifest)
	classifications := map[string][]string{
		"postgres": {},
		"internal": {},
	}
	for _, assertion := range actualAssertions {
		oracle, ok := existingClassifications[assertion.ID]
		if _, isPostgresSource := postgresSources[assertion.Source]; isPostgresSource {
			oracle = "postgres"
			ok = true
		}
		if !ok {
			require.NotEmpty(t, defaultOracle,
				"missing classification for %s; edit %s or set DOLTGRES_SCRIPT_ASSERTION_DEFAULT_ORACLE=internal|postgres for a bulk update",
				assertion.ID, scriptAssertionOracleInventoryPath)
			oracle = defaultOracle
		}
		classifications[oracle] = append(classifications[oracle], assertion.ID)
	}
	for oracle := range classifications {
		sort.Strings(classifications[oracle])
	}
	return scriptAssertionOracleInventoryFile{
		Version:                1,
		Scope:                  manifest.Inventory.Scope,
		CanonicalPostgresMajor: manifest.CanonicalPostgresMajor,
		NormalizationProfile:   manifest.NormalizationProfile,
		AssertionFields:        append([]string(nil), manifest.Inventory.AssertionFields...),
		Classifications:        classifications,
	}
}

func writeScriptAssertionOracleInventory(t testing.TB, inventory scriptAssertionOracleInventoryFile) {
	t.Helper()
	data, err := json.MarshalIndent(inventory, "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')
	require.NoError(t, os.WriteFile(scriptAssertionOracleInventoryPath, data, 0o644))
}

func validateScriptAssertionOracleInventory(
	t testing.TB,
	manifest postgresOracleManifest,
	inventory scriptAssertionOracleInventoryFile,
	actualAssertions []scriptAssertionOracleRecord,
) {
	t.Helper()
	require.Equal(t, 1, inventory.Version)
	require.Equal(t, manifest.Inventory.Scope, inventory.Scope)
	require.Equal(t, manifest.CanonicalPostgresMajor, inventory.CanonicalPostgresMajor)
	require.Equal(t, manifest.NormalizationProfile, inventory.NormalizationProfile)
	require.Equal(t, manifest.Inventory.AssertionFields, inventory.AssertionFields)
	require.Contains(t, inventory.Classifications, "postgres")
	require.Contains(t, inventory.Classifications, "internal")

	classifications := classificationByAssertionID(t, inventory)
	actualByID := map[string]struct{}{}
	for _, assertion := range actualAssertions {
		actualByID[assertion.ID] = struct{}{}
	}

	missing := make([]string, 0)
	for _, assertion := range actualAssertions {
		if _, ok := classifications[assertion.ID]; !ok {
			missing = append(missing, assertion.ID)
		}
	}
	stale := make([]string, 0)
	for id := range classifications {
		if _, ok := actualByID[id]; !ok {
			stale = append(stale, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	require.Empty(t, firstInventoryDiffs(missing), "unclassified ScriptTest assertions; update %s", scriptAssertionOracleInventoryPath)
	require.Empty(t, firstInventoryDiffs(stale), "stale ScriptTest assertion classifications; update %s", scriptAssertionOracleInventoryPath)
	require.Len(t, classifications, len(actualAssertions))
	require.Greater(t, len(inventory.Classifications["internal"]), 10000)
	require.NotEmpty(t, inventory.Classifications["postgres"])

	postgresSources := postgresScriptTestOracleSources(manifest)
	for source := range postgresSources {
		require.True(t, inventoryContainsSource(inventory.Classifications["postgres"], source),
			"Postgres oracle source %s is not classified as postgres in %s", source, scriptAssertionOracleInventoryPath)
	}
	for _, id := range inventory.Classifications["postgres"] {
		source, _, ok := strings.Cut(id, "#")
		require.True(t, ok, "assertion classification id must be source#ordinal: %s", id)
		_, ok = postgresSources[source]
		require.True(t, ok, "postgres-classified assertion %s must have a postgres oracle manifest entry", id)
	}
}

func classificationByAssertionID(t testing.TB, inventory scriptAssertionOracleInventoryFile) map[string]string {
	t.Helper()
	classifications := map[string]string{}
	for oracle, ids := range inventory.Classifications {
		require.Contains(t, []string{"postgres", "internal"}, oracle)
		require.True(t, sort.StringsAreSorted(ids), "%s classifications must be sorted", oracle)
		for _, id := range ids {
			require.NotEmpty(t, id)
			if prior, ok := classifications[id]; ok {
				t.Fatalf("assertion %s classified as both %s and %s", id, prior, oracle)
			}
			classifications[id] = oracle
		}
	}
	return classifications
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

func inventoryContainsSource(ids []string, source string) bool {
	for _, id := range ids {
		if strings.HasPrefix(id, source+"#") {
			return true
		}
	}
	return false
}

func firstInventoryDiffs(values []string) []string {
	if len(values) <= 20 {
		return values
	}
	return append(values[:20], fmt.Sprintf("... %d more", len(values)-20))
}
