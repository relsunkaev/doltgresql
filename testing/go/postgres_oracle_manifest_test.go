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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

const postgresOracleDefaultDSN = "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable"

//go:generate go run gen_postgres_oracle_manifest.go

type postgresOracleManifest struct {
	GeneratedBy            string                  `json:"generatedBy"`
	Version                int                     `json:"version"`
	CanonicalPostgresMajor int                     `json:"canonicalPostgresMajor"`
	NormalizationProfile   string                  `json:"normalizationProfile"`
	DefaultOracle          string                  `json:"defaultOracle"`
	Inventory              postgresOracleInventory `json:"inventory"`
	Entries                []postgresOracleEntry   `json:"entries"`
}

type postgresOracleInventory struct {
	Scope                   string   `json:"scope"`
	AssertionsDefaultOracle string   `json:"assertionsDefaultOracle"`
	PostgresOverrides       string   `json:"postgresOverrides"`
	AssertionFields         []string `json:"assertionFields"`
}

type postgresOracleEntry struct {
	ID                    string            `json:"id"`
	Source                string            `json:"source"`
	Ordinal               int               `json:"ordinal"`
	Oracle                string            `json:"oracle"`
	Compare               string            `json:"compare"`
	Setup                 []string          `json:"setup"`
	Query                 string            `json:"query"`
	ExpectedRows          [][]postgresCell  `json:"expectedRows"`
	ExpectedSQLState      string            `json:"expectedSqlstate"`
	ExpectedErrorSeverity string            `json:"expectedErrorSeverity"`
	ColumnModes           []string          `json:"columnModes"`
	Cleanup               []string          `json:"cleanup"`
	Variables             map[string]string `json:"variables"`
}

type postgresCell struct {
	Value *string `json:"value,omitempty"`
	Regex string  `json:"regex,omitempty"`
	Any   bool    `json:"any,omitempty"`
	Null  bool    `json:"null,omitempty"`
}

func TestPostgresOracleManifestSchema(t *testing.T) {
	validatePostgresOracleManifest(t, loadPostgresOracleManifest(t))
}

func TestPostgresOracleCacheCoversManifestScriptEntries(t *testing.T) {
	manifest := loadPostgresOracleManifest(t)
	cache, err := loadPostgresOracleCache()
	require.NoError(t, err)

	cachedEntries := 0
	for _, entry := range manifest.Entries {
		if entry.Oracle != "postgres" || entry.Ordinal == 0 {
			continue
		}
		testName, ok := postgresOracleCacheSourceTestName(entry.Source)
		require.True(t, ok, "source for %s", entry.ID)
		cached := cache.entriesByAssertion[postgresOracleCacheKey(testName, entry.Ordinal)]
		require.NotNil(t, cached, "cache entry for %s", entry.ID)
		require.Equal(t, entry.ID, cached.ID)
		cachedEntries++
	}
	require.Greater(t, cachedEntries, 20)
}

func TestPostgresOracleManifestGenerated(t *testing.T) {
	expected, err := os.ReadFile("testdata/postgres_oracle_manifest.json")
	require.NoError(t, err)

	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go", "--stdout")
	actual, err := cmd.CombinedOutput()
	require.NoError(t, err, string(actual))
	require.Equal(t, string(expected), string(actual), "run go generate ./testing/go after editing oracle manifest inputs")
}

func TestPostgresOracleMigrationCandidatesGenerated(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "oracle-migration")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go", "--migration-candidates-dir", outDir)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	files, err := filepath.Glob(filepath.Join(outDir, "*.oracle-map.json"))
	require.NoError(t, err)
	require.Greater(t, len(files), 100)

	totalAssertions := 0
	postgresAssertions := 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		var generated struct {
			GeneratedBy       string `json:"generatedBy"`
			SourceFile        string `json:"sourceFile"`
			DefaultOracle     string `json:"defaultOracle"`
			AssertionKeyStyle string `json:"assertionKeyStyle"`
			Assertions        []struct {
				Key          string `json:"key"`
				Source       string `json:"source"`
				Ordinal      int    `json:"ordinal"`
				Oracle       string `json:"oracle"`
				SuggestedID  string `json:"suggestedId"`
				ExpectedKind string `json:"expectedKind"`
				QuerySHA256  string `json:"querySha256"`
			} `json:"assertions"`
		}
		require.NoError(t, json.Unmarshal(data, &generated), file)
		require.Equal(t, "go run gen_postgres_oracle_manifest.go --migration-candidates-dir", generated.GeneratedBy)
		require.True(t, strings.HasPrefix(generated.SourceFile, "testing/go/"), file)
		require.Equal(t, "internal", generated.DefaultOracle)
		require.NotEmpty(t, generated.AssertionKeyStyle)
		require.NotEmpty(t, generated.Assertions)
		totalAssertions += len(generated.Assertions)
		for _, assertion := range generated.Assertions {
			require.NotEmpty(t, assertion.Key)
			require.NotEmpty(t, assertion.Source)
			require.Positive(t, assertion.Ordinal)
			require.Contains(t, []string{"internal", "postgres"}, assertion.Oracle)
			if assertion.Oracle == "postgres" {
				postgresAssertions++
			}
			require.NotEmpty(t, assertion.SuggestedID)
			require.NotEmpty(t, assertion.ExpectedKind)
		}
	}
	require.Greater(t, totalAssertions, 10000)
	require.Greater(t, postgresAssertions, 0)
}

func TestPostgresOraclePromotedMapGenerated(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "expression_operator_repro_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "expression_operator_repro_test.go",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		GeneratedBy string `json:"generatedBy"`
		SourceFile  string `json:"sourceFile"`
		Assertions  []struct {
			Oracle      string `json:"oracle"`
			PostgresID  string `json:"postgresId"`
			SuggestedID string `json:"suggestedId"`
			Compare     string `json:"compare"`
			Query       string `json:"query"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Equal(t, "go run gen_postgres_oracle_manifest.go --promote-oracle-map expression_operator_repro_test.go", generated.GeneratedBy)
	require.Equal(t, "testing/go/expression_operator_repro_test.go", generated.SourceFile)
	require.Len(t, generated.Assertions, 11)
	for _, assertion := range generated.Assertions {
		require.Equal(t, "postgres", assertion.Oracle)
		require.NotEmpty(t, assertion.PostgresID)
		require.NotEmpty(t, assertion.SuggestedID)
		require.NotEmpty(t, assertion.Compare)
		require.NotEmpty(t, assertion.Query)
	}
}

func TestPostgresOraclePromotedMapSkipsBindVars(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "limit_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "limit_test.go",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		Assertions []struct {
			Oracle     string   `json:"oracle"`
			Query      string   `json:"query"`
			NonLiteral []string `json:"nonLiteral"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Len(t, generated.Assertions, 4)
	for _, assertion := range generated.Assertions {
		if strings.Contains(assertion.Query, "LIMIT $1") {
			require.Equal(t, "internal", assertion.Oracle)
			require.Contains(t, assertion.NonLiteral, "BindVars")
			continue
		}
		require.Equal(t, "postgres", assertion.Oracle)
	}
}

func TestPostgresOraclePromotedMapSkipsDoltSpecificQueries(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "dolt_functions_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "dolt_functions_test.go",
		"--oracle-test-name", "TestDoltAdd",
		"--oracle-script-name", "Add all using dot",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		Assertions []struct {
			Oracle     string   `json:"oracle"`
			Query      string   `json:"query"`
			NonLiteral []string `json:"nonLiteral"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Len(t, generated.Assertions, 3)
	for _, assertion := range generated.Assertions {
		require.Equal(t, "internal", assertion.Oracle)
		require.Contains(t, assertion.NonLiteral, "DoltSpecific")
		require.True(t, strings.Contains(strings.ToLower(assertion.Query), "dolt_"))
	}
}

func TestPostgresOraclePromotedMapKeepsDoltNameFilters(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "functions_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "functions_test.go",
		"--oracle-script-name", "pg_table_is_visible",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		Assertions []struct {
			Oracle     string   `json:"oracle"`
			Query      string   `json:"query"`
			NonLiteral []string `json:"nonLiteral"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	foundFilter := false
	for _, assertion := range generated.Assertions {
		if !strings.Contains(assertion.Query, "left(relname, 5) <> 'dolt_'") {
			continue
		}
		foundFilter = true
		require.Equal(t, "postgres", assertion.Oracle)
		require.NotContains(t, assertion.NonLiteral, "DoltSpecific")
	}
	require.True(t, foundFilter)
}

func TestPostgresOraclePromotedMapSkipsPriorNonLiteralSetup(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "domain_correctness_repro_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "domain_correctness_repro_test.go",
		"--oracle-test-name", "TestDomainTypmodCopyFromUsesCoercedValueRepro,TestDomainTypmodBindVarsUseCoercedValueRepro",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		Assertions []struct {
			Source     string   `json:"source"`
			Oracle     string   `json:"oracle"`
			NonLiteral []string `json:"nonLiteral"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Len(t, generated.Assertions, 2)
	for _, assertion := range generated.Assertions {
		require.Equal(t, "internal", assertion.Oracle)
		switch assertion.Source {
		case "testing/go/domain_correctness_repro_test.go:TestDomainTypmodCopyFromUsesCoercedValueRepro":
			require.Contains(t, assertion.NonLiteral, "PriorCopyFromStdInFile")
		case "testing/go/domain_correctness_repro_test.go:TestDomainTypmodBindVarsUseCoercedValueRepro":
			require.Contains(t, assertion.NonLiteral, "PriorBindVars")
		default:
			require.Failf(t, "unexpected assertion source", "%s", assertion.Source)
		}
	}
}

func TestPostgresOraclePromotedMapSupportsTestNameFilter(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "publication_subscription_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "publication_subscription_test.go",
		"--oracle-test-name", "TestReplicaIdentityDDLAndCatalogs",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		GeneratedBy string `json:"generatedBy"`
		SourceFile  string `json:"sourceFile"`
		Assertions  []struct {
			Source string `json:"source"`
			Oracle string `json:"oracle"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Equal(t, "go run gen_postgres_oracle_manifest.go --promote-oracle-map publication_subscription_test.go --oracle-test-name TestReplicaIdentityDDLAndCatalogs", generated.GeneratedBy)
	require.Equal(t, "testing/go/publication_subscription_test.go", generated.SourceFile)
	require.Len(t, generated.Assertions, 6)
	for _, assertion := range generated.Assertions {
		require.Equal(t, "testing/go/publication_subscription_test.go:TestReplicaIdentityDDLAndCatalogs", assertion.Source)
		require.Equal(t, "postgres", assertion.Oracle)
	}
}

func TestPostgresOraclePromotedMapSupportsScriptNameFilter(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "publication_subscription_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "publication_subscription_test.go",
		"--oracle-test-name", "TestPublicationDDLAndCatalogs",
		"--oracle-script-name", "publication table lists accept repeated and omitted table keywords",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		GeneratedBy string `json:"generatedBy"`
		SourceFile  string `json:"sourceFile"`
		Assertions  []struct {
			Source     string `json:"source"`
			ScriptName string `json:"scriptName"`
			Ordinal    int    `json:"ordinal"`
			Oracle     string `json:"oracle"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Equal(t, "go run gen_postgres_oracle_manifest.go --promote-oracle-map publication_subscription_test.go --oracle-test-name TestPublicationDDLAndCatalogs --oracle-script-name publication table lists accept repeated and omitted table keywords", generated.GeneratedBy)
	require.Equal(t, "testing/go/publication_subscription_test.go", generated.SourceFile)
	require.Len(t, generated.Assertions, 2)
	require.Equal(t, 12, generated.Assertions[0].Ordinal)
	require.Equal(t, 13, generated.Assertions[1].Ordinal)
	for _, assertion := range generated.Assertions {
		require.Equal(t, "testing/go/publication_subscription_test.go:TestPublicationDDLAndCatalogs", assertion.Source)
		require.Equal(t, "publication table lists accept repeated and omitted table keywords", assertion.ScriptName)
		require.Equal(t, "postgres", assertion.Oracle)
	}
}

func TestPostgresOraclePromotedMapSupportsPackageScriptTestVariables(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "set_test.oracle-map.json")
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go",
		"--promote-oracle-map", "set_test.go",
		"--oracle-test-name", "TestSetStatements",
		"--oracle-script-name", "special case for NAMES",
		"--promote-oracle-map-output", outPath)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	data, err := os.ReadFile(outPath)
	require.NoError(t, err)
	var generated struct {
		GeneratedBy string `json:"generatedBy"`
		SourceFile  string `json:"sourceFile"`
		Assertions  []struct {
			Source     string `json:"source"`
			ScriptName string `json:"scriptName"`
			Ordinal    int    `json:"ordinal"`
			Oracle     string `json:"oracle"`
			Query      string `json:"query"`
		} `json:"assertions"`
	}
	require.NoError(t, json.Unmarshal(data, &generated))
	require.Equal(t, "go run gen_postgres_oracle_manifest.go --promote-oracle-map set_test.go --oracle-test-name TestSetStatements --oracle-script-name special case for NAMES", generated.GeneratedBy)
	require.Equal(t, "testing/go/set_test.go", generated.SourceFile)
	require.Len(t, generated.Assertions, 6)
	require.Equal(t, 23, generated.Assertions[0].Ordinal)
	require.Equal(t, "SHOW client_encoding", generated.Assertions[0].Query)
	for _, assertion := range generated.Assertions {
		require.Equal(t, "testing/go/set_test.go:TestSetStatements", assertion.Source)
		require.Equal(t, "special case for NAMES", assertion.ScriptName)
		require.Equal(t, "postgres", assertion.Oracle)
	}
}

func TestPostgresOracleReplicaIdentityCacheUsesCatalogCharText(t *testing.T) {
	manifest := loadPostgresOracleManifest(t)
	seen := map[string]string{}
	for _, entry := range manifest.Entries {
		if entry.Source != "testing/go/publication_subscription_test.go:TestReplicaIdentityDDLAndCatalogs" ||
			len(entry.ExpectedRows) == 0 || len(entry.ExpectedRows[0]) == 0 || entry.ExpectedRows[0][0].Value == nil {
			continue
		}
		seen[entry.ID] = *entry.ExpectedRows[0][0].Value
	}
	require.Equal(t, "d", seen["publication-subscription-test-testreplicaidentityddlandcatalogs-0001-select-relreplident-from-pg_catalog.pg_class-where"])
	require.Equal(t, "f", seen["publication-subscription-test-testreplicaidentityddlandcatalogs-0002-select-relreplident-from-pg_catalog.pg_class-where"])
	require.Equal(t, "n", seen["publication-subscription-test-testreplicaidentityddlandcatalogs-0003-select-relreplident-from-pg_catalog.pg_class-where"])
}

func TestPostgresOracleElectricInspectorArrayCacheUsesArrayText(t *testing.T) {
	manifest := loadPostgresOracleManifest(t)
	for _, entry := range manifest.Entries {
		if entry.Source != "testing/go/publication_subscription_test.go:TestElectricInspectorArrayAlias" {
			continue
		}
		require.Len(t, entry.ExpectedRows, 1)
		require.Len(t, entry.ExpectedRows[0], 1)
		require.NotNil(t, entry.ExpectedRows[0][0].Value)
		require.Equal(t, "{electric_alias,electric_alias_items}", *entry.ExpectedRows[0][0].Value)
		return
	}
	require.Fail(t, "expected Electric inspector oracle entry")
}

func TestPostgresOracleManifestCleansGeneratedObjects(t *testing.T) {
	cmd := exec.Command("go", "run", "gen_postgres_oracle_manifest.go", "--stdout")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	var generated postgresOracleManifest
	require.NoError(t, json.Unmarshal(output, &generated))
	entriesByID := map[string]postgresOracleEntry{}
	for _, entry := range generated.Entries {
		entriesByID[entry.ID] = entry
	}

	publicationEntry := entriesByID["publication-oracle-repro-test-testpublicationrejectsschemaaddaftercolumnlistorfilterrepro-0001-alter-publication-pub_filter_pub-add-tables"]
	require.Contains(t, publicationEntry.Cleanup, "DROP PUBLICATION IF EXISTS pub_filter_pub")
	require.Contains(t, publicationEntry.Cleanup, "DROP SCHEMA IF EXISTS pub_filter_aux CASCADE")
	require.Contains(t, publicationEntry.Cleanup, "DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE")

	extensionEntry := entriesByID["extension-comment-drop-repro-test-testdropextensionclearscommentrepro-0001-select-obj_description-select-oid-from"]
	require.Contains(t, extensionEntry.Cleanup, "DROP EXTENSION IF EXISTS hstore CASCADE")

	databaseEntry := entriesByID["database-comment-drop-repro-test-testdropdatabaseclearscommentrepro-0001-select-shobj_description-select-oid-from"]
	require.Contains(t, databaseEntry.Cleanup, "DROP DATABASE IF EXISTS drop_recreate_comment_database")

	languageEntry := entriesByID["language-comment-drop-repro-test-testdroplanguageclearscommentrepro-0001-select-obj_description-select-oid-from"]
	require.Contains(t, languageEntry.Cleanup, "DROP LANGUAGE IF EXISTS drop_recreate_comment_language CASCADE")

	roleEntry := entriesByID["role-comment-drop-repro-test-testdroproleclearscommentrepro-0001-select-shobj_description-select-oid-from"]
	require.Contains(t, roleEntry.Cleanup, "DROP ROLE IF EXISTS drop_recreate_comment_role")

	largeObjectEntry := entriesByID["large-object-comment-drop-repro-test-testlargeobjectunlinkclearscommentrepro-0002-select-lo_create-424260"]
	require.Contains(t, largeObjectEntry.Cleanup, "SELECT pg_catalog.lo_unlink(424260) WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_largeobject_metadata WHERE oid = 424260)")
}

func TestPostgresOracleManifestInventory(t *testing.T) {
	manifest := loadPostgresOracleManifest(t)
	validatePostgresOracleManifest(t, manifest)
	inventory := scanScriptTestExpectationInventory(t, manifest.Inventory.AssertionFields)
	postgresEntries := postgresOracleEntries(manifest)

	require.Greater(t, inventory.TestFunctions, 250)
	require.Less(t, inventory.ExpectationLiterals, len(postgresEntries))
	require.Greater(t, len(postgresEntries), 9000)
	require.Equal(t, manifest.DefaultOracle, manifest.Inventory.AssertionsDefaultOracle)
	require.Equal(t, "entries where oracle == postgres", manifest.Inventory.PostgresOverrides)
	require.NotEmpty(t, postgresEntries)
}

func TestPostgresOracleManifest(t *testing.T) {
	dsn, ok := postgresOracleDSN()
	if !ok {
		t.Skip("set DOLTGRES_ORACLE=1, DOLTGRES_POSTGRES_TEST_DSN, or POSTGRES_TEST_DSN")
	}

	manifest := loadPostgresOracleManifest(t)
	validatePostgresOracleManifest(t, manifest)

	ctx := context.Background()
	conn := connectPostgresOracle(t, ctx, dsn)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()
	requirePostgresMajor(t, ctx, conn, manifest.CanonicalPostgresMajor)

	for _, entry := range manifest.Entries {
		entry := entry
		if entry.Oracle != "postgres" {
			continue
		}
		t.Run(entry.ID, func(t *testing.T) {
			runPostgresOracleEntry(t, ctx, conn, manifest.NormalizationProfile, entry)
		})
	}
}

func validatePostgresOracleManifest(t testing.TB, manifest postgresOracleManifest) {
	t.Helper()
	require.Equal(t, "go generate ./testing/go", manifest.GeneratedBy)
	require.Equal(t, 1, manifest.Version)
	require.Equal(t, 16, manifest.CanonicalPostgresMajor)
	require.Equal(t, "pg16-structural-v1", manifest.NormalizationProfile)
	require.Equal(t, "internal", manifest.DefaultOracle)
	require.Equal(t, "testing/go/*_test.go ScriptTest expectation literals", manifest.Inventory.Scope)
	require.Equal(t, "internal", manifest.Inventory.AssertionsDefaultOracle)
	require.NotEmpty(t, manifest.Inventory.AssertionFields)
	require.NotEmpty(t, manifest.Entries)

	seen := map[string]struct{}{}
	for _, entry := range manifest.Entries {
		require.NotEmpty(t, entry.ID)
		require.NotEmpty(t, entry.Source, "source for %s", entry.ID)
		if _, ok := seen[entry.ID]; ok {
			t.Fatalf("duplicate oracle manifest id %q", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		require.Contains(t, []string{"postgres", "internal"}, entry.Oracle, "oracle classification for %s", entry.ID)
		require.Contains(t, []string{"exact", "structural", "regex", "sqlstate"}, entry.Compare, "comparison mode for %s", entry.ID)
		for _, mode := range entry.ColumnModes {
			require.Contains(t, []string{"exact", "structural", "numeric", "timestamp", "timestamptz", "array", "json", "bytea", "schema", "explain"}, mode,
				"column mode for %s", entry.ID)
		}
		if entry.Oracle == "postgres" {
			requireOracleSourceExists(t, entry)
			require.NotEmpty(t, entry.Query, "query for %s", entry.ID)
			if entry.ExpectedSQLState != "" {
				require.Empty(t, entry.ExpectedRows, "sqlstate oracle entries cannot also expect rows: %s", entry.ID)
			} else {
				require.NotNil(t, entry.ExpectedRows, "expected rows for %s", entry.ID)
				require.NotEqual(t, "sqlstate", entry.Compare, "sqlstate comparison requires expectedSqlstate for %s", entry.ID)
			}
		}
	}
}

type scriptTestExpectationInventory struct {
	TestFunctions       int
	ExpectationLiterals int
}

func scanScriptTestExpectationInventory(t testing.TB, assertionFields []string) scriptTestExpectationInventory {
	t.Helper()
	require.NotEmpty(t, assertionFields)
	fieldSet := map[string]struct{}{}
	for _, field := range assertionFields {
		fieldSet[field] = struct{}{}
	}

	files, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	var inventory scriptTestExpectationInventory
	for _, file := range files {
		if file == "postgres_oracle_manifest_test.go" {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		require.NoError(t, err)
		ast.Inspect(parsed, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.FuncDecl:
				if strings.HasPrefix(typed.Name.Name, "Test") {
					inventory.TestFunctions++
				}
			case *ast.CompositeLit:
				if compositeHasExpectationField(typed, fieldSet) {
					inventory.ExpectationLiterals++
				}
			}
			return true
		})
	}
	return inventory
}

func compositeHasExpectationField(lit *ast.CompositeLit, fieldSet map[string]struct{}) bool {
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		if _, ok = fieldSet[key.Name]; ok {
			return true
		}
	}
	return false
}

func postgresOracleEntries(manifest postgresOracleManifest) []postgresOracleEntry {
	entries := make([]postgresOracleEntry, 0)
	for _, entry := range manifest.Entries {
		if entry.Oracle == "postgres" {
			entries = append(entries, entry)
		}
	}
	return entries
}

func requireOracleSourceExists(t testing.TB, entry postgresOracleEntry) {
	t.Helper()
	sourceFile, testName, ok := strings.Cut(entry.Source, ":")
	require.True(t, ok, "source must be file:TestName for %s", entry.ID)
	data, err := os.ReadFile(sourceFile)
	if err != nil {
		data, err = os.ReadFile(filepath.Join("..", "..", sourceFile))
	}
	require.NoError(t, err, "source file for %s", entry.ID)
	pattern := regexp.MustCompile(`func\s+` + regexp.QuoteMeta(testName) + `\s*\(`)
	require.Regexp(t, pattern, string(data), "source test for %s", entry.ID)
}

func loadPostgresOracleManifest(t testing.TB) postgresOracleManifest {
	t.Helper()
	data, err := os.ReadFile("testdata/postgres_oracle_manifest.json")
	require.NoError(t, err)
	var manifest postgresOracleManifest
	require.NoError(t, json.Unmarshal(data, &manifest))
	return manifest
}

func postgresOracleDSN() (string, bool) {
	if dsn := os.Getenv("DOLTGRES_POSTGRES_TEST_DSN"); dsn != "" {
		return dsn, true
	}
	if dsn := os.Getenv("POSTGRES_TEST_DSN"); dsn != "" {
		return dsn, true
	}
	if os.Getenv("DOLTGRES_ORACLE") != "" {
		return postgresOracleDefaultDSN, true
	}
	return "", false
}

func connectPostgresOracle(t testing.TB, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	config.Database = "postgres"
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	return conn
}

func requirePostgresMajor(t testing.TB, ctx context.Context, conn *pgx.Conn, expectedMajor int) {
	t.Helper()
	var versionNumString string
	require.NoError(t, conn.QueryRow(ctx, `SHOW server_version_num;`).Scan(&versionNumString))
	versionNum, err := strconv.Atoi(versionNumString)
	require.NoError(t, err)
	require.Equal(t, expectedMajor, versionNum/10000, "canonical PostgreSQL major version mismatch")
}

func runPostgresOracleEntry(t testing.TB, ctx context.Context, conn *pgx.Conn, profile string, entry postgresOracleEntry) {
	t.Helper()
	require.Contains(t, []string{"exact", "structural", "regex", "sqlstate"}, entry.Compare, "comparison mode for %s", entry.ID)
	variables := oracleVariables(entry)
	resetOracleSession(t, ctx, conn)
	runOracleStatements(t, ctx, conn, variables, entry.Cleanup)
	defer func() {
		runOracleStatements(t, ctx, conn, variables, entry.Cleanup)
		resetOracleSession(t, ctx, conn)
	}()
	resetOracleSession(t, ctx, conn)
	runOracleStatements(t, ctx, conn, variables, entry.Setup)

	query := expandOracleVariables(entry.Query, variables)
	if entry.ExpectedSQLState != "" {
		_, err := conn.Exec(ctx, query)
		require.Error(t, err)
		pgErr, ok := err.(*pgconn.PgError)
		require.True(t, ok, "expected PostgreSQL error for %s, got %T: %v", entry.ID, err, err)
		require.Equal(t, entry.ExpectedSQLState, pgErr.Code)
		if entry.ExpectedErrorSeverity != "" {
			require.Equal(t, entry.ExpectedErrorSeverity, pgErr.Severity)
		}
		return
	}
	require.NotEqual(t, "sqlstate", entry.Compare, "sqlstate comparison requires expectedSqlstate for %s", entry.ID)
	rows, err := conn.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	actual := make([][]string, 0)
	fields := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		require.NoError(t, err)
		row := make([]string, len(values))
		for i, value := range values {
			mode := columnMode(entry, i)
			row[i] = normalizePostgresOracleValue(profile, mode, value, fields[i].DataTypeOID)
		}
		actual = append(actual, row)
	}
	require.NoError(t, rows.Err())
	comparePostgresOracleRows(t, entry, actual)
}

func runOracleStatements(t testing.TB, ctx context.Context, conn *pgx.Conn, variables map[string]string, statements []string) {
	t.Helper()
	for _, statement := range statements {
		_, err := conn.Exec(ctx, expandOracleVariables(statement, variables))
		require.NoError(t, err, "oracle statement failed: %s", statement)
	}
}

func resetOracleSession(t testing.TB, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	_, err := conn.Exec(ctx, "RESET ROLE")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "RESET ALL")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "RESET search_path")
	require.NoError(t, err)
}

func oracleVariables(entry postgresOracleEntry) map[string]string {
	variables := map[string]string{}
	for key, value := range entry.Variables {
		variables[key] = value
	}
	if _, ok := variables["schema"]; !ok {
		variables["schema"] = fmt.Sprintf("dg_oracle_%d", time.Now().UnixNano())
	}
	variables["quotedSchema"] = quoteOracleIdentifier(variables["schema"])
	return variables
}

func expandOracleVariables(query string, variables map[string]string) string {
	expanded := query
	for key, value := range variables {
		expanded = strings.ReplaceAll(expanded, "{{"+key+"}}", value)
	}
	return expanded
}

func columnMode(entry postgresOracleEntry, index int) string {
	if index < len(entry.ColumnModes) && entry.ColumnModes[index] != "" {
		return entry.ColumnModes[index]
	}
	if entry.Compare == "exact" {
		return "exact"
	}
	return "structural"
}

func normalizePostgresOracleValue(profile string, mode string, value any, oid uint32) string {
	if value == nil {
		return "<null>"
	}
	if oid == 18 {
		return normalizePostgresOracleChar(value)
	}
	if mode == "exact" {
		return fmt.Sprint(value)
	}
	if mode == "structural" {
		mode = inferPostgresOracleMode(oid)
	}
	switch v := value.(type) {
	case bool:
		if v {
			return "t"
		}
		return "f"
	case int16, int32, int64, int:
		return fmt.Sprint(v)
	case float32, float64:
		return fmt.Sprint(v)
	case pgtype.Numeric:
		return normalizePostgresOraclePgNumeric(v)
	case []byte:
		switch mode {
		case "bytea":
			return "\\x" + hex.EncodeToString(v)
		case "explain":
			return normalizePostgresOracleExplain(string(v))
		case "schema":
			return normalizePostgresOracleSchema(string(v))
		case "json":
			return normalizePostgresOracleJSON(string(v))
		case "numeric":
			return normalizePostgresOracleNumeric(string(v))
		case "array":
			return normalizePostgresOracleArray(string(v))
		default:
			return string(v)
		}
	case time.Time:
		if mode == "timestamptz" {
			return v.UTC().Format(time.RFC3339Nano)
		}
		return v.Format("2006-01-02T15:04:05.999999999")
	case string:
		switch mode {
		case "explain":
			return normalizePostgresOracleExplain(v)
		case "schema":
			return normalizePostgresOracleSchema(v)
		case "json":
			return normalizePostgresOracleJSON(v)
		case "numeric":
			return normalizePostgresOracleNumeric(v)
		case "array":
			return normalizePostgresOracleArray(v)
		default:
			return v
		}
	default:
		if mode == "json" {
			if canonical, err := json.Marshal(v); err == nil {
				return string(canonical)
			}
		}
		if mode == "array" {
			if normalized, ok := normalizePostgresOracleSlice(v); ok {
				return normalized
			}
		}
		text := fmt.Sprint(v)
		if mode == "numeric" {
			return normalizePostgresOracleNumeric(text)
		}
		if mode == "schema" {
			return normalizePostgresOracleSchema(text)
		}
		if mode == "explain" {
			return normalizePostgresOracleExplain(text)
		}
		return text
	}
}

func normalizePostgresOracleChar(value any) string {
	switch v := value.(type) {
	case byte:
		return string([]byte{v})
	case int16:
		return string(rune(v))
	case int32:
		return string(rune(v))
	case int64:
		return string(rune(v))
	case int:
		return string(rune(v))
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func inferPostgresOracleMode(oid uint32) string {
	switch oid {
	case 17:
		return "bytea"
	case 114, 3802:
		return "json"
	case 1700:
		return "numeric"
	case 1114, 1082, 1083:
		return "timestamp"
	case 1184, 1266:
		return "timestamptz"
	case 1000, 1005, 1007, 1016, 1021, 1022, 1009, 1015, 1231:
		return "array"
	default:
		return "structural"
	}
}

func normalizePostgresOracleNumeric(value string) string {
	dec, err := decimal.NewFromString(strings.TrimSpace(value))
	if err != nil {
		return strings.TrimSpace(value)
	}
	if dec.IsZero() {
		return "0"
	}
	return dec.String()
}

func normalizePostgresOraclePgNumeric(value pgtype.Numeric) string {
	if !value.Valid {
		return "<null>"
	}
	if value.NaN {
		return "NaN"
	}
	if value.Int == nil || value.Int.Sign() == 0 {
		return "0"
	}
	return decimal.NewFromBigInt(value.Int, value.Exp).String()
}

var postgresOracleSchemaNamePattern = regexp.MustCompile(`dg_oracle_[0-9]+`)

func normalizePostgresOracleSchema(value string) string {
	return postgresOracleSchemaNamePattern.ReplaceAllString(value, "{{schema}}")
}

var (
	postgresOracleExplainActualTimePattern = regexp.MustCompile(`actual time=[0-9]+(?:\.[0-9]+)?\.\.[0-9]+(?:\.[0-9]+)?`)
	postgresOracleExplainPlanningPattern   = regexp.MustCompile(`Planning Time: [0-9]+(?:\.[0-9]+)? ms`)
	postgresOracleExplainExecutionPattern  = regexp.MustCompile(`Execution Time: [0-9]+(?:\.[0-9]+)? ms`)
)

func normalizePostgresOracleExplain(value string) string {
	value = normalizePostgresOracleSchema(value)
	value = postgresOracleExplainActualTimePattern.ReplaceAllString(value, "actual time=<time>..<time>")
	value = postgresOracleExplainPlanningPattern.ReplaceAllString(value, "Planning Time: <time> ms")
	value = postgresOracleExplainExecutionPattern.ReplaceAllString(value, "Execution Time: <time> ms")
	return value
}

func normalizePostgresOracleJSON(value string) string {
	trimmed := strings.TrimSpace(value)
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
		canonical, err := json.Marshal(decoded)
		if err == nil {
			return string(canonical)
		}
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, []byte(trimmed)); err == nil {
		return compacted.String()
	}
	return trimmed
}

func normalizePostgresOracleArray(value string) string {
	trimmed := strings.TrimSpace(value)
	if normalized, ok := normalizePostgresOracleBracketArray(trimmed); ok {
		return normalized
	}
	trimmed = strings.ReplaceAll(trimmed, ", ", ",")
	return trimmed
}

func normalizePostgresOracleBracketArray(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
		return "", false
	}
	parts := strings.Fields(strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]"))
	if len(parts) == 0 {
		return "", false
	}
	return "{" + strings.Join(parts, ",") + "}", true
}

func normalizePostgresOracleSlice(value any) (string, bool) {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return "", false
	}
	parts := make([]string, rv.Len())
	for i := range parts {
		element := rv.Index(i)
		if element.Kind() == reflect.Pointer && element.IsNil() {
			parts[i] = "NULL"
			continue
		}
		parts[i] = fmt.Sprint(element.Interface())
	}
	return "{" + strings.Join(parts, ",") + "}", true
}

func comparePostgresOracleRows(t testing.TB, entry postgresOracleEntry, actual [][]string) {
	t.Helper()
	require.Len(t, actual, len(entry.ExpectedRows), "row count mismatch for %s", entry.ID)
	for rowIndex, expectedRow := range entry.ExpectedRows {
		require.Len(t, actual[rowIndex], len(expectedRow), "column count mismatch for %s row %d", entry.ID, rowIndex)
		for columnIndex, expected := range expectedRow {
			if expected.Any {
				continue
			}
			actualValue := actual[rowIndex][columnIndex]
			if expected.Null {
				require.Equal(t, "<null>", actualValue, "cell mismatch for %s row %d column %d", entry.ID, rowIndex, columnIndex)
				continue
			}
			if expected.Regex != "" || entry.Compare == "regex" {
				pattern := expected.Regex
				if pattern == "" && expected.Value != nil {
					pattern = *expected.Value
				}
				require.Regexp(t, regexp.MustCompile(pattern), actualValue, "cell mismatch for %s row %d column %d", entry.ID, rowIndex, columnIndex)
				continue
			}
			require.NotNil(t, expected.Value, "expected value missing for %s row %d column %d", entry.ID, rowIndex, columnIndex)
			require.Equal(t, *expected.Value, actualValue, "cell mismatch for %s row %d column %d", entry.ID, rowIndex, columnIndex)
		}
	}
}

func quoteOracleIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
