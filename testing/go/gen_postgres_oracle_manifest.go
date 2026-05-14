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

//go:build ignore

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
)

var manifestAssertionFields = []string{
	"Expected",
	"ExpectedRaw",
	"ExpectedErr",
	"ExpectedTag",
	"ExpectedColNames",
	"ExpectedColTypes",
	"ExpectedNotices",
}

type manifest struct {
	GeneratedBy            string    `json:"generatedBy"`
	Version                int       `json:"version"`
	CanonicalPostgresMajor int       `json:"canonicalPostgresMajor"`
	NormalizationProfile   string    `json:"normalizationProfile"`
	DefaultOracle          string    `json:"defaultOracle"`
	Inventory              inventory `json:"inventory"`
	Entries                []entry   `json:"entries"`
}

type inventory struct {
	Scope                   string   `json:"scope"`
	AssertionsDefaultOracle string   `json:"assertionsDefaultOracle"`
	PostgresOverrides       string   `json:"postgresOverrides"`
	AssertionFields         []string `json:"assertionFields"`
}

type entry struct {
	ID                    string            `json:"id"`
	Source                string            `json:"source"`
	Ordinal               int               `json:"ordinal,omitempty"`
	Oracle                string            `json:"oracle"`
	Compare               string            `json:"compare"`
	Setup                 []string          `json:"setup,omitempty"`
	SetupStatements       []oracleStatement `json:"setupStatements,omitempty"`
	Query                 string            `json:"query"`
	BindVars              []oracleBindVar   `json:"bindVars,omitempty"`
	ExpectedRows          *[][]cell         `json:"expectedRows,omitempty"`
	ExpectedSQLState      string            `json:"expectedSqlstate,omitempty"`
	ExpectedErrorSeverity string            `json:"expectedErrorSeverity,omitempty"`
	ExpectedTag           *string           `json:"expectedTag,omitempty"`
	ColumnModes           []string          `json:"columnModes,omitempty"`
	Cleanup               []string          `json:"cleanup,omitempty"`
	Variables             map[string]string `json:"variables,omitempty"`
}

type oracleStatement struct {
	Query    string          `json:"query"`
	BindVars []oracleBindVar `json:"bindVars,omitempty"`
}

type oracleBindVar struct {
	Kind   string   `json:"kind"`
	Value  string   `json:"value,omitempty"`
	Values []string `json:"values,omitempty"`
	Null   bool     `json:"null,omitempty"`
}

type cell struct {
	Value *string `json:"value,omitempty"`
	Regex string  `json:"regex,omitempty"`
	Any   bool    `json:"any,omitempty"`
	Null  bool    `json:"null,omitempty"`
}

type migrationFile struct {
	GeneratedBy       string               `json:"generatedBy"`
	SourceFile        string               `json:"sourceFile"`
	DefaultOracle     string               `json:"defaultOracle"`
	AssertionKeyStyle string               `json:"assertionKeyStyle"`
	AssertionFields   []string             `json:"assertionFields"`
	Assertions        []migrationAssertion `json:"assertions"`
}

type migrationAssertion struct {
	Key             string          `json:"key"`
	Source          string          `json:"source"`
	Ordinal         int             `json:"ordinal"`
	ScriptName      string          `json:"scriptName,omitempty"`
	Oracle          string          `json:"oracle"`
	PostgresID      string          `json:"postgresId,omitempty"`
	SuggestedID     string          `json:"suggestedId"`
	Compare         string          `json:"compare,omitempty"`
	ColumnModes     []string        `json:"columnModes,omitempty"`
	ExpectedKind    string          `json:"expectedKind"`
	SQLState        string          `json:"sqlstate,omitempty"`
	ErrorSeverity   string          `json:"errorSeverity,omitempty"`
	Username        string          `json:"username,omitempty"`
	Query           string          `json:"query,omitempty"`
	QuerySHA256     string          `json:"querySha256,omitempty"`
	BindVars        []oracleBindVar `json:"bindVars,omitempty"`
	ExpectedRows    *[][]cell       `json:"expectedRows,omitempty"`
	ExpectedTag     *string         `json:"expectedTag,omitempty"`
	NonLiteral      []string        `json:"nonLiteral,omitempty"`
	Cleanup         []string        `json:"cleanup,omitempty"`
	NeedsCleanup    bool            `json:"needsCleanup"`
	CleanupProvided bool            `json:"cleanupProvided"`
}

const defaultPostgresOracleDSN = "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable"

func main() {
	stdout := flag.Bool("stdout", false, "write generated manifest to stdout instead of testdata/postgres_oracle_manifest.json")
	canonicalPostgresMajor := flag.Int("canonical-postgres-major", 16, "PostgreSQL major version used as the canonical oracle for generated expectations")
	migrationCandidatesDir := flag.String("migration-candidates-dir", "", "write per-file ScriptTest oracle migration candidate maps to this directory")
	promoteOracleMap := flag.String("promote-oracle-map", "", "write a postgres oracle migration map for one ScriptTest source file")
	refreshOracleMap := flag.String("refresh-oracle-map", "", "promote one ScriptTest source file and refresh its cached expected rows from PostgreSQL")
	promoteOracleMapOutput := flag.String("promote-oracle-map-output", "", "output path for --promote-oracle-map; defaults to testdata/postgres_oracle_migrations/<source>.oracle-map.json")
	rewriteOracleSourcesFlag := flag.Bool("rewrite-oracle-sources", false, "remove handwritten Expected* fields from source assertions covered by PostgreSQL oracle maps and add explicit PostgresOracle markers")
	rewriteOracleSourceFile := flag.String("rewrite-oracle-source-file", "", "optional comma-separated source file filter for --rewrite-oracle-sources")
	rewriteOracleKey := flag.String("rewrite-oracle-key", "", "optional comma-separated migration key filter for --rewrite-oracle-sources")
	rewriteOraclePostgresID := flag.String("rewrite-oracle-postgres-id", "", "optional comma-separated PostgresOracle ID filter for --rewrite-oracle-sources")
	oracleTestName := flag.String("oracle-test-name", "", "optional comma-separated Test function filter for --promote-oracle-map or --refresh-oracle-map")
	oracleScriptName := flag.String("oracle-script-name", "", "optional comma-separated ScriptTest Name filter for --promote-oracle-map or --refresh-oracle-map")
	oracleSkipScriptName := flag.String("oracle-skip-script-name", "", "optional comma-separated ScriptTest Name filter to exclude for --promote-oracle-map or --refresh-oracle-map")
	oraclePostgresID := flag.String("oracle-postgres-id", "", "optional comma-separated PostgresOracle ID filter for --promote-oracle-map or --refresh-oracle-map")
	forcePostgresOracle := flag.Bool("force-postgres-oracle", false, "for --refresh-oracle-map, promote literal-query assertions to PostgreSQL even when expected fields are non-literal")
	skipRefreshErrors := flag.Bool("skip-refresh-errors", false, "for --refresh-oracle-map, leave entries internal when PostgreSQL refresh fails and continue refreshing the rest")
	postgresDSN := flag.String("postgres-dsn", "", "PostgreSQL DSN for --refresh-oracle-map; defaults to DOLTGRES_POSTGRES_TEST_DSN, POSTGRES_TEST_DSN, or DOLTGRES_ORACLE default")
	flag.Parse()

	if *rewriteOracleSourcesFlag {
		if *stdout || *migrationCandidatesDir != "" || *promoteOracleMap != "" || *refreshOracleMap != "" || *promoteOracleMapOutput != "" || *oracleTestName != "" || *oracleScriptName != "" || *oracleSkipScriptName != "" || *oraclePostgresID != "" || *forcePostgresOracle || *skipRefreshErrors || *postgresDSN != "" {
			fmt.Fprintln(os.Stderr, "--rewrite-oracle-sources cannot be combined with other generator modes or options")
			os.Exit(1)
		}
		sourceFiles := parseOracleSourceFileFilter(*rewriteOracleSourceFile)
		keys := parseOracleKeyFilter(*rewriteOracleKey)
		postgresIDs := parseOraclePostgresIDFilter(*rewriteOraclePostgresID)
		if err := rewriteOracleSources(sourceFiles, keys, postgresIDs); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *refreshOracleMap != "" {
		if *stdout || *migrationCandidatesDir != "" || *promoteOracleMap != "" {
			fmt.Fprintln(os.Stderr, "--refresh-oracle-map cannot be combined with --stdout, --migration-candidates-dir, or --promote-oracle-map")
			os.Exit(1)
		}
		dsn, err := postgresOracleDSN(*postgresDSN)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		testNames := parseOracleTestNameFilter(*oracleTestName)
		scriptNames := parseOracleScriptNameFilter(*oracleScriptName)
		skipScriptNames := parseOracleScriptNameFilter(*oracleSkipScriptName)
		postgresIDs := parseOraclePostgresIDFilter(*oraclePostgresID)
		if err := refreshPromotedOracleMap(*refreshOracleMap, *promoteOracleMapOutput, testNames, scriptNames, skipScriptNames, postgresIDs, dsn, *forcePostgresOracle, *skipRefreshErrors); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *promoteOracleMap != "" {
		if *skipRefreshErrors {
			fmt.Fprintln(os.Stderr, "--skip-refresh-errors requires --refresh-oracle-map")
			os.Exit(1)
		}
		if *forcePostgresOracle {
			fmt.Fprintln(os.Stderr, "--force-postgres-oracle requires --refresh-oracle-map")
			os.Exit(1)
		}
		if *stdout || *migrationCandidatesDir != "" {
			fmt.Fprintln(os.Stderr, "--promote-oracle-map cannot be combined with --stdout or --migration-candidates-dir")
			os.Exit(1)
		}
		testNames := parseOracleTestNameFilter(*oracleTestName)
		scriptNames := parseOracleScriptNameFilter(*oracleScriptName)
		skipScriptNames := parseOracleScriptNameFilter(*oracleSkipScriptName)
		postgresIDs := parseOraclePostgresIDFilter(*oraclePostgresID)
		if err := writePromotedOracleMap(*promoteOracleMap, *promoteOracleMapOutput, testNames, scriptNames, skipScriptNames, postgresIDs, false); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *migrationCandidatesDir != "" {
		if *forcePostgresOracle {
			fmt.Fprintln(os.Stderr, "--force-postgres-oracle requires --refresh-oracle-map")
			os.Exit(1)
		}
		if *stdout {
			fmt.Fprintln(os.Stderr, "--stdout cannot be combined with --migration-candidates-dir")
			os.Exit(1)
		}
		if err := writeMigrationCandidates(*migrationCandidatesDir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *rewriteOracleSourceFile != "" {
		fmt.Fprintln(os.Stderr, "--rewrite-oracle-source-file requires --rewrite-oracle-sources")
		os.Exit(1)
	}
	if *rewriteOracleKey != "" {
		fmt.Fprintln(os.Stderr, "--rewrite-oracle-key requires --rewrite-oracle-sources")
		os.Exit(1)
	}
	if *rewriteOraclePostgresID != "" {
		fmt.Fprintln(os.Stderr, "--rewrite-oracle-postgres-id requires --rewrite-oracle-sources")
		os.Exit(1)
	}

	if *forcePostgresOracle {
		fmt.Fprintln(os.Stderr, "--force-postgres-oracle requires --refresh-oracle-map")
		os.Exit(1)
	}
	if *skipRefreshErrors {
		fmt.Fprintln(os.Stderr, "--skip-refresh-errors requires --refresh-oracle-map")
		os.Exit(1)
	}

	data, err := generateManifest(*canonicalPostgresMajor)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *stdout {
		_, _ = os.Stdout.Write(data)
		return
	}
	if err := os.WriteFile("testdata/postgres_oracle_manifest.json", data, 0644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOracleTestNameFilter(value string) map[string]struct{} {
	return parseOracleNameFilter(value)
}

func parseOracleScriptNameFilter(value string) map[string]struct{} {
	return parseOracleNameFilter(value)
}

func parseOraclePostgresIDFilter(value string) map[string]struct{} {
	return parseOracleNameFilter(value)
}

func parseOracleKeyFilter(value string) map[string]struct{} {
	return parseOracleNameFilter(value)
}

func parseOracleSourceFileFilter(value string) map[string]struct{} {
	rawFilter := parseOracleNameFilter(value)
	if len(rawFilter) == 0 {
		return nil
	}
	filter := map[string]struct{}{}
	for file := range rawFilter {
		file = strings.TrimPrefix(file, "testing/go/")
		filter[file] = struct{}{}
	}
	return filter
}

func parseOracleNameFilter(value string) map[string]struct{} {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	filter := map[string]struct{}{}
	for _, part := range strings.Split(value, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		filter[name] = struct{}{}
	}
	return filter
}

func currentSourceRoot() string {
	const root = "testing/go"
	wd, err := os.Getwd()
	if err != nil {
		return root
	}
	slashWD := filepath.ToSlash(wd)
	anchor := "/" + root
	if idx := strings.LastIndex(slashWD, anchor); idx >= 0 {
		return slashWD[idx+1:]
	}
	if slashWD == root || strings.HasSuffix(slashWD, anchor[1:]) {
		if idx := strings.LastIndex(slashWD, root); idx >= 0 {
			return slashWD[idx:]
		}
	}
	return root
}

func sourcePathForFile(file string) string {
	return currentSourceRoot() + "/" + file
}

func generateManifest(canonicalPostgresMajor int) ([]byte, error) {
	migrationOverrides, err := loadMigrationOverrides()
	if err != nil {
		return nil, err
	}
	sourceRoot := currentSourceRoot()
	scriptEntries, err := annotatedScriptTestEntries(migrationOverrides)
	if err != nil {
		return nil, err
	}
	entries := scriptEntries
	if sourceRoot == "testing/go" {
		entries = append(oracleSelftestEntries(sourceRoot), append(dropDefinitionEntries(), scriptEntries...)...)
	}
	m := manifest{
		GeneratedBy:            "go generate ./" + sourceRoot,
		Version:                1,
		CanonicalPostgresMajor: canonicalPostgresMajor,
		NormalizationProfile:   fmt.Sprintf("pg%d-structural-v1", canonicalPostgresMajor),
		DefaultOracle:          "internal",
		Inventory: inventory{
			Scope:                   sourceRoot + "/*_test.go ScriptTest expectation literals",
			AssertionsDefaultOracle: "internal",
			PostgresOverrides:       "entries where oracle == postgres",
			AssertionFields:         manifestAssertionFields,
		},
		Entries: entries,
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func oracleSelftestEntries(sourceRoot string) []entry {
	return []entry{
		{
			ID:                    "oracle-selftest-sqlstate-division-by-zero",
			Source:                sourceRoot + "/postgres_oracle_manifest_test.go:TestPostgresOracleManifest",
			Oracle:                "postgres",
			Compare:               "sqlstate",
			Query:                 "SELECT 1 / 0",
			ExpectedSQLState:      "22012",
			ExpectedErrorSeverity: "ERROR",
		},
		{
			ID:      "oracle-selftest-normalization-regex-and-wildcard",
			Source:  sourceRoot + "/postgres_oracle_manifest_test.go:TestPostgresOracleManifest",
			Oracle:  "postgres",
			Compare: "structural",
			Query:   "SELECT 1.2300::numeric, '-0'::numeric, '{\"b\":2,\"a\":1}'::jsonb, ARRAY[1, 2]::int[], now(), pg_backend_pid()",
			ExpectedRows: rows(row(
				value("1.23"),
				value("0"),
				value("{\"a\":1,\"b\":2}"),
				value("{1,2}"),
				any(),
				regex("^[0-9]+$"),
			)),
			ColumnModes: []string{"numeric", "numeric", "json", "array", "timestamptz", "structural"},
		},
		{
			ID:      "oracle-selftest-text-values-that-look-like-arrays-stay-text",
			Source:  sourceRoot + "/postgres_oracle_manifest_test.go:TestPostgresOracleManifest",
			Oracle:  "postgres",
			Compare: "structural",
			Query:   `SELECT '[{"a": 1}]'::text, '[1, 2]'::text`,
			ExpectedRows: rows(row(
				value(`[{"a": 1}]`),
				value(`[1, 2]`),
			)),
		},
		{
			ID:      "oracle-selftest-name-array-normalizes-to-postgres-text",
			Source:  sourceRoot + "/postgres_oracle_manifest_test.go:TestPostgresOracleManifest",
			Oracle:  "postgres",
			Compare: "structural",
			Query:   `SELECT ARRAY['one', 'two']::name[]`,
			ExpectedRows: rows(row(
				value(`{one,two}`),
			)),
		},
	}
}

func dropDefinitionEntries() []entry {
	return []entry{
		{
			ID:      "drop-operator-if-exists-removes-existing-operator",
			Source:  "testing/go/operator_definition_repro_test.go:TestDropOperatorIfExistsDropsExistingOperatorRepro",
			Oracle:  "postgres",
			Compare: "structural",
			Setup: []string{
				"CREATE SCHEMA {{quotedSchema}}",
				"SET search_path TO {{quotedSchema}}, pg_catalog",
				"CREATE FUNCTION drop_if_exists_operator_func(integer, integer) RETURNS boolean LANGUAGE SQL IMMUTABLE AS $$ SELECT ($1 % 2) = ($2 % 2) $$",
				"CREATE OPERATOR === (LEFTARG = integer, RIGHTARG = integer, PROCEDURE = drop_if_exists_operator_func)",
				"DROP OPERATOR IF EXISTS === (integer, integer)",
			},
			Query:        "SELECT COUNT(*) FROM pg_catalog.pg_operator WHERE oprname = '===' AND oprnamespace = '{{schema}}'::regnamespace AND oprleft = 'integer'::regtype AND oprright = 'integer'::regtype",
			ExpectedRows: rows(row(value("0"))),
			ColumnModes:  []string{"structural"},
			Cleanup:      []string{"DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE"},
		},
		{
			ID:      "drop-text-search-configuration-if-exists-removes-existing-config",
			Source:  "testing/go/text_search_definition_repro_test.go:TestDropTextSearchConfigurationIfExistsDropsExistingRepro",
			Oracle:  "postgres",
			Compare: "structural",
			Setup: []string{
				"CREATE SCHEMA {{quotedSchema}}",
				"SET search_path TO {{quotedSchema}}, pg_catalog",
				"CREATE TEXT SEARCH CONFIGURATION drop_existing_ts_config_repro (COPY = pg_catalog.simple)",
				"DROP TEXT SEARCH CONFIGURATION IF EXISTS drop_existing_ts_config_repro",
			},
			Query:        "SELECT COUNT(*) FROM pg_catalog.pg_ts_config WHERE cfgname = 'drop_existing_ts_config_repro' AND cfgnamespace = '{{schema}}'::regnamespace",
			ExpectedRows: rows(row(value("0"))),
			ColumnModes:  []string{"structural"},
			Cleanup:      []string{"DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE"},
		},
		{
			ID:      "drop-rule-if-exists-removes-existing-rule-side-effects",
			Source:  "testing/go/rule_correctness_repro_test.go:TestDropRuleIfExistsRemovesExistingRuleRepro",
			Oracle:  "postgres",
			Compare: "structural",
			Setup: []string{
				"CREATE SCHEMA {{quotedSchema}}",
				"SET search_path TO {{quotedSchema}}, pg_catalog",
				"CREATE TABLE drop_rule_source_items (id integer PRIMARY KEY, label text)",
				"CREATE TABLE drop_rule_audit_items (source_id integer, label text)",
				"CREATE RULE drop_rule_source_items_audit AS ON INSERT TO drop_rule_source_items DO ALSO INSERT INTO drop_rule_audit_items VALUES (NEW.id, NEW.label)",
				"DROP RULE IF EXISTS drop_rule_source_items_audit ON drop_rule_source_items",
				"INSERT INTO drop_rule_source_items VALUES (1, 'after drop')",
			},
			Query:        "SELECT COUNT(*) FROM drop_rule_audit_items",
			ExpectedRows: rows(row(value("0"))),
			ColumnModes:  []string{"structural"},
			Cleanup:      []string{"DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE"},
		},
	}
}

type oracleMeta struct {
	ID                    string
	Compare               string
	ColumnModes           []string
	ExpectedRows          *[][]cell
	ExpectedSQLState      string
	ExpectedErrorSeverity string
	ExpectedTag           *string
	Cleanup               []string
}

func loadMigrationOverrides() (map[string]oracleMeta, error) {
	return loadMigrationOverridesExcludingSource("")
}

func loadMigrationOverridesExcludingSource(excludedSourceFile string) (map[string]oracleMeta, error) {
	overrides := map[string]oracleMeta{}
	files, err := filepath.Glob("testdata/postgres_oracle_migrations/*.oracle-map.json")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var mapped migrationFile
		if err := json.Unmarshal(data, &mapped); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		if excludedSourceFile != "" && mapped.SourceFile == excludedSourceFile {
			continue
		}
		if err := addMigrationOverrides(overrides, mapped, file, false); err != nil {
			return nil, err
		}
	}
	return overrides, nil
}

func addMigrationOverrides(overrides map[string]oracleMeta, mapped migrationFile, source string, allowReplace bool) error {
	for _, assertion := range mapped.Assertions {
		if assertion.Oracle != "postgres" {
			continue
		}
		if assertion.Key == "" {
			return fmt.Errorf("%s: postgres migration entry missing key", source)
		}
		if assertion.PostgresID == "" {
			return fmt.Errorf("%s: %s postgres migration entry missing postgresId", source, assertion.Key)
		}
		if _, ok := overrides[assertion.Key]; ok && !allowReplace {
			return fmt.Errorf("%s: duplicate migration key %s", source, assertion.Key)
		}
		overrides[assertion.Key] = oracleMeta{
			ID:                    assertion.PostgresID,
			Compare:               assertion.Compare,
			ColumnModes:           assertion.ColumnModes,
			ExpectedRows:          assertion.ExpectedRows,
			ExpectedSQLState:      assertion.SQLState,
			ExpectedErrorSeverity: assertion.ErrorSeverity,
			ExpectedTag:           assertion.ExpectedTag,
			Cleanup:               assertion.Cleanup,
		}
	}
	return nil
}

func rewriteOracleSources(sourceFileFilter map[string]struct{}, keyFilter map[string]struct{}, postgresIDFilter map[string]struct{}) error {
	migrationOverrides, err := loadMigrationOverrides()
	if err != nil {
		return err
	}
	files := map[string]struct{}{}
	for key, meta := range migrationOverrides {
		if len(keyFilter) > 0 {
			if _, ok := keyFilter[key]; !ok {
				continue
			}
		}
		if len(postgresIDFilter) > 0 {
			if _, ok := postgresIDFilter[meta.ID]; !ok {
				continue
			}
		}
		source, _, ok := strings.Cut(key, "#")
		if !ok {
			continue
		}
		sourceFile, _, ok := strings.Cut(source, ":")
		if !ok {
			continue
		}
		sourceFile = strings.TrimPrefix(sourceFile, currentSourceRoot()+"/")
		if sourceFile == "" {
			continue
		}
		if len(sourceFileFilter) > 0 {
			if _, ok := sourceFileFilter[sourceFile]; !ok {
				continue
			}
		}
		files[sourceFile] = struct{}{}
	}
	sortedFiles := make([]string, 0, len(files))
	for file := range files {
		sortedFiles = append(sortedFiles, file)
	}
	sort.Strings(sortedFiles)
	if len(sourceFileFilter) > 0 && len(sortedFiles) == 0 {
		return fmt.Errorf("no PostgreSQL oracle source entries matched --rewrite-oracle-source-file %s", strings.Join(sortedFilterKeys(sourceFileFilter), ","))
	}
	if len(postgresIDFilter) > 0 && len(sortedFiles) == 0 {
		return fmt.Errorf("no PostgreSQL oracle source entries matched --rewrite-oracle-postgres-id %s", strings.Join(sortedFilterKeys(postgresIDFilter), ","))
	}
	if len(keyFilter) > 0 && len(sortedFiles) == 0 {
		return fmt.Errorf("no PostgreSQL oracle source entries matched --rewrite-oracle-key %s", strings.Join(sortedFilterKeys(keyFilter), ","))
	}
	matchedAny := false
	for _, file := range sortedFiles {
		matched, err := rewriteOracleSourceFile(file, migrationOverrides, keyFilter, postgresIDFilter)
		if err != nil {
			return err
		}
		matchedAny = matchedAny || matched
	}
	if len(postgresIDFilter) > 0 && !matchedAny {
		return fmt.Errorf("no PostgreSQL oracle source assertions matched --rewrite-oracle-postgres-id %s", strings.Join(sortedFilterKeys(postgresIDFilter), ","))
	}
	if len(keyFilter) > 0 && !matchedAny {
		return fmt.Errorf("no PostgreSQL oracle source assertions matched --rewrite-oracle-key %s", strings.Join(sortedFilterKeys(keyFilter), ","))
	}
	return nil
}

func rewriteOracleSourceFile(file string, migrationOverrides map[string]oracleMeta, keyFilter map[string]struct{}, postgresIDFilter map[string]struct{}) (bool, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		return false, err
	}
	packageScriptTests := packageScriptTestSlices(parsed)
	scriptTestHelpers := scriptTestHelperReturns(parsed)
	packageStrings := packageStringSlices(parsed)
	changed := false
	matched := false
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
			continue
		}
		source := fmt.Sprintf("%s:%s", sourcePathForFile(file), fn.Name.Name)
		stringSlices := mergeStringSlices(packageStrings, localStringSlices(fn.Body))
		ordinal := 0
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if ok && isScriptTestSliceType(lit.Type) {
				sliceChanged, sliceMatched := rewriteOracleScriptTestSlice(lit, source, &ordinal, stringSlices, migrationOverrides, scriptTestHelpers, keyFilter, postgresIDFilter)
				if sliceChanged {
					changed = true
				}
				matched = matched || sliceMatched
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if lit, ok := packageScriptTestSliceForRunScripts(call, packageScriptTests); ok {
				sliceChanged, sliceMatched := rewriteOracleScriptTestSlice(lit, source, &ordinal, stringSlices, migrationOverrides, scriptTestHelpers, keyFilter, postgresIDFilter)
				if sliceChanged {
					changed = true
				}
				matched = matched || sliceMatched
				return false
			}
			return true
		})
	}
	if removeUnusedImport(parsed, "sql", "github.com/dolthub/go-mysql-server/sql") {
		changed = true
	}
	if !changed {
		return matched, nil
	}
	var buf bytes.Buffer
	if err := format.Node(&buf, fset, parsed); err != nil {
		return false, err
	}
	return matched, os.WriteFile(file, buf.Bytes(), 0644)
}

func rewriteOracleScriptTestSlice(lit *ast.CompositeLit, source string, ordinal *int, stringSlices map[string][]string, migrationOverrides map[string]oracleMeta, scriptTestHelpers map[string]*ast.CompositeLit, keyFilter map[string]struct{}, postgresIDFilter map[string]struct{}) (bool, bool) {
	changed := false
	matched := false
	for _, element := range lit.Elts {
		scriptLit, generatedHelper, ok := scriptTestLiteralForElement(element, scriptTestHelpers)
		if !ok {
			continue
		}
		scriptFields := compositeFields(scriptLit)
		assertionsLit, ok := scriptFields["Assertions"].(*ast.CompositeLit)
		if !ok {
			continue
		}
		for _, assertionExpr := range assertionsLit.Elts {
			assertionLit, ok := assertionExpr.(*ast.CompositeLit)
			if !ok {
				continue
			}
			assertionFields := compositeFields(assertionLit)
			if !isOracleOrdinalAnchor(assertionFields) {
				continue
			}
			*ordinal = *ordinal + 1
			if generatedHelper {
				continue
			}
			key := fmt.Sprintf("%s#%04d", source, *ordinal)
			if len(keyFilter) > 0 {
				if _, ok := keyFilter[key]; !ok {
					continue
				}
			}
			meta, ok := migrationOverrides[key]
			if !ok || meta.ID == "" {
				continue
			}
			if len(postgresIDFilter) > 0 {
				if _, ok := postgresIDFilter[meta.ID]; !ok {
					continue
				}
			}
			matched = true
			if rewriteOracleAssertion(assertionLit, meta) {
				changed = true
			}
		}
	}
	return changed, matched
}

func removeUnusedImport(parsed *ast.File, packageName string, importPath string) bool {
	changed := false
	decls := parsed.Decls[:0]
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.IMPORT {
			decls = append(decls, decl)
			continue
		}
		specs := gen.Specs[:0]
		for _, spec := range gen.Specs {
			importSpec, ok := spec.(*ast.ImportSpec)
			if !ok || importSpec.Path.Value != strconv.Quote(importPath) {
				specs = append(specs, spec)
				continue
			}
			localName := packageName
			if importSpec.Name != nil {
				localName = importSpec.Name.Name
			}
			if localName == "." || localName == "_" || fileUsesIdent(parsed, localName) {
				specs = append(specs, spec)
				continue
			}
			changed = true
		}
		gen.Specs = specs
		if len(gen.Specs) > 0 {
			decls = append(decls, gen)
		}
	}
	parsed.Decls = decls
	if changed {
		imports := parsed.Imports[:0]
		for _, importSpec := range parsed.Imports {
			if importSpec.Path.Value == strconv.Quote(importPath) {
				continue
			}
			imports = append(imports, importSpec)
		}
		parsed.Imports = imports
	}
	return changed
}

func fileUsesIdent(parsed *ast.File, name string) bool {
	used := false
	for _, decl := range parsed.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.IMPORT {
			continue
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			if used {
				return false
			}
			ident, ok := node.(*ast.Ident)
			if ok && ident.Name == name {
				used = true
				return false
			}
			return true
		})
		if used {
			return true
		}
	}
	return used
}

func isOracleOrdinalAnchor(fields map[string]ast.Expr) bool {
	if _, ok := fields["PostgresOracle"]; ok {
		return true
	}
	return hasAnyField(fields, assertionFieldSet())
}

func rewriteOracleAssertion(lit *ast.CompositeLit, meta oracleMeta) bool {
	expectedFields := map[string]struct{}{}
	for _, field := range manifestAssertionFields {
		expectedFields[field] = struct{}{}
	}
	changed := false
	rewritten := make([]ast.Expr, 0, len(lit.Elts)+1)
	hasPostgresOracle := false
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			rewritten = append(rewritten, element)
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			rewritten = append(rewritten, element)
			continue
		}
		if _, ok := expectedFields[key.Name]; ok {
			changed = true
			continue
		}
		if key.Name == "PostgresOracle" {
			hasPostgresOracle = true
			replacement := postgresOracleMarker(meta)
			if !reflect.DeepEqual(kv.Value, replacement) {
				kv.Value = replacement
				changed = true
			}
		}
		rewritten = append(rewritten, element)
	}
	if !hasPostgresOracle {
		rewritten = append(rewritten, &ast.KeyValueExpr{
			Key:   ast.NewIdent("PostgresOracle"),
			Value: postgresOracleMarker(meta),
		})
		changed = true
	}
	if changed {
		lit.Elts = rewritten
	}
	return changed
}

func postgresOracleMarker(meta oracleMeta) *ast.CompositeLit {
	elts := []ast.Expr{
		&ast.KeyValueExpr{
			Key: &ast.Ident{Name: "ID"},
			Value: &ast.BasicLit{
				Kind:  token.STRING,
				Value: strconv.Quote(meta.ID),
			},
		},
	}
	if meta.Compare != "" && meta.Compare != "structural" {
		elts = append(elts, &ast.KeyValueExpr{
			Key: &ast.Ident{Name: "Compare"},
			Value: &ast.BasicLit{
				Kind:  token.STRING,
				Value: strconv.Quote(meta.Compare),
			},
		})
	}
	if len(meta.ColumnModes) > 0 {
		elts = append(elts, &ast.KeyValueExpr{
			Key:   &ast.Ident{Name: "ColumnModes"},
			Value: stringSliceLiteral(meta.ColumnModes),
		})
	}
	if len(meta.Cleanup) > 0 {
		elts = append(elts, &ast.KeyValueExpr{
			Key:   &ast.Ident{Name: "Cleanup"},
			Value: stringSliceLiteral(meta.Cleanup),
		})
	}
	return &ast.CompositeLit{
		Type: ast.NewIdent("ScriptTestPostgresOracle"),
		Elts: elts,
	}
}

func stringSliceLiteral(values []string) *ast.CompositeLit {
	elts := make([]ast.Expr, 0, len(values))
	for _, value := range values {
		elts = append(elts, &ast.BasicLit{
			Kind:  token.STRING,
			Value: strconv.Quote(value),
		})
	}
	return &ast.CompositeLit{
		Type: &ast.ArrayType{Elt: ast.NewIdent("string")},
		Elts: elts,
	}
}

func annotatedScriptTestEntries(migrationOverrides map[string]oracleMeta) ([]entry, error) {
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return annotatedScriptTestEntriesForFiles(files, migrationOverrides)
}

func annotatedScriptTestEntriesForFiles(files []string, migrationOverrides map[string]oracleMeta) ([]entry, error) {
	entries := make([]entry, 0)
	for _, file := range files {
		if strings.HasPrefix(file, "postgres_oracle_") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			return nil, err
		}
		packageScriptTests := packageScriptTestSlices(parsed)
		scriptTestHelpers := scriptTestHelperReturns(parsed)
		packageStrings := packageStringSlices(parsed)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			stringSlices := mergeStringSlices(packageStrings, localStringSlices(fn.Body))
			source := fmt.Sprintf("%s:%s", sourcePathForFile(file), fn.Name.Name)
			ordinal := 0
			var inspectErr error
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if inspectErr != nil {
					return false
				}
				lit, ok := node.(*ast.CompositeLit)
				if ok && isScriptTestSliceType(lit.Type) {
					generated, err := entriesFromScriptTestSlice(source, &ordinal, lit, stringSlices, migrationOverrides, scriptTestHelpers)
					if err != nil {
						inspectErr = fmt.Errorf("%s: %w", source, err)
						return false
					}
					entries = append(entries, generated...)
					return false
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				if lit, ok := packageScriptTestSliceForRunScripts(call, packageScriptTests); ok {
					generated, err := entriesFromScriptTestSlice(source, &ordinal, lit, stringSlices, migrationOverrides, scriptTestHelpers)
					if err != nil {
						inspectErr = fmt.Errorf("%s: %w", source, err)
						return false
					}
					entries = append(entries, generated...)
					return false
				}
				return true
			})
			if inspectErr != nil {
				return nil, inspectErr
			}
		}
	}
	return entries, nil
}

func writeMigrationCandidates(dir string) error {
	migrationOverrides, err := loadMigrationOverrides()
	if err != nil {
		return err
	}
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		return err
	}
	sort.Strings(files)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for _, file := range files {
		if strings.HasPrefix(file, "postgres_oracle_") {
			continue
		}
		candidates, err := migrationCandidatesForFile(file, migrationOverrides, nil, nil, nil, nil)
		if err != nil {
			return err
		}
		if len(candidates.Assertions) == 0 {
			continue
		}
		data, err := marshalJSON(candidates)
		if err != nil {
			return err
		}
		outPath := filepath.Join(dir, strings.TrimSuffix(file, ".go")+".oracle-map.json")
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func writePromotedOracleMap(sourceFile string, outputPath string, testNameFilter map[string]struct{}, scriptNameFilter map[string]struct{}, skipScriptNameFilter map[string]struct{}, postgresIDFilter map[string]struct{}, forcePostgresOracle bool) error {
	sourceFile = strings.TrimPrefix(sourceFile, "testing/go/")
	if sourceFile == "" || strings.Contains(sourceFile, string(filepath.Separator)+".."+string(filepath.Separator)) || strings.HasPrefix(sourceFile, "..") {
		return fmt.Errorf("invalid source file %q", sourceFile)
	}
	if !strings.HasSuffix(sourceFile, "_test.go") {
		return fmt.Errorf("source file must be a *_test.go file: %s", sourceFile)
	}

	migrationOverrides, err := loadMigrationOverridesExcludingSource(sourcePathForFile(sourceFile))
	if err != nil {
		return err
	}
	candidates, err := migrationCandidatesForFile(sourceFile, migrationOverrides, testNameFilter, scriptNameFilter, skipScriptNameFilter, postgresIDFilter)
	if err != nil {
		return err
	}
	if len(candidates.Assertions) == 0 {
		return fmt.Errorf("%s has no ScriptTest expectation assertions", sourceFile)
	}
	candidates.GeneratedBy = "go run gen_postgres_oracle_manifest.go --promote-oracle-map " + sourceFile
	if len(testNameFilter) > 0 {
		candidates.GeneratedBy += " --oracle-test-name " + strings.Join(sortedFilterKeys(testNameFilter), ",")
	}
	if len(scriptNameFilter) > 0 {
		candidates.GeneratedBy += " --oracle-script-name " + strings.Join(sortedFilterKeys(scriptNameFilter), ",")
	}
	if len(skipScriptNameFilter) > 0 {
		candidates.GeneratedBy += " --oracle-skip-script-name " + strings.Join(sortedFilterKeys(skipScriptNameFilter), ",")
	}
	if len(postgresIDFilter) > 0 {
		candidates.GeneratedBy += " --oracle-postgres-id " + strings.Join(sortedFilterKeys(postgresIDFilter), ",")
	}
	if forcePostgresOracle {
		candidates.GeneratedBy += " --force-postgres-oracle"
	}
	for i := range candidates.Assertions {
		assertion := &candidates.Assertions[i]
		if assertion.Query == "" {
			continue
		}
		if hasPostgresOracleBlockingNonLiteral(assertion.NonLiteral) {
			markOracleAssertionInternal(assertion)
			continue
		}
		if !forcePostgresOracle && len(assertion.NonLiteral) > 0 {
			continue
		}
		switch assertion.ExpectedKind {
		case "rows":
			assertion.Oracle = "postgres"
			if assertion.PostgresID == "" {
				assertion.PostgresID = assertion.SuggestedID
			}
			if assertion.Compare == "" {
				assertion.Compare = "structural"
			}
			if isExplainQuery(assertion.Query) && len(assertion.ColumnModes) == 0 {
				assertion.ColumnModes = []string{"explain"}
			}
			assertion.NeedsCleanup = false
		case "error":
			assertion.Oracle = "postgres"
			if assertion.PostgresID == "" {
				assertion.PostgresID = assertion.SuggestedID
			}
			assertion.Compare = "sqlstate"
			assertion.NeedsCleanup = false
		case "tag":
			assertion.Oracle = "postgres"
			if assertion.PostgresID == "" {
				assertion.PostgresID = assertion.SuggestedID
			}
			assertion.Compare = "tag"
			assertion.NeedsCleanup = false
		}
	}

	data, err := marshalJSON(candidates)
	if err != nil {
		return err
	}
	if outputPath == "" {
		outputPath = filepath.Join("testdata", "postgres_oracle_migrations", strings.TrimSuffix(sourceFile, ".go")+".oracle-map.json")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}
	return os.WriteFile(outputPath, data, 0644)
}

func refreshPromotedOracleMap(sourceFile string, outputPath string, testNameFilter map[string]struct{}, scriptNameFilter map[string]struct{}, skipScriptNameFilter map[string]struct{}, postgresIDFilter map[string]struct{}, dsn string, forcePostgresOracle bool, skipRefreshErrors bool) error {
	sourceFile = strings.TrimPrefix(sourceFile, "testing/go/")
	if outputPath == "" {
		outputPath = filepath.Join("testdata", "postgres_oracle_migrations", strings.TrimSuffix(sourceFile, ".go")+".oracle-map.json")
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return err
	}
	tempOutput, err := os.CreateTemp(filepath.Dir(outputPath), "."+filepath.Base(outputPath)+".*.tmp")
	if err != nil {
		return err
	}
	tempOutputPath := tempOutput.Name()
	if err := tempOutput.Close(); err != nil {
		_ = os.Remove(tempOutputPath)
		return err
	}
	defer func() {
		_ = os.Remove(tempOutputPath)
	}()

	if err := writePromotedOracleMap(sourceFile, tempOutputPath, testNameFilter, scriptNameFilter, skipScriptNameFilter, postgresIDFilter, forcePostgresOracle); err != nil {
		return err
	}

	data, err := os.ReadFile(tempOutputPath)
	if err != nil {
		return err
	}
	var mapped migrationFile
	if err := json.Unmarshal(data, &mapped); err != nil {
		return err
	}
	refreshedKeys := map[string]struct{}{}
	for _, assertion := range mapped.Assertions {
		refreshedKeys[assertion.Key] = struct{}{}
	}
	existing, ok, err := readExistingMigrationFile(outputPath)
	if err != nil {
		return err
	}
	if ok {
		if len(testNameFilter) > 0 || len(scriptNameFilter) > 0 || len(skipScriptNameFilter) > 0 || len(postgresIDFilter) > 0 {
			mapped, err = mergeMigrationFiles(existing, mapped)
			if err != nil {
				return err
			}
		} else {
			preserveGeneratedCachedExpectations(&mapped, existing)
		}
	}
	assertionsByKey := make(map[string]*migrationAssertion, len(mapped.Assertions))
	for i := range mapped.Assertions {
		if forcePostgresOracle && mapped.Assertions[i].Oracle == "postgres" && mapped.Assertions[i].Compare != "sqlstate" && mapped.Assertions[i].SQLState == "" && mapped.Assertions[i].ExpectedRows == nil {
			placeholder := make([][]cell, 0)
			mapped.Assertions[i].ExpectedRows = &placeholder
		}
		assertionsByKey[mapped.Assertions[i].Key] = &mapped.Assertions[i]
	}

	migrationOverrides, err := loadMigrationOverrides()
	if err != nil {
		return err
	}
	if err := addMigrationOverrides(migrationOverrides, mapped, outputPath, true); err != nil {
		return err
	}
	entries, err := annotatedScriptTestEntriesForFiles([]string{sourceFile}, migrationOverrides)
	if err != nil {
		return err
	}

	ctx := context.Background()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if config.RuntimeParams == nil {
		config.RuntimeParams = map[string]string{}
	}
	if _, ok := config.RuntimeParams["statement_timeout"]; !ok {
		config.RuntimeParams["statement_timeout"] = "30s"
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}
	reconnect := func() error {
		_ = conn.Close(ctx)
		next, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			return err
		}
		conn = next
		return nil
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	sourcePrefix := sourcePathForFile(sourceFile) + ":"
	for _, entry := range entries {
		if entry.Oracle != "postgres" || !strings.HasPrefix(entry.Source, sourcePrefix) || entry.Ordinal == 0 {
			continue
		}
		key := fmt.Sprintf("%s#%04d", entry.Source, entry.Ordinal)
		assertion := assertionsByKey[key]
		if assertion == nil {
			continue
		}
		if len(refreshedKeys) > 0 {
			if _, ok := refreshedKeys[key]; !ok {
				continue
			}
		}
		if hasPostgresOracleBlockingNonLiteral(assertion.NonLiteral) {
			err := fmt.Errorf("generated oracle entry depends on non-literal ScriptTest setup that the PostgreSQL refresh harness cannot replay")
			if skipRefreshErrors {
				fmt.Fprintf(os.Stderr, "skipping %s: %v\n", entry.ID, err)
				markOracleAssertionInternal(assertion)
				continue
			}
			return fmt.Errorf("%s: %w", entry.ID, err)
		}
		if hasDoltSpecificStatement(entry) {
			err := fmt.Errorf("generated oracle entry references Dolt-specific SQL that PostgreSQL cannot be source-of-truth for")
			if skipRefreshErrors {
				fmt.Fprintf(os.Stderr, "skipping %s: %v\n", entry.ID, err)
				markOracleAssertionInternal(assertion)
				continue
			}
			return fmt.Errorf("%s: %w", entry.ID, err)
		}
		if hasUnsafeAutoIsolatedPublicReference(entry) {
			err := fmt.Errorf("generated oracle entry uses an isolated schema but setup or query explicitly references public; add an explicit PostgresOracle override or skip this migration")
			if skipRefreshErrors {
				fmt.Fprintf(os.Stderr, "skipping %s: %v\n", entry.ID, err)
				markOracleAssertionInternal(assertion)
				continue
			}
			return fmt.Errorf("%s: %w", entry.ID, err)
		}
		expectedRows, columnModes, sqlstate, severity, expectedTag, err := readPostgresOracleExpected(ctx, conn, entry)
		if err != nil {
			if skipRefreshErrors {
				fmt.Fprintf(os.Stderr, "skipping %s: %v\n", entry.ID, err)
				markOracleAssertionInternal(assertion)
				if reconnectErr := reconnect(); reconnectErr != nil {
					return fmt.Errorf("%s: reconnecting PostgreSQL oracle after refresh error: %w", entry.ID, reconnectErr)
				}
				continue
			}
			return fmt.Errorf("%s: %w", entry.ID, err)
		}
		for index, mode := range columnModes {
			setGeneratedColumnMode(assertion, index, mode)
		}
		if sqlstate != "" {
			assertion.Compare = "sqlstate"
			assertion.ExpectedKind = "error"
			assertion.SQLState = sqlstate
			assertion.ErrorSeverity = severity
			assertion.ExpectedRows = nil
			assertion.ExpectedTag = nil
			continue
		}
		if expectedTag != nil {
			assertion.Compare = "tag"
			assertion.ExpectedKind = "tag"
			assertion.ExpectedTag = expectedTag
			assertion.ExpectedRows = nil
			assertion.SQLState = ""
			assertion.ErrorSeverity = ""
			continue
		}
		applyGeneratedExpectedValueModes(assertion, expectedRows)
		assertion.ExpectedKind = "rows"
		assertion.ExpectedRows = expectedRows
		assertion.ExpectedTag = nil
		if assertion.Compare == "sqlstate" {
			assertion.Compare = "structural"
		}
		assertion.SQLState = ""
		assertion.ErrorSeverity = ""
	}

	refreshed, err := marshalJSON(mapped)
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, refreshed, 0644)
}

func markOracleAssertionInternal(assertion *migrationAssertion) {
	assertion.Oracle = "internal"
	assertion.PostgresID = ""
	assertion.Compare = ""
	assertion.ColumnModes = nil
	assertion.SQLState = ""
	assertion.ErrorSeverity = ""
	assertion.ExpectedRows = nil
	assertion.ExpectedTag = nil
	assertion.Cleanup = nil
	assertion.NeedsCleanup = true
	assertion.CleanupProvided = false
}

func readExistingMigrationFile(path string) (migrationFile, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return migrationFile{}, false, nil
		}
		return migrationFile{}, false, err
	}
	var mapped migrationFile
	if err := json.Unmarshal(data, &mapped); err != nil {
		return migrationFile{}, false, fmt.Errorf("%s: %w", path, err)
	}
	return mapped, true, nil
}

func mergeMigrationFiles(existing migrationFile, generated migrationFile) (migrationFile, error) {
	if existing.SourceFile != "" && generated.SourceFile != "" && existing.SourceFile != generated.SourceFile {
		return migrationFile{}, fmt.Errorf("cannot merge oracle maps for different source files: %s and %s", existing.SourceFile, generated.SourceFile)
	}
	replacementByKey := make(map[string]migrationAssertion, len(generated.Assertions))
	for _, assertion := range generated.Assertions {
		replacementByKey[assertion.Key] = assertion
	}
	merged := make([]migrationAssertion, 0, len(existing.Assertions)+len(generated.Assertions))
	replaced := make(map[string]struct{}, len(generated.Assertions))
	for _, assertion := range existing.Assertions {
		if replacement, ok := replacementByKey[assertion.Key]; ok {
			preserveCachedExpectation(&replacement, assertion)
			merged = append(merged, replacement)
			replaced[assertion.Key] = struct{}{}
			continue
		}
		merged = append(merged, assertion)
	}
	for _, assertion := range generated.Assertions {
		if _, ok := replaced[assertion.Key]; ok {
			continue
		}
		merged = append(merged, assertion)
	}
	if existing.SourceFile == "" {
		existing.SourceFile = generated.SourceFile
	}
	if existing.GeneratedBy == "" {
		existing.GeneratedBy = generated.GeneratedBy
	}
	if existing.DefaultOracle == "" {
		existing.DefaultOracle = generated.DefaultOracle
	}
	if existing.AssertionKeyStyle == "" {
		existing.AssertionKeyStyle = generated.AssertionKeyStyle
	}
	if len(existing.AssertionFields) == 0 {
		existing.AssertionFields = generated.AssertionFields
	}
	existing.Assertions = merged
	return existing, nil
}

func preserveGeneratedCachedExpectations(generated *migrationFile, existing migrationFile) {
	existingByKey := make(map[string]migrationAssertion, len(existing.Assertions))
	for _, assertion := range existing.Assertions {
		existingByKey[assertion.Key] = assertion
	}
	for i := range generated.Assertions {
		if existingAssertion, ok := existingByKey[generated.Assertions[i].Key]; ok {
			preserveCachedExpectation(&generated.Assertions[i], existingAssertion)
		}
	}
}

func preserveCachedExpectation(replacement *migrationAssertion, existing migrationAssertion) {
	if replacement.Oracle != "postgres" {
		return
	}
	if replacement.ExpectedRows != nil || replacement.SQLState != "" || replacement.ExpectedTag != nil {
		return
	}
	replacement.ExpectedKind = existing.ExpectedKind
	replacement.ExpectedRows = existing.ExpectedRows
	replacement.ExpectedTag = existing.ExpectedTag
	replacement.SQLState = existing.SQLState
	replacement.ErrorSeverity = existing.ErrorSeverity
	if replacement.Compare == "" {
		replacement.Compare = existing.Compare
	}
	if len(replacement.ColumnModes) == 0 {
		replacement.ColumnModes = existing.ColumnModes
	}
}

func hasUnsafeAutoIsolatedPublicReference(entry entry) bool {
	setup := entrySetupQueries(entry)
	if len(setup) < 2 || setup[0] != "CREATE SCHEMA {{quotedSchema}}" || !strings.Contains(setup[1], "{{quotedSchema}}") {
		return false
	}
	for _, statement := range append(append([]string{}, setup...), entry.Query) {
		if hasExplicitPublicSchemaReference(statement) {
			return true
		}
	}
	return false
}

func hasExplicitPublicSchemaReference(statement string) bool {
	normalized := strings.ToLower(statement)
	normalized = strings.ReplaceAll(normalized, "\n", " ")
	normalized = strings.ReplaceAll(normalized, "\t", " ")
	for strings.Contains(normalized, "  ") {
		normalized = strings.ReplaceAll(normalized, "  ", " ")
	}
	checks := []string{
		"public.",
		"schema public",
		"with schema public",
		"set search_path to public",
		"array['public'",
		"array[ 'public'",
		"table_schema = 'public'",
		"table_schema='public'",
		"schemaname = 'public'",
		"schemaname='public'",
	}
	for _, check := range checks {
		if strings.Contains(normalized, check) {
			return true
		}
	}
	return false
}

func hasDoltSpecificStatement(entry entry) bool {
	for _, statement := range entrySetupQueries(entry) {
		if hasDoltSpecificSQL(statement) {
			return true
		}
	}
	return hasDoltSpecificSQL(entry.Query)
}

func hasDoltSpecificSQL(statement string) bool {
	code, stringLiterals := sqlCodeAndStringLiterals(strings.ToLower(statement))
	if doltSpecificIdentifierPattern.MatchString(code) {
		return true
	}
	if doltSpecificInternalIdentifierPattern.MatchString(code) {
		return true
	}
	for _, literal := range stringLiterals {
		if isDoltSpecificStringLiteral(literal) {
			return true
		}
	}
	return false
}

var doltSpecificIdentifierPattern = regexp.MustCompile(`(^|[^a-z0-9_])dolt[._][a-z0-9_]+`)
var doltSpecificInternalIdentifierPattern = regexp.MustCompile(`(^|[^a-z0-9_])dg_[a-z0-9_]*_posting_chunks($|[^a-z0-9_])`)

func sqlCodeAndStringLiterals(statement string) (string, []string) {
	var code strings.Builder
	var literals []string
	for i := 0; i < len(statement); {
		if statement[i] != '\'' {
			code.WriteByte(statement[i])
			i++
			continue
		}

		code.WriteByte(' ')
		i++
		var literal strings.Builder
		for i < len(statement) {
			if statement[i] == '\'' {
				if i+1 < len(statement) && statement[i+1] == '\'' {
					literal.WriteByte('\'')
					code.WriteString("  ")
					i += 2
					continue
				}
				code.WriteByte(' ')
				i++
				break
			}
			literal.WriteByte(statement[i])
			code.WriteByte(' ')
			i++
		}
		literals = append(literals, literal.String())
	}
	return code.String(), literals
}

func isDoltSpecificStringLiteral(literal string) bool {
	if literal == "dolt_" || literal == "dolt_%" {
		return false
	}
	return strings.Contains(literal, "dolt.") ||
		doltSpecificLiteralPattern.MatchString(literal) ||
		doltSpecificInternalIdentifierPattern.MatchString(literal)
}

var doltSpecificLiteralPattern = regexp.MustCompile(`(^|[^a-z0-9_])dolt_[a-z0-9][a-z0-9_]*`)

func sortedFilterKeys(filter map[string]struct{}) []string {
	keys := make([]string, 0, len(filter))
	for key := range filter {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func hasAnyNonLiteral(nonLiteral []string, names ...string) bool {
	if len(nonLiteral) == 0 {
		return false
	}
	nameSet := make(map[string]struct{}, len(names))
	for _, name := range names {
		nameSet[name] = struct{}{}
	}
	for _, field := range nonLiteral {
		if _, ok := nameSet[field]; ok {
			return true
		}
	}
	return false
}

func hasPostgresOracleBlockingNonLiteral(nonLiteral []string) bool {
	return hasAnyNonLiteral(nonLiteral,
		"Query",
		"Username",
		"BindVars",
		"CopyFromStdInFile",
		"SetUpScript",
		"DoltSpecific",
		"GeneratedHelper",
		"PriorQuery",
		"PriorUsername",
		"PriorBindVars",
		"PriorCopyFromStdInFile",
		"PriorDoltSpecific",
	)
}

func postgresOracleDSN(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if dsn := os.Getenv("DOLTGRES_POSTGRES_TEST_DSN"); dsn != "" {
		return dsn, nil
	}
	if dsn := os.Getenv("POSTGRES_TEST_DSN"); dsn != "" {
		return dsn, nil
	}
	if os.Getenv("DOLTGRES_ORACLE") != "" {
		return defaultPostgresOracleDSN, nil
	}
	return "", fmt.Errorf("set --postgres-dsn, DOLTGRES_POSTGRES_TEST_DSN, POSTGRES_TEST_DSN, or DOLTGRES_ORACLE=1")
}

func isExplainQuery(query string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "explain")
}

func readPostgresOracleExpected(ctx context.Context, conn *pgx.Conn, entry entry) (*[][]cell, []string, string, string, *string, error) {
	variables := oracleVariables(entry)
	if err := resetOracleSession(ctx, conn); err != nil {
		return nil, nil, "", "", nil, err
	}
	if err := runOracleStatements(ctx, conn, variables, entry.Cleanup); err != nil {
		return nil, nil, "", "", nil, err
	}
	defer func() {
		_ = runOracleStatements(ctx, conn, variables, entry.Cleanup)
		_ = resetOracleSession(ctx, conn)
	}()
	if err := resetOracleSession(ctx, conn); err != nil {
		return nil, nil, "", "", nil, err
	}
	if err := runOracleStatementSteps(ctx, conn, variables, entrySetupStatements(entry)); err != nil {
		return nil, nil, "", "", nil, err
	}

	queryStatement := oracleStatement{Query: entry.Query, BindVars: entry.BindVars}
	if entry.Compare == "sqlstate" || entry.ExpectedSQLState != "" {
		_, err := execOracleStatement(ctx, conn, variables, queryStatement)
		if err == nil {
			if entry.ExpectedSQLState != "" {
				return nil, nil, "", "", nil, fmt.Errorf("expected SQLSTATE %s but query succeeded", entry.ExpectedSQLState)
			}
		} else {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				return nil, nil, "", "", nil, err
			}
			return nil, nil, pgErr.Code, pgErr.Severity, nil, nil
		}
	}
	if entry.Compare == "tag" || entry.ExpectedTag != nil {
		commandTag, err := execOracleStatement(ctx, conn, variables, queryStatement)
		if err != nil {
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				return nil, nil, "", "", nil, err
			}
			return nil, nil, pgErr.Code, pgErr.Severity, nil, nil
		}
		tag := commandTag.String()
		return nil, nil, "", "", &tag, nil
	}

	rows, err := queryOracleStatement(ctx, conn, variables, queryStatement)
	if err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			return nil, nil, "", "", nil, err
		}
		return nil, nil, pgErr.Code, pgErr.Severity, nil, nil
	}
	defer rows.Close()

	expected := make([][]cell, 0)
	columnModes := append([]string(nil), entry.ColumnModes...)
	fields, err := oracleFieldDescriptions(rows)
	if err != nil {
		return nil, nil, "", "", nil, err
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, nil, "", "", nil, err
		}
		row := make([]cell, 0, len(values))
		for i, rowValue := range values {
			if rowValue == nil {
				row = append(row, cell{Null: true})
				continue
			}
			if inferGeneratedPostgresMode(fields[i].DataTypeOID) == "bytea" {
				setGeneratedColumnModeValue(&columnModes, i, "bytea")
			}
			row = append(row, value(normalizeGeneratedPostgresValue(entry, i, rowValue, fields[i].DataTypeOID)))
		}
		expected = append(expected, row)
	}
	if err := rows.Err(); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			return nil, nil, "", "", nil, err
		}
		return nil, nil, pgErr.Code, pgErr.Severity, nil, nil
	}
	return &expected, columnModes, "", "", nil, nil
}

func oracleFieldDescriptions(rows pgx.Rows) (fields []pgconn.FieldDescription, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("query returned no row description: %v", recovered)
		}
	}()
	return rows.FieldDescriptions(), nil
}

func normalizeGeneratedPostgresValue(entry entry, index int, value interface{}, oid uint32) string {
	if oid == 18 {
		return normalizeGeneratedPostgresChar(value)
	}
	mode := generatedColumnMode(entry, index)
	if mode == "structural" {
		mode = inferGeneratedPostgresMode(oid)
	}
	if mode == "exact" {
		return fmt.Sprint(value)
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
		return normalizeGeneratedPostgresPgNumeric(v)
	case pgtype.Interval:
		if !v.Valid {
			return "<null>"
		}
		return duration.MakeDuration(v.Microseconds*duration.NanosPerMicro, int64(v.Days), int64(v.Months)).String()
	case []byte:
		switch mode {
		case "bytea":
			return "\\x" + hex.EncodeToString(v)
		case "explain":
			return normalizeGeneratedPostgresExplain(string(v))
		case "schema":
			return normalizeGeneratedPostgresSchema(string(v))
		case "json":
			return normalizeGeneratedPostgresJSON(string(v))
		case "numeric":
			return normalizeGeneratedPostgresNumeric(string(v))
		case "array":
			return normalizeGeneratedPostgresArray(string(v))
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
			return normalizeGeneratedPostgresExplain(v)
		case "schema":
			return normalizeGeneratedPostgresSchema(v)
		case "json":
			return normalizeGeneratedPostgresJSON(v)
		case "numeric":
			return normalizeGeneratedPostgresNumeric(v)
		case "array":
			return normalizeGeneratedPostgresArray(v)
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
			if normalized, ok := normalizeGeneratedPostgresSlice(v); ok {
				return normalized
			}
		}
		text := fmt.Sprint(v)
		if mode == "numeric" {
			return normalizeGeneratedPostgresNumeric(text)
		}
		if mode == "schema" {
			return normalizeGeneratedPostgresSchema(text)
		}
		if mode == "explain" {
			return normalizeGeneratedPostgresExplain(text)
		}
		return text
	}
}

func normalizeGeneratedPostgresChar(value interface{}) string {
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

func generatedColumnMode(entry entry, index int) string {
	if index < len(entry.ColumnModes) && entry.ColumnModes[index] != "" {
		return entry.ColumnModes[index]
	}
	if entry.Compare == "exact" {
		return "exact"
	}
	return "structural"
}

func inferGeneratedPostgresMode(oid uint32) string {
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
	case 1186:
		return "interval"
	case 199, 1000, 1001, 1002, 1003, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020, 1021, 1022, 1027, 1028, 1034, 1040, 1041, 1115, 1182, 1183, 1185, 1187, 1231, 1270, 2951, 3807:
		return "array"
	default:
		return "structural"
	}
}

func applyGeneratedExpectedValueModes(assertion *migrationAssertion, expectedRows *[][]cell) {
	if expectedRows == nil {
		return
	}
	explain := isExplainQuery(assertion.Query)
	for rowIndex := range *expectedRows {
		for columnIndex := range (*expectedRows)[rowIndex] {
			valuePtr := (*expectedRows)[rowIndex][columnIndex].Value
			if valuePtr == nil {
				continue
			}
			value := *valuePtr
			if explain {
				normalized := normalizeGeneratedPostgresExplain(value)
				(*expectedRows)[rowIndex][columnIndex].Value = &normalized
				setGeneratedColumnMode(assertion, columnIndex, "explain")
				continue
			}
			normalized := normalizeGeneratedPostgresSchema(value)
			if normalized != value {
				(*expectedRows)[rowIndex][columnIndex].Value = &normalized
				setGeneratedColumnMode(assertion, columnIndex, "schema")
			}
		}
	}
}

func setGeneratedColumnMode(assertion *migrationAssertion, index int, mode string) {
	setGeneratedColumnModeValue(&assertion.ColumnModes, index, mode)
}

func setGeneratedColumnModeValue(columnModes *[]string, index int, mode string) {
	for len(*columnModes) <= index {
		*columnModes = append(*columnModes, "structural")
	}
	switch (*columnModes)[index] {
	case "", "structural":
		(*columnModes)[index] = mode
	}
	for i := range *columnModes {
		if (*columnModes)[i] == "" {
			(*columnModes)[i] = "structural"
		}
	}
	for len(*columnModes) > 0 && (*columnModes)[len(*columnModes)-1] == "" {
		*columnModes = (*columnModes)[:len(*columnModes)-1]
	}
}

var generatedSchemaNamePattern = regexp.MustCompile(`dg_oracle_[0-9]+`)

func normalizeGeneratedPostgresSchema(value string) string {
	return generatedSchemaNamePattern.ReplaceAllString(value, "{{schema}}")
}

var (
	generatedExplainActualTimePattern = regexp.MustCompile(`actual time=[0-9]+(?:\.[0-9]+)?\.\.[0-9]+(?:\.[0-9]+)?`)
	generatedExplainPlanningPattern   = regexp.MustCompile(`Planning Time: [0-9]+(?:\.[0-9]+)? ms`)
	generatedExplainExecutionPattern  = regexp.MustCompile(`Execution Time: [0-9]+(?:\.[0-9]+)? ms`)
)

func normalizeGeneratedPostgresExplain(value string) string {
	value = normalizeGeneratedPostgresSchema(value)
	value = generatedExplainActualTimePattern.ReplaceAllString(value, "actual time=<time>..<time>")
	value = generatedExplainPlanningPattern.ReplaceAllString(value, "Planning Time: <time> ms")
	value = generatedExplainExecutionPattern.ReplaceAllString(value, "Execution Time: <time> ms")
	return value
}

func normalizeGeneratedPostgresNumeric(value string) string {
	dec, err := decimal.NewFromString(strings.TrimSpace(value))
	if err != nil {
		return strings.TrimSpace(value)
	}
	if dec.IsZero() {
		return "0"
	}
	return dec.String()
}

func normalizeGeneratedPostgresPgNumeric(value pgtype.Numeric) string {
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

func normalizeGeneratedPostgresJSON(value string) string {
	trimmed := strings.TrimSpace(value)
	var decoded interface{}
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

func normalizeGeneratedPostgresArray(value string) string {
	trimmed := strings.TrimSpace(value)
	if normalized, ok := normalizeGeneratedPostgresBracketArray(trimmed); ok {
		return normalized
	}
	trimmed = strings.ReplaceAll(trimmed, ", ", ",")
	return trimmed
}

func normalizeGeneratedPostgresBracketArray(value string) (string, bool) {
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

func normalizeGeneratedPostgresSlice(value interface{}) (string, bool) {
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return "", false
	}
	parts := make([]string, rv.Len())
	for i := range parts {
		parts[i] = normalizeGeneratedPostgresArrayElement(rv.Index(i))
	}
	return "{" + strings.Join(parts, ",") + "}", true
}

func normalizeGeneratedPostgresArrayElement(element reflect.Value) string {
	if !element.IsValid() {
		return "NULL"
	}
	for element.Kind() == reflect.Interface || element.Kind() == reflect.Pointer {
		if element.IsNil() {
			return "NULL"
		}
		element = element.Elem()
	}
	if uuid, ok := postgresUUIDArrayElement(element); ok {
		return uuid
	}
	switch value := element.Interface().(type) {
	case pgtype.Numeric:
		if !value.Valid {
			return "NULL"
		}
		return normalizeGeneratedPostgresPgNumeric(value)
	case pgtype.UUID:
		if !value.Valid {
			return "NULL"
		}
		return formatPostgresUUID(value.Bytes)
	default:
		return fmt.Sprint(value)
	}
}

func postgresUUIDArrayElement(element reflect.Value) (string, bool) {
	if element.Kind() != reflect.Array || element.Len() != 16 || element.Type().Elem().Kind() != reflect.Uint8 {
		return "", false
	}
	var bytes [16]byte
	for i := range bytes {
		bytes[i] = byte(element.Index(i).Uint())
	}
	return formatPostgresUUID(bytes), true
}

func formatPostgresUUID(bytes [16]byte) string {
	encoded := hex.EncodeToString(bytes[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func entrySetupStatements(entry entry) []oracleStatement {
	if len(entry.SetupStatements) > 0 {
		return entry.SetupStatements
	}
	return statementsFromQueries(entry.Setup)
}

func entrySetupQueries(entry entry) []string {
	return statementQueries(entrySetupStatements(entry))
}

func statementsFromQueries(queries []string) []oracleStatement {
	statements := make([]oracleStatement, 0, len(queries))
	for _, query := range queries {
		statements = append(statements, oracleStatement{Query: query})
	}
	return statements
}

func statementQueries(statements []oracleStatement) []string {
	queries := make([]string, 0, len(statements))
	for _, statement := range statements {
		queries = append(queries, statement.Query)
	}
	return queries
}

func statementsHaveBindVars(statements []oracleStatement) bool {
	for _, statement := range statements {
		if len(statement.BindVars) > 0 {
			return true
		}
	}
	return false
}

func runOracleStatements(ctx context.Context, conn *pgx.Conn, variables map[string]string, statements []string) error {
	return runOracleStatementSteps(ctx, conn, variables, statementsFromQueries(statements))
}

func runOracleStatementSteps(ctx context.Context, conn *pgx.Conn, variables map[string]string, statements []oracleStatement) error {
	for _, statement := range statements {
		if _, err := execOracleStatement(ctx, conn, variables, statement); err != nil {
			return fmt.Errorf("oracle statement failed: %s: %w", statement.Query, err)
		}
	}
	return nil
}

func execOracleStatement(ctx context.Context, conn *pgx.Conn, variables map[string]string, statement oracleStatement) (pgconn.CommandTag, error) {
	query := expandOracleVariables(statement.Query, variables)
	args, err := oracleQueryArgs(statement.BindVars)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return conn.Exec(ctx, query, args...)
}

func queryOracleStatement(ctx context.Context, conn *pgx.Conn, variables map[string]string, statement oracleStatement) (pgx.Rows, error) {
	query := expandOracleVariables(statement.Query, variables)
	args, err := oracleQueryArgs(statement.BindVars)
	if err != nil {
		return nil, err
	}
	return conn.Query(ctx, query, args...)
}

func oracleQueryArgs(bindVars []oracleBindVar) ([]interface{}, error) {
	if len(bindVars) == 0 {
		return nil, nil
	}
	args := make([]interface{}, 0, len(bindVars)+1)
	args = append(args, pgx.QueryExecModeExec)
	for _, bindVar := range bindVars {
		value, err := oracleBindVarValue(bindVar)
		if err != nil {
			return nil, err
		}
		args = append(args, value)
	}
	return args, nil
}

func oracleBindVarValue(bindVar oracleBindVar) (interface{}, error) {
	if bindVar.Null || bindVar.Kind == "null" {
		return nil, nil
	}
	switch bindVar.Kind {
	case "string":
		return bindVar.Value, nil
	case "int":
		value, err := strconv.ParseInt(bindVar.Value, 10, 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	case "float":
		value, err := strconv.ParseFloat(bindVar.Value, 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	case "bool":
		value, err := strconv.ParseBool(bindVar.Value)
		if err != nil {
			return nil, err
		}
		return value, nil
	case "date":
		parsed, err := time.Parse("2006-01-02", bindVar.Value)
		if err != nil {
			return nil, err
		}
		var value pgtype.Date
		if err := value.Scan(parsed); err != nil {
			return nil, err
		}
		return value, nil
	case "timestamp":
		parsed, err := time.Parse("2006-01-02 15:04:05", bindVar.Value)
		if err != nil {
			return nil, err
		}
		var value pgtype.Timestamp
		if err := value.Scan(parsed); err != nil {
			return nil, err
		}
		return value, nil
	case "uuid":
		var value pgtype.UUID
		if err := value.Scan(bindVar.Value); err != nil {
			return nil, err
		}
		return value, nil
	case "numeric":
		var value pgtype.Numeric
		if err := value.Scan(bindVar.Value); err != nil {
			return nil, err
		}
		return value, nil
	case "bytes":
		return hex.DecodeString(bindVar.Value)
	case "stringArray":
		return append([]string(nil), bindVar.Values...), nil
	default:
		return nil, fmt.Errorf("unsupported bind var kind %q", bindVar.Kind)
	}
}

func resetOracleSession(ctx context.Context, conn *pgx.Conn) error {
	// Previous oracle probes may intentionally fail inside a transaction. Roll
	// back first so RESET commands do not cascade with SQLSTATE 25P02.
	_, _ = conn.Exec(ctx, "ROLLBACK")
	if _, err := conn.Exec(ctx, "RESET ROLE"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "RESET ALL"); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "RESET search_path"); err != nil {
		return err
	}
	return nil
}

func oracleVariables(entry entry) map[string]string {
	variables := map[string]string{}
	for key, value := range entry.Variables {
		variables[key] = value
	}
	if _, ok := variables["schema"]; !ok {
		variables["schema"] = fmt.Sprintf("dg_oracle_%d", time.Now().UnixNano())
	}
	variables["quotedSchema"] = quoteIdentifier(variables["schema"])
	return variables
}

func expandOracleVariables(query string, variables map[string]string) string {
	expanded := query
	for key, value := range variables {
		expanded = strings.ReplaceAll(expanded, "{{"+key+"}}", value)
	}
	return expanded
}

func migrationCandidatesForFile(file string, migrationOverrides map[string]oracleMeta, testNameFilter map[string]struct{}, scriptNameFilter map[string]struct{}, skipScriptNameFilter map[string]struct{}, postgresIDFilter map[string]struct{}) (migrationFile, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		return migrationFile{}, err
	}
	candidates := migrationFile{
		GeneratedBy:       "go run gen_postgres_oracle_manifest.go --migration-candidates-dir",
		SourceFile:        sourcePathForFile(file),
		DefaultOracle:     "internal",
		AssertionKeyStyle: currentSourceRoot() + "/<file>:<TestName>#<expectation-ordinal>",
		AssertionFields:   manifestAssertionFields,
	}
	fieldSet := assertionFieldSet()
	packageScriptTests := packageScriptTestSlices(parsed)
	scriptTestHelpers := scriptTestHelperReturns(parsed)
	packageStrings := packageStringSlices(parsed)
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
			continue
		}
		if len(testNameFilter) > 0 {
			if _, ok := testNameFilter[fn.Name.Name]; !ok {
				continue
			}
		}
		stringSlices := mergeStringSlices(packageStrings, localStringSlices(fn.Body))
		source := fmt.Sprintf("%s:%s", sourcePathForFile(file), fn.Name.Name)
		ordinal := 0
		var inspectErr error
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if inspectErr != nil {
				return false
			}
			lit, ok := node.(*ast.CompositeLit)
			if ok && isScriptTestSliceType(lit.Type) {
				generated, err := migrationCandidatesFromScriptTestSlice(source, &ordinal, lit, stringSlices, fieldSet, scriptNameFilter, skipScriptNameFilter, postgresIDFilter, migrationOverrides, scriptTestHelpers)
				if err != nil {
					inspectErr = err
					return false
				}
				candidates.Assertions = append(candidates.Assertions, generated...)
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if lit, ok := packageScriptTestSliceForRunScripts(call, packageScriptTests); ok {
				generated, err := migrationCandidatesFromScriptTestSlice(source, &ordinal, lit, stringSlices, fieldSet, scriptNameFilter, skipScriptNameFilter, postgresIDFilter, migrationOverrides, scriptTestHelpers)
				if err != nil {
					inspectErr = err
					return false
				}
				candidates.Assertions = append(candidates.Assertions, generated...)
				return false
			}
			return true
		})
		if inspectErr != nil {
			return migrationFile{}, inspectErr
		}
	}
	return candidates, nil
}

func migrationCandidatesFromScriptTestSlice(source string, ordinal *int, lit *ast.CompositeLit, stringSlices map[string][]string, fieldSet map[string]struct{}, scriptNameFilter map[string]struct{}, skipScriptNameFilter map[string]struct{}, postgresIDFilter map[string]struct{}, migrationOverrides map[string]oracleMeta, scriptTestHelpers map[string]*ast.CompositeLit) ([]migrationAssertion, error) {
	candidates := make([]migrationAssertion, 0)
	for _, element := range lit.Elts {
		scriptLit, generatedHelper, ok := scriptTestLiteralForElement(element, scriptTestHelpers)
		if !ok {
			continue
		}
		helperNonLiteral := []string(nil)
		if generatedHelper {
			helperNonLiteral = append(helperNonLiteral, "GeneratedHelper")
		}
		scriptFields := compositeFields(scriptLit)
		scriptName, _ := optionalStringLiteral(scriptFields["Name"])
		scriptNonLiteral := candidateStringSlice(scriptFields["SetUpScript"], "SetUpScript", stringSlices, helperNonLiteral)
		if setup, err := stringSlice(scriptFields["SetUpScript"], stringSlices); err == nil {
			for _, statement := range setup {
				if hasDoltSpecificSQL(statement) {
					scriptNonLiteral = appendNonLiteral(scriptNonLiteral, "DoltSpecific")
					break
				}
			}
		}
		includeScript := len(scriptNameFilter) == 0
		if !includeScript {
			_, includeScript = scriptNameFilter[scriptName]
		}
		if _, skipScript := skipScriptNameFilter[scriptName]; skipScript {
			includeScript = false
		}
		assertionsLit, ok := scriptFields["Assertions"].(*ast.CompositeLit)
		if !ok {
			continue
		}
		priorNonLiteral := make([]string, 0)
		for _, assertionExpr := range assertionsLit.Elts {
			assertionLit, ok := assertionExpr.(*ast.CompositeLit)
			if !ok {
				continue
			}
			assertionFields := compositeFields(assertionLit)
			if !hasAnyField(assertionFields, fieldSet) {
				priorNonLiteral = appendPriorSetupNonLiteral(priorNonLiteral, assertionFields)
				continue
			}
			*ordinal = *ordinal + 1
			if !includeScript {
				priorNonLiteral = appendPriorSetupNonLiteral(priorNonLiteral, assertionFields)
				continue
			}
			candidate, err := migrationCandidate(source, *ordinal, scriptName, assertionFields, stringSlices, migrationOverrides)
			if err != nil {
				return nil, fmt.Errorf("%s#%04d: %w", source, *ordinal, err)
			}
			candidate.NonLiteral = append(candidate.NonLiteral, scriptNonLiteral...)
			candidate.NonLiteral = append(candidate.NonLiteral, priorNonLiteral...)
			if !migrationCandidateMatchesPostgresIDFilter(candidate, postgresIDFilter) {
				priorNonLiteral = appendPriorSetupNonLiteral(priorNonLiteral, assertionFields)
				continue
			}
			candidates = append(candidates, candidate)
			priorNonLiteral = appendPriorSetupNonLiteral(priorNonLiteral, assertionFields)
		}
	}
	return candidates, nil
}

func migrationCandidateMatchesPostgresIDFilter(candidate migrationAssertion, postgresIDFilter map[string]struct{}) bool {
	if len(postgresIDFilter) == 0 {
		return true
	}
	if candidate.PostgresID != "" {
		if _, ok := postgresIDFilter[candidate.PostgresID]; ok {
			return true
		}
	}
	if candidate.SuggestedID != "" {
		if _, ok := postgresIDFilter[candidate.SuggestedID]; ok {
			return true
		}
	}
	return false
}

func migrationCandidate(source string, ordinal int, scriptName string, fields map[string]ast.Expr, stringSlices map[string][]string, migrationOverrides map[string]oracleMeta) (migrationAssertion, error) {
	query, nonLiteral := candidateStringLiteral(fields["Query"], "Query", nil)
	key := fmt.Sprintf("%s#%04d", source, ordinal)
	expectedKind := expectationKind(fields)
	candidate := migrationAssertion{
		Key:          key,
		Source:       source,
		Ordinal:      ordinal,
		ScriptName:   scriptName,
		Oracle:       "internal",
		SuggestedID:  suggestedOracleID(source, ordinal, query),
		ExpectedKind: expectedKind,
		Query:        query,
		NonLiteral:   nonLiteral,
		NeedsCleanup: true,
	}
	if query != "" {
		sum := sha256.Sum256([]byte(query))
		candidate.QuerySHA256 = fmt.Sprintf("%x", sum[:])
	}
	username, nonLiteral := candidateStringLiteral(fields["Username"], "Username", candidate.NonLiteral)
	candidate.Username = username
	candidate.NonLiteral = nonLiteral
	if bindVarsExpr := fields["BindVars"]; bindVarsExpr != nil {
		bindVars, err := bindVarsFromExpr(bindVarsExpr)
		if err != nil {
			candidate.NonLiteral = append(candidate.NonLiteral, "BindVars")
		} else {
			candidate.BindVars = bindVars
		}
	}
	if fields["CopyFromStdInFile"] != nil {
		candidate.NonLiteral = append(candidate.NonLiteral, "CopyFromStdInFile")
	}
	if hasDoltSpecificSQL(query) {
		candidate.NonLiteral = append(candidate.NonLiteral, "DoltSpecific")
	}
	if expectedKind == "rows" {
		if _, err := expectedRowsFromExpr(fields["Expected"]); err != nil {
			candidate.NonLiteral = append(candidate.NonLiteral, "Expected")
		}
	}
	if len(candidate.NonLiteral) == 0 {
		candidate.NonLiteral = nil
	}
	if metaExpr, ok := fields["PostgresOracle"]; ok {
		meta, err := parseOracleMeta(metaExpr, stringSlices)
		if err != nil {
			return migrationAssertion{}, fmt.Errorf("PostgresOracle: %w", err)
		}
		if meta.ID != "" {
			candidate.Oracle = "postgres"
			candidate.PostgresID = meta.ID
			candidate.Compare = meta.Compare
			candidate.ColumnModes = meta.ColumnModes
			candidate.SQLState = meta.ExpectedSQLState
			candidate.ErrorSeverity = meta.ExpectedErrorSeverity
			candidate.ExpectedTag = meta.ExpectedTag
			candidate.Cleanup = meta.Cleanup
			candidate.CleanupProvided = len(meta.Cleanup) > 0
		}
	}
	if meta, ok := migrationOverrides[key]; ok {
		candidate.Oracle = "postgres"
		candidate.PostgresID = meta.ID
		candidate.Compare = meta.Compare
		candidate.ColumnModes = meta.ColumnModes
		candidate.SQLState = meta.ExpectedSQLState
		candidate.ErrorSeverity = meta.ExpectedErrorSeverity
		candidate.ExpectedTag = meta.ExpectedTag
		candidate.Cleanup = meta.Cleanup
		candidate.CleanupProvided = len(meta.Cleanup) > 0
	}
	if candidate.Oracle == "postgres" && candidate.ExpectedKind == "unknown" {
		if candidate.Compare == "tag" || candidate.ExpectedTag != nil {
			candidate.ExpectedKind = "tag"
		} else if candidate.Compare == "sqlstate" || candidate.SQLState != "" {
			candidate.ExpectedKind = "error"
		} else {
			candidate.ExpectedKind = "rows"
		}
	}
	return candidate, nil
}

func candidateStringLiteral(expr ast.Expr, name string, nonLiteral []string) (string, []string) {
	if expr == nil {
		return "", nonLiteral
	}
	value, err := optionalStringLiteral(expr)
	if err == nil {
		return value, nonLiteral
	}
	return "", append(nonLiteral, name)
}

func candidateStringSlice(expr ast.Expr, name string, stringSlices map[string][]string, nonLiteral []string) []string {
	if expr == nil {
		return nonLiteral
	}
	if _, err := stringSlice(expr, stringSlices); err == nil {
		return nonLiteral
	}
	return append(nonLiteral, name)
}

func assertionFieldSet() map[string]struct{} {
	fields := map[string]struct{}{}
	for _, field := range manifestAssertionFields {
		fields[field] = struct{}{}
	}
	fields["PostgresOracle"] = struct{}{}
	return fields
}

func hasAnyField(fields map[string]ast.Expr, fieldSet map[string]struct{}) bool {
	for field := range fieldSet {
		if _, ok := fields[field]; ok {
			return true
		}
	}
	return false
}

func expectationKind(fields map[string]ast.Expr) string {
	switch {
	case fields["ExpectedErr"] != nil:
		return "error"
	case fields["Expected"] != nil:
		return "rows"
	case fields["ExpectedRaw"] != nil:
		return "rawRows"
	case fields["ExpectedTag"] != nil:
		return "tag"
	case fields["ExpectedColNames"] != nil || fields["ExpectedColTypes"] != nil:
		return "columns"
	case fields["ExpectedNotices"] != nil:
		return "notices"
	default:
		return "unknown"
	}
}

func setupStatementFromAssertion(fields map[string]ast.Expr) (oracleStatement, bool) {
	if expectationKind(fields) == "error" || fields["CopyFromStdInFile"] != nil {
		return oracleStatement{}, false
	}
	query, err := optionalStringLiteral(fields["Query"])
	if err != nil || query == "" {
		return oracleStatement{}, false
	}
	statement := oracleStatement{Query: query}
	if bindVarsExpr := fields["BindVars"]; bindVarsExpr != nil {
		bindVars, err := bindVarsFromExpr(bindVarsExpr)
		if err != nil {
			return oracleStatement{}, false
		}
		statement.BindVars = bindVars
	}
	return statement, true
}

func appendPriorSetupNonLiteral(nonLiteral []string, fields map[string]ast.Expr) []string {
	if expectationKind(fields) == "error" {
		return nonLiteral
	}
	if query, err := optionalStringLiteral(fields["Query"]); err != nil {
		nonLiteral = append(nonLiteral, "PriorQuery")
	} else if hasDoltSpecificSQL(query) {
		nonLiteral = appendNonLiteral(nonLiteral, "PriorDoltSpecific")
	}
	if fields["Username"] != nil {
		nonLiteral = append(nonLiteral, "PriorUsername")
	}
	if fields["BindVars"] != nil {
		if _, err := bindVarsFromExpr(fields["BindVars"]); err != nil {
			nonLiteral = append(nonLiteral, "PriorBindVars")
		}
	}
	if fields["CopyFromStdInFile"] != nil {
		nonLiteral = append(nonLiteral, "PriorCopyFromStdInFile")
	}
	return nonLiteral
}

func appendNonLiteral(nonLiteral []string, value string) []string {
	for _, existing := range nonLiteral {
		if existing == value {
			return nonLiteral
		}
	}
	return append(nonLiteral, value)
}

func suggestedOracleID(source string, ordinal int, query string) string {
	source = strings.TrimPrefix(source, "testing/go/")
	source = strings.TrimSuffix(source, "_test.go")
	source = strings.ReplaceAll(source, ".go:", "-")
	source = strings.ReplaceAll(source, ":", "-")
	source = strings.ReplaceAll(source, "_", "-")
	source = strings.ToLower(source)
	queryWords := oracleIDWords(query)
	if queryWords != "" {
		return fmt.Sprintf("%s-%04d-%s", source, ordinal, queryWords)
	}
	return fmt.Sprintf("%s-%04d", source, ordinal)
}

func oracleIDWords(query string) string {
	query = strings.ToLower(query)
	replacer := strings.NewReplacer("\n", " ", "\t", " ", "(", " ", ")", " ", ",", " ", ";", " ", "'", " ", `"`, " ")
	query = replacer.Replace(query)
	parts := strings.Fields(query)
	kept := make([]string, 0, 5)
	for _, part := range parts {
		part = strings.Trim(part, "_-")
		if part == "" || len(part) > 40 {
			continue
		}
		kept = append(kept, part)
		if len(kept) == 5 {
			break
		}
	}
	return strings.Join(kept, "-")
}

func entriesFromScriptTestSlice(source string, ordinal *int, lit *ast.CompositeLit, stringSlices map[string][]string, migrationOverrides map[string]oracleMeta, scriptTestHelpers map[string]*ast.CompositeLit) ([]entry, error) {
	entries := make([]entry, 0)
	for _, element := range lit.Elts {
		scriptLit, _, ok := scriptTestLiteralForElement(element, scriptTestHelpers)
		if !ok {
			continue
		}
		generated, err := entriesFromScriptTest(source, ordinal, scriptLit, stringSlices, migrationOverrides)
		if err != nil {
			return nil, err
		}
		entries = append(entries, generated...)
	}
	return entries, nil
}

func entriesFromScriptTest(source string, ordinal *int, scriptLit *ast.CompositeLit, stringSlices map[string][]string, migrationOverrides map[string]oracleMeta) ([]entry, error) {
	fields := compositeFields(scriptLit)
	assertionsLit, ok := fields["Assertions"].(*ast.CompositeLit)
	if !ok {
		return nil, nil
	}

	fieldSet := assertionFieldSet()
	type mappedAssertion struct {
		lit     *ast.CompositeLit
		ordinal int
		setup   []oracleStatement
		meta    oracleMeta
	}
	assertionLits := make([]mappedAssertion, 0)
	priorSetup := make([]oracleStatement, 0)
	for _, assertionExpr := range assertionsLit.Elts {
		assertionLit, ok := assertionExpr.(*ast.CompositeLit)
		if !ok {
			continue
		}
		assertionFields := compositeFields(assertionLit)
		key := ""
		assertionOrdinal := 0
		if hasAnyField(assertionFields, fieldSet) {
			*ordinal = *ordinal + 1
			assertionOrdinal = *ordinal
			key = fmt.Sprintf("%s#%04d", source, assertionOrdinal)
		}
		meta := oracleMeta{}
		if key != "" {
			if mapped, ok := migrationOverrides[key]; ok {
				meta = mapped
			}
		}
		if meta.ID == "" {
			metaExpr, ok := assertionFields["PostgresOracle"]
			if !ok {
				if statement, ok := setupStatementFromAssertion(assertionFields); ok {
					priorSetup = append(priorSetup, statement)
				}
				continue
			}
			parsed, err := parseOracleMeta(metaExpr, stringSlices)
			if err != nil {
				return nil, fmt.Errorf("PostgresOracle: %w", err)
			}
			meta = parsed
		}
		if meta.ID != "" {
			assertionLits = append(assertionLits, mappedAssertion{lit: assertionLit, ordinal: assertionOrdinal, setup: append([]oracleStatement(nil), priorSetup...), meta: meta})
		}
		if statement, ok := setupStatementFromAssertion(assertionFields); ok && !oracleMetaExpectsError(meta) {
			priorSetup = append(priorSetup, statement)
		}
	}
	if len(assertionLits) == 0 {
		return nil, nil
	}

	setup, err := stringSlice(fields["SetUpScript"], stringSlices)
	if err != nil {
		return nil, fmt.Errorf("SetUpScript: %w", err)
	}

	entries := make([]entry, 0, len(assertionLits))
	for _, assertion := range assertionLits {
		assertionSetup := statementsFromQueries(setup)
		assertionSetup = append(assertionSetup, assertion.setup...)
		generated, ok, err := entryFromScriptTestAssertion(source, assertionSetup, assertion.ordinal, assertion.lit, assertion.meta)
		if err != nil {
			return nil, err
		}
		if ok {
			entries = append(entries, generated)
		}
	}
	return entries, nil
}

func oracleMetaExpectsError(meta oracleMeta) bool {
	return meta.ExpectedSQLState != "" || meta.Compare == "sqlstate"
}

func entryFromScriptTestAssertion(source string, setup []oracleStatement, ordinal int, assertionLit *ast.CompositeLit, meta oracleMeta) (entry, bool, error) {
	fields := compositeFields(assertionLit)
	if meta.ID == "" {
		return entry{}, false, nil
	}
	query, err := stringLiteral(fields["Query"])
	if err != nil {
		return entry{}, false, fmt.Errorf("%s Query: %w", meta.ID, err)
	}
	bindVars, err := bindVarsFromExpr(fields["BindVars"])
	if err != nil {
		return entry{}, false, fmt.Errorf("%s BindVars: %w", meta.ID, err)
	}

	generatedSetup := append([]oracleStatement(nil), setup...)
	if username, err := optionalStringLiteral(fields["Username"]); err != nil {
		return entry{}, false, fmt.Errorf("%s Username: %w", meta.ID, err)
	} else if username != "" {
		generatedSetup = append(generatedSetup, oracleStatement{Query: "SET ROLE " + quoteIdentifier(username)})
	}
	generatedCleanup := append([]string(nil), meta.Cleanup...)
	if (len(generatedSetup) > 0 || queryNeedsGeneratedCleanup(query)) && len(generatedCleanup) == 0 {
		if len(generatedSetup) > 0 {
			generatedSetup = append([]oracleStatement{
				{Query: "CREATE SCHEMA {{quotedSchema}}"},
				{Query: "SET search_path TO {{quotedSchema}}, public, pg_catalog"},
			}, generatedSetup...)
			generatedSetup = rewriteAutoIsolatedSetupSchemaReferences(generatedSetup)
		}
		setupQueries := statementQueries(generatedSetup)
		if setupNeedsPlpgsql(setupQueries) {
			generatedSetup = append([]oracleStatement{{Query: "CREATE EXTENSION IF NOT EXISTS plpgsql"}}, generatedSetup...)
			setupQueries = statementQueries(generatedSetup)
		}
		cleanupStatements := append(append([]string{}, setupQueries...), query)
		if setupSetsRole(setupQueries) {
			generatedCleanup = append(generatedCleanup, "RESET ROLE")
		}
		if len(generatedSetup) == 0 {
			generatedCleanup = append(generatedCleanup, cleanupForCreatedTables([]string{query})...)
		}
		generatedCleanup = append(generatedCleanup, cleanupForCreatedForeignDataWrappers(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedSubscriptions(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedPublications(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedExtensions(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedLargeObjects(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedSchemas(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedLanguages(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedDatabases(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedTypes(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedUsers(cleanupStatements)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedRoles(cleanupStatements)...)
		if len(generatedSetup) > 0 {
			generatedCleanup = append(generatedCleanup, "DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE")
		}
	}

	generated := entry{
		ID:                    meta.ID,
		Source:                source,
		Ordinal:               ordinal,
		Oracle:                "postgres",
		Compare:               meta.Compare,
		Query:                 query,
		BindVars:              bindVars,
		ExpectedSQLState:      meta.ExpectedSQLState,
		ExpectedErrorSeverity: meta.ExpectedErrorSeverity,
		ExpectedTag:           meta.ExpectedTag,
		ColumnModes:           meta.ColumnModes,
		Cleanup:               generatedCleanup,
	}
	if statementsHaveBindVars(generatedSetup) {
		generated.SetupStatements = generatedSetup
	} else {
		generated.Setup = statementQueries(generatedSetup)
	}
	if generated.Compare == "" {
		generated.Compare = "structural"
	}
	if generated.Compare == "sqlstate" || generated.ExpectedSQLState != "" {
		return generated, true, nil
	}
	if generated.Compare == "tag" || generated.ExpectedTag != nil {
		return generated, true, nil
	}

	expectedRows := meta.ExpectedRows
	if expectedRows == nil {
		var err error
		expectedRows, err = expectedRowsFromExpr(fields["Expected"])
		if err != nil {
			return entry{}, false, fmt.Errorf("%s Expected: %w", meta.ID, err)
		}
	}
	generated.ExpectedRows = expectedRows
	return generated, true, nil
}

func setupSetsRole(statements []string) bool {
	for _, statement := range statements {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(statement)), "set role ") {
			return true
		}
	}
	return false
}

func setupNeedsPlpgsql(statements []string) bool {
	for _, statement := range statements {
		normalized := strings.ToLower(strings.TrimSpace(statement))
		if strings.Contains(normalized, "language plpgsql") || strings.HasPrefix(normalized, "do ") {
			return true
		}
	}
	return false
}

func queryNeedsGeneratedCleanup(query string) bool {
	return len(cleanupForCreatedTables([]string{query})) > 0 ||
		len(cleanupForCreatedForeignDataWrappers([]string{query})) > 0 ||
		len(cleanupForCreatedTypes([]string{query})) > 0 ||
		len(cleanupForCreatedDatabases([]string{query})) > 0 ||
		len(cleanupForCreatedSchemas([]string{query})) > 0 ||
		len(cleanupForCreatedLargeObjects([]string{query})) > 0 ||
		len(cleanupForCreatedExtensions([]string{query})) > 0 ||
		len(cleanupForCreatedLanguages([]string{query})) > 0 ||
		len(cleanupForCreatedPublications([]string{query})) > 0 ||
		len(cleanupForCreatedSubscriptions([]string{query})) > 0 ||
		len(cleanupForCreatedUsers([]string{query})) > 0 ||
		len(cleanupForCreatedRoles([]string{query})) > 0
}

func rewriteAutoIsolatedSetupSchemaReferences(statements []oracleStatement) []oracleStatement {
	rewritten := make([]oracleStatement, len(statements))
	for i, statement := range statements {
		statement.Query = replaceCaseInsensitive(statement.Query, "ON SCHEMA public TO", "ON SCHEMA {{quotedSchema}} TO")
		statement.Query = replaceCaseInsensitive(statement.Query, "ON SCHEMA \"public\" TO", "ON SCHEMA {{quotedSchema}} TO")
		rewritten[i] = statement
	}
	return rewritten
}

func replaceCaseInsensitive(statement string, old string, replacement string) string {
	index := strings.Index(strings.ToLower(statement), strings.ToLower(old))
	if index < 0 {
		return statement
	}
	return statement[:index] + replacement + statement[index+len(old):]
}

func cleanupForCreatedSubscriptions(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create subscription ", "DROP SUBSCRIPTION IF EXISTS ")
}

func cleanupForCreatedPublications(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create publication ", "DROP PUBLICATION IF EXISTS ")
}

func cleanupForCreatedExtensions(statements []string) []string {
	return cleanupForCreatedObjectsFiltered(statements, "create extension ", "DROP EXTENSION IF EXISTS ", " CASCADE", map[string]struct{}{"plpgsql": {}})
}

func cleanupForCreatedTables(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create table ", "DROP TABLE IF EXISTS ", " CASCADE")
}

func cleanupForCreatedForeignDataWrappers(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create foreign data wrapper ", "DROP FOREIGN DATA WRAPPER IF EXISTS ", " CASCADE")
}

func cleanupForCreatedLargeObjects(statements []string) []string {
	seen := map[string]struct{}{}
	cleanup := make([]string, 0)
	for _, statement := range statements {
		oid, ok := createdLargeObjectOID(statement)
		if !ok {
			continue
		}
		if _, ok := seen[oid]; ok {
			continue
		}
		seen[oid] = struct{}{}
		cleanup = append(cleanup, "SELECT pg_catalog.lo_unlink("+oid+") WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_largeobject_metadata WHERE oid = "+oid+")")
	}
	return cleanup
}

func createdLargeObjectOID(statement string) (string, bool) {
	lower := strings.ToLower(statement)
	index := strings.Index(lower, "lo_create(")
	if index < 0 {
		return "", false
	}
	rest := statement[index+len("lo_create("):]
	end := strings.Index(rest, ")")
	if end < 0 {
		return "", false
	}
	oid := strings.TrimSpace(rest[:end])
	if oid == "" {
		return "", false
	}
	for _, r := range oid {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	return oid, true
}

func cleanupForCreatedDatabases(statements []string) []string {
	return cleanupForCreatedDatabaseObjects(statements)
}

func cleanupForCreatedUsers(statements []string) []string {
	return cleanupForCreatedRoleObjects(statements, "create user ")
}

func cleanupForCreatedRoles(statements []string) []string {
	return cleanupForCreatedRoleObjects(statements, "create role ")
}

func cleanupForCreatedRoleObjects(statements []string, prefix string) []string {
	seen := map[string]struct{}{}
	cleanup := make([]string, 0)
	for _, statement := range statements {
		name, ok := createdObjectName(statement, prefix)
		if !ok {
			continue
		}
		if strings.Contains(name, "{{") || strings.Contains(name, "}}") {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cleanup = append(cleanup, "CREATE EXTENSION IF NOT EXISTS plpgsql", dropOwnedByIfRoleExists(name), "DROP ROLE IF EXISTS "+name)
	}
	return cleanup
}

func cleanupForCreatedDatabaseObjects(statements []string) []string {
	seen := map[string]struct{}{}
	cleanup := make([]string, 0)
	for _, statement := range statements {
		name, ok := createdObjectName(statement, "create database ")
		if !ok {
			continue
		}
		if strings.Contains(name, "{{") || strings.Contains(name, "}}") {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cleanup = append(cleanup, "CREATE EXTENSION IF NOT EXISTS plpgsql", alterDatabaseTemplateFalseIfExists(name), "DROP DATABASE IF EXISTS "+name)
	}
	return cleanup
}

func alterDatabaseTemplateFalseIfExists(name string) string {
	dbName := unquoteSQLName(name)
	dbLiteral := quoteSQLString(dbName)
	return "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = " + dbLiteral + ") THEN EXECUTE 'ALTER DATABASE ' || quote_ident(" + dbLiteral + ") || ' IS_TEMPLATE false'; END IF; END $$"
}

func dropOwnedByIfRoleExists(name string) string {
	roleName := unquoteSQLName(name)
	roleLiteral := quoteSQLString(roleName)
	return "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = " + roleLiteral + ") THEN EXECUTE 'DROP OWNED BY ' || quote_ident(" + roleLiteral + "); END IF; END $$"
}

func cleanupForCreatedLanguages(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create language ", "DROP LANGUAGE IF EXISTS ", " CASCADE")
}

func cleanupForCreatedTypes(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create type ", "DROP TYPE IF EXISTS ", " CASCADE")
}

func cleanupForCreatedObjects(statements []string, prefix string, dropPrefix string, dropSuffixes ...string) []string {
	dropSuffix := ""
	if len(dropSuffixes) > 0 {
		dropSuffix = dropSuffixes[0]
	}
	return cleanupForCreatedObjectsFiltered(statements, prefix, dropPrefix, dropSuffix, nil)
}

func cleanupForCreatedObjectsFiltered(statements []string, prefix string, dropPrefix string, dropSuffix string, excluded map[string]struct{}) []string {
	seen := map[string]struct{}{}
	cleanup := make([]string, 0)
	for _, statement := range statements {
		name, ok := createdObjectName(statement, prefix)
		if !ok {
			continue
		}
		if strings.Contains(name, "{{") || strings.Contains(name, "}}") {
			continue
		}
		if _, ok := excluded[strings.ToLower(unquoteSQLName(name))]; ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cleanup = append(cleanup, dropPrefix+name+dropSuffix)
	}
	return cleanup
}

func createdObjectName(statement string, prefix string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, prefix) {
		return "", false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	lowerRest := strings.ToLower(rest)
	const ifNotExists = "if not exists "
	if strings.HasPrefix(lowerRest, ifNotExists) {
		rest = strings.TrimSpace(rest[len(ifNotExists):])
	}
	return firstSQLName(rest)
}

func cleanupForCreatedSchemas(statements []string) []string {
	seen := map[string]struct{}{}
	cleanup := make([]string, 0)
	for _, statement := range statements {
		schema, ok := createdSchemaName(statement)
		if !ok {
			continue
		}
		if strings.Contains(schema, "{{") || strings.Contains(schema, "}}") {
			continue
		}
		unquoted := strings.ToLower(strings.Trim(schema, `"`))
		if unquoted == "public" || unquoted == "pg_catalog" || unquoted == "information_schema" {
			continue
		}
		if _, ok := seen[schema]; ok {
			continue
		}
		seen[schema] = struct{}{}
		cleanup = append(cleanup, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	}
	return cleanup
}

func createdSchemaName(statement string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	lower := strings.ToLower(trimmed)
	const prefix = "create schema "
	if !strings.HasPrefix(lower, prefix) {
		return "", false
	}
	rest := strings.TrimSpace(trimmed[len(prefix):])
	lowerRest := strings.ToLower(rest)
	const ifNotExists = "if not exists "
	if strings.HasPrefix(lowerRest, ifNotExists) {
		rest = strings.TrimSpace(rest[len(ifNotExists):])
	}
	return firstSQLName(rest)
}

func firstSQLName(rest string) (string, bool) {
	if rest == "" {
		return "", false
	}
	if rest[0] == '"' {
		for i := 1; i < len(rest); i++ {
			if rest[i] != '"' {
				continue
			}
			if i+1 < len(rest) && rest[i+1] == '"' {
				i++
				continue
			}
			return rest[:i+1], true
		}
		return "", false
	}
	end := len(rest)
	for i, r := range rest {
		if r == ';' || r == '(' || r == '\t' || r == '\n' || r == '\r' || r == ' ' {
			end = i
			break
		}
	}
	schema := strings.TrimSpace(rest[:end])
	if schema == "" {
		return "", false
	}
	return schema, true
}

func parseOracleMeta(expr ast.Expr, stringSlices map[string][]string) (oracleMeta, error) {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return oracleMeta{}, fmt.Errorf("must be a ScriptTestPostgresOracle literal")
	}
	fields := compositeFields(lit)
	meta := oracleMeta{}
	var err error
	if meta.ID, err = stringLiteral(fields["ID"]); err != nil {
		return oracleMeta{}, fmt.Errorf("ID: %w", err)
	}
	if meta.Compare, err = optionalStringLiteral(fields["Compare"]); err != nil {
		return oracleMeta{}, fmt.Errorf("Compare: %w", err)
	}
	if meta.ColumnModes, err = stringSlice(fields["ColumnModes"], stringSlices); err != nil {
		return oracleMeta{}, fmt.Errorf("ColumnModes: %w", err)
	}
	if meta.ExpectedSQLState, err = optionalStringLiteral(fields["ExpectedSQLState"]); err != nil {
		return oracleMeta{}, fmt.Errorf("ExpectedSQLState: %w", err)
	}
	if meta.ExpectedErrorSeverity, err = optionalStringLiteral(fields["ExpectedErrorSeverity"]); err != nil {
		return oracleMeta{}, fmt.Errorf("ExpectedErrorSeverity: %w", err)
	}
	if meta.Cleanup, err = stringSlice(fields["Cleanup"], stringSlices); err != nil {
		return oracleMeta{}, fmt.Errorf("Cleanup: %w", err)
	}
	return meta, nil
}

func marshalJSON(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func compositeFields(lit *ast.CompositeLit) map[string]ast.Expr {
	fields := map[string]ast.Expr{}
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok {
			continue
		}
		fields[key.Name] = kv.Value
	}
	return fields
}

func isScriptTestSliceType(expr ast.Expr) bool {
	array, ok := expr.(*ast.ArrayType)
	if !ok {
		return false
	}
	return isScriptTestType(array.Elt)
}

func isScriptTestType(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "ScriptTest"
}

func isScriptTestLiteral(lit *ast.CompositeLit) bool {
	return lit != nil && isScriptTestType(lit.Type)
}

func packageScriptTestSlices(parsed *ast.File) map[string]*ast.CompositeLit {
	slices := map[string]*ast.CompositeLit{}
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					continue
				}
				lit, ok := valueSpec.Values[i].(*ast.CompositeLit)
				if !ok || !isScriptTestSliceType(lit.Type) {
					continue
				}
				slices[name.Name] = lit
			}
		}
	}
	return slices
}

func scriptTestHelperReturns(parsed *ast.File) map[string]*ast.CompositeLit {
	helpers := map[string]*ast.CompositeLit{}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !returnsSingleScriptTest(fn.Type) {
			continue
		}
		var returned *ast.CompositeLit
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if returned != nil {
				return false
			}
			ret, ok := node.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 1 {
				return true
			}
			lit, ok := ret.Results[0].(*ast.CompositeLit)
			if !ok || !isScriptTestLiteral(lit) {
				return true
			}
			returned = lit
			return false
		})
		if returned != nil {
			helpers[fn.Name.Name] = returned
		}
	}
	return helpers
}

func returnsSingleScriptTest(fnType *ast.FuncType) bool {
	if fnType == nil || fnType.Results == nil || len(fnType.Results.List) != 1 {
		return false
	}
	return isScriptTestType(fnType.Results.List[0].Type)
}

func scriptTestLiteralForElement(expr ast.Expr, scriptTestHelpers map[string]*ast.CompositeLit) (*ast.CompositeLit, bool, bool) {
	if lit, ok := expr.(*ast.CompositeLit); ok {
		return lit, false, true
	}
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return nil, false, false
	}
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return nil, false, false
	}
	lit, ok := scriptTestHelpers[ident.Name]
	if !ok {
		return nil, false, false
	}
	return lit, true, true
}

func packageStringSlices(parsed *ast.File) map[string][]string {
	slices := map[string][]string{}
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					continue
				}
				values, err := stringSlice(valueSpec.Values[i], nil)
				if err == nil {
					slices[name.Name] = values
				}
			}
		}
	}
	return slices
}

func mergeStringSlices(base map[string][]string, overlay map[string][]string) map[string][]string {
	if len(base) == 0 {
		return overlay
	}
	merged := make(map[string][]string, len(base)+len(overlay))
	for name, values := range base {
		merged[name] = values
	}
	for name, values := range overlay {
		merged[name] = values
	}
	return merged
}

func packageScriptTestSliceForRunScripts(call *ast.CallExpr, packageScriptTests map[string]*ast.CompositeLit) (*ast.CompositeLit, bool) {
	if len(call.Args) < 2 || !isIdentCall(call, "RunScripts") {
		return nil, false
	}
	ident, ok := call.Args[1].(*ast.Ident)
	if !ok {
		return nil, false
	}
	lit, ok := packageScriptTests[ident.Name]
	return lit, ok
}

func isIdentCall(call *ast.CallExpr, name string) bool {
	ident, ok := call.Fun.(*ast.Ident)
	return ok && ident.Name == name
}

func localStringSlices(body *ast.BlockStmt) map[string][]string {
	locals := map[string][]string{}
	ast.Inspect(body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, lhs := range assign.Lhs {
			if i >= len(assign.Rhs) {
				continue
			}
			ident, ok := lhs.(*ast.Ident)
			if !ok {
				continue
			}
			values, err := stringSlice(assign.Rhs[i], nil)
			if err == nil {
				locals[ident.Name] = values
			}
		}
		return true
	})
	return locals
}

func expectedRowsFromExpr(expr ast.Expr) (*[][]cell, error) {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("must be a []sql.Row literal")
	}
	parsedRows := make([][]cell, 0, len(lit.Elts))
	for _, rowExpr := range lit.Elts {
		rowLit, ok := rowExpr.(*ast.CompositeLit)
		if !ok {
			return nil, fmt.Errorf("row must be a sql.Row literal")
		}
		parsedRow := make([]cell, 0, len(rowLit.Elts))
		for _, cellExpr := range rowLit.Elts {
			parsedCell, err := cellFromExpr(cellExpr)
			if err != nil {
				return nil, err
			}
			parsedRow = append(parsedRow, parsedCell)
		}
		parsedRows = append(parsedRows, parsedRow)
	}
	return rows(parsedRows...), nil
}

func bindVarsFromExpr(expr ast.Expr) ([]oracleBindVar, error) {
	if expr == nil {
		return nil, nil
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("must be a bind var slice literal")
	}
	bindVars := make([]oracleBindVar, 0, len(lit.Elts))
	for _, element := range lit.Elts {
		bindVar, err := bindVarFromExpr(element)
		if err != nil {
			return nil, err
		}
		bindVars = append(bindVars, bindVar)
	}
	return bindVars, nil
}

func bindVarFromExpr(expr ast.Expr) (oracleBindVar, error) {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		switch typed.Kind {
		case token.STRING:
			value, err := strconv.Unquote(typed.Value)
			if err != nil {
				return oracleBindVar{}, err
			}
			return oracleBindVar{Kind: "string", Value: value}, nil
		case token.INT:
			return oracleBindVar{Kind: "int", Value: typed.Value}, nil
		case token.FLOAT:
			return oracleBindVar{Kind: "float", Value: typed.Value}, nil
		default:
			return oracleBindVar{}, fmt.Errorf("unsupported bind var literal %s", typed.Value)
		}
	case *ast.Ident:
		switch typed.Name {
		case "nil":
			return oracleBindVar{Kind: "null", Null: true}, nil
		case "true", "false":
			return oracleBindVar{Kind: "bool", Value: typed.Name}, nil
		default:
			return oracleBindVar{}, fmt.Errorf("unsupported bind var identifier %s", typed.Name)
		}
	case *ast.UnaryExpr:
		if typed.Op != token.SUB {
			return oracleBindVar{}, fmt.Errorf("unsupported bind var unary operator %s", typed.Op)
		}
		value, err := bindVarFromExpr(typed.X)
		if err != nil {
			return oracleBindVar{}, err
		}
		switch value.Kind {
		case "int", "float":
			value.Value = "-" + value.Value
			return value, nil
		default:
			return oracleBindVar{}, fmt.Errorf("unsupported negative bind var kind %s", value.Kind)
		}
	case *ast.CallExpr:
		return bindVarFromCallExpr(typed)
	case *ast.CompositeLit:
		return bindVarFromCompositeLit(typed)
	default:
		return oracleBindVar{}, fmt.Errorf("unsupported bind var expression %T", expr)
	}
}

func bindVarFromCallExpr(call *ast.CallExpr) (oracleBindVar, error) {
	name := callName(call.Fun)
	if len(call.Args) != 1 {
		return oracleBindVar{}, fmt.Errorf("unsupported bind var call %s", name)
	}
	switch name {
	case "Date", "Timestamp", "UUID", "Numeric":
		value, err := stringLiteral(call.Args[0])
		if err != nil {
			return oracleBindVar{}, fmt.Errorf("%s: %w", name, err)
		}
		return oracleBindVar{Kind: strings.ToLower(name), Value: value}, nil
	case "int", "int8", "int16", "int32", "int64":
		value, err := bindVarFromExpr(call.Args[0])
		if err != nil {
			return oracleBindVar{}, err
		}
		if value.Kind != "int" {
			return oracleBindVar{}, fmt.Errorf("unsupported %s bind var argument kind %s", name, value.Kind)
		}
		return value, nil
	case "float32", "float64":
		value, err := bindVarFromExpr(call.Args[0])
		if err != nil {
			return oracleBindVar{}, err
		}
		if value.Kind != "float" && value.Kind != "int" {
			return oracleBindVar{}, fmt.Errorf("unsupported %s bind var argument kind %s", name, value.Kind)
		}
		value.Kind = "float"
		return value, nil
	default:
		return oracleBindVar{}, fmt.Errorf("unsupported bind var call %s", name)
	}
}

func bindVarFromCompositeLit(lit *ast.CompositeLit) (oracleBindVar, error) {
	switch arrayElementName(lit.Type) {
	case "byte":
		bytes := make([]byte, 0, len(lit.Elts))
		for _, element := range lit.Elts {
			basic, ok := element.(*ast.BasicLit)
			if !ok || basic.Kind != token.INT {
				return oracleBindVar{}, fmt.Errorf("byte bind var element must be an integer literal")
			}
			value, err := strconv.ParseUint(basic.Value, 0, 8)
			if err != nil {
				return oracleBindVar{}, err
			}
			bytes = append(bytes, byte(value))
		}
		return oracleBindVar{Kind: "bytes", Value: hex.EncodeToString(bytes)}, nil
	case "string":
		values := make([]string, 0, len(lit.Elts))
		for _, element := range lit.Elts {
			value, err := stringLiteral(element)
			if err != nil {
				return oracleBindVar{}, err
			}
			values = append(values, value)
		}
		return oracleBindVar{Kind: "stringArray", Values: values}, nil
	default:
		return oracleBindVar{}, fmt.Errorf("unsupported bind var composite literal")
	}
}

func callName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		prefix := callName(typed.X)
		if prefix == "" {
			return typed.Sel.Name
		}
		return prefix + "." + typed.Sel.Name
	default:
		return ""
	}
}

func arrayElementName(expr ast.Expr) string {
	array, ok := expr.(*ast.ArrayType)
	if !ok {
		return ""
	}
	switch typed := array.Elt.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.SelectorExpr:
		return callName(typed)
	default:
		return ""
	}
}

func cellFromExpr(expr ast.Expr) (cell, error) {
	switch typed := expr.(type) {
	case *ast.BasicLit:
		switch typed.Kind {
		case token.STRING:
			v, err := strconv.Unquote(typed.Value)
			if err != nil {
				return cell{}, err
			}
			return value(v), nil
		case token.INT, token.FLOAT:
			return value(typed.Value), nil
		default:
			return cell{}, fmt.Errorf("unsupported literal %s", typed.Value)
		}
	case *ast.Ident:
		switch typed.Name {
		case "nil":
			return cell{Null: true}, nil
		case "true", "false":
			return value(typed.Name), nil
		default:
			return cell{}, fmt.Errorf("unsupported identifier %s", typed.Name)
		}
	case *ast.UnaryExpr:
		if typed.Op != token.SUB {
			return cell{}, fmt.Errorf("unsupported unary operator %s", typed.Op)
		}
		lit, ok := typed.X.(*ast.BasicLit)
		if !ok || (lit.Kind != token.INT && lit.Kind != token.FLOAT) {
			return cell{}, fmt.Errorf("unsupported negative literal")
		}
		return value("-" + lit.Value), nil
	default:
		return cell{}, fmt.Errorf("unsupported expected cell %T", expr)
	}
}

func stringSlice(expr ast.Expr, locals map[string][]string) ([]string, error) {
	if expr == nil {
		return nil, nil
	}
	if ident, ok := expr.(*ast.Ident); ok {
		if locals != nil {
			if values, ok := locals[ident.Name]; ok {
				return append([]string(nil), values...), nil
			}
		}
		return nil, fmt.Errorf("unknown string slice identifier %s", ident.Name)
	}
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return nil, fmt.Errorf("must be a string slice literal")
	}
	values := make([]string, 0, len(lit.Elts))
	for _, element := range lit.Elts {
		value, err := stringLiteral(element)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func stringLiteral(expr ast.Expr) (string, error) {
	if expr == nil {
		return "", fmt.Errorf("missing string literal")
	}
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", fmt.Errorf("must be a string literal")
	}
	return strconv.Unquote(lit.Value)
}

func optionalStringLiteral(expr ast.Expr) (string, error) {
	if expr == nil {
		return "", nil
	}
	return stringLiteral(expr)
}

func quoteIdentifier(identifier string) string {
	if isSimpleIdentifier(identifier) {
		return identifier
	}
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func unquoteSQLName(name string) string {
	if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
		return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	return strings.ToLower(name)
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func isSimpleIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for i, r := range identifier {
		if i == 0 {
			if (r < 'a' || r > 'z') && r != '_' {
				return false
			}
			continue
		}
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func rows(rs ...[]cell) *[][]cell {
	out := make([][]cell, 0, len(rs))
	out = append(out, rs...)
	return &out
}

func row(cells ...cell) []cell {
	return append([]cell(nil), cells...)
}

func value(v string) cell {
	return cell{Value: &v}
}

func regex(pattern string) cell {
	return cell{Regex: pattern}
}

func any() cell {
	return cell{Any: true}
}
