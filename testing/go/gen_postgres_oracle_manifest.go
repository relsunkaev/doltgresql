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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

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
	Query                 string            `json:"query"`
	ExpectedRows          *[][]cell         `json:"expectedRows,omitempty"`
	ExpectedSQLState      string            `json:"expectedSqlstate,omitempty"`
	ExpectedErrorSeverity string            `json:"expectedErrorSeverity,omitempty"`
	ColumnModes           []string          `json:"columnModes,omitempty"`
	Cleanup               []string          `json:"cleanup,omitempty"`
	Variables             map[string]string `json:"variables,omitempty"`
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
	Key             string    `json:"key"`
	Source          string    `json:"source"`
	Ordinal         int       `json:"ordinal"`
	ScriptName      string    `json:"scriptName,omitempty"`
	Oracle          string    `json:"oracle"`
	PostgresID      string    `json:"postgresId,omitempty"`
	SuggestedID     string    `json:"suggestedId"`
	Compare         string    `json:"compare,omitempty"`
	ColumnModes     []string  `json:"columnModes,omitempty"`
	ExpectedKind    string    `json:"expectedKind"`
	SQLState        string    `json:"sqlstate,omitempty"`
	ErrorSeverity   string    `json:"errorSeverity,omitempty"`
	Username        string    `json:"username,omitempty"`
	Query           string    `json:"query,omitempty"`
	QuerySHA256     string    `json:"querySha256,omitempty"`
	ExpectedRows    *[][]cell `json:"expectedRows,omitempty"`
	NonLiteral      []string  `json:"nonLiteral,omitempty"`
	Cleanup         []string  `json:"cleanup,omitempty"`
	NeedsCleanup    bool      `json:"needsCleanup"`
	CleanupProvided bool      `json:"cleanupProvided"`
}

const defaultPostgresOracleDSN = "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable"

func main() {
	stdout := flag.Bool("stdout", false, "write generated manifest to stdout instead of testdata/postgres_oracle_manifest.json")
	migrationCandidatesDir := flag.String("migration-candidates-dir", "", "write per-file ScriptTest oracle migration candidate maps to this directory")
	promoteOracleMap := flag.String("promote-oracle-map", "", "write a postgres oracle migration map for one ScriptTest source file")
	refreshOracleMap := flag.String("refresh-oracle-map", "", "promote one ScriptTest source file and refresh its cached expected rows from PostgreSQL")
	promoteOracleMapOutput := flag.String("promote-oracle-map-output", "", "output path for --promote-oracle-map; defaults to testdata/postgres_oracle_migrations/<source>.oracle-map.json")
	postgresDSN := flag.String("postgres-dsn", "", "PostgreSQL DSN for --refresh-oracle-map; defaults to DOLTGRES_POSTGRES_TEST_DSN, POSTGRES_TEST_DSN, or DOLTGRES_ORACLE default")
	flag.Parse()

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
		if err := refreshPromotedOracleMap(*refreshOracleMap, *promoteOracleMapOutput, dsn); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *promoteOracleMap != "" {
		if *stdout || *migrationCandidatesDir != "" {
			fmt.Fprintln(os.Stderr, "--promote-oracle-map cannot be combined with --stdout or --migration-candidates-dir")
			os.Exit(1)
		}
		if err := writePromotedOracleMap(*promoteOracleMap, *promoteOracleMapOutput); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *migrationCandidatesDir != "" {
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

	data, err := generateManifest()
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

func generateManifest() ([]byte, error) {
	migrationOverrides, err := loadMigrationOverrides()
	if err != nil {
		return nil, err
	}
	scriptEntries, err := annotatedScriptTestEntries(migrationOverrides)
	if err != nil {
		return nil, err
	}
	m := manifest{
		GeneratedBy:            "go generate ./testing/go",
		Version:                1,
		CanonicalPostgresMajor: 16,
		NormalizationProfile:   "pg16-structural-v1",
		DefaultOracle:          "internal",
		Inventory: inventory{
			Scope:                   "testing/go/*_test.go ScriptTest expectation literals",
			AssertionsDefaultOracle: "internal",
			PostgresOverrides:       "entries where oracle == postgres",
			AssertionFields:         manifestAssertionFields,
		},
		Entries: append(
			oracleSelftestEntries(),
			append(dropDefinitionEntries(), scriptEntries...)...,
		),
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

func oracleSelftestEntries() []entry {
	return []entry{
		{
			ID:                    "oracle-selftest-sqlstate-division-by-zero",
			Source:                "testing/go/postgres_oracle_manifest_test.go:TestPostgresOracleManifest",
			Oracle:                "postgres",
			Compare:               "sqlstate",
			Query:                 "SELECT 1 / 0",
			ExpectedSQLState:      "22012",
			ExpectedErrorSeverity: "ERROR",
		},
		{
			ID:      "oracle-selftest-normalization-regex-and-wildcard",
			Source:  "testing/go/postgres_oracle_manifest_test.go:TestPostgresOracleManifest",
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
		for _, assertion := range mapped.Assertions {
			if assertion.Oracle != "postgres" {
				continue
			}
			if assertion.Key == "" {
				return nil, fmt.Errorf("%s: postgres migration entry missing key", file)
			}
			if assertion.PostgresID == "" {
				return nil, fmt.Errorf("%s: %s postgres migration entry missing postgresId", file, assertion.Key)
			}
			if _, ok := overrides[assertion.Key]; ok {
				return nil, fmt.Errorf("%s: duplicate migration key %s", file, assertion.Key)
			}
			overrides[assertion.Key] = oracleMeta{
				ID:                    assertion.PostgresID,
				Compare:               assertion.Compare,
				ColumnModes:           assertion.ColumnModes,
				ExpectedRows:          assertion.ExpectedRows,
				ExpectedSQLState:      assertion.SQLState,
				ExpectedErrorSeverity: assertion.ErrorSeverity,
				Cleanup:               assertion.Cleanup,
			}
		}
	}
	return overrides, nil
}

func annotatedScriptTestEntries(migrationOverrides map[string]oracleMeta) ([]entry, error) {
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

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
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			stringSlices := localStringSlices(fn.Body)
			source := fmt.Sprintf("testing/go/%s:%s", file, fn.Name.Name)
			ordinal := 0
			var inspectErr error
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if inspectErr != nil {
					return false
				}
				lit, ok := node.(*ast.CompositeLit)
				if !ok || !isScriptTestSliceType(lit.Type) {
					return true
				}
				for _, element := range lit.Elts {
					scriptLit, ok := element.(*ast.CompositeLit)
					if !ok {
						continue
					}
					generated, err := entriesFromScriptTest(source, &ordinal, scriptLit, stringSlices, migrationOverrides)
					if err != nil {
						inspectErr = fmt.Errorf("%s: %w", source, err)
						return false
					}
					entries = append(entries, generated...)
				}
				return false
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
		candidates, err := migrationCandidatesForFile(file, migrationOverrides)
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

func writePromotedOracleMap(sourceFile string, outputPath string) error {
	sourceFile = strings.TrimPrefix(sourceFile, "testing/go/")
	if sourceFile == "" || strings.Contains(sourceFile, string(filepath.Separator)+".."+string(filepath.Separator)) || strings.HasPrefix(sourceFile, "..") {
		return fmt.Errorf("invalid source file %q", sourceFile)
	}
	if !strings.HasSuffix(sourceFile, "_test.go") {
		return fmt.Errorf("source file must be a *_test.go file: %s", sourceFile)
	}

	migrationOverrides, err := loadMigrationOverridesExcludingSource("testing/go/" + sourceFile)
	if err != nil {
		return err
	}
	candidates, err := migrationCandidatesForFile(sourceFile, migrationOverrides)
	if err != nil {
		return err
	}
	if len(candidates.Assertions) == 0 {
		return fmt.Errorf("%s has no ScriptTest expectation assertions", sourceFile)
	}
	candidates.GeneratedBy = "go run gen_postgres_oracle_manifest.go --promote-oracle-map " + sourceFile
	for i := range candidates.Assertions {
		assertion := &candidates.Assertions[i]
		if assertion.Query == "" || len(assertion.NonLiteral) > 0 {
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
			assertion.NeedsCleanup = false
		case "error":
			assertion.Oracle = "postgres"
			if assertion.PostgresID == "" {
				assertion.PostgresID = assertion.SuggestedID
			}
			assertion.Compare = "sqlstate"
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

func refreshPromotedOracleMap(sourceFile string, outputPath string, dsn string) error {
	sourceFile = strings.TrimPrefix(sourceFile, "testing/go/")
	if outputPath == "" {
		outputPath = filepath.Join("testdata", "postgres_oracle_migrations", strings.TrimSuffix(sourceFile, ".go")+".oracle-map.json")
	}
	if err := writePromotedOracleMap(sourceFile, outputPath); err != nil {
		return err
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		return err
	}
	var mapped migrationFile
	if err := json.Unmarshal(data, &mapped); err != nil {
		return err
	}
	assertionsByKey := make(map[string]*migrationAssertion, len(mapped.Assertions))
	for i := range mapped.Assertions {
		assertionsByKey[mapped.Assertions[i].Key] = &mapped.Assertions[i]
	}

	migrationOverrides, err := loadMigrationOverrides()
	if err != nil {
		return err
	}
	entries, err := annotatedScriptTestEntries(migrationOverrides)
	if err != nil {
		return err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	sourcePrefix := "testing/go/" + sourceFile + ":"
	for _, entry := range entries {
		if entry.Oracle != "postgres" || !strings.HasPrefix(entry.Source, sourcePrefix) || entry.Ordinal == 0 {
			continue
		}
		key := fmt.Sprintf("%s#%04d", entry.Source, entry.Ordinal)
		assertion := assertionsByKey[key]
		if assertion == nil {
			return fmt.Errorf("missing oracle map assertion for %s", key)
		}
		expectedRows, sqlstate, severity, err := readPostgresOracleExpected(ctx, conn, entry)
		if err != nil {
			return fmt.Errorf("%s: %w", entry.ID, err)
		}
		if sqlstate != "" {
			assertion.SQLState = sqlstate
			assertion.ErrorSeverity = severity
			assertion.ExpectedRows = nil
			continue
		}
		assertion.ExpectedRows = expectedRows
	}

	refreshed, err := marshalJSON(mapped)
	if err != nil {
		return err
	}
	return os.WriteFile(outputPath, refreshed, 0644)
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

func readPostgresOracleExpected(ctx context.Context, conn *pgx.Conn, entry entry) (*[][]cell, string, string, error) {
	variables := oracleVariables(entry)
	if err := resetOracleSession(ctx, conn); err != nil {
		return nil, "", "", err
	}
	if err := runOracleStatements(ctx, conn, variables, entry.Cleanup); err != nil {
		return nil, "", "", err
	}
	defer func() {
		_ = runOracleStatements(ctx, conn, variables, entry.Cleanup)
		_ = resetOracleSession(ctx, conn)
	}()
	if err := resetOracleSession(ctx, conn); err != nil {
		return nil, "", "", err
	}
	if err := runOracleStatements(ctx, conn, variables, entry.Setup); err != nil {
		return nil, "", "", err
	}

	query := expandOracleVariables(entry.Query, variables)
	if entry.Compare == "sqlstate" || entry.ExpectedSQLState != "" {
		_, err := conn.Exec(ctx, query)
		if err == nil {
			return nil, "", "", fmt.Errorf("expected SQLSTATE %s but query succeeded", entry.ExpectedSQLState)
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			return nil, "", "", err
		}
		return nil, pgErr.Code, pgErr.Severity, nil
	}

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, "", "", err
	}
	defer rows.Close()

	expected := make([][]cell, 0)
	fields := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, "", "", err
		}
		row := make([]cell, 0, len(values))
		for i, rowValue := range values {
			if rowValue == nil {
				row = append(row, cell{Null: true})
				continue
			}
			row = append(row, value(normalizeGeneratedPostgresValue(entry, i, rowValue, fields[i].DataTypeOID)))
		}
		expected = append(expected, row)
	}
	if err := rows.Err(); err != nil {
		return nil, "", "", err
	}
	return &expected, "", "", nil
}

func normalizeGeneratedPostgresValue(entry entry, index int, value interface{}, oid uint32) string {
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
	case []byte:
		switch mode {
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
		return text
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
	trimmed = strings.ReplaceAll(trimmed, ", ", ",")
	return trimmed
}

func normalizeGeneratedPostgresSlice(value interface{}) (string, bool) {
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

func runOracleStatements(ctx context.Context, conn *pgx.Conn, variables map[string]string, statements []string) error {
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, expandOracleVariables(statement, variables)); err != nil {
			return fmt.Errorf("oracle statement failed: %s: %w", statement, err)
		}
	}
	return nil
}

func resetOracleSession(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, "RESET ROLE"); err != nil {
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

func migrationCandidatesForFile(file string, migrationOverrides map[string]oracleMeta) (migrationFile, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		return migrationFile{}, err
	}
	candidates := migrationFile{
		GeneratedBy:       "go run gen_postgres_oracle_manifest.go --migration-candidates-dir",
		SourceFile:        "testing/go/" + file,
		DefaultOracle:     "internal",
		AssertionKeyStyle: "testing/go/<file>:<TestName>#<expectation-ordinal>",
		AssertionFields:   manifestAssertionFields,
	}
	fieldSet := assertionFieldSet()
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
			continue
		}
		stringSlices := localStringSlices(fn.Body)
		source := fmt.Sprintf("testing/go/%s:%s", file, fn.Name.Name)
		ordinal := 0
		var inspectErr error
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if inspectErr != nil {
				return false
			}
			lit, ok := node.(*ast.CompositeLit)
			if !ok || !isScriptTestSliceType(lit.Type) {
				return true
			}
			for _, element := range lit.Elts {
				scriptLit, ok := element.(*ast.CompositeLit)
				if !ok {
					continue
				}
				scriptFields := compositeFields(scriptLit)
				scriptName, _ := optionalStringLiteral(scriptFields["Name"])
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
					if !hasAnyField(assertionFields, fieldSet) {
						continue
					}
					ordinal++
					candidate, err := migrationCandidate(source, ordinal, scriptName, assertionFields, stringSlices, migrationOverrides)
					if err != nil {
						inspectErr = fmt.Errorf("%s#%04d: %w", source, ordinal, err)
						return false
					}
					candidates.Assertions = append(candidates.Assertions, candidate)
				}
			}
			return false
		})
		if inspectErr != nil {
			return migrationFile{}, inspectErr
		}
	}
	return candidates, nil
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
		candidate.Cleanup = meta.Cleanup
		candidate.CleanupProvided = len(meta.Cleanup) > 0
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

func assertionFieldSet() map[string]struct{} {
	fields := map[string]struct{}{}
	for _, field := range manifestAssertionFields {
		fields[field] = struct{}{}
	}
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

func setupQueryFromAssertion(fields map[string]ast.Expr) (string, bool) {
	if expectationKind(fields) == "error" || fields["BindVars"] != nil || fields["CopyFromStdInFile"] != nil {
		return "", false
	}
	query, err := optionalStringLiteral(fields["Query"])
	if err != nil || query == "" {
		return "", false
	}
	return query, true
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
		setup   []string
		meta    oracleMeta
	}
	assertionLits := make([]mappedAssertion, 0)
	priorQueries := make([]string, 0)
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
				if query, ok := setupQueryFromAssertion(assertionFields); ok {
					priorQueries = append(priorQueries, query)
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
			assertionLits = append(assertionLits, mappedAssertion{lit: assertionLit, ordinal: assertionOrdinal, setup: append([]string(nil), priorQueries...), meta: meta})
		}
		if query, ok := setupQueryFromAssertion(assertionFields); ok {
			priorQueries = append(priorQueries, query)
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
		assertionSetup := append([]string(nil), setup...)
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

func entryFromScriptTestAssertion(source string, setup []string, ordinal int, assertionLit *ast.CompositeLit, meta oracleMeta) (entry, bool, error) {
	fields := compositeFields(assertionLit)
	if meta.ID == "" {
		return entry{}, false, nil
	}
	query, err := stringLiteral(fields["Query"])
	if err != nil {
		return entry{}, false, fmt.Errorf("%s Query: %w", meta.ID, err)
	}

	generatedSetup := append([]string(nil), setup...)
	if username, err := optionalStringLiteral(fields["Username"]); err != nil {
		return entry{}, false, fmt.Errorf("%s Username: %w", meta.ID, err)
	} else if username != "" {
		generatedSetup = append(generatedSetup, "SET ROLE "+quoteIdentifier(username))
	}
	generatedCleanup := append([]string(nil), meta.Cleanup...)
	if len(generatedSetup) > 0 && len(generatedCleanup) == 0 {
		generatedSetup = append([]string{
			"CREATE SCHEMA {{quotedSchema}}",
			"SET search_path TO {{quotedSchema}}, public, pg_catalog",
		}, generatedSetup...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedSubscriptions(generatedSetup)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedPublications(generatedSetup)...)
		generatedCleanup = append(generatedCleanup, cleanupForCreatedSchemas(generatedSetup)...)
		generatedCleanup = append(generatedCleanup, "DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE")
	}

	generated := entry{
		ID:                    meta.ID,
		Source:                source,
		Ordinal:               ordinal,
		Oracle:                "postgres",
		Compare:               meta.Compare,
		Setup:                 generatedSetup,
		Query:                 query,
		ExpectedSQLState:      meta.ExpectedSQLState,
		ExpectedErrorSeverity: meta.ExpectedErrorSeverity,
		ColumnModes:           meta.ColumnModes,
		Cleanup:               generatedCleanup,
	}
	if generated.Compare == "" {
		generated.Compare = "structural"
	}
	if generated.Compare == "sqlstate" || generated.ExpectedSQLState != "" {
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

func cleanupForCreatedSubscriptions(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create subscription ", "DROP SUBSCRIPTION IF EXISTS ")
}

func cleanupForCreatedPublications(statements []string) []string {
	return cleanupForCreatedObjects(statements, "create publication ", "DROP PUBLICATION IF EXISTS ")
}

func cleanupForCreatedObjects(statements []string, prefix string, dropPrefix string) []string {
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
		cleanup = append(cleanup, dropPrefix+name)
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
	ident, ok := array.Elt.(*ast.Ident)
	return ok && ident.Name == "ScriptTest"
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
