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
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

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

func main() {
	stdout := flag.Bool("stdout", false, "write generated manifest to stdout instead of testdata/postgres_oracle_manifest.json")
	flag.Parse()

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
	scriptEntries, err := annotatedScriptTestEntries()
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
			AssertionFields: []string{
				"Expected",
				"ExpectedRaw",
				"ExpectedErr",
				"ExpectedTag",
				"ExpectedColNames",
				"ExpectedColTypes",
				"ExpectedNotices",
			},
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
	ExpectedSQLState      string
	ExpectedErrorSeverity string
	Cleanup               []string
}

func annotatedScriptTestEntries() ([]entry, error) {
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
					source := fmt.Sprintf("testing/go/%s:%s", file, fn.Name.Name)
					generated, err := entriesFromScriptTest(source, scriptLit)
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

func entriesFromScriptTest(source string, scriptLit *ast.CompositeLit) ([]entry, error) {
	fields := compositeFields(scriptLit)
	assertionsLit, ok := fields["Assertions"].(*ast.CompositeLit)
	if !ok {
		return nil, nil
	}

	assertionLits := make([]*ast.CompositeLit, 0)
	for _, assertionExpr := range assertionsLit.Elts {
		assertionLit, ok := assertionExpr.(*ast.CompositeLit)
		if !ok {
			continue
		}
		if _, ok := compositeFields(assertionLit)["PostgresOracle"]; ok {
			assertionLits = append(assertionLits, assertionLit)
		}
	}
	if len(assertionLits) == 0 {
		return nil, nil
	}

	setup, err := stringSlice(fields["SetUpScript"])
	if err != nil {
		return nil, fmt.Errorf("SetUpScript: %w", err)
	}

	entries := make([]entry, 0, len(assertionLits))
	for _, assertionLit := range assertionLits {
		generated, ok, err := entryFromScriptTestAssertion(source, setup, assertionLit)
		if err != nil {
			return nil, err
		}
		if ok {
			entries = append(entries, generated)
		}
	}
	return entries, nil
}

func entryFromScriptTestAssertion(source string, setup []string, assertionLit *ast.CompositeLit) (entry, bool, error) {
	fields := compositeFields(assertionLit)
	metaExpr, ok := fields["PostgresOracle"]
	if !ok {
		return entry{}, false, nil
	}
	meta, err := parseOracleMeta(metaExpr)
	if err != nil {
		return entry{}, false, fmt.Errorf("PostgresOracle: %w", err)
	}
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

	generated := entry{
		ID:                    meta.ID,
		Source:                source,
		Oracle:                "postgres",
		Compare:               meta.Compare,
		Setup:                 generatedSetup,
		Query:                 query,
		ExpectedSQLState:      meta.ExpectedSQLState,
		ExpectedErrorSeverity: meta.ExpectedErrorSeverity,
		ColumnModes:           meta.ColumnModes,
		Cleanup:               meta.Cleanup,
	}
	if generated.Compare == "" {
		generated.Compare = "structural"
	}
	if generated.ExpectedSQLState != "" {
		return generated, true, nil
	}

	expectedRows, err := expectedRows(fields["Expected"])
	if err != nil {
		return entry{}, false, fmt.Errorf("%s Expected: %w", meta.ID, err)
	}
	generated.ExpectedRows = expectedRows
	return generated, true, nil
}

func parseOracleMeta(expr ast.Expr) (oracleMeta, error) {
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
	if meta.ColumnModes, err = stringSlice(fields["ColumnModes"]); err != nil {
		return oracleMeta{}, fmt.Errorf("ColumnModes: %w", err)
	}
	if meta.ExpectedSQLState, err = optionalStringLiteral(fields["ExpectedSQLState"]); err != nil {
		return oracleMeta{}, fmt.Errorf("ExpectedSQLState: %w", err)
	}
	if meta.ExpectedErrorSeverity, err = optionalStringLiteral(fields["ExpectedErrorSeverity"]); err != nil {
		return oracleMeta{}, fmt.Errorf("ExpectedErrorSeverity: %w", err)
	}
	if meta.Cleanup, err = stringSlice(fields["Cleanup"]); err != nil {
		return oracleMeta{}, fmt.Errorf("Cleanup: %w", err)
	}
	return meta, nil
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

func expectedRows(expr ast.Expr) (*[][]cell, error) {
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

func stringSlice(expr ast.Expr) ([]string, error) {
	if expr == nil {
		return nil, nil
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
