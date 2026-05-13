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
	"os"
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
			append(dropDefinitionEntries(), rlsPolicyRoleEntries()...)...,
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

func rlsPolicyRoleEntries() []entry {
	return []entry{
		{
			ID:           "rls-policy-role-list-allows-listed-role",
			Source:       "testing/go/row_level_security_policy_role_repro_test.go:TestRowLevelSecurityPolicyRoleListRestrictsPolicyRepro",
			Oracle:       "postgres",
			Compare:      "structural",
			Setup:        append(rlsPolicyRoleSelectSetup(), "SET ROLE {{schema}}_allowed"),
			Query:        "SELECT id, label FROM {{quotedSchema}}.docs ORDER BY id",
			ExpectedRows: rows(row(value("1"), value("allowed row"))),
			ColumnModes:  []string{"structural", "structural"},
			Cleanup:      rlsPolicyRoleSelectCleanup(),
		},
		{
			ID:           "rls-policy-role-list-denies-unlisted-role",
			Source:       "testing/go/row_level_security_policy_role_repro_test.go:TestRowLevelSecurityPolicyRoleListRestrictsPolicyRepro",
			Oracle:       "postgres",
			Compare:      "structural",
			Setup:        append(rlsPolicyRoleSelectSetup(), "SET ROLE {{schema}}_unlisted"),
			Query:        "SELECT id, label FROM {{quotedSchema}}.docs ORDER BY id",
			ExpectedRows: rows(),
			ColumnModes:  []string{"structural", "structural"},
			Cleanup:      rlsPolicyRoleSelectCleanup(),
		},
	}
}

func rlsPolicyRoleSelectSetup() []string {
	return []string{
		"CREATE SCHEMA {{quotedSchema}}",
		"CREATE ROLE {{schema}}_allowed LOGIN",
		"CREATE ROLE {{schema}}_unlisted LOGIN",
		"CREATE TABLE {{quotedSchema}}.docs (id integer PRIMARY KEY, owner_name text, label text)",
		"INSERT INTO {{quotedSchema}}.docs VALUES (1, '{{schema}}_allowed', 'allowed row'), (2, '{{schema}}_unlisted', 'unlisted row')",
		"GRANT USAGE ON SCHEMA {{quotedSchema}} TO {{schema}}_allowed, {{schema}}_unlisted",
		"GRANT SELECT ON {{quotedSchema}}.docs TO {{schema}}_allowed, {{schema}}_unlisted",
		"CREATE POLICY docs_owner_select ON {{quotedSchema}}.docs FOR SELECT TO {{schema}}_allowed USING (owner_name = current_user)",
		"ALTER TABLE {{quotedSchema}}.docs ENABLE ROW LEVEL SECURITY",
	}
}

func rlsPolicyRoleSelectCleanup() []string {
	return []string{
		"RESET ROLE",
		"DROP SCHEMA IF EXISTS {{quotedSchema}} CASCADE",
		"DROP ROLE IF EXISTS {{schema}}_allowed",
		"DROP ROLE IF EXISTS {{schema}}_unlisted",
	}
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
