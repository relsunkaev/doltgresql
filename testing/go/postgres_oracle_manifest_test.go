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
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

const postgresOracleDefaultDSN = "postgres://postgres:password@127.0.0.1:5432/postgres?sslmode=disable"

type postgresOracleManifest struct {
	Version                int                   `json:"version"`
	CanonicalPostgresMajor int                   `json:"canonicalPostgresMajor"`
	NormalizationProfile   string                `json:"normalizationProfile"`
	Entries                []postgresOracleEntry `json:"entries"`
}

type postgresOracleEntry struct {
	ID                    string            `json:"id"`
	Source                string            `json:"source"`
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
	require.Equal(t, 1, manifest.Version)
	require.Equal(t, 16, manifest.CanonicalPostgresMajor)
	require.Equal(t, "pg16-structural-v1", manifest.NormalizationProfile)
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
			require.Contains(t, []string{"exact", "structural", "numeric", "timestamp", "timestamptz", "array", "json"}, mode,
				"column mode for %s", entry.ID)
		}
		if entry.Oracle == "postgres" {
			require.NotEmpty(t, entry.Query, "query for %s", entry.ID)
			if entry.ExpectedSQLState != "" {
				require.Empty(t, entry.ExpectedRows, "sqlstate oracle entries cannot also expect rows: %s", entry.ID)
			} else {
				require.NotEmpty(t, entry.ExpectedRows, "expected rows for %s", entry.ID)
				require.NotEqual(t, "sqlstate", entry.Compare, "sqlstate comparison requires expectedSqlstate for %s", entry.ID)
			}
		}
	}
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
	defer runOracleStatements(t, ctx, conn, variables, entry.Cleanup)
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
	if mode == "exact" {
		return fmt.Sprint(value)
	}
	if mode == "structural" {
		mode = inferPostgresOracleMode(oid)
	}
	switch v := value.(type) {
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int16, int32, int64, int:
		return fmt.Sprint(v)
	case float32, float64:
		return fmt.Sprint(v)
	case []byte:
		switch mode {
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
		text := fmt.Sprint(v)
		if mode == "numeric" {
			return normalizePostgresOracleNumeric(text)
		}
		return text
	}
}

func inferPostgresOracleMode(oid uint32) string {
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
	trimmed = strings.ReplaceAll(trimmed, ", ", ",")
	return trimmed
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
