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

func TestPostgresOracleManifest(t *testing.T) {
	dsn, ok := postgresOracleDSN()
	if !ok {
		t.Skip("set DOLTGRES_ORACLE=1, DOLTGRES_POSTGRES_TEST_DSN, or POSTGRES_TEST_DSN")
	}

	manifest := loadPostgresOracleManifest(t)
	require.Equal(t, 1, manifest.Version)
	require.Equal(t, "pg16-structural-v1", manifest.NormalizationProfile)
	require.NotEmpty(t, manifest.Entries)

	ctx := context.Background()
	conn := connectPostgresOracle(t, ctx, dsn)
	defer func() {
		require.NoError(t, conn.Close(ctx))
	}()
	requirePostgresMajor(t, ctx, conn, manifest.CanonicalPostgresMajor)

	seen := map[string]struct{}{}
	for _, entry := range manifest.Entries {
		entry := entry
		require.NotEmpty(t, entry.ID)
		if _, ok := seen[entry.ID]; ok {
			t.Fatalf("duplicate oracle manifest id %q", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		require.Contains(t, []string{"postgres", "internal"}, entry.Oracle, "oracle classification for %s", entry.ID)
		if entry.Oracle != "postgres" {
			continue
		}
		t.Run(entry.ID, func(t *testing.T) {
			runPostgresOracleEntry(t, ctx, conn, manifest.NormalizationProfile, entry)
		})
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
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
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
