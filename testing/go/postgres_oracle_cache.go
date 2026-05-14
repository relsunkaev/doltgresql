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
	"encoding/hex"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

type postgresOracleCachedManifest struct {
	Entries []postgresOracleCachedEntry `json:"entries"`
}

type postgresOracleCachedEntry struct {
	ID                    string                        `json:"id"`
	Source                string                        `json:"source"`
	Ordinal               int                           `json:"ordinal"`
	Oracle                string                        `json:"oracle"`
	ExpectedRows          *[][]postgresOracleCachedCell `json:"expectedRows"`
	ExpectedSQLState      string                        `json:"expectedSqlstate"`
	ExpectedErrorSeverity string                        `json:"expectedErrorSeverity"`
	ExpectedTag           *string                       `json:"expectedTag"`
	ColumnModes           []string                      `json:"columnModes"`
}

type postgresOracleCachedCell struct {
	Value *string `json:"value,omitempty"`
	Regex string  `json:"regex,omitempty"`
	Any   bool    `json:"any,omitempty"`
	Null  bool    `json:"null,omitempty"`
}

type postgresOracleCachedIndex struct {
	entriesByAssertion map[string]*postgresOracleCachedEntry
	tests              map[string]struct{}
}

var (
	postgresOracleCacheOnce  sync.Once
	postgresOracleCacheIndex *postgresOracleCachedIndex
	postgresOracleCacheErr   error
)

func attachPostgresOracleCache(t testing.TB, scripts []ScriptTest) []ScriptTest {
	t.Helper()
	cache, err := loadPostgresOracleCache()
	require.NoError(t, err)

	testName := rootPostgresOracleCacheTestName(t.Name())
	if _, ok := cache.tests[testName]; !ok {
		return scripts
	}

	cachedScripts := append([]ScriptTest(nil), scripts...)
	ordinal := 0
	for scriptIndex := range cachedScripts {
		cachedScripts[scriptIndex].Assertions = append([]ScriptTestAssertion(nil), cachedScripts[scriptIndex].Assertions...)
		for assertionIndex := range cachedScripts[scriptIndex].Assertions {
			if !isPostgresOracleCacheCandidate(cachedScripts[scriptIndex].Assertions[assertionIndex]) {
				continue
			}
			ordinal++
			key := postgresOracleCacheKey(testName, ordinal)
			if cached := cache.entriesByAssertion[key]; cached != nil {
				cachedScripts[scriptIndex].Assertions[assertionIndex].postgresOracleCached = cached
			} else if hasPostgresOracleMarker(cachedScripts[scriptIndex].Assertions[assertionIndex]) {
				require.Failf(t, "missing cached PostgreSQL oracle entry", "missing cached PostgreSQL oracle entry for %s", key)
			}
		}
	}
	return cachedScripts
}

func loadPostgresOracleCache() (*postgresOracleCachedIndex, error) {
	postgresOracleCacheOnce.Do(func() {
		data, err := os.ReadFile("testdata/postgres_oracle_manifest.json")
		if err != nil {
			postgresOracleCacheErr = err
			return
		}
		var manifest postgresOracleCachedManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			postgresOracleCacheErr = err
			return
		}
		cache := &postgresOracleCachedIndex{
			entriesByAssertion: map[string]*postgresOracleCachedEntry{},
			tests:              map[string]struct{}{},
		}
		for _, entry := range manifest.Entries {
			if entry.Oracle != "postgres" || entry.Ordinal == 0 {
				continue
			}
			testName, ok := postgresOracleCacheSourceTestName(entry.Source)
			if !ok {
				continue
			}
			key := postgresOracleCacheKey(testName, entry.Ordinal)
			if _, ok := cache.entriesByAssertion[key]; ok {
				postgresOracleCacheErr = fmt.Errorf("duplicate postgres oracle cache assertion %s", key)
				return
			}
			entry := entry
			cache.entriesByAssertion[key] = &entry
			cache.tests[testName] = struct{}{}
		}
		postgresOracleCacheIndex = cache
	})
	return postgresOracleCacheIndex, postgresOracleCacheErr
}

func postgresOracleCacheSourceTestName(source string) (string, bool) {
	_, testName, ok := strings.Cut(source, ":")
	if !ok || testName == "" {
		return "", false
	}
	return testName, true
}

func rootPostgresOracleCacheTestName(name string) string {
	if root, _, ok := strings.Cut(name, "/"); ok {
		return root
	}
	return name
}

func postgresOracleCacheKey(testName string, ordinal int) string {
	return fmt.Sprintf("%s#%04d", testName, ordinal)
}

func isPostgresOracleCacheCandidate(assertion ScriptTestAssertion) bool {
	return hasPostgresOracleMarker(assertion) ||
		assertion.Expected != nil ||
		assertion.ExpectedRaw != nil ||
		assertion.ExpectedErr != "" ||
		assertion.ExpectedTag != "" ||
		assertion.ExpectedColNames != nil ||
		assertion.ExpectedColTypes != nil ||
		assertion.ExpectedNotices != nil
}

func hasPostgresOracleMarker(assertion ScriptTestAssertion) bool {
	return assertion.PostgresOracle.ID != "" ||
		assertion.PostgresOracle.Compare != "" ||
		len(assertion.PostgresOracle.ColumnModes) != 0 ||
		assertion.PostgresOracle.ExpectedSQLState != "" ||
		assertion.PostgresOracle.ExpectedErrorSeverity != "" ||
		len(assertion.PostgresOracle.Cleanup) != 0
}

func (assertion ScriptTestAssertion) postgresOracleCachedRows() ([]sql.Row, error) {
	if assertion.postgresOracleCached == nil || assertion.postgresOracleCached.ExpectedRows == nil {
		return nil, nil
	}
	return assertion.postgresOracleCached.rows()
}

func (entry *postgresOracleCachedEntry) rows() ([]sql.Row, error) {
	rows := make([]sql.Row, 0, len(*entry.ExpectedRows))
	for rowIndex, row := range *entry.ExpectedRows {
		cachedRow := make(sql.Row, 0, len(row))
		for columnIndex, cell := range row {
			if cell.Any || cell.Regex != "" {
				return nil, fmt.Errorf("%s row %d column %d uses pattern cells, which normal ScriptTest comparisons do not support", entry.ID, rowIndex, columnIndex)
			}
			switch {
			case cell.Null:
				cachedRow = append(cachedRow, nil)
			case cell.Value != nil:
				cachedRow = append(cachedRow, entry.normalizeCachedExpectedValue(columnIndex, *cell.Value))
			default:
				return nil, fmt.Errorf("%s row %d column %d has no cached value", entry.ID, rowIndex, columnIndex)
			}
		}
		rows = append(rows, cachedRow)
	}
	return rows, nil
}

func (entry *postgresOracleCachedEntry) normalizeCachedExpectedValue(index int, value string) string {
	mode := entry.columnMode(index)
	switch mode {
	case "schema", "explain":
		return strings.ReplaceAll(value, "{{schema}}", "public")
	default:
		return value
	}
}

func postgresOracleStringRows(rows []sql.Row) []sql.Row {
	return postgresOracleStringRowsWithModes(rows, nil)
}

func (entry *postgresOracleCachedEntry) stringRows(rows []sql.Row) []sql.Row {
	return postgresOracleStringRowsWithModes(rows, entry.ColumnModes)
}

func postgresOracleStringRowsWithModes(rows []sql.Row, modes []string) []sql.Row {
	stringRows := make([]sql.Row, len(rows))
	for rowIndex, row := range rows {
		stringRow := make(sql.Row, len(row))
		for columnIndex, value := range row {
			if value == nil {
				stringRow[columnIndex] = nil
			} else {
				stringRow[columnIndex] = postgresOracleStringValueWithMode(value, cachedPostgresOracleColumnMode(modes, columnIndex))
			}
		}
		stringRows[rowIndex] = stringRow
	}
	return stringRows
}

func postgresOracleStringValue(value any) string {
	return postgresOracleStringValueWithMode(value, "")
}

func postgresOracleStringValueWithMode(value any, mode string) string {
	if mode == "exact" {
		return fmt.Sprint(value)
	}
	switch v := value.(type) {
	case pgtype.Numeric:
		return postgresOracleNumericString(v)
	case []byte:
		if mode == "bytea" {
			return "\\x" + hex.EncodeToString(v)
		}
		text := string(v)
		if mode == "schema" {
			return normalizeCachedPostgresOracleSchema(text)
		}
		if mode == "explain" {
			return normalizeCachedPostgresOracleExplain(text)
		}
		if mode == "json" {
			return normalizeCachedPostgresOracleJSON(text)
		}
		return text
	default:
		text := fmt.Sprint(v)
		if mode == "schema" {
			return normalizeCachedPostgresOracleSchema(text)
		}
		if mode == "explain" {
			return normalizeCachedPostgresOracleExplain(text)
		}
		if mode == "json" {
			return normalizeCachedPostgresOracleJSON(text)
		}
		return text
	}
}

func (entry *postgresOracleCachedEntry) columnMode(index int) string {
	return cachedPostgresOracleColumnMode(entry.ColumnModes, index)
}

func cachedPostgresOracleColumnMode(modes []string, index int) string {
	if index < len(modes) && modes[index] != "" {
		return modes[index]
	}
	return "structural"
}

var cachedPostgresOracleSchemaNamePattern = regexp.MustCompile(`dg_oracle_[0-9]+`)

func normalizeCachedPostgresOracleSchema(value string) string {
	return cachedPostgresOracleSchemaNamePattern.ReplaceAllString(value, "public")
}

var (
	cachedPostgresOracleExplainActualTimePattern = regexp.MustCompile(`actual time=[0-9]+(?:\.[0-9]+)?\.\.[0-9]+(?:\.[0-9]+)?`)
	cachedPostgresOracleExplainPlanningPattern   = regexp.MustCompile(`Planning Time: [0-9]+(?:\.[0-9]+)? ms`)
	cachedPostgresOracleExplainExecutionPattern  = regexp.MustCompile(`Execution Time: [0-9]+(?:\.[0-9]+)? ms`)
)

func normalizeCachedPostgresOracleExplain(value string) string {
	value = normalizeCachedPostgresOracleSchema(value)
	value = cachedPostgresOracleExplainActualTimePattern.ReplaceAllString(value, "actual time=<time>..<time>")
	value = cachedPostgresOracleExplainPlanningPattern.ReplaceAllString(value, "Planning Time: <time> ms")
	value = cachedPostgresOracleExplainExecutionPattern.ReplaceAllString(value, "Execution Time: <time> ms")
	return value
}

func normalizeCachedPostgresOracleJSON(value string) string {
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

func postgresOracleNumericString(value pgtype.Numeric) string {
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

func requirePostgresOracleCachedSQLState(t testing.TB, err error, entry *postgresOracleCachedEntry) {
	t.Helper()
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, goerrors.As(err, &pgErr), "expected PostgreSQL error for %s, got %T: %v", entry.ID, err, err)
	require.Equal(t, entry.ExpectedSQLState, pgErr.Code)
	if entry.ExpectedErrorSeverity != "" {
		require.Equal(t, entry.ExpectedErrorSeverity, pgErr.Severity)
	}
}
