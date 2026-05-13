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
	goerrors "errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5/pgconn"
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
	return assertion.Expected != nil ||
		assertion.ExpectedRaw != nil ||
		assertion.ExpectedErr != "" ||
		assertion.ExpectedTag != "" ||
		assertion.ExpectedColNames != nil ||
		assertion.ExpectedColTypes != nil ||
		assertion.ExpectedNotices != nil
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
				cachedRow = append(cachedRow, *cell.Value)
			default:
				return nil, fmt.Errorf("%s row %d column %d has no cached value", entry.ID, rowIndex, columnIndex)
			}
		}
		rows = append(rows, cachedRow)
	}
	return rows, nil
}

func postgresOracleStringRows(rows []sql.Row) []sql.Row {
	stringRows := make([]sql.Row, len(rows))
	for rowIndex, row := range rows {
		stringRow := make(sql.Row, len(row))
		for columnIndex, value := range row {
			if value == nil {
				stringRow[columnIndex] = nil
			} else {
				stringRow[columnIndex] = fmt.Sprint(value)
			}
		}
		stringRows[rowIndex] = stringRow
	}
	return stringRows
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
