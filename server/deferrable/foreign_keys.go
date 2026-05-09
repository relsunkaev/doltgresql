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

package deferrable

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type Timing struct {
	Deferrable        bool
	InitiallyDeferred bool
}

type ParsedForeignKey struct {
	Name          string
	Table         string
	Columns       []string
	ParentTable   string
	ParentColumns []string
	Timing        Timing
}

type Check struct {
	ForeignKey sql.ForeignKeyConstraint
	Query      string
}

type txnState struct {
	active bool
	dirty  map[string]sql.ForeignKeyConstraint
	modes  map[string]bool
}

const foreignKeyKeySeparator = "\x00"

var registry = struct {
	sync.Mutex
	parsed []ParsedForeignKey
	timing map[string]Timing
	txns   map[uint32]*txnState
}{
	timing: make(map[string]Timing),
	txns:   make(map[uint32]*txnState),
}

func RegisterParsedForeignKey(parsed ParsedForeignKey) {
	parsed.Name = normalize(parsed.Name)
	parsed.Table = normalize(parsed.Table)
	parsed.ParentTable = normalize(parsed.ParentTable)
	parsed.Columns = normalizeSlice(parsed.Columns)
	parsed.ParentColumns = normalizeSlice(parsed.ParentColumns)

	registry.Lock()
	defer registry.Unlock()
	registry.parsed = append(registry.parsed, parsed)
}

func BindForeignKey(fk sql.ForeignKeyConstraint) {
	registry.Lock()
	defer registry.Unlock()

	key := foreignKeyKey(fk)
	for i := len(registry.parsed) - 1; i >= 0; i-- {
		parsed := registry.parsed[i]
		if parsed.matches(fk) {
			if parsed.Timing.Deferrable {
				registry.timing[key] = parsed.Timing
			} else {
				delete(registry.timing, key)
			}
			return
		}
	}
}

// ForeignKeyTiming returns the DEFERRABLE timing captured for fk from
// Doltgres-parsed DDL. A zero Timing means NOT DEFERRABLE or unknown.
func ForeignKeyTiming(fk sql.ForeignKeyConstraint) Timing {
	registry.Lock()
	defer registry.Unlock()
	timing, _ := lookupTimingLocked(fk)
	return timing
}

func Begin(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	registry.txns[connectionID] = &txnState{
		active: true,
		dirty:  make(map[string]sql.ForeignKeyConstraint),
		modes:  make(map[string]bool),
	}
}

func Rollback(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	delete(registry.txns, connectionID)
}

func Commit(connectionID uint32) {
	Rollback(connectionID)
}

func Active(connectionID uint32) bool {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	return state != nil && state.active
}

func ShouldDefer(connectionID uint32, fk sql.ForeignKeyConstraint) bool {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	if state == nil || !state.active {
		return false
	}
	key := foreignKeyKey(fk)
	timing, ok := lookupTimingLocked(fk)
	if !ok {
		return false
	}
	if !timing.Deferrable {
		return false
	}
	if deferred, ok := state.modes[key]; ok {
		return deferred
	}
	return timing.InitiallyDeferred
}

func lookupTimingLocked(fk sql.ForeignKeyConstraint) (Timing, bool) {
	if timing, ok := registry.timing[foreignKeyKey(fk)]; ok {
		return timing, true
	}
	for i := len(registry.parsed) - 1; i >= 0; i-- {
		parsed := registry.parsed[i]
		if parsed.matches(fk) {
			return parsed.Timing, true
		}
	}
	return Timing{}, false
}

func MarkDirty(connectionID uint32, fk sql.ForeignKeyConstraint) {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	if state == nil || !state.active {
		return
	}
	state.dirty[foreignKeyKey(fk)] = fk
}

func PendingChecks(connectionID uint32) []Check {
	return pendingChecksFor(connectionID, nil, true)
}

func PendingChecksForConstraints(connectionID uint32, names []string, all bool) []Check {
	return pendingChecksFor(connectionID, names, all)
}

func ClearPendingChecksForConstraints(connectionID uint32, names []string, all bool) {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	if state == nil || len(state.dirty) == 0 {
		return
	}
	normalizedNames := normalizeSlice(names)
	for key, fk := range state.dirty {
		if foreignKeySelected(fk, normalizedNames, all) {
			delete(state.dirty, key)
		}
	}
}

func SetConstraints(connectionID uint32, names []string, all bool, deferred bool) {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	if state == nil || !state.active {
		return
	}
	normalizedNames := normalizeSlice(names)
	for key, timing := range registry.timing {
		if !timing.Deferrable {
			continue
		}
		if all || containsString(normalizedNames, foreignKeyNameFromKey(key)) {
			state.modes[key] = deferred
		}
	}
}

func pendingChecksFor(connectionID uint32, names []string, all bool) []Check {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	if state == nil || len(state.dirty) == 0 {
		return nil
	}
	normalizedNames := normalizeSlice(names)
	checks := make([]Check, 0, len(state.dirty))
	for _, fk := range state.dirty {
		if !foreignKeySelected(fk, normalizedNames, all) {
			continue
		}
		checks = append(checks, Check{
			ForeignKey: fk,
			Query:      validationQuery(fk),
		})
	}
	return checks
}

func (parsed ParsedForeignKey) matches(fk sql.ForeignKeyConstraint) bool {
	if parsed.Name != "" && parsed.Name != normalize(fk.Name) {
		return false
	}
	return parsed.Table == normalize(fk.Table) &&
		parsed.ParentTable == normalize(fk.ParentTable) &&
		equalStringSlices(parsed.Columns, normalizeSlice(fk.Columns)) &&
		(len(parsed.ParentColumns) == 0 || equalStringSlices(parsed.ParentColumns, normalizeSlice(fk.ParentColumns)))
}

func foreignKeyKey(fk sql.ForeignKeyConstraint) string {
	return strings.Join([]string{
		normalize(fk.Database),
		normalize(fk.Table),
		normalize(fk.Name),
		strings.Join(normalizeSlice(fk.Columns), ","),
		normalize(fk.ParentDatabase),
		normalize(fk.ParentTable),
		strings.Join(normalizeSlice(fk.ParentColumns), ","),
	}, foreignKeyKeySeparator)
}

func foreignKeyNameFromKey(key string) string {
	parts := strings.Split(key, foreignKeyKeySeparator)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

func foreignKeySelected(fk sql.ForeignKeyConstraint, normalizedNames []string, all bool) bool {
	return all || containsString(normalizedNames, normalize(fk.Name))
}

func validationQuery(fk sql.ForeignKeyConstraint) string {
	child := qualifiedTableName(fk.SchemaName, fk.Table)
	parent := qualifiedTableName(fk.ParentSchema, fk.ParentTable)

	notNull := make([]string, len(fk.Columns))
	join := make([]string, len(fk.Columns))
	for i := range fk.Columns {
		notNull[i] = fmt.Sprintf("child.%s IS NOT NULL", quoteIdent(fk.Columns[i]))
		join[i] = fmt.Sprintf("parent.%s = child.%s", quoteIdent(fk.ParentColumns[i]), quoteIdent(fk.Columns[i]))
	}

	return fmt.Sprintf(
		"SELECT 1 FROM %s AS child WHERE %s AND NOT EXISTS (SELECT 1 FROM %s AS parent WHERE %s) LIMIT 1",
		child,
		strings.Join(notNull, " AND "),
		parent,
		strings.Join(join, " AND "),
	)
}

func qualifiedTableName(schemaName, tableName string) string {
	if schemaName == "" {
		return quoteIdent(tableName)
	}
	return quoteIdent(schemaName) + "." + quoteIdent(tableName)
}

func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func normalize(value string) string {
	return strings.ToLower(value)
}

func normalizeSlice(values []string) []string {
	normalized := make([]string, len(values))
	for i, value := range values {
		normalized[i] = normalize(value)
	}
	return normalized
}

func equalStringSlices(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
