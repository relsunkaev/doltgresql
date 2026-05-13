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

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/fkmetadata"
	"github.com/dolthub/doltgresql/core/id"
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
	active     bool
	dirty      map[string]sql.ForeignKeyConstraint
	modes      map[string]bool
	namedModes map[string]bool
	allMode    *bool
}

type pendingForeignKeyTiming struct {
	key    string
	fk     sql.ForeignKeyConstraint
	timing Timing
}

const foreignKeyKeySeparator = "\x00"

var registry = struct {
	sync.Mutex
	parsed  []ParsedForeignKey
	timing  map[string]Timing
	pending map[uint32]map[string]pendingForeignKeyTiming
	txns    map[uint32]*txnState
}{
	timing:  make(map[string]Timing),
	pending: make(map[uint32]map[string]pendingForeignKeyTiming),
	txns:    make(map[uint32]*txnState),
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

func BindForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	registry.Lock()
	defer registry.Unlock()

	key := foreignKeyKey(fk)
	for i := len(registry.parsed) - 1; i >= 0; i-- {
		parsed := registry.parsed[i]
		if parsed.matches(fk) {
			connectionID := ctx.Session.ID()
			if registry.pending[connectionID] == nil {
				registry.pending[connectionID] = make(map[string]pendingForeignKeyTiming)
			}
			registry.pending[connectionID][key] = pendingForeignKeyTiming{
				key:    key,
				fk:     fk,
				timing: parsed.Timing,
			}
			return nil
		}
	}

	return nil
}

// ForeignKeyTiming returns the DEFERRABLE timing captured for fk from
// Doltgres-parsed DDL. A zero Timing means NOT DEFERRABLE or unknown.
func ForeignKeyTiming(ctx *sql.Context, fk sql.ForeignKeyConstraint) (Timing, error) {
	return ForeignKeyTimingForID(ctx, id.NullForeignKey, fk)
}

// ForeignKeyTimingForID returns the DEFERRABLE timing captured for fk from
// Doltgres-parsed DDL or persisted Doltgres metadata.
func ForeignKeyTimingForID(ctx *sql.Context, fkID id.ForeignKey, fk sql.ForeignKeyConstraint) (Timing, error) {
	registry.Lock()
	timing, ok := lookupTimingLocked(fk)
	if ok {
		registry.Unlock()
		return timing, nil
	}
	registry.Unlock()

	timing, ok, err := persistentForeignKeyTiming(ctx, fkID, fk)
	if err != nil || ok {
		return timing, err
	}
	return Timing{}, nil
}

func Begin(connectionID uint32) {
	registry.Lock()
	defer registry.Unlock()
	registry.txns[connectionID] = &txnState{
		active:     true,
		dirty:      make(map[string]sql.ForeignKeyConstraint),
		modes:      make(map[string]bool),
		namedModes: make(map[string]bool),
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

// ResetForTests clears all in-memory DEFERRABLE foreign-key state.
func ResetForTests() {
	registry.Lock()
	defer registry.Unlock()
	registry.parsed = nil
	registry.timing = make(map[string]Timing)
	registry.pending = make(map[uint32]map[string]pendingForeignKeyTiming)
	registry.txns = make(map[uint32]*txnState)
}

// FlushPendingForeignKeys promotes FK timing metadata captured while analyzing the
// current statement. Callers must only flush after the statement has succeeded.
func FlushPendingForeignKeys(ctx *sql.Context) error {
	if ctx == nil || ctx.Session == nil {
		return nil
	}
	connectionID := ctx.Session.ID()

	registry.Lock()
	pendingByKey := registry.pending[connectionID]
	if len(pendingByKey) == 0 {
		registry.Unlock()
		return nil
	}
	pending := make([]pendingForeignKeyTiming, 0, len(pendingByKey))
	for _, timing := range pendingByKey {
		pending = append(pending, timing)
	}
	delete(registry.pending, connectionID)
	registry.Unlock()

	for _, timing := range pending {
		if err := persistForeignKeyTiming(ctx, timing.fk, timing.timing); err != nil {
			return err
		}
	}

	registry.Lock()
	defer registry.Unlock()
	for _, timing := range pending {
		if timing.timing.Deferrable {
			registry.timing[timing.key] = timing.timing
		} else {
			delete(registry.timing, timing.key)
		}
	}
	return nil
}

// SetForeignKeyTiming persists and publishes timing metadata for an existing FK.
func SetForeignKeyTiming(ctx *sql.Context, fk sql.ForeignKeyConstraint, timing Timing) error {
	if err := persistForeignKeyTiming(ctx, fk, timing); err != nil {
		return err
	}
	registry.Lock()
	defer registry.Unlock()
	key := foreignKeyKey(fk)
	if timing.Deferrable {
		registry.timing[key] = timing
	} else {
		delete(registry.timing, key)
	}
	return nil
}

// DiscardPendingForeignKeys drops FK timing metadata captured for a statement
// that failed before it could be persisted.
func DiscardPendingForeignKeys(ctx *sql.Context) {
	if ctx == nil || ctx.Session == nil {
		return
	}
	registry.Lock()
	defer registry.Unlock()
	delete(registry.pending, ctx.Session.ID())
}

func Active(connectionID uint32) bool {
	registry.Lock()
	defer registry.Unlock()
	state := registry.txns[connectionID]
	return state != nil && state.active
}

func ShouldDefer(ctx *sql.Context, connectionID uint32, fk sql.ForeignKeyConstraint) (bool, error) {
	registry.Lock()
	state := registry.txns[connectionID]
	if state == nil || !state.active {
		registry.Unlock()
		return false, nil
	}
	registry.Unlock()

	timing, err := ForeignKeyTiming(ctx, fk)
	if err != nil {
		return false, err
	}
	if !timing.Deferrable {
		return false, nil
	}

	registry.Lock()
	defer registry.Unlock()
	state = registry.txns[connectionID]
	if state == nil || !state.active {
		return false, nil
	}
	key := foreignKeyKey(fk)
	if deferred, ok := state.modes[key]; ok {
		return deferred, nil
	}
	if deferred, ok := state.namedModes[normalize(fk.Name)]; ok {
		return deferred, nil
	}
	if state.allMode != nil {
		return *state.allMode, nil
	}
	return timing.InitiallyDeferred, nil
}

func lookupTimingLocked(fk sql.ForeignKeyConstraint) (Timing, bool) {
	if timing, ok := registry.timing[foreignKeyKey(fk)]; ok {
		return timing, true
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
	if all {
		state.allMode = &deferred
		clear(state.modes)
		clear(state.namedModes)
		return
	}
	for _, name := range normalizedNames {
		state.namedModes[name] = deferred
	}
}

func persistForeignKeyTiming(ctx *sql.Context, fk sql.ForeignKeyConstraint, timing Timing) error {
	if !core.IsContextValid(ctx) {
		return nil
	}
	if fk.Table == "" || fk.Name == "" {
		return nil
	}
	schemaName := fk.SchemaName
	if schemaName == "" {
		var err error
		schemaName, err = core.GetSchemaName(ctx, nil, "")
		if err != nil {
			return err
		}
	}
	collection, err := core.GetForeignKeyMetadataCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	return collection.SetTiming(ctx, fkmetadata.MetadataFromForeignKey(schemaName, fk, fkmetadata.Timing{
		Deferrable:        timing.Deferrable,
		InitiallyDeferred: timing.InitiallyDeferred,
	}))
}

func persistentForeignKeyTiming(ctx *sql.Context, fkID id.ForeignKey, fk sql.ForeignKeyConstraint) (Timing, bool, error) {
	if !core.IsContextValid(ctx) {
		return Timing{}, false, nil
	}
	if !fkID.IsValid() {
		if fk.Table == "" || fk.Name == "" {
			return Timing{}, false, nil
		}
		schemaName := fk.SchemaName
		if schemaName == "" {
			var err error
			schemaName, err = core.GetSchemaName(ctx, nil, "")
			if err != nil {
				return Timing{}, false, err
			}
		}
		fkID = id.NewForeignKey(schemaName, fk.Table, fk.Name)
	}
	collection, err := core.GetForeignKeyMetadataCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return Timing{}, false, err
	}
	timing, ok, err := collection.TimingForForeignKey(ctx, fkID, fk)
	if err != nil || !ok {
		return Timing{}, ok, err
	}
	return Timing{
		Deferrable:        timing.Deferrable,
		InitiallyDeferred: timing.InitiallyDeferred,
	}, true, nil
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
