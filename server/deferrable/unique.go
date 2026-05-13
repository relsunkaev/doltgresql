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

	"github.com/dolthub/go-mysql-server/sql"
)

// UniqueConstraint is the transaction-local shape needed to validate a
// PostgreSQL DEFERRABLE unique or primary-key constraint.
type UniqueConstraint struct {
	Database          string
	SchemaName        string
	Table             string
	Name              string
	Columns           []string
	Primary           bool
	NullsNotDistinct  bool
	InitiallyDeferred bool
}

// UniqueCheck is a deferred unique/primary-key validation query.
type UniqueCheck struct {
	Constraint UniqueConstraint
	Query      string
}

func ShouldDeferUnique(connectionID uint32, constraint UniqueConstraint) bool {
	registry.Lock()
	defer registry.Unlock()

	state := registry.txns[connectionID]
	if state == nil || !state.active {
		return false
	}
	key := uniqueKey(constraint)
	if deferred, ok := state.modes[key]; ok {
		return deferred
	}
	if deferred, ok := state.namedModes[normalize(constraint.Name)]; ok {
		return deferred
	}
	if state.allMode != nil {
		return *state.allMode
	}
	return constraint.InitiallyDeferred
}

func MarkUniqueDirty(connectionID uint32, constraint UniqueConstraint) {
	registry.Lock()
	defer registry.Unlock()

	state := registry.txns[connectionID]
	if state == nil || !state.active {
		return
	}
	state.dirtyUnique[uniqueKey(constraint)] = constraint
}

func PendingUniqueChecks(connectionID uint32) []UniqueCheck {
	return pendingUniqueChecksFor(connectionID, nil, true)
}

func PendingUniqueChecksForConstraints(connectionID uint32, names []string, all bool) []UniqueCheck {
	return pendingUniqueChecksFor(connectionID, names, all)
}

func ClearPendingUniqueChecksForConstraints(connectionID uint32, names []string, all bool) {
	registry.Lock()
	defer registry.Unlock()

	state := registry.txns[connectionID]
	if state == nil || len(state.dirtyUnique) == 0 {
		return
	}
	normalizedNames := normalizeSlice(names)
	for key, constraint := range state.dirtyUnique {
		if uniqueSelected(constraint, normalizedNames, all) {
			delete(state.dirtyUnique, key)
		}
	}
}

func UniqueViolationError(constraint UniqueConstraint) error {
	return sql.NewUniqueKeyErr(constraint.Name, constraint.Primary, nil)
}

func pendingUniqueChecksFor(connectionID uint32, names []string, all bool) []UniqueCheck {
	registry.Lock()
	defer registry.Unlock()

	state := registry.txns[connectionID]
	if state == nil || len(state.dirtyUnique) == 0 {
		return nil
	}
	normalizedNames := normalizeSlice(names)
	checks := make([]UniqueCheck, 0, len(state.dirtyUnique))
	for _, constraint := range state.dirtyUnique {
		if !uniqueSelected(constraint, normalizedNames, all) {
			continue
		}
		checks = append(checks, UniqueCheck{
			Constraint: constraint,
			Query:      uniqueValidationQuery(constraint),
		})
	}
	return checks
}

func uniqueKey(constraint UniqueConstraint) string {
	return strings.Join([]string{
		normalize(constraint.Database),
		normalize(constraint.SchemaName),
		normalize(constraint.Table),
		normalize(constraint.Name),
		strings.Join(normalizeSlice(constraint.Columns), ","),
	}, foreignKeyKeySeparator)
}

func uniqueSelected(constraint UniqueConstraint, normalizedNames []string, all bool) bool {
	return all || containsString(normalizedNames, normalize(constraint.Name))
}

func uniqueValidationQuery(constraint UniqueConstraint) string {
	table := qualifiedTableName(constraint.SchemaName, constraint.Table)
	groupBy := make([]string, len(constraint.Columns))
	notNull := make([]string, 0, len(constraint.Columns))
	for i, column := range constraint.Columns {
		quoted := quoteIdent(column)
		groupBy[i] = quoted
		if !constraint.NullsNotDistinct {
			notNull = append(notNull, fmt.Sprintf("%s IS NOT NULL", quoted))
		}
	}
	where := ""
	if len(notNull) > 0 {
		where = " WHERE " + strings.Join(notNull, " AND ")
	}
	return fmt.Sprintf(
		"SELECT 1 FROM %s%s GROUP BY %s HAVING COUNT(*) > 1 LIMIT 1",
		table,
		where,
		strings.Join(groupBy, ", "),
	)
}
