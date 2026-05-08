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

package fastpath

// DefaultRowChangeLimit is the architecture's typical-case ceiling for the
// constrained-merge fast path. Above this, batches are large enough that the
// fallback Dolt merge is appropriate and the fast path's subsecond contract
// stops applying.
const DefaultRowChangeLimit = 10000

// Config tunes the fast path. It is a per-deployment snapshot: Decide reads it
// once at the top of the call, so operators must publish a new Config to
// change behavior. Hot-reload during a merge is not supported by design (each
// merge runs against the Config it observed at start).
//
// Fields with their architecture grounding:
//
//   - RowChangeLimit: caps the per-merge changed-row count. Above the limit,
//     Decide returns StatusDeclinedBatchTooLarge. The doc-stated 10k target is
//     a typical case; operators may tune up or down per deployment.
//   - UnsupportedColumns: an operator-set table → column-name set. If a
//     branch-side ChangedScalars or TouchedComplex entry mentions any column
//     in the set for that table, Decide forces StatusDeclinedUnsupportedColumn
//     even if the producer otherwise declared the column as a plain scalar.
//     This is the operator's escape hatch for columns whose merge semantics
//     are not safe in this deployment (e.g. columns with downstream FTS
//     triggers, materialized-view dependencies, or app-level invariants).
type Config struct {
	RowChangeLimit     int
	UnsupportedColumns map[string]map[string]struct{}
}

// DefaultConfig returns the architecture's default tuning.
func DefaultConfig() Config {
	return Config{
		RowChangeLimit: DefaultRowChangeLimit,
	}
}

// columnIsUnsupported reports whether the given (table, column) is on the
// operator-blocked list. Lookups are O(1).
func (c Config) columnIsUnsupported(table, column string) bool {
	tcols, ok := c.UnsupportedColumns[table]
	if !ok {
		return false
	}
	_, blocked := tcols[column]
	return blocked
}

// branchTouchedUnsupportedScalar returns the names of branch-side scalar
// columns that fall into the operator's blocked set for the given table.
// Order is the input order (caller decides if sorting is needed); duplicates
// are filtered out by the producer-side validation in deltameta.
func (c Config) branchTouchedUnsupportedScalars(table string, changedScalars []string) []string {
	if len(c.UnsupportedColumns) == 0 || len(changedScalars) == 0 {
		return nil
	}
	tcols, ok := c.UnsupportedColumns[table]
	if !ok {
		return nil
	}
	var out []string
	for _, col := range changedScalars {
		if _, blocked := tcols[col]; blocked {
			out = append(out, col)
		}
	}
	return out
}
