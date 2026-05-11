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

package ast

import "strings"

// dropColumnIfExistsMarker tags a column name as having been written with
// ALTER TABLE ... DROP COLUMN IF EXISTS. PostgreSQL identifiers cannot contain
// NUL, so the marker can never collide with a real column name.
const dropColumnIfExistsMarker = "\x00pg_drop_column_if_exists\x00"

// EncodeDropColumnIfExists wraps a column name with the IF EXISTS marker so
// that the doltgres analyzer rule resolveDropColumnIfExists can detect the
// clause when it later inspects the produced *plan.DropColumn node. The marker
// is the only side channel doltgres has — GMS does not yet model column-level
// IF EXISTS on its DDL plan nodes, so the AST translator threads the flag
// through the column-name field.
func EncodeDropColumnIfExists(column string) string {
	return dropColumnIfExistsMarker + column
}

// DecodeDropColumnIfExists strips the marker added by EncodeDropColumnIfExists
// and reports whether it was present.
func DecodeDropColumnIfExists(column string) (cleaned string, hadMarker bool) {
	if strings.HasPrefix(column, dropColumnIfExistsMarker) {
		return strings.TrimPrefix(column, dropColumnIfExistsMarker), true
	}
	return column, false
}
