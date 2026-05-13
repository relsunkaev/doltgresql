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

// notValidCheckConstraintMarker tags CHECK constraints declared with
// ALTER TABLE ... ADD CONSTRAINT ... NOT VALID. GMS plan nodes do not model
// PostgreSQL's "skip existing rows but enforce future writes" state, so the
// AST converter threads the bit through the constraint name until the analyzer
// can wrap the resolved *plan.CreateCheck.
const notValidCheckConstraintMarker = "\x00pg_not_valid_check\x00"

// EncodeNotValidCheckConstraintName wraps a constraint name with the NOT VALID
// marker.
func EncodeNotValidCheckConstraintName(name string) string {
	return notValidCheckConstraintMarker + name
}

// DecodeNotValidCheckConstraintName strips the NOT VALID marker and reports
// whether it was present.
func DecodeNotValidCheckConstraintName(name string) (cleaned string, hadMarker bool) {
	if strings.HasPrefix(name, notValidCheckConstraintMarker) {
		return strings.TrimPrefix(name, notValidCheckConstraintMarker), true
	}
	return name, false
}
