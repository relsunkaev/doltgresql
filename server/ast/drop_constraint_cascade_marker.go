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

// dropConstraintCascadeMarker tags a constraint name from ALTER TABLE ... DROP
// CONSTRAINT ... CASCADE. GMS does not model the drop behavior on
// *plan.DropConstraint, so the AST translator threads the flag through the
// constraint name until Doltgres analyzer rules can inspect it.
const dropConstraintCascadeMarker = "\x00pg_drop_constraint_cascade\x00"

// EncodeDropConstraintCascade wraps a physical constraint name with the
// CASCADE marker.
func EncodeDropConstraintCascade(name string) string {
	return dropConstraintCascadeMarker + name
}

// DecodeDropConstraintCascade strips the CASCADE marker from a constraint name
// and reports whether it was present.
func DecodeDropConstraintCascade(name string) (cleaned string, cascade bool) {
	if strings.HasPrefix(name, dropConstraintCascadeMarker) {
		return strings.TrimPrefix(name, dropConstraintCascadeMarker), true
	}
	return name, false
}
