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
const notValidCheckConstraintMarker = "dgcv_"

// noInheritCheckConstraintMarker tags CHECK constraints declared with
// NO INHERIT. GMS plan nodes do not model the bit, but inherited ALTER
// propagation must know to keep the constraint local to the parent table.
const noInheritCheckConstraintMarker = "dgci_"

// notValidForeignKeyConstraintMarker tags foreign keys declared with
// ALTER TABLE ... ADD CONSTRAINT ... NOT VALID until the analyzer can wrap the
// resolved *plan.CreateForeignKey.
const notValidForeignKeyConstraintMarker = "\x00pg_not_valid_fk\x00"

// CheckConstraintNameOptions records CHECK constraint options threaded through
// the GMS plan by internal name markers.
type CheckConstraintNameOptions struct {
	NotValid  bool
	NoInherit bool
}

// EncodeNotValidCheckConstraintName wraps a constraint name with the NOT VALID
// marker.
func EncodeNotValidCheckConstraintName(name string) string {
	return notValidCheckConstraintMarker + name
}

// EncodeNoInheritCheckConstraintName wraps a constraint name with the NO
// INHERIT marker.
func EncodeNoInheritCheckConstraintName(name string) string {
	return noInheritCheckConstraintMarker + name
}

// DecodeNotValidCheckConstraintName strips the NOT VALID marker and reports
// whether it was present.
func DecodeNotValidCheckConstraintName(name string) (cleaned string, hadMarker bool) {
	cleaned, options := DecodeCheckConstraintNameOptions(name)
	return cleaned, options.NotValid
}

// DecodeNoInheritCheckConstraintName strips the NO INHERIT marker and reports
// whether it was present.
func DecodeNoInheritCheckConstraintName(name string) (cleaned string, hadMarker bool) {
	cleaned, options := DecodeCheckConstraintNameOptions(name)
	return cleaned, options.NoInherit
}

// DecodeCheckConstraintNameOptions strips all CHECK option markers and returns
// the decoded option bits.
func DecodeCheckConstraintNameOptions(name string) (cleaned string, options CheckConstraintNameOptions) {
	for {
		switch {
		case strings.HasPrefix(name, notValidCheckConstraintMarker):
			options.NotValid = true
			name = strings.TrimPrefix(name, notValidCheckConstraintMarker)
		case strings.HasPrefix(name, noInheritCheckConstraintMarker):
			options.NoInherit = true
			name = strings.TrimPrefix(name, noInheritCheckConstraintMarker)
		default:
			return name, options
		}
	}
}

// HasCheckConstraintNameOptionMarker reports whether name carries any internal
// CHECK constraint option marker.
func HasCheckConstraintNameOptionMarker(name string) bool {
	_, options := DecodeCheckConstraintNameOptions(name)
	return options.NotValid || options.NoInherit
}

// EncodeNotValidForeignKeyConstraintName wraps a foreign key name with the NOT
// VALID marker.
func EncodeNotValidForeignKeyConstraintName(name string) string {
	return notValidForeignKeyConstraintMarker + name
}

// DecodeNotValidForeignKeyConstraintName strips the NOT VALID foreign key
// marker and reports whether it was present.
func DecodeNotValidForeignKeyConstraintName(name string) (cleaned string, hadMarker bool) {
	if strings.HasPrefix(name, notValidForeignKeyConstraintMarker) {
		return strings.TrimPrefix(name, notValidForeignKeyConstraintMarker), true
	}
	return name, false
}
