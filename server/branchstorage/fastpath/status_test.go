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

import (
	"strings"
	"testing"
)

// The architecture document fixes the on-the-wire status vocabulary; these
// strings are part of the public diagnostics contract. If a change to the
// values fails this test, update the architecture document and the diagnostic
// surface in lockstep.
func TestStatusStrings(t *testing.T) {
	cases := map[Status]string{
		StatusFastPathApplied:              "fast_path_applied",
		StatusDeclinedConflict:             "declined_conflict",
		StatusDeclinedUnsupportedColumn:    "declined_unsupported_column",
		StatusDeclinedMissingDeltaMetadata: "declined_missing_delta_metadata",
		StatusDeclinedBatchTooLarge:        "declined_batch_too_large",
		StatusDeclinedSchemaChange:         "declined_schema_change",
	}
	if len(cases) != 6 {
		t.Fatalf("architecture defines exactly 6 statuses, test has %d", len(cases))
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Fatalf("%v: want %q got %q", s, want, got)
		}
	}
}

func TestStatusIsApplied(t *testing.T) {
	if !StatusFastPathApplied.IsApplied() {
		t.Fatalf("StatusFastPathApplied must report IsApplied")
	}
	for _, s := range []Status{
		StatusDeclinedConflict,
		StatusDeclinedUnsupportedColumn,
		StatusDeclinedMissingDeltaMetadata,
		StatusDeclinedBatchTooLarge,
		StatusDeclinedSchemaChange,
	} {
		if s.IsApplied() {
			t.Fatalf("%s must not report IsApplied", s)
		}
		if !s.IsDecline() {
			t.Fatalf("%s must report IsDecline", s)
		}
	}
}

// TestUnknownStatusStringIsDistinct guards against drift: if a new status is
// added without updating the String switch, it should at least be uniquely
// identifiable in logs (not collide with a real status name).
func TestUnknownStatusStringIsDistinct(t *testing.T) {
	bogus := Status(0xff)
	got := bogus.String()
	if !strings.HasPrefix(got, "unknown_status") {
		t.Fatalf("unknown status string should start with %q, got %q", "unknown_status", got)
	}
}

func TestResultDeclineCarriesContext(t *testing.T) {
	r := Decline(StatusDeclinedConflict, Context{
		Tables:           []string{"public.journals"},
		ConflictingPK:    []byte{0x01, 0x02},
		ConflictingTable: "public.journals",
		ConflictColumns:  []string{"amount"},
		Detail:           "same-column edit on amount",
	})
	if r.Status != StatusDeclinedConflict {
		t.Fatalf("status: %s", r.Status)
	}
	if r.Context.ConflictingTable != "public.journals" {
		t.Fatalf("context table missing")
	}
	if len(r.Context.ConflictColumns) != 1 || r.Context.ConflictColumns[0] != "amount" {
		t.Fatalf("conflict columns missing")
	}
	if r.Context.Detail == "" {
		t.Fatalf("detail missing")
	}
}

// TestSameInputProducesSameStatus is the determinism contract from the
// architecture: same inputs must produce the same status. The decision logic
// itself is exercised in decide_test.go; here we just guard that the Result
// struct holds context the merge driver and operators can rely on for triage.
func TestApplyResultContextEmpty(t *testing.T) {
	r := Applied()
	if !r.Status.IsApplied() {
		t.Fatalf("applied result should report IsApplied")
	}
	if r.Context.ConflictingTable != "" || len(r.Context.ConflictColumns) != 0 {
		t.Fatalf("applied result must not carry decline context")
	}
}
