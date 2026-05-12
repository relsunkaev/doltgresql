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

package functions

import (
	"testing"
	"time"

	"github.com/dolthub/doltgresql/postgres/parser/uuid"
)

func TestFormatIntegerBase(t *testing.T) {
	if got, want := formatIntegerBase(int32(10), 2), "1010"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got, want := formatIntegerBase(int64(10), 8), "12"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestUuidExtractHelpers(t *testing.T) {
	v4 := uuid.Must(uuid.FromString("41db1265-8bc1-4ab3-992f-885799a4af1d"))
	if got, want := uuidExtractVersion(v4), uuid.V4; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got, err := uuidExtractTimestamp(v4); err != nil || got != nil {
		t.Fatalf("got %#v, err %v; want nil timestamp without error", got, err)
	}

	v1 := uuid.NamespaceDNS
	got, err := uuidExtractTimestamp(v1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.(time.Time); !ok {
		t.Fatalf("got %#v, want time.Time", got)
	}
}

func TestTypeMetadataHelpers(t *testing.T) {
	if got, want := parseTypeModifier("varchar(32)"), int32(36); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got, want := parseTypeModifier("text"), int32(-1); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestUnicodeInformationHelpers(t *testing.T) {
	if !unicodeAssigned("abc") {
		t.Fatal("expected valid UTF-8 text to be assigned")
	}
	if unicodeAssigned(string([]byte{0xff})) {
		t.Fatal("expected invalid UTF-8 text to be unassigned")
	}
}
