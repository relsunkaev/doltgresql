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

	"github.com/dolthub/doltgresql/postgres/parser/duration"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestPeriodsOverlap(t *testing.T) {
	day := func(day int) time.Time {
		return time.Date(2024, time.January, day, 0, 0, 0, 0, time.UTC)
	}
	if !periodsOverlap(day(1), day(10), day(5), day(20)) {
		t.Fatal("expected intersecting periods to overlap")
	}
	if periodsOverlap(day(1), day(2), day(2), day(3)) {
		t.Fatal("expected adjacent periods not to overlap")
	}
	if !periodsOverlap(day(10), day(1), day(5), day(20)) {
		t.Fatal("expected reversed endpoints to be normalized")
	}
}

func TestDoltgresOverlapsDateInterval(t *testing.T) {
	start1 := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	start2 := time.Date(2024, time.January, 2, 0, 0, 0, 0, time.UTC)
	params := [5]*pgtypes.DoltgresType{}
	got, err := doltgres_overlaps_date_interval.Callable(
		nil,
		params,
		start1,
		duration.MakeDuration(0, 2, 0),
		start2,
		duration.MakeDuration(0, 1, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != true {
		t.Fatalf("got %v, want true", got)
	}
}
