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

package indexpredicate

import "testing"

func TestImpliesNumericRangeFromConjuncts(t *testing.T) {
	if !Implies("score > 0", "(tenant = 1) AND (score > 10)") {
		t.Fatalf("expected stronger numeric predicate to imply partial index predicate")
	}
	if Implies("score > 0", "(tenant = 1) AND (score >= 0)") {
		t.Fatalf("did not expect inclusive lower bound to imply exclusive partial index predicate")
	}
}
