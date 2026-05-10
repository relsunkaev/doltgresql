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

func TestImpliesIsNotNullFromStrictPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"email IS NOT NULL", "email = 'ada@example.com'"},
		{"email IS NOT NULL", "email IN ('ada@example.com', 'grace@example.com')"},
		{"score IS NOT NULL", "score > 10"},
		{"active IS NOT NULL", "active = true"},
		{"lower(email) IS NOT NULL", "lower(email) = 'ada@example.com'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"email IS NOT NULL", "email IS DISTINCT FROM 'ada@example.com'"},
		{"email IS NOT NULL", "email IS NULL"},
		{"score IS NOT NULL", "score IS DISTINCT FROM 10"},
		{"email IS NOT NULL", "lower(email) = 'ada@example.com'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesExclusionPredicatesFromDisjointValues(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"status != 'archived'", "status = 'active'"},
		{"status != 'archived'", "status IN ('active', 'pending')"},
		{"status NOT IN ('archived', 'deleted')", "status = 'active'"},
		{"status NOT IN ('archived', 'deleted')", "status IN ('active', 'pending')"},
		{"lower(email) NOT IN ('blocked@example.com')", "lower(email) = 'ada@example.com'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"status != 'archived'", "status = 'archived'"},
		{"status != 'archived'", "status IN ('active', 'archived')"},
		{"status NOT IN ('archived', 'deleted')", "status = 'deleted'"},
		{"status NOT IN ('archived', 'deleted')", "status IN ('active', 'deleted')"},
		{"status NOT IN ('archived', 'deleted')", "status IS DISTINCT FROM 'archived'"},
		{"status NOT IN ('archived', 'deleted')", "lower(status) = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesNullPredicatesFromNullSafeNullComparisons(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"deleted_at IS NULL", "deleted_at IS NOT DISTINCT FROM NULL"},
		{"deleted_at IS NOT DISTINCT FROM NULL", "deleted_at IS NULL"},
		{"lower(email) IS NULL", "lower(email) IS NOT DISTINCT FROM NULL"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"deleted_at IS NULL", "deleted_at IS NOT DISTINCT FROM '2026-01-01'"},
		{"deleted_at IS NULL", "archived_at IS NOT DISTINCT FROM NULL"},
		{"deleted_at IS NULL", "deleted_at IS DISTINCT FROM NULL"},
		{"lower(email) IS NULL", "email IS NOT DISTINCT FROM NULL"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesDistinctFromPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"status IS DISTINCT FROM 'archived'", "status = 'active'"},
		{"status IS DISTINCT FROM 'archived'", "status IN ('active', 'pending')"},
		{"status IS DISTINCT FROM 'archived'", "status IS NULL"},
		{"deleted_at IS DISTINCT FROM NULL", "deleted_at = '2026-01-01'"},
		{"deleted_at IS DISTINCT FROM NULL", "deleted_at IS NOT NULL"},
		{"lower(status) IS DISTINCT FROM 'archived'", "lower(status) = 'active'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"status IS DISTINCT FROM 'archived'", "status = 'archived'"},
		{"status IS DISTINCT FROM 'archived'", "status IN ('active', 'archived')"},
		{"status IS DISTINCT FROM 'archived'", "status IS NOT DISTINCT FROM 'archived'"},
		{"status != 'archived'", "status IS NULL"},
		{"deleted_at IS DISTINCT FROM NULL", "deleted_at IS NULL"},
		{"status IS DISTINCT FROM 'archived'", "lower(status) = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesCrossColumnEqualityPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"tenant_id = owner_tenant_id", "tenant_id = 1 AND owner_tenant_id = 1"},
		{"tenant_id IS NOT DISTINCT FROM owner_tenant_id", "tenant_id = 1 AND owner_tenant_id = 1"},
		{"tenant_id IS NOT DISTINCT FROM owner_tenant_id", "tenant_id IS NULL AND owner_tenant_id IS NULL"},
		{"tenant_id = owner_tenant_id", "tenant_id = workspace_tenant_id AND workspace_tenant_id = owner_tenant_id"},
		{"tenant_id IS NOT DISTINCT FROM owner_tenant_id", "tenant_id = workspace_tenant_id AND workspace_tenant_id = owner_tenant_id"},
		{"tenant_id IS NOT DISTINCT FROM owner_tenant_id", "tenant_id IS NOT DISTINCT FROM workspace_tenant_id AND workspace_tenant_id IS NOT DISTINCT FROM owner_tenant_id"},
		{"lower(email) = canonical_email", "lower(email) = 'ada@example.com' AND canonical_email = 'ada@example.com'"},
		{"lower(email) = canonical_email", "lower(email) = alias_email AND alias_email = canonical_email"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"tenant_id = owner_tenant_id", "tenant_id = 1 AND owner_tenant_id = 2"},
		{"tenant_id = owner_tenant_id", "tenant_id IS NULL AND owner_tenant_id IS NULL"},
		{"tenant_id = owner_tenant_id", "tenant_id = 1"},
		{"tenant_id = owner_tenant_id", "tenant_id IN (1, 2) AND owner_tenant_id IN (1, 2)"},
		{"tenant_id = owner_tenant_id", "tenant_id IS NOT DISTINCT FROM workspace_tenant_id AND workspace_tenant_id IS NOT DISTINCT FROM owner_tenant_id"},
		{"tenant_id IS NOT DISTINCT FROM owner_tenant_id", "tenant_id = workspace_tenant_id"},
		{"tenant_id IS NOT DISTINCT FROM owner_tenant_id", "tenant_id = 1 AND owner_tenant_id = 2"},
		{"lower(email) = canonical_email", "email = 'ada@example.com' AND canonical_email = 'ada@example.com'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesTrimFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"ltrim(code) = 'active'", "ltrim(code) = 'active'"},
		{"rtrim(code) IN ('active', 'pending')", "rtrim(code) = 'active'"},
		{"btrim(code) = 'active'", "btrim(code) = 'active'"},
		{"ltrim(code) IS NOT NULL", "ltrim(code) = 'active'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"ltrim(code) = 'active'", "code = 'active'"},
		{"ltrim(code) = 'active'", "rtrim(code) = 'active'"},
		{"rtrim(code) IN ('active', 'pending')", "rtrim(code) IN ('active', 'archived')"},
		{"btrim(code) = 'active'", "btrim(code) = 'archived'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesTextLengthFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"length(code) = 6", "length(code) = 6"},
		{"length(code) = 6", "char_length(code) = 6"},
		{"char_length(code) IN (6, 7)", "length(code) = 6"},
		{"length(code) IS NOT NULL", "char_length(code) = 6"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"length(code) = 6", "length(code) = 7"},
		{"length(code) = 6", "code = 'active'"},
		{"length(code) = 6", "octet_length(code) = 6"},
		{"char_length(code) IN (6, 7)", "length(code) IN (6, 8)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}
