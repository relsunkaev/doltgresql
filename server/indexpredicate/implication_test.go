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

func TestImpliesExclusionPredicatesFromExclusionSubsets(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"status NOT IN ('archived', 'deleted')", "status NOT IN ('archived', 'deleted', 'blocked')"},
		{"status NOT IN ('archived', 'deleted')", "NOT (status IN ('archived', 'deleted', 'blocked'))"},
		{"status != 'archived'", "status NOT IN ('archived', 'deleted')"},
		{"status IS DISTINCT FROM 'archived'", "status != 'archived'"},
		{"status IS DISTINCT FROM 'archived'", "status NOT IN ('archived', 'deleted')"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"status NOT IN ('archived', 'deleted')", "status != 'archived'"},
		{"status NOT IN ('archived', 'deleted')", "status IS DISTINCT FROM 'archived'"},
		{"status != 'archived'", "status IS DISTINCT FROM 'archived'"},
		{"status NOT IN ('archived', 'deleted')", "status NOT IN ('archived', 'blocked')"},
		{"status NOT IN ('archived', 'deleted')", "lower(status) NOT IN ('archived', 'deleted')"},
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
		{"ltrim(code, '0_') = 'active'", "ltrim(code, '0_') = 'active'"},
		{"rtrim(code, '_') IN ('active', 'pending')", "rtrim(code, '_') = 'active'"},
		{"btrim(code, 'x_') = 'active'", "btrim(code, 'x_') = 'active'"},
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
		{"ltrim(code, '0_') = 'active'", "ltrim(code, '_') = 'active'"},
		{"ltrim(code, '0_') = 'active'", "ltrim(code) = 'active'"},
		{"rtrim(code, '_') = 'active'", "rtrim(code, '-') = 'active'"},
		{"btrim(code, 'x_') = 'active'", "btrim(code, 'x') = 'active_'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesRepeatFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"repeat(code, 2) = 'activeactive'", "repeat(code, 2) = 'activeactive'"},
		{"repeat(code, 2) IN ('activeactive', 'pendingpending')", "repeat(code, 2) = 'activeactive'"},
		{"repeat(code, 0) = ''", "repeat(code, 0) = ''"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"repeat(code, 2) = 'activeactive'", "code = 'active'"},
		{"repeat(code, 2) = 'activeactive'", "repeat(code, 3) = 'activeactiveactive'"},
		{"repeat(code, 2) = 'activeactive'", "repeat(code, 2) = 'pendingpending'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesConcatFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"concat('acct-', code) = 'acct-active'", "concat('acct-', code) = 'acct-active'"},
		{"concat(prefix, '-', code) IN ('acct-active', 'acct-pending')", "concat(prefix, '-', code) = 'acct-active'"},
		{"concat(code, NULL) = 'active'", "concat(code, NULL) = 'active'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"concat('acct-', code) = 'acct-active'", "code = 'active'"},
		{"concat('acct-', code) = 'acct-active'", "concat('acct:', code) = 'acct:active'"},
		{"concat(prefix, '-', code) = 'acct-active'", "concat(prefix, code) = 'acctactive'"},
		{"concat(prefix, '-', code) = 'acct-active'", "concat(prefix, '-', code) = 'acct-pending'"},
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

func TestImpliesOctetLengthFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"octet_length(code) = 6", "octet_length(code) = 6"},
		{"octet_length(code) IN (6, 7)", "octet_length(code) = 6"},
		{"octet_length(code) IS NOT NULL", "octet_length(code) = 6"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"octet_length(code) = 6", "octet_length(code) = 7"},
		{"octet_length(code) = 6", "length(code) = 6"},
		{"octet_length(code) = 6", "char_length(code) = 6"},
		{"octet_length(code) IN (6, 7)", "octet_length(code) IN (6, 8)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesBitLengthFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"bit_length(code) = 24", "bit_length(code) = 24"},
		{"bit_length(code) IN (24, 32)", "bit_length(code) = 24"},
		{"bit_length(code) IS NOT NULL", "bit_length(code) = 24"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"bit_length(code) = 24", "bit_length(code) = 16"},
		{"bit_length(code) = 24", "octet_length(code) = 3"},
		{"bit_length(code) = 24", "length(code) = 3"},
		{"bit_length(code) IN (24, 32)", "bit_length(code) IN (24, 40)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesStrposFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"strpos(code, 'active') = 1", "strpos(code, 'active') = 1"},
		{"strpos(code, 'active') IN (1, 3)", "strpos(code, 'active') = 1"},
		{"strpos(code, 'active') IS NOT NULL", "strpos(code, 'active') = 1"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"strpos(code, 'active') = 1", "strpos(code, 'active') = 3"},
		{"strpos(code, 'active') = 1", "strpos(code, 'pending') = 1"},
		{"strpos(code, 'active') IN (1, 3)", "strpos(code, 'active') IN (1, 4)"},
		{"strpos(code, 'active') = 1", "code = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesStartsWithFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"starts_with(code, 'active')", "starts_with(code, 'active')"},
		{"starts_with(code, 'active')", "starts_with(code, 'active') = true"},
		{"starts_with(code, 'active') = true", "starts_with(code, 'active')"},
		{"starts_with(code, 'active') IS NOT NULL", "starts_with(code, 'active') = true"},
		{"starts_with(code, 'active') = false", "NOT starts_with(code, 'active')"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"starts_with(code, 'active')", "starts_with(code, 'pending')"},
		{"starts_with(code, 'active')", "starts_with(code, 'active') = false"},
		{"starts_with(code, 'active') = true", "code = 'active-a'"},
		{"starts_with(code, 'active') = false", "starts_with(code, 'active')"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesPrefixLikePredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"code LIKE 'active%'", "code LIKE 'active-a%'"},
		{"code LIKE 'active%'", "code = 'active-a'"},
		{"code LIKE 'active%'", "code IN ('active-a', 'active-b')"},
		{"lower(code) LIKE 'active%'", "lower(code) LIKE 'active-a%'"},
		{"code IS NOT NULL", "code LIKE 'active%'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"code LIKE 'active%'", "code LIKE 'pending%'"},
		{"code LIKE 'active%'", "code LIKE 'act_ve%'"},
		{"code LIKE 'active%'", "code LIKE '%active%'"},
		{"code LIKE 'active%'", "code = 'pending'"},
		{"code LIKE 'active%'", "lower(code) LIKE 'active%'"},
		{"code LIKE 'active%'", "code NOT LIKE 'pending%'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesLeftRightFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"left(code, 3) = 'act'", "left(code, 3) = 'act'"},
		{"left(code, 3) IN ('act', 'run')", "left(code, 3) = 'act'"},
		{"right(code, 2) IS NOT NULL", "right(code, 2) = 'ok'"},
		{"right(code, -1) = 'ctive'", "right(code, -1) = 'ctive'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"left(code, 3) = 'act'", "left(code, 3) = 'run'"},
		{"left(code, 3) = 'act'", "left(code, 4) = 'acti'"},
		{"left(code, 3) = 'act'", "right(code, 3) = 'act'"},
		{"right(code, 2) = 'ok'", "right(code, -2) = 'ok'"},
		{"left(code, 3) = 'act'", "code = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesReplaceFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"replace(code, '-', '') = 'activea'", "replace(code, '-', '') = 'activea'"},
		{"replace(code, '-', '') IN ('activea', 'pending')", "replace(code, '-', '') = 'activea'"},
		{"replace(code, '-', '') IS NOT NULL", "replace(code, '-', '') = 'activea'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"replace(code, '-', '') = 'activea'", "replace(code, '-', '') = 'pending'"},
		{"replace(code, '-', '') = 'activea'", "replace(code, '_', '') = 'activea'"},
		{"replace(code, '-', '') = 'activea'", "replace(code, '-', '_') = 'activea'"},
		{"replace(code, '-', '') = 'activea'", "code = 'active-a'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesTranslateFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"translate(code, '-_', '') = 'activea'", "translate(code, '-_', '') = 'activea'"},
		{"translate(code, '-_', '') IN ('activea', 'pending')", "translate(code, '-_', '') = 'activea'"},
		{"translate(code, '-_', '') IS NOT NULL", "translate(code, '-_', '') = 'activea'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"translate(code, '-_', '') = 'activea'", "translate(code, '-_', '') = 'pending'"},
		{"translate(code, '-_', '') = 'activea'", "translate(code, '-.', '') = 'activea'"},
		{"translate(code, '-_', '') = 'activea'", "translate(code, '-_', 'x') = 'activexa'"},
		{"translate(code, '-_', '') = 'activea'", "code = 'active-a'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesMd5FunctionPredicates(t *testing.T) {
	activeHash := "c76a5e84e4bdee527e274ea30c680d79"
	pendingHash := "7c6c2e5d48ab37a007cbf70d3ea25fa4"
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"md5(code) = '" + activeHash + "'", "md5(code) = '" + activeHash + "'"},
		{"md5(code) IN ('" + activeHash + "', '" + pendingHash + "')", "md5(code) = '" + activeHash + "'"},
		{"md5(code) IS NOT NULL", "md5(code) = '" + activeHash + "'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"md5(code) = '" + activeHash + "'", "md5(code) = '" + pendingHash + "'"},
		{"md5(code) = '" + activeHash + "'", "lower(code) = 'active'"},
		{"md5(code) = '" + activeHash + "'", "code = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesSplitPartFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"split_part(email, '@', 2) = 'example.com'", "split_part(email, '@', 2) = 'example.com'"},
		{"split_part(email, '@', 2) IN ('example.com', 'example.org')", "split_part(email, '@', 2) = 'example.com'"},
		{"split_part(email, '/', -1) IS NOT NULL", "split_part(email, '/', -1) = 'profile'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"split_part(email, '@', 2) = 'example.com'", "split_part(email, '@', 2) = 'example.org'"},
		{"split_part(email, '@', 2) = 'example.com'", "split_part(email, '.', 2) = 'com'"},
		{"split_part(email, '@', 2) = 'example.com'", "split_part(email, '@', 1) = 'first'"},
		{"split_part(email, '@', 2) = 'example.com'", "email = 'first@example.com'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesSubstringFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"substring(code, 1, 3) = 'Adm'", "substring(code, 1, 3) = 'Adm'"},
		{"substr(code, 1, 3) IN ('Adm', 'Alp')", "substring(code, 1, 3) = 'Adm'"},
		{"substring(code, 2) = 'dmin'", "substr(code, 2) = 'dmin'"},
		{"substring(code, 1, 3) IS NOT NULL", "substr(code, 1, 3) = 'Adm'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"substring(code, 1, 3) = 'Adm'", "substring(code, 1, 2) = 'Ad'"},
		{"substring(code, 1, 3) = 'Adm'", "substring(code, 2, 3) = 'dmi'"},
		{"substring(code, 2) = 'dmin'", "substr(code, 3) = 'min'"},
		{"substring(code, 1, 3) = 'Adm'", "code = 'Admin'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesPadFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"lpad(code, 6, '0') = '00ABCD'", "lpad(code, 6, '0') = '00ABCD'"},
		{"lpad(code, 6, '0') IN ('00ABCD', '000XYZ')", "lpad(code, 6, '0') = '00ABCD'"},
		{"lpad(code, 6) = '  ABCD'", "lpad(code, 6) = '  ABCD'"},
		{"lpad(code, 6, '0') IS NOT NULL", "lpad(code, 6, '0') = '00ABCD'"},
		{"rpad(code, 6, '_') = 'ABCD__'", "rpad(code, 6, '_') = 'ABCD__'"},
		{"rpad(code, 6, '_') IN ('ABCD__', 'XYZ___')", "rpad(code, 6, '_') = 'ABCD__'"},
		{"rpad(code, 6) = 'ABCD  '", "rpad(code, 6) = 'ABCD  '"},
		{"rpad(code, 6, '_') IS NOT NULL", "rpad(code, 6, '_') = 'ABCD__'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"lpad(code, 6, '0') = '00ABCD'", "lpad(code, 6, '_') = '__ABCD'"},
		{"lpad(code, 6, '0') = '00ABCD'", "lpad(code, 5, '0') = '0ABCD'"},
		{"lpad(code, 6, '0') = '00ABCD'", "rpad(code, 6, '0') = 'ABCD00'"},
		{"lpad(code, 6, '0') = '00ABCD'", "code = 'ABCD'"},
		{"rpad(code, 6, '_') = 'ABCD__'", "rpad(code, 6, '-') = 'ABCD--'"},
		{"rpad(code, 6, '_') = 'ABCD__'", "rpad(code, 5, '_') = 'ABCD_'"},
		{"rpad(code, 6, '_') = 'ABCD__'", "lpad(code, 6, '_') = '__ABCD'"},
		{"rpad(code, 6, '_') = 'ABCD__'", "code = 'ABCD'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesReverseFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"reverse(code) = 'nimdA'", "reverse(code) = 'nimdA'"},
		{"reverse(code) IN ('nimdA', 'ahplA')", "reverse(code) = 'nimdA'"},
		{"reverse(code) IS NOT NULL", "reverse(code) = 'nimdA'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"reverse(code) = 'nimdA'", "reverse(code) = 'larimdA'"},
		{"reverse(code) = 'nimdA'", "lower(code) = 'admin'"},
		{"reverse(code) = 'nimdA'", "code = 'Admin'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesToHexFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"to_hex(account_id) = 'a'", "to_hex(account_id) = 'a'"},
		{"to_hex(account_id) IN ('a', 'b')", "to_hex(account_id) = 'a'"},
		{"to_hex(account_id) IS NOT NULL", "to_hex(account_id) = 'a'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"to_hex(account_id) = 'a'", "to_hex(account_id) = 'b'"},
		{"to_hex(account_id) = 'a'", "abs(account_id) = 10"},
		{"to_hex(account_id) = 'a'", "account_id = 10"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesInitcapFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"initcap(role) = 'Admin User'", "initcap(role) = 'Admin User'"},
		{"initcap(role) IN ('Admin User', 'Billing User')", "initcap(role) = 'Admin User'"},
		{"initcap(role) IS NOT NULL", "initcap(role) = 'Admin User'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"initcap(role) = 'Admin User'", "initcap(role) = 'Billing User'"},
		{"initcap(role) = 'Admin User'", "lower(role) = 'admin user'"},
		{"initcap(role) = 'Admin User'", "role = 'admin user'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesQuoteLiteralFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"quote_literal(role) = '''admin user'''", "quote_literal(role) = '''admin user'''"},
		{"quote_literal(role) IN ('''admin user''', '''billing user''')", "quote_literal(role) = '''admin user'''"},
		{"quote_literal(role) IS NOT NULL", "quote_literal(role) = '''admin user'''"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"quote_literal(role) = '''admin user'''", "quote_literal(role) = '''billing user'''"},
		{"quote_literal(role) = '''admin user'''", "lower(role) = 'admin user'"},
		{"quote_literal(role) = '''admin user'''", "role = 'admin user'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesQuoteIdentFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"quote_ident(role) = '\"admin user\"'", "quote_ident(role) = '\"admin user\"'"},
		{"quote_ident(role) IN ('\"admin user\"', '\"billing user\"')", "quote_ident(role) = '\"admin user\"'"},
		{"quote_ident(role) IS NOT NULL", "quote_ident(role) = '\"admin user\"'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"quote_ident(role) = '\"admin user\"'", "quote_ident(role) = '\"billing user\"'"},
		{"quote_ident(role) = '\"admin user\"'", "lower(role) = 'admin user'"},
		{"quote_ident(role) = '\"admin user\"'", "role = 'admin user'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesAsciiFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"ascii(code) = 65", "ascii(code) = 65"},
		{"ascii(code) IN (65, 66)", "ascii(code) = 65"},
		{"ascii(code) IS NOT NULL", "ascii(code) = 65"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"ascii(code) = 65", "ascii(code) = 66"},
		{"ascii(code) = 65", "lower(code) = 'active'"},
		{"ascii(code) = 65", "code = 'Active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesCoalesceFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"coalesce(status, 'inactive') = 'active'", "coalesce(status, 'inactive') = 'active'"},
		{"coalesce(status, 'inactive') IS NOT NULL", "coalesce(status, 'inactive') = 'active'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"coalesce(status, 'inactive') = 'active'", "status = 'active'"},
		{"coalesce(status, 'inactive') = 'active'", "coalesce(status, 'inactive') = 'pending'"},
		{"coalesce(status, 'inactive') = 'active'", "coalesce(status, 'archived') = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesNullIfFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"nullif(status, '') = 'active'", "nullif(status, '') = 'active'"},
		{"nullif(status, '') IN ('active', 'pending')", "nullif(status, '') = 'active'"},
		{"nullif(status, '') IS NOT NULL", "nullif(status, '') = 'active'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"nullif(status, '') = 'active'", "status = 'active'"},
		{"nullif(status, '') = 'active'", "nullif(status, '') = 'pending'"},
		{"nullif(status, '') = 'active'", "nullif(status, 'inactive') = 'active'"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesArithmeticExpressionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"score + 1 = 8", "score + 1 = 8"},
		{"score - 1 = 6", "score - 1 = 6"},
		{"score * 2 = 14", "score * 2 = 14"},
		{"score + 1 = 8", "1 + score = 8"},
		{"score * 2 = 14", "2 * score = 14"},
		{"score + 1 IN (8, 9)", "score + 1 = 8"},
		{"score + 1 IS NOT NULL", "score + 1 = 8"},
		{"score + 1 = 8", "(tenant = 1) AND ((score + 1) = 8)"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"score + 1 = 8", "score = 7"},
		{"score + 1 = 8", "score + 2 = 8"},
		{"score + 1 = 8", "score - 1 = 8"},
		{"score - 1 = 6", "1 - score = 6"},
		{"score * 2 = 14", "score * 2 = 16"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesAbsFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"abs(delta) = 10", "abs(delta) = 10"},
		{"abs(delta) IN (10, 20)", "abs(delta) = 10"},
		{"abs(delta) IS NOT NULL", "abs(delta) = 10"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"abs(delta) = 10", "delta = 10"},
		{"abs(delta) = 10", "abs(delta) = 11"},
		{"abs(delta) IN (10, 20)", "abs(delta) IN (10, 30)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesSignFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"sign(delta) = 1", "sign(delta) = 1"},
		{"sign(delta) IN (-1, 1)", "sign(delta) = 1"},
		{"sign(delta) IS NOT NULL", "sign(delta) = 1"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"sign(delta) = 1", "delta > 0"},
		{"sign(delta) = 1", "sign(delta) = -1"},
		{"sign(delta) IN (-1, 1)", "sign(delta) IN (0, 1)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesGcdFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"gcd(width, height) = 4", "gcd(width, height) = 4"},
		{"gcd(width, height) IN (2, 4)", "gcd(width, height) = 4"},
		{"gcd(width, height) IS NOT NULL", "gcd(width, height) = 4"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"gcd(width, height) = 4", "width = 8 AND height = 12"},
		{"gcd(width, height) = 4", "gcd(height, width) = 4"},
		{"gcd(width, height) = 4", "gcd(width, height) = 3"},
		{"gcd(width, height) IN (2, 4)", "gcd(width, height) IN (4, 6)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesLcmFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"lcm(width, height) = 12", "lcm(width, height) = 12"},
		{"lcm(width, height) IN (12, 24)", "lcm(width, height) = 12"},
		{"lcm(width, height) IS NOT NULL", "lcm(width, height) = 12"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"lcm(width, height) = 12", "width = 3 AND height = 4"},
		{"lcm(width, height) = 12", "lcm(height, width) = 12"},
		{"lcm(width, height) = 12", "lcm(width, height) = 24"},
		{"lcm(width, height) IN (12, 24)", "lcm(width, height) IN (24, 36)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesModFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"mod(account_id, shard_count) = 1", "mod(account_id, shard_count) = 1"},
		{"mod(account_id, shard_count) IN (1, 2)", "mod(account_id, shard_count) = 1"},
		{"mod(account_id, shard_count) IS NOT NULL", "mod(account_id, shard_count) = 1"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"mod(account_id, shard_count) = 1", "account_id = 7 AND shard_count = 3"},
		{"mod(account_id, shard_count) = 1", "mod(shard_count, account_id) = 1"},
		{"mod(account_id, shard_count) = 1", "mod(account_id, shard_count) = 2"},
		{"mod(account_id, shard_count) IN (1, 2)", "mod(account_id, shard_count) IN (2, 3)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesChrFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"chr(codepoint) = 'A'", "chr(codepoint) = 'A'"},
		{"chr(codepoint) IN ('A', 'B')", "chr(codepoint) = 'A'"},
		{"chr(codepoint) IS NOT NULL", "chr(codepoint) = 'A'"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"chr(codepoint) = 'A'", "codepoint = 65"},
		{"chr(codepoint) = 'A'", "chr(codepoint) = 'B'"},
		{"chr(codepoint) IN ('A', 'B')", "chr(codepoint) IN ('A', 'C')"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesHashTextFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"hashtext(code) = -785388649", "hashtext(code) = -785388649"},
		{"hashtext(code) IN (-785388649, 1425101999)", "hashtext(code) = -785388649"},
		{"hashtext(code) IS NOT NULL", "hashtext(code) = -785388649"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"hashtext(code) = -785388649", "code = 'abc'"},
		{"hashtext(code) = -785388649", "hashtext(name) = -785388649"},
		{"hashtext(code) = -785388649", "hashtext(code) = 1425101999"},
		{"hashtext(code) IN (-785388649, 1425101999)", "hashtext(code) IN (1425101999, 0)"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesFloorCeilFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"floor(score) = 7", "floor(score) = 7"},
		{"floor(score) IN (7, 8)", "floor(score) = 7"},
		{"floor(score) IS NOT NULL", "floor(score) = 7"},
		{"ceil(score) = 7", "ceiling(score) = 7"},
		{"ceiling(score) = 7", "ceil(score) = 7"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"floor(score) = 7", "score = 7"},
		{"floor(score) = 7", "floor(other_score) = 7"},
		{"floor(score) = 7", "floor(score) = 8"},
		{"floor(score) IN (7, 8)", "floor(score) IN (8, 9)"},
		{"ceil(score) = 7", "floor(score) = 7"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}

func TestImpliesRoundTruncFunctionPredicates(t *testing.T) {
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"round(score) = 7", "round(score) = 7"},
		{"round(score) IN (7, 8)", "round(score) = 7"},
		{"round(score) IS NOT NULL", "round(score) = 7"},
		{"trunc(score) = 7", "trunc(score) = 7"},
	} {
		if !Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("expected %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
	for _, tt := range []struct {
		indexPredicate string
		queryPredicate string
	}{
		{"round(score) = 7", "score = 7"},
		{"round(score) = 7", "round(other_score) = 7"},
		{"round(score) = 7", "round(score) = 8"},
		{"round(score) IN (7, 8)", "round(score) IN (8, 9)"},
		{"trunc(score) = 7", "score = 7"},
		{"trunc(score) = 7", "round(score) = 7"},
	} {
		if Implies(tt.indexPredicate, tt.queryPredicate) {
			t.Fatalf("did not expect %q to imply %q", tt.queryPredicate, tt.indexPredicate)
		}
	}
}
