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

package schema_config

import (
	"os"
	"strings"
	"testing"
)

func TestGenerateSamplesCustomerMappings(t *testing.T) {
	opts := DefaultOptions()
	opts.DoltgresShardPorts = consecutivePorts(15438, 4)
	opts.CustomerIDs = consecutiveCustomerIDs(1, 32)

	generated, err := Generate(opts)
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}

	expectedMappings := map[int64]int{
		1:  1,
		2:  2,
		4:  4,
		5:  1,
		32: 4,
	}
	for customerID, expectedShard := range expectedMappings {
		if actual := generated.CustomerToShard[customerID]; actual != expectedShard {
			t.Fatalf("customer %d mapped to shard %d, expected %d", customerID, actual, expectedShard)
		}
	}

	if got := strings.Count(generated.PgDogTOML, "[[databases]]"); got != 5 {
		t.Fatalf("expected source plus 4 Doltgres database entries, got %d", got)
	}
	if got := strings.Count(generated.PgDogTOML, "[[sharded_mappings]]"); got != 5 {
		t.Fatalf("expected 4 list mappings plus default mapping, got %d", got)
	}
	requireContains(t, generated.PgDogTOML, "cross_shard_disabled = true")
	requireContains(t, generated.PgDogTOML, "name = \"shared\"")
	requireContains(t, generated.PgDogTOML, "schema = \"customer\"")
	requireContains(t, generated.PgDogTOML, "values = [1, 5, 9, 13, 17, 21, 25, 29]")
	requireContains(t, generated.PgDogTOML, "kind = \"default\"\nshard = 0")
	if strings.Contains(generated.PgDogTOML, "[[sharded_schemas]]\ndatabase = \"pgdog\"\nshard = 0") {
		t.Fatalf("generated config must not include unnamed default sharded_schemas")
	}
}

func TestGenerateOneDatabasePerCustomerShape(t *testing.T) {
	opts := DefaultOptions()
	opts.DoltgresShardPorts = consecutivePorts(16400, 8)
	opts.CustomerIDs = consecutiveCustomerIDs(100, 8)

	generated, err := Generate(opts)
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}

	for _, customerID := range opts.CustomerIDs {
		expectedShard := int(customerID - 99)
		if actual := generated.CustomerToShard[customerID]; actual != expectedShard {
			t.Fatalf("customer %d mapped to shard %d, expected %d", customerID, actual, expectedShard)
		}
		if customers := generated.ShardToCustomers[expectedShard]; len(customers) != 1 || customers[0] != customerID {
			t.Fatalf("shard %d customers = %v, expected only %d", expectedShard, customers, customerID)
		}
	}
}

func TestGenerateRejectsInvalidShapes(t *testing.T) {
	opts := DefaultOptions()
	opts.DoltgresShardPorts = []int{15438}
	opts.CustomerIDs = []int64{42, 42}
	if _, err := Generate(opts); err == nil || !strings.Contains(err.Error(), "duplicate customer ID") {
		t.Fatalf("expected duplicate customer ID error, got %v", err)
	}

	opts.CustomerIDs = []int64{42}
	opts.DoltgresShardPorts = []int{15437}
	if _, err := Generate(opts); err == nil || !strings.Contains(err.Error(), "duplicate backend port") {
		t.Fatalf("expected duplicate backend port error, got %v", err)
	}
}

func TestGenerateStressShape(t *testing.T) {
	customerCount := 512
	shardCount := 32
	if os.Getenv("PGDOG_SCHEMA_CONFIG_STRESS") != "" {
		customerCount = 10000
		shardCount = 512
	}

	opts := DefaultOptions()
	opts.DoltgresShardPorts = consecutivePorts(20000, shardCount)
	opts.CustomerIDs = consecutiveCustomerIDs(1, customerCount)

	generated, err := Generate(opts)
	if err != nil {
		t.Fatalf("Generate returned error: %v", err)
	}
	if got := len(generated.CustomerToShard); got != customerCount {
		t.Fatalf("expected %d customer mappings, got %d", customerCount, got)
	}
	if got := len(generated.ShardToCustomers); got != shardCount {
		t.Fatalf("expected %d shard mapping groups, got %d", shardCount, got)
	}

	samples := []int64{1, int64(shardCount), int64(shardCount + 1), int64(customerCount)}
	for _, customerID := range samples {
		expectedShard := 1 + int((customerID-1)%int64(shardCount))
		if actual := generated.CustomerToShard[customerID]; actual != expectedShard {
			t.Fatalf("customer %d mapped to shard %d, expected %d", customerID, actual, expectedShard)
		}
	}
}

func consecutivePorts(start int, count int) []int {
	ports := make([]int, count)
	for i := range ports {
		ports[i] = start + i
	}
	return ports
}

func consecutiveCustomerIDs(start int64, count int) []int64 {
	customerIDs := make([]int64, count)
	for i := range customerIDs {
		customerIDs[i] = start + int64(i)
	}
	return customerIDs
}

func requireContains(t *testing.T, haystack string, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("expected generated config to contain %q", needle)
	}
}
