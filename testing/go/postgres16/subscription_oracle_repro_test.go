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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestSubscriptionAddPublicationRefreshRejectedWhenDisabledRepro pins
// PostgreSQL's metadata-only subscription boundary: ADD PUBLICATION still
// implies REFRESH unless refresh=false is requested, so disabled subscriptions
// must reject it even when copy_data=false.
func TestSubscriptionAddPublicationRefreshRejectedWhenDisabledRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "subscription add publication refresh rejected when disabled",
			SetUpScript: []string{
				"CREATE TABLE sub_disabled_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE PUBLICATION sub_disabled_pub FOR TABLE sub_disabled_items;",
				"CREATE SUBSCRIPTION sub_disabled_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION sub_disabled_pub WITH (connect = false, enabled = false, slot_name = NONE, create_slot = false);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER SUBSCRIPTION sub_disabled_sub ADD PUBLICATION sub_disabled_extra WITH (copy_data = false);", PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-oracle-repro-test-testsubscriptionaddpublicationrefreshrejectedwhendisabledrepro-0001-alter-subscription-sub_disabled_sub-add-publication", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestSubscriptionAddPublicationRefreshFalseUpdatesCatalog pins the valid
// metadata-only companion to the default-refresh rejection: a disabled
// subscription may add a publication when refresh=false is explicit.
func TestSubscriptionAddPublicationRefreshFalseUpdatesCatalog(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "subscription add publication refresh false updates catalog",
			SetUpScript: []string{
				"CREATE TABLE sub_refresh_false_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE TABLE sub_refresh_false_extra_items (tenant_id BIGINT PRIMARY KEY, label TEXT);",
				"CREATE PUBLICATION sub_refresh_false_pub FOR TABLE sub_refresh_false_items;",
				"CREATE PUBLICATION sub_refresh_false_extra FOR TABLE sub_refresh_false_extra_items;",
				"CREATE SUBSCRIPTION sub_refresh_false_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION sub_refresh_false_pub WITH (connect = false, enabled = false, slot_name = NONE, create_slot = false);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER SUBSCRIPTION sub_refresh_false_sub ADD PUBLICATION sub_refresh_false_extra WITH (copy_data = false, refresh = false);",
				},
				{
					Query: "SELECT array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'sub_refresh_false_sub';", PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-oracle-repro-test-testsubscriptionaddpublicationrefreshfalseupdatescatalog-0001-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}
