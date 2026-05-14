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

package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestSubscriptionAddPublicationNamesAreCaseSensitiveRepro reproduces a
// subscription metadata correctness bug: PostgreSQL treats quoted publication
// names as case-sensitive strings, so adding a lower-case publication name does
// not duplicate an existing quoted mixed-case name.
func TestSubscriptionAddPublicationNamesAreCaseSensitiveRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION ADD PUBLICATION is case-sensitive",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_case_add_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION "CasePublication"
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_case_add_sub
						ADD PUBLICATION casepublication
						WITH (refresh = false);`,
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_case_add_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionaddpublicationnamesarecasesensitiverepro-0001-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionAddPublicationRefreshFalseRepro reproduces a subscription
// metadata correctness bug: PostgreSQL allows disabled subscriptions to add a
// publication when refresh=false is specified.
func TestSubscriptionAddPublicationRefreshFalseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION ADD PUBLICATION accepts refresh false",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_add_refresh_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION add_refresh_pub1
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_add_refresh_sub
						ADD PUBLICATION add_refresh_pub2
						WITH (refresh = false);`,
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_add_refresh_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionaddpublicationrefreshfalserepro-0001-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionSetPublicationRefreshFalseRepro reproduces a subscription
// metadata correctness bug: PostgreSQL allows disabled subscriptions to replace
// publication membership when refresh=false is specified.
func TestSubscriptionSetPublicationRefreshFalseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION SET PUBLICATION accepts refresh false",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_set_refresh_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION set_refresh_pub1
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_set_refresh_sub
						SET PUBLICATION set_refresh_pub2
						WITH (refresh = false);`,
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_set_refresh_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionsetpublicationrefreshfalserepro-0001-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionDropPublicationRefreshFalseRepro reproduces a subscription
// metadata correctness bug: PostgreSQL allows disabled subscriptions to drop a
// publication when refresh=false is specified.
func TestSubscriptionDropPublicationRefreshFalseRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION DROP PUBLICATION accepts refresh false",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_case_drop_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION "CasePublication", casepublication
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_case_drop_sub
						DROP PUBLICATION casepublication
						WITH (refresh = false);`,
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_case_drop_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptiondroppublicationrefreshfalserepro-0001-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionSetSlotNameNoneRequiresDisabledRepro reproduces a
// subscription metadata consistency bug: PostgreSQL refuses to disassociate an
// enabled subscription from its replication slot.
func TestSubscriptionSetSlotNameNoneRequiresDisabledRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION SET slot_name none requires disabled subscription",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_enabled_slot_guard
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION enabled_slot_pub
					WITH (connect = false, enabled = false, create_slot = false);`,
				`ALTER SUBSCRIPTION subscription_enabled_slot_guard ENABLE;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_enabled_slot_guard
						SET (slot_name = NONE);`,
					ExpectedErr: `enabled subscription`,
				},
				{
					Query: `SELECT subenabled, subslotname
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_enabled_slot_guard';`,
					Expected: []sql.Row{{"t", "subscription_enabled_slot_guard"}},
				},
			},
		},
	})
}

// TestSubscriptionCreatePreservesPublicationOrderRepro reproduces a
// subscription metadata correctness bug: PostgreSQL preserves the publication
// name order supplied by CREATE SUBSCRIPTION.
func TestSubscriptionCreatePreservesPublicationOrderRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION preserves publication order",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_create_order_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION z_pub_order, a_pub_order
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_create_order_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreatepreservespublicationorderrepro-0001-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsDuplicatePublicationRepro reproduces a
// subscription metadata correctness bug: PostgreSQL rejects duplicate
// publication names rather than silently compacting them.
func TestSubscriptionCreateRejectsDuplicatePublicationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects duplicate publications",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_duplicate_create_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION duplicate_pub, duplicate_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicatepublicationrepro-0001-create-subscription-subscription_duplicate_create_sub-connection-host=127.0.0.1",

						// TestSubscriptionCreateRejectsDuplicateConnectOptionRepro reproduces a
						// subscription option correctness bug: PostgreSQL rejects duplicate option
						// names instead of silently keeping one value.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSubscriptionCreateRejectsDuplicateConnectOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects duplicate connect options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_duplicate_connect_option_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION duplicate_connect_pub
						WITH (connect = false, connect = false,
							enabled = false, create_slot = false, slot_name = NONE);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicateconnectoptionrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_connect_option_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicateconnectoptionrepro-0002-select-count-*-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsDuplicateEnabledOptionRepro reproduces a
// subscription option correctness bug: PostgreSQL rejects duplicate enabled
// options before creating subscription metadata.
func TestSubscriptionCreateRejectsDuplicateEnabledOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects duplicate enabled options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_duplicate_enabled_option_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION duplicate_enabled_pub
						WITH (connect = false, enabled = false, enabled = false,
							create_slot = false, slot_name = NONE);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicateenabledoptionrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_enabled_option_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicateenabledoptionrepro-0002-select-count-*-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionAlterRejectsDuplicateSynchronousCommitOptionRepro reproduces
// a subscription option consistency bug: duplicate options in ALTER
// SUBSCRIPTION must be rejected before mutating subscription metadata.
func TestSubscriptionAlterRejectsDuplicateSynchronousCommitOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects duplicate synchronous_commit options",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_duplicate_sync_commit_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION duplicate_sync_commit_pub
					WITH (connect = false, enabled = false,
						create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_duplicate_sync_commit_sub
						SET (synchronous_commit = 'off',
							synchronous_commit = 'local');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsduplicatesynchronouscommitoptionrepro-0001-alter-subscription-subscription_duplicate_sync_commit_sub-set-synchronous_commit", Compare: "sqlstate"},
				},
				{
					Query: `SELECT subsynccommit
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_sync_commit_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsduplicatesynchronouscommitoptionrepro-0002-select-subsynccommit-from-pg_catalog.pg_subscription-where"},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsDuplicateSlotNameOptionRepro reproduces a
// subscription option correctness bug: PostgreSQL rejects duplicate slot_name
// options before creating subscription metadata.
func TestSubscriptionCreateRejectsDuplicateSlotNameOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects duplicate slot_name options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_duplicate_slot_name_create_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION duplicate_slot_name_create_pub
						WITH (connect = false, enabled = false, create_slot = false,
							slot_name = NONE, slot_name = 'kept_slot');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicateslotnameoptionrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_slot_name_create_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicateslotnameoptionrepro-0002-select-count-*-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionAlterRejectsDuplicateSlotNameOptionRepro reproduces a
// subscription option consistency bug: duplicate slot_name options in ALTER
// SUBSCRIPTION must be rejected before changing the stored replication slot.
func TestSubscriptionAlterRejectsDuplicateSlotNameOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects duplicate slot_name options",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_duplicate_slot_name_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION duplicate_slot_name_alter_pub
					WITH (connect = false, enabled = false, create_slot = false,
						slot_name = 'original_slot');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_duplicate_slot_name_alter_sub
						SET (slot_name = 'original_slot', slot_name = NONE);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsduplicateslotnameoptionrepro-0001-alter-subscription-set-slot_name-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT subslotname
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_slot_name_alter_sub';`,
					Expected: []sql.Row{{"original_slot"}},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsDuplicateSynchronousCommitOptionRepro
// reproduces a subscription option correctness bug: duplicate
// synchronous_commit options in CREATE SUBSCRIPTION must be rejected before
// subscription metadata is created.
func TestSubscriptionCreateRejectsDuplicateSynchronousCommitOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects duplicate synchronous_commit options",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_duplicate_sync_commit_create_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION duplicate_sync_commit_create_pub
						WITH (connect = false, enabled = false, create_slot = false,
							slot_name = NONE, synchronous_commit = 'off',
							synchronous_commit = 'local');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicatesynchronouscommitoptionrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres", Compare: "sqlstate"},
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_sync_commit_create_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsduplicatesynchronouscommitoptionrepro-0002-select-count-*-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionAlterRejectsDuplicateBinaryOptionRepro reproduces a
// subscription option consistency bug: duplicate binary options in ALTER
// SUBSCRIPTION must be rejected before mutating subscription metadata.
func TestSubscriptionAlterRejectsDuplicateBinaryOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects duplicate binary options",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_duplicate_binary_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION duplicate_binary_alter_pub
					WITH (connect = false, enabled = false, create_slot = false,
						slot_name = NONE, binary = false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_duplicate_binary_alter_sub
						SET (binary = false, binary = true);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsduplicatebinaryoptionrepro-0001-alter-subscription-subscription_duplicate_binary_alter_sub-set-binary", Compare: "sqlstate"},
				},
				{
					Query: `SELECT subbinary
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_duplicate_binary_alter_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsduplicatebinaryoptionrepro-0002-select-subbinary-from-pg_catalog.pg_subscription-where"},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsEmptySlotNameRepro reproduces a subscription
// metadata validation bug: PostgreSQL rejects an empty replication slot name.
func TestSubscriptionCreateRejectsEmptySlotNameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects empty slot name",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_empty_slot_create_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION empty_slot_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = '');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsemptyslotnamerepro-0001-create-subscription-subscription_empty_slot_create_sub-connection-host=127.0.0.1",

						// TestSubscriptionAlterRejectsEmptySlotNameRepro reproduces a subscription
						// metadata validation bug: PostgreSQL rejects an empty replication slot name
						// and preserves the existing slot association.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSubscriptionAlterRejectsEmptySlotNameRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects empty slot name",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_empty_slot_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION empty_slot_pub
					WITH (connect = false, enabled = false, create_slot = false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_empty_slot_alter_sub
						SET (slot_name = '');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsemptyslotnamerepro-0001-alter-subscription-subscription_empty_slot_alter_sub-set-slot_name", Compare: "sqlstate"},
				},
				{
					Query: `SELECT subslotname
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_empty_slot_alter_sub';`,
					Expected: []sql.Row{{"subscription_empty_slot_alter_sub"}},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsInvalidStreamingOptionRepro reproduces a
// subscription option validation bug: PostgreSQL rejects streaming values that
// are neither boolean nor "parallel".
func TestSubscriptionCreateRejectsInvalidStreamingOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects invalid streaming option",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_invalid_streaming_create_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION invalid_streaming_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, streaming = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsinvalidstreamingoptionrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres",

						// TestSubscriptionAlterRejectsInvalidStreamingOptionRepro reproduces a
						// subscription option validation bug: PostgreSQL rejects invalid streaming
						// values and preserves the stored streaming flag.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSubscriptionAlterRejectsInvalidStreamingOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects invalid streaming option",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_invalid_streaming_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION invalid_streaming_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, streaming = true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_invalid_streaming_alter_sub
						SET (streaming = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsinvalidstreamingoptionrepro-0001-alter-subscription-subscription_invalid_streaming_alter_sub-set-streaming", Compare: "sqlstate"},
				},
				{
					Query: `SELECT substream
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_invalid_streaming_alter_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsinvalidstreamingoptionrepro-0002-select-substream-from-pg_catalog.pg_subscription-where"},
				},
			},
		},
	})
}

// TestSubscriptionAlterRejectsInvalidSynchronousCommitRepro reproduces a
// subscription option validation bug: PostgreSQL validates synchronous_commit
// against the allowed enum values.
func TestSubscriptionAlterRejectsInvalidSynchronousCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects invalid synchronous_commit",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_invalid_synccommit_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION invalid_synccommit_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, synchronous_commit = 'remote_apply');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_invalid_synccommit_alter_sub
						SET (synchronous_commit = 'definitely_invalid');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsinvalidsynchronouscommitrepro-0001-alter-subscription-set-synchronous_commit-=", Compare: "sqlstate"},
				},
				{
					Query: `SELECT subsynccommit
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_invalid_synccommit_alter_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectsinvalidsynchronouscommitrepro-0002-select-subsynccommit-from-pg_catalog.pg_subscription-where"},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsInvalidSynchronousCommitRepro reproduces a
// subscription option validation bug: PostgreSQL validates synchronous_commit
// during CREATE SUBSCRIPTION as well as ALTER SUBSCRIPTION.
func TestSubscriptionCreateRejectsInvalidSynchronousCommitRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects invalid synchronous_commit",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_invalid_synccommit_create_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION invalid_synccommit_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, synchronous_commit = 'definitely_invalid');`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsinvalidsynchronouscommitrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres",

						// TestSubscriptionAlterRejectsTwoPhaseOptionRepro reproduces a subscription
						// metadata validation bug: PostgreSQL does not allow altering the two_phase
						// subscription parameter on this baseline.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSubscriptionAlterRejectsTwoPhaseOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION rejects two_phase option",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_two_phase_alter_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION two_phase_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, two_phase = true);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_two_phase_alter_sub
						SET (two_phase = false);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectstwophaseoptionrepro-0001-alter-subscription-subscription_two_phase_alter_sub-set-two_phase", Compare: "sqlstate"},
				},
				{
					Query: `SELECT subtwophasestate
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_two_phase_alter_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionalterrejectstwophaseoptionrepro-0002-select-subtwophasestate-from-pg_catalog.pg_subscription-where"},
				},
			},
		},
	})
}

// TestSubscriptionSkipLsnNoneClearsSkipLsnRepro reproduces a subscription
// metadata correctness bug: PostgreSQL accepts SKIP (lsn = NONE) and clears the
// stored skip LSN.
func TestSubscriptionSkipLsnNoneClearsSkipLsnRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION SKIP lsn none clears skip lsn",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_skip_lsn_none_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION skip_lsn_pub
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
				`ALTER SUBSCRIPTION subscription_skip_lsn_none_sub
					SKIP (lsn = '0/12345');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_skip_lsn_none_sub
						SKIP (lsn = NONE);`,
				},
				{
					Query: `SELECT subskiplsn::text
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_skip_lsn_none_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionskiplsnnoneclearsskiplsnrepro-0001-select-subskiplsn::text-from-pg_catalog.pg_subscription-where"},
				},
			},
		},
	})
}

// TestSubscriptionAddPublicationRejectsInvalidCopyDataRepro reproduces a
// subscription option validation bug: PostgreSQL rejects non-boolean copy_data
// and leaves the publication list unchanged.
func TestSubscriptionAddPublicationRejectsInvalidCopyDataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION ADD PUBLICATION rejects invalid copy_data",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_add_copy_data_invalid_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION add_copy_data_pub1
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_add_copy_data_invalid_sub
						ADD PUBLICATION add_copy_data_pub2
						WITH (copy_data = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionaddpublicationrejectsinvalidcopydatarepro-0001-alter-subscription-subscription_add_copy_data_invalid_sub-add-publication", Compare: "sqlstate"},
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_add_copy_data_invalid_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionaddpublicationrejectsinvalidcopydatarepro-0002-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionSetPublicationRejectsInvalidCopyDataRepro reproduces a
// subscription option validation bug: PostgreSQL rejects non-boolean copy_data
// and preserves the previous publication list.
func TestSubscriptionSetPublicationRejectsInvalidCopyDataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION SET PUBLICATION rejects invalid copy_data",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_set_copy_data_invalid_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION set_copy_data_pub1
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_set_copy_data_invalid_sub
						SET PUBLICATION set_copy_data_pub2
						WITH (copy_data = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionsetpublicationrejectsinvalidcopydatarepro-0001-alter-subscription-subscription_set_copy_data_invalid_sub-set-publication", Compare: "sqlstate"},
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_set_copy_data_invalid_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionsetpublicationrejectsinvalidcopydatarepro-0002-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionDropPublicationRejectsInvalidCopyDataRepro reproduces a
// subscription option validation bug: PostgreSQL rejects non-boolean copy_data
// and preserves the previous publication list.
func TestSubscriptionDropPublicationRejectsInvalidCopyDataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION DROP PUBLICATION rejects invalid copy_data",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_drop_copy_data_invalid_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION drop_copy_data_pub1, drop_copy_data_pub2
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_drop_copy_data_invalid_sub
						DROP PUBLICATION drop_copy_data_pub2
						WITH (copy_data = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptiondroppublicationrejectsinvalidcopydatarepro-0001-alter-subscription-subscription_drop_copy_data_invalid_sub-drop-publication", Compare: "sqlstate"},
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_drop_copy_data_invalid_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptiondroppublicationrejectsinvalidcopydatarepro-0002-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionSetPublicationRejectsDuplicatePublicationRepro reproduces a
// subscription metadata correctness bug: PostgreSQL rejects duplicate
// publication names in ALTER SUBSCRIPTION SET PUBLICATION before replacing the
// stored publication list.
func TestSubscriptionSetPublicationRejectsDuplicatePublicationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER SUBSCRIPTION SET PUBLICATION rejects duplicate publications",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_set_duplicate_pub_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION set_dup_original
					WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER SUBSCRIPTION subscription_set_duplicate_pub_sub
						SET PUBLICATION set_dup_pub, set_dup_pub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionsetpublicationrejectsduplicatepublicationrepro-0001-alter-subscription-subscription_set_duplicate_pub_sub-set-publication", Compare: "sqlstate"},
				},
				{
					Query: `SELECT array_to_string(subpublications, ',')
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_set_duplicate_pub_sub';`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptionsetpublicationrejectsduplicatepublicationrepro-0002-select-array_to_string-subpublications-from-pg_catalog.pg_subscription"},
				},
			},
		},
	})
}

// TestSubscriptionCreateRejectsInvalidOriginOptionRepro reproduces a
// subscription option validation bug: PostgreSQL rejects unknown origin values.
func TestSubscriptionCreateRejectsInvalidOriginOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects invalid origin option",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_invalid_origin_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION invalid_origin_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, origin = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsinvalidoriginoptionrepro-0001-create-subscription-subscription_invalid_origin_sub-connection-host=127.0.0.1",

						// TestSubscriptionCreateRejectsInvalidPasswordRequiredOptionRepro reproduces a
						// subscription option validation bug: PostgreSQL validates password_required as
						// a boolean subscription option.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSubscriptionCreateRejectsInvalidPasswordRequiredOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects invalid password_required option",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_invalid_password_required_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION invalid_password_required_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, password_required = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsinvalidpasswordrequiredoptionrepro-0001-create-subscription-connection-host=127.0.0.1-dbname=postgres",

						// TestSubscriptionCreateRejectsInvalidRunAsOwnerOptionRepro reproduces a
						// subscription option validation bug: PostgreSQL validates run_as_owner as a
						// boolean subscription option.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestSubscriptionCreateRejectsInvalidRunAsOwnerOptionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SUBSCRIPTION rejects invalid run_as_owner option",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE SUBSCRIPTION subscription_invalid_run_as_owner_sub
						CONNECTION 'host=127.0.0.1 dbname=postgres'
						PUBLICATION invalid_run_as_owner_pub
						WITH (connect = false, enabled = false, create_slot = false, slot_name = NONE, run_as_owner = definitely_invalid);`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testsubscriptioncreaterejectsinvalidrunasowneroptionrepro-0001-create-subscription-subscription_invalid_run_as_owner_sub-connection-host=127.0.0.1",

						// TestDropSubscriptionWithSlotInsideTransactionRejectedRepro reproduces a
						// subscription metadata consistency bug: PostgreSQL refuses to drop a
						// slot-backed subscription inside a transaction block.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestDropSubscriptionWithSlotInsideTransactionRejectedRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SUBSCRIPTION with slot is rejected inside transaction",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_drop_xact_slot_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION drop_xact_pub
					WITH (connect = false, enabled = false, create_slot = false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `DROP SUBSCRIPTION subscription_drop_xact_slot_sub;`, PostgresOracle: ScriptTestPostgresOracle{ID: "subscription-correctness-repro-test-testdropsubscriptionwithslotinsidetransactionrejectedrepro-0001-drop-subscription-subscription_drop_xact_slot_sub", Compare: "sqlstate"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT subslotname
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_drop_xact_slot_sub';`,
					Expected: []sql.Row{{"subscription_drop_xact_slot_sub"}},
				},
			},
		},
	})
}

// TestDropSubscriptionWithMissingRemoteSlotPreservesCatalogRepro reproduces a
// subscription metadata consistency bug: PostgreSQL keeps the subscription row
// when dropping the remote replication slot fails.
func TestDropSubscriptionWithMissingRemoteSlotPreservesCatalogRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP SUBSCRIPTION preserves catalog when remote slot drop fails",
			SetUpScript: []string{
				`CREATE SUBSCRIPTION subscription_drop_missing_slot_sub
					CONNECTION 'host=127.0.0.1 dbname=postgres'
					PUBLICATION drop_missing_slot_pub
					WITH (connect = false, enabled = false, create_slot = false);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `DROP SUBSCRIPTION subscription_drop_missing_slot_sub;`,
					ExpectedErr: `replication slot`,
				},
				{
					Query: `SELECT subslotname
						FROM pg_catalog.pg_subscription
						WHERE subname = 'subscription_drop_missing_slot_sub';`,
					Expected: []sql.Row{{"subscription_drop_missing_slot_sub"}},
				},
			},
		},
	})
}
