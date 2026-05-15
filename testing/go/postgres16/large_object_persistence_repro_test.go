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

// TestLargeObjectCreatePersistsMetadataRepro reproduces a large-object
// persistence bug: lo_create should create a large object and expose its OID
// through pg_largeobject_metadata.
func TestLargeObjectCreatePersistsMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_create persists large object metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lo_create(424242);`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectcreatepersistsmetadatarepro-0001-select-lo_create-424242"},
				},
				{
					Query: `SELECT oid::TEXT
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424242;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectcreatepersistsmetadatarepro-0002-select-oid::text-from-pg_catalog.pg_largeobject_metadata-where"},
				},
				{
					Query: `SELECT lo_unlink(424242);`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectcreatepersistsmetadatarepro-0003-select-lo_unlink-424242"},
				},
			},
		},
	})
}

// TestAlterLargeObjectOwnerReachesValidationRepro reproduces a large-object
// ownership parity gap: ALTER LARGE OBJECT should parse and reach large-object
// existence/ownership validation.
func TestAlterLargeObjectOwnerReachesValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER LARGE OBJECT OWNER reaches object validation",
			SetUpScript: []string{
				`CREATE ROLE large_object_owner_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER LARGE OBJECT 515151
						OWNER TO large_object_owner_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testalterlargeobjectownerreachesvalidationrepro-0001-alter-large-object-515151-owner",

						// TestAlterLargeObjectOwnerRequiresOwnershipRepro reproduces a security bug:
						// non-owners must not be able to transfer ownership of a large object.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestLargeObjectGrantPopulatesMetadataAclRepro reproduces a large-object
// privilege/catalog gap: GRANT SELECT ON LARGE OBJECT should persist in
// pg_largeobject_metadata.lomacl.
func TestLargeObjectGrantPopulatesMetadataAclRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "GRANT SELECT ON LARGE OBJECT populates metadata ACL",
			SetUpScript: []string{
				`CREATE ROLE large_object_acl_reader;`,
				`SELECT lo_create(424243);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `GRANT SELECT ON LARGE OBJECT 424243
						TO large_object_acl_reader;`,
				},
				{
					Query: `SELECT COALESCE(
							lomacl::TEXT LIKE '%large_object_acl_reader=r/%',
							false
						)::TEXT
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424243;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectgrantpopulatesmetadataaclrepro-0001-select-coalesce-lomacl::text-like-%large_object_acl_reader=r/%"},
				},
			},
		},
	})
}

// TestLargeObjectByteaRoundTripRepro reproduces a large-object persistence
// bug: lo_from_bytea should create a large object whose bytes are readable by
// lo_get through the returned OID.
func TestLargeObjectByteaRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object bytea round trip",
			SetUpScript: []string{
				`CREATE TABLE large_object_round_trip_stash (loid OID);`,
				`INSERT INTO large_object_round_trip_stash
					SELECT lo_from_bytea(0, decode('deadbeef', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT encode(lo_get(loid), 'hex')
						FROM large_object_round_trip_stash;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectbytearoundtriprepro-0001-select-encode-lo_get-loid-hex"},
				},
			},
		},
	})
}

// TestLargeObjectPutAndSlicedGetRepro reproduces a large-object persistence
// gap: PostgreSQL supports in-place byte writes with lo_put and offset/length
// reads with lo_get.
func TestLargeObjectPutAndSlicedGetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object lo_put and sliced lo_get",
			SetUpScript: []string{
				`CREATE TABLE large_object_put_stash (loid OID);`,
				`INSERT INTO large_object_put_stash
					SELECT lo_from_bytea(0, decode('001122334455', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT lo_put(loid, 2, decode('aabb', 'hex'))
						FROM large_object_put_stash;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectputandslicedgetrepro-0001-select-lo_put-loid-2-decode"},
				},
				{
					Query: `SELECT encode(lo_get(loid, 0, 6), 'hex')
						FROM large_object_put_stash;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectputandslicedgetrepro-0002-select-encode-lo_get-loid-0"},
				},
				{
					Query: `SELECT encode(lo_get(loid, 2, 2), 'hex')
						FROM large_object_put_stash;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectputandslicedgetrepro-0003-select-encode-lo_get-loid-2"},
				},
			},
		},
	})
}

// TestLargeObjectGetRequiresSelectPrivilegeRepro reproduces a security bug:
// lo_get should require ownership, SELECT on the large object, or superuser
// privileges when lo_compat_privileges is off.
func TestLargeObjectGetRequiresSelectPrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_get requires large-object SELECT privilege",
			SetUpScript: []string{
				`CREATE USER large_object_get_intruder PASSWORD 'pw';`,
				`SELECT lo_from_bytea(424244, decode('6869', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT encode(lo_get(424244), 'hex');`,

					Username: `large_object_get_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{

						// TestLargeObjectPutRequiresUpdatePrivilegeRepro reproduces a security bug:
						// lo_put should require ownership, UPDATE on the large object, or superuser
						// privileges when lo_compat_privileges is off.
						ID: "large-object-persistence-repro-test-testlargeobjectgetrequiresselectprivilegerepro-0001-select-encode-lo_get-424244-hex", Compare: "sqlstate"},
				},
			},
		},
	})
}

// TestLargeObjectCreateRollsBackRepro reproduces a transaction consistency bug:
// creating a large object inside a transaction must be undone by ROLLBACK.
func TestLargeObjectCreateRollsBackRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object creation rolls back",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:            `SELECT lo_from_bytea(424248, decode('01020304', 'hex'));`,
					SkipResultsCheck: true,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424248;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectcreaterollsbackrepro-0001-select-count-*-from-pg_catalog.pg_largeobject_metadata"},
				},
			},
		},
	})
}

// TestLargeObjectPutRollsBackRepro reproduces a transaction consistency bug:
// writes to large-object contents must be undone by ROLLBACK.
func TestLargeObjectPutRollsBackRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object writes roll back",
			SetUpScript: []string{
				`SELECT lo_from_bytea(424249, decode('00112233', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query:            `SELECT lo_put(424249, 1, decode('ff', 'hex'));`,
					SkipResultsCheck: true,
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT encode(lo_get(424249), 'hex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectputrollsbackrepro-0001-select-encode-lo_get-424249-hex"},
				},
			},
		},
	})
}

// TestLargeObjectUnlinkRollsBackRepro reproduces a transaction consistency bug:
// deleting a large object with lo_unlink must be undone by ROLLBACK.
func TestLargeObjectUnlinkRollsBackRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object unlink rolls back",
			SetUpScript: []string{
				`SELECT lo_from_bytea(424250, decode('aabbccdd', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SELECT lo_unlink(424250);`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectunlinkrollsbackrepro-0001-select-lo_unlink-424250"},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query: `SELECT encode(lo_get(424250), 'hex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectunlinkrollsbackrepro-0002-select-encode-lo_get-424250-hex"},
				},
			},
		},
	})
}

// TestLargeObjectCreateRollsBackToSavepointRepro reproduces a transaction
// consistency bug: large-object creation after a savepoint must be undone by
// ROLLBACK TO SAVEPOINT.
func TestLargeObjectCreateRollsBackToSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object creation rolls back to savepoint",
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT before_large_object_create;`,
				},
				{
					Query:            `SELECT lo_from_bytea(424251, decode('11223344', 'hex'));`,
					SkipResultsCheck: true,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT before_large_object_create;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT count(*)
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424251;`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectcreaterollsbacktosavepointrepro-0001-select-count-*-from-pg_catalog.pg_largeobject_metadata"},
				},
			},
		},
	})
}

// TestLargeObjectPutRollsBackToSavepointRepro reproduces a transaction
// consistency bug: writes to large-object contents after a savepoint must be
// undone by ROLLBACK TO SAVEPOINT.
func TestLargeObjectPutRollsBackToSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object writes roll back to savepoint",
			SetUpScript: []string{
				`SELECT lo_from_bytea(424252, decode('10203040', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT before_large_object_write;`,
				},
				{
					Query:            `SELECT lo_put(424252, 2, decode('ffee', 'hex'));`,
					SkipResultsCheck: true,
				},
				{
					Query: `ROLLBACK TO SAVEPOINT before_large_object_write;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT encode(lo_get(424252), 'hex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectputrollsbacktosavepointrepro-0001-select-encode-lo_get-424252-hex"},
				},
			},
		},
	})
}

// TestLargeObjectUnlinkRollsBackToSavepointRepro reproduces a transaction
// consistency bug: large-object deletion after a savepoint must be undone by
// ROLLBACK TO SAVEPOINT.
func TestLargeObjectUnlinkRollsBackToSavepointRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object unlink rolls back to savepoint",
			SetUpScript: []string{
				`SELECT lo_from_bytea(424253, decode('55667788', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `BEGIN;`,
				},
				{
					Query: `SAVEPOINT before_large_object_unlink;`,
				},
				{
					Query: `SELECT lo_unlink(424253);`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectunlinkrollsbacktosavepointrepro-0001-select-lo_unlink-424253"},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT before_large_object_unlink;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query: `SELECT encode(lo_get(424253), 'hex');`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectunlinkrollsbacktosavepointrepro-0002-select-encode-lo_get-424253-hex"},
				},
			},
		},
	})
}
