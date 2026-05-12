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
	"os"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
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
					Query:    `SELECT lo_create(424242);`,
					Expected: []sql.Row{{"424242"}},
				},
				{
					Query: `SELECT oid::TEXT
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424242;`,
					Expected: []sql.Row{{"424242"}},
				},
				{
					Query:    `SELECT lo_unlink(424242);`,
					Expected: []sql.Row{{1}},
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
						OWNER TO large_object_owner_target;`,
					ExpectedErr: `large object`,
				},
			},
		},
	})
}

// TestAlterLargeObjectOwnerRequiresOwnershipRepro reproduces a security bug:
// non-owners must not be able to transfer ownership of a large object.
func TestAlterLargeObjectOwnerRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER LARGE OBJECT OWNER TO requires ownership",
			SetUpScript: []string{
				`CREATE USER large_object_owner_intruder PASSWORD 'pw';`,
				`CREATE ROLE large_object_owner_hijack_target;`,
				`SELECT lo_from_bytea(424255, decode('0102', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER LARGE OBJECT 424255
						OWNER TO large_object_owner_hijack_target;`,
					ExpectedErr: `permission denied`,
					Username:    `large_object_owner_intruder`,
					Password:    `pw`,
				},
				{
					Query: `SELECT pg_get_userbyid(lomowner)
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424255;`,
					Expected: []sql.Row{{"postgres"}},
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
						WHERE oid = 424243;`,
					Expected: []sql.Row{{"true"}},
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
						FROM large_object_round_trip_stash;`,
					Expected: []sql.Row{{"deadbeef"}},
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
						FROM large_object_put_stash;`,
					Expected: []sql.Row{{""}},
				},
				{
					Query: `SELECT encode(lo_get(loid, 0, 6), 'hex')
						FROM large_object_put_stash;`,
					Expected: []sql.Row{{"0011aabb4455"}},
				},
				{
					Query: `SELECT encode(lo_get(loid, 2, 2), 'hex')
						FROM large_object_put_stash;`,
					Expected: []sql.Row{{"aabb"}},
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
					Query:       `SELECT encode(lo_get(424244), 'hex');`,
					ExpectedErr: `permission denied`,
					Username:    `large_object_get_intruder`,
					Password:    `pw`,
				},
			},
		},
	})
}

// TestLargeObjectPutRequiresUpdatePrivilegeRepro reproduces a security bug:
// lo_put should require ownership, UPDATE on the large object, or superuser
// privileges when lo_compat_privileges is off.
func TestLargeObjectPutRequiresUpdatePrivilegeRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_put requires large-object UPDATE privilege",
			SetUpScript: []string{
				`CREATE USER large_object_put_intruder PASSWORD 'pw';`,
				`SELECT lo_from_bytea(424245, decode('00112233', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT lo_put(424245, 1, decode('ff', 'hex'));`,
					ExpectedErr: `permission denied`,
					Username:    `large_object_put_intruder`,
					Password:    `pw`,
				},
				{
					Query:    `SELECT encode(lo_get(424245), 'hex');`,
					Expected: []sql.Row{{"00112233"}},
				},
			},
		},
	})
}

// TestLargeObjectUnlinkRequiresOwnershipRepro reproduces a security bug:
// lo_unlink should require ownership or superuser privileges.
func TestLargeObjectUnlinkRequiresOwnershipRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_unlink requires large-object ownership",
			SetUpScript: []string{
				`CREATE USER large_object_unlink_intruder PASSWORD 'pw';`,
				`SELECT lo_from_bytea(424246, decode('aabbccdd', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT lo_unlink(424246);`,
					ExpectedErr: `permission denied`,
					Username:    `large_object_unlink_intruder`,
					Password:    `pw`,
				},
				{
					Query: `SELECT oid::TEXT
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424246;`,
					Expected: []sql.Row{{"424246"}},
				},
			},
		},
	})
}

// TestLargeObjectSurvivesServerRestartRepro reproduces a persistence bug:
// PostgreSQL large objects are durable database objects, so their metadata and
// contents should survive a server restart.
func TestLargeObjectSurvivesServerRestartRepro(t *testing.T) {
	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dbDir)
	})

	port, err := sql.GetEmptyPort()
	require.NoError(t, err)

	ctx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	_, err = conn.Exec(ctx, `SELECT lo_from_bytea(424247, decode('cafebabe', 'hex'));`)
	require.NoError(t, err)
	conn.Close(ctx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	ctx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	defer func() {
		conn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	var oid string
	var contents string
	err = conn.Current.QueryRow(ctx, `SELECT oid::TEXT, encode(lo_get(oid), 'hex')
		FROM pg_catalog.pg_largeobject_metadata
		WHERE oid = 424247;`).Scan(&oid, &contents)
	require.NoError(t, err, "large-object metadata and contents should survive server restart")
	require.Equal(t, "424247", oid)
	require.Equal(t, "cafebabe", contents)
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
						WHERE oid = 424248;`,
					Expected: []sql.Row{{int64(0)}},
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
					Query:    `SELECT encode(lo_get(424249), 'hex');`,
					Expected: []sql.Row{{"00112233"}},
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
					Query:    `SELECT lo_unlink(424250);`,
					Expected: []sql.Row{{int32(1)}},
				},
				{
					Query: `ROLLBACK;`,
				},
				{
					Query:    `SELECT encode(lo_get(424250), 'hex');`,
					Expected: []sql.Row{{"aabbccdd"}},
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
						WHERE oid = 424251;`,
					Expected: []sql.Row{{int64(0)}},
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
					Query:    `SELECT encode(lo_get(424252), 'hex');`,
					Expected: []sql.Row{{"10203040"}},
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
					Query:    `SELECT lo_unlink(424253);`,
					Expected: []sql.Row{{int32(1)}},
				},
				{
					Query: `ROLLBACK TO SAVEPOINT before_large_object_unlink;`,
				},
				{
					Query: `COMMIT;`,
				},
				{
					Query:    `SELECT encode(lo_get(424253), 'hex');`,
					Expected: []sql.Row{{"55667788"}},
				},
			},
		},
	})
}

// TestLargeObjectRegistryIsDatabaseLocalRepro reproduces a data isolation bug:
// PostgreSQL large objects live in one database's catalogs and must not be
// visible from another database in the same server.
func TestLargeObjectRegistryIsDatabaseLocalRepro(t *testing.T) {
	port, err := sql.GetEmptyPort()
	require.NoError(t, err)
	ctx, adminConn, controller := CreateServerWithPort(t, "postgres", port)
	defer func() {
		adminConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	dbOne := newTestDatabaseConnection(t, ctx, "large_object_db_one", serverHost, port)
	defer dbOne.Close(ctx)
	dbTwo := newTestDatabaseConnection(t, ctx, "large_object_db_two", serverHost, port)
	defer dbTwo.Close(ctx)

	_, err = dbOne.Exec(ctx, `SELECT lo_from_bytea(424254, decode('abcdef', 'hex'));`)
	require.NoError(t, err)

	var visibleInOtherDB int64
	err = dbTwo.Current.QueryRow(ctx, `SELECT count(*)
		FROM pg_catalog.pg_largeobject_metadata
		WHERE oid = 424254;`).Scan(&visibleInOtherDB)
	require.NoError(t, err)
	require.EqualValues(t, 0, visibleInOtherDB, "large objects should be database-local")
}
