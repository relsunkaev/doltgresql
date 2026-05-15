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

					Username: `large_object_owner_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testalterlargeobjectownerrequiresownershiprepro-0001-alter-large-object-424255-owner", Compare: "sqlstate"},
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
					Query: `SELECT lo_put(424245, 1, decode('ff', 'hex'));`,

					Username: `large_object_put_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectputrequiresupdateprivilegerepro-0001-select-lo_put-424245-1-decode",

						// TestLargeObjectCompatPrivilegesRequiresSuperuserRepro reproduces a security
						// bug: lo_compat_privileges disables large-object ACL checks, so ordinary roles
						// must not be able to enable it for their session.
						Compare: "sqlstate"},
				},
				{
					Query:    `SELECT encode(lo_get(424245), 'hex');`,
					Expected: []sql.Row{{"00112233"}},
				},
			},
		},
	})
}

func TestLargeObjectCompatPrivilegesRequiresSuperuserRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_compat_privileges requires superuser",
			SetUpScript: []string{
				`CREATE USER large_object_compat_intruder PASSWORD 'pw';`,
				`SELECT lo_from_bytea(424256, decode('627970617373', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SET lo_compat_privileges = on;`,

					Username: `large_object_compat_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectcompatprivilegesrequiressuperuserrepro-0001-set-lo_compat_privileges-=-on", Compare: "sqlstate"},
				},
				{
					Query:       `SELECT encode(lo_get(424256), 'hex');`,
					ExpectedErr: `permission denied`,
					Username:    `large_object_compat_intruder`,
					Password:    `pw`,
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
					Query: `SELECT lo_unlink(424246);`,

					Username: `large_object_unlink_intruder`,
					Password: `pw`, PostgresOracle: ScriptTestPostgresOracle{ID: "large-object-persistence-repro-test-testlargeobjectunlinkrequiresownershiprepro-0001-select-lo_unlink-424246", Compare: "sqlstate"},
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
