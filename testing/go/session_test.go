package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestDiscard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Test discard",
			SetUpScript: []string{
				`CREATE temporary TABLE test (a INT)`,
				`insert into test values (1)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from test", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testdiscard-0001-select-*-from-test"},
				},
				{
					Query: "DISCARD ALL", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testdiscard-0002-discard-all"},
				},
				{
					Query: "select * from test", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testdiscard-0003-select-*-from-test", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Test discard errors",
			SetUpScript: []string{
				`CREATE temporary TABLE test (a INT)`,
				`insert into test values (1)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "DISCARD SEQUENCES", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testdiscard-0004-discard-sequences"},
				},
				{
					Query: "select * from test",
					Expected: []sql.Row{
						{1},
					},
				},
			},
		},
		{
			Name: "Test discard in transaction",
			SetUpScript: []string{
				`CREATE temporary TABLE test (a INT)`,
				`insert into test values (1)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN",
				},
				{
					Query: "DISCARD ALL",

					Skip: true, PostgresOracle: // not yet implemented
					ScriptTestPostgresOracle{ID: "session-test-testdiscard-0006-discard-all", Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestRollback(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Test rollback transaction",
			SetUpScript: []string{
				`BEGIN`,
				`CREATE temporary TABLE test (a INT)`,
				`insert into test values (1)`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "select * from test", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testrollback-0001-select-*-from-test"},
				},
				{
					Query: "ROLLBACK", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testrollback-0002-rollback"},
				},
				{
					Query:       "select * from test",
					ExpectedErr: "table not found",
					Skip:        true, // temp table should be dropped after ROLLBACK
				},
				{
					Query: "create temp table test (b int)",

					Skip: true, PostgresOracle: // temp table should be dropped after ROLLBACK
					ScriptTestPostgresOracle{ID: "session-test-testrollback-0004-create-temp-table-test-b"},
				},
			},
		},
	})
}

func TestSetTransaction(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Electric snapshot transaction setup",
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0001-begin"},
				},
				{
					Query: "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0002-set-transaction-isolation-level-repeatable"},
				},
				{
					Query: "SET TRANSACTION SNAPSHOT 'doltgres-snapshot-dgzero_0'", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0003-set-transaction-snapshot-doltgres-snapshot-dgzero_0", Compare: "sqlstate"},
				},
				{
					Query: `SELECT pg_current_snapshot(), pg_current_wal_lsn();`,
					Expected: []sql.Row{
						{"1:1:", "0/0"},
					},
				},
				{
					Query:    "COMMIT",
					Expected: []sql.Row{},
				},
			},
		},
		{
			Name: "Begin transaction serializable isolation mode",
			Assertions: []ScriptTestAssertion{
				{
					Query: "BEGIN ISOLATION LEVEL SERIALIZABLE", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0006-begin-isolation-level-serializable"},
				},
				{
					Query: "COMMIT", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0007-commit"},
				},
				{
					Query: "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0008-start-transaction-isolation-level-serializable"},
				},
				{
					Query: "COMMIT", PostgresOracle: ScriptTestPostgresOracle{ID: "session-test-testsettransaction-0009-commit"},
				},
			},
		},
	})
}
