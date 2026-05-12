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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCopyFromStdinBadRowIsStatementAtomicRepro guards PostgreSQL COPY FROM
// STDIN atomicity: if a later input row cannot be coerced, no earlier rows from
// the COPY statement should persist.
func TestCopyFromStdinBadRowIsStatementAtomicRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_bad_row_items (
		id INT PRIMARY KEY,
		v INT
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\tnot_an_int\n"),
		`COPY copy_stdin_bad_row_items (id, v) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the malformed row; tag=%s", tag.String())

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_bad_row_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinExplicitTransactionRollbackRepro reproduces a COPY
// transaction consistency bug: COPY FROM STDIN inside an explicit transaction
// must be rolled back by the surrounding ROLLBACK.
func TestCopyFromStdinExplicitTransactionRollbackRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_tx_rollback_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	_, err = connection.Default.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\trolled-back\n2\talso-rolled-back\n"),
		`COPY copy_stdin_tx_rollback_items (id, label) FROM STDIN;`,
	)
	require.NoError(t, err, "COPY FROM should succeed inside the explicit transaction; tag=%s", tag.String())
	_, err = connection.Default.Exec(ctx, `ROLLBACK;`)
	require.NoError(t, err)

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_tx_rollback_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromServerFileExplicitTransactionRollbackRepro reproduces a COPY
// transaction consistency bug: COPY FROM a server-side file inside an explicit
// transaction must be rolled back by the surrounding ROLLBACK.
func TestCopyFromServerFileExplicitTransactionRollbackRepro(t *testing.T) {
	copyPath := filepath.Join(t.TempDir(), "copy_server_file_tx.csv")
	require.NoError(t, os.WriteFile(copyPath, []byte("1,rolled-back\n2,also-rolled-back\n"), 0644))
	escapedPath := strings.ReplaceAll(copyPath, "'", "''")

	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_server_file_tx_rollback_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	_, err = connection.Default.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, fmt.Sprintf(
		`COPY copy_server_file_tx_rollback_items (id, label) FROM '%s' WITH (FORMAT CSV);`,
		escapedPath,
	))
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `ROLLBACK;`)
	require.NoError(t, err)

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_server_file_tx_rollback_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinSavepointRollbackRepro reproduces a COPY transaction
// consistency bug: COPY FROM STDIN after a savepoint must be discarded by
// ROLLBACK TO SAVEPOINT while preserving earlier transaction work.
func TestCopyFromStdinSavepointRollbackRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_savepoint_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	_, err = connection.Default.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `INSERT INTO copy_stdin_savepoint_items VALUES (1, 'kept');`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `SAVEPOINT copy_sp;`)
	require.NoError(t, err)
	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("2\trolled-back\n3\talso-rolled-back\n"),
		`COPY copy_stdin_savepoint_items (id, label) FROM STDIN;`,
	)
	require.NoError(t, err, "COPY FROM should succeed after the savepoint; tag=%s", tag.String())
	_, err = connection.Default.Exec(ctx, `ROLLBACK TO SAVEPOINT copy_sp;`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `COMMIT;`)
	require.NoError(t, err)

	var ids string
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT string_agg(id::text, ',' ORDER BY id) FROM copy_stdin_savepoint_items;`,
	).Scan(&ids))
	require.Equal(t, "1", ids)
}

// TestCopyFromServerFileSavepointRollbackRepro reproduces a COPY transaction
// consistency bug: COPY FROM a server-side file after a savepoint must be
// discarded by ROLLBACK TO SAVEPOINT while preserving earlier transaction work.
func TestCopyFromServerFileSavepointRollbackRepro(t *testing.T) {
	copyPath := filepath.Join(t.TempDir(), "copy_server_file_savepoint.csv")
	require.NoError(t, os.WriteFile(copyPath, []byte("2,rolled-back\n3,also-rolled-back\n"), 0644))
	escapedPath := strings.ReplaceAll(copyPath, "'", "''")

	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_server_file_savepoint_items (
		id INT PRIMARY KEY,
		label TEXT
	);`)
	require.NoError(t, err)

	_, err = connection.Default.Exec(ctx, `BEGIN;`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `INSERT INTO copy_server_file_savepoint_items VALUES (1, 'kept');`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `SAVEPOINT copy_sp;`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, fmt.Sprintf(
		`COPY copy_server_file_savepoint_items (id, label) FROM '%s' WITH (FORMAT CSV);`,
		escapedPath,
	))
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `ROLLBACK TO SAVEPOINT copy_sp;`)
	require.NoError(t, err)
	_, err = connection.Default.Exec(ctx, `COMMIT;`)
	require.NoError(t, err)

	var ids string
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT string_agg(id::text, ',' ORDER BY id) FROM copy_server_file_savepoint_items;`,
	).Scan(&ids))
	require.Equal(t, "1", ids)
}

// TestCopyFromStdinRejectsDuplicateTargetColumnsRepro reproduces a COPY
// correctness bug: PostgreSQL rejects COPY target column lists that mention the
// same column more than once and leaves the table unchanged.
func TestCopyFromStdinRejectsDuplicateTargetColumnsRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_duplicate_column_items (
		id INT PRIMARY KEY,
		a INT,
		b INT
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\t20\n"),
		`COPY copy_stdin_duplicate_column_items (id, a, a) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the duplicate target column; tag=%s", tag.String())
	require.Contains(t, err.Error(), `column "a" specified more than once`)

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_duplicate_column_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinDuplicateKeyIsStatementAtomicRepro guards PostgreSQL COPY
// FROM STDIN atomicity: duplicate-key errors should roll back all rows from the
// COPY statement, including earlier accepted input rows.
func TestCopyFromStdinDuplicateKeyIsStatementAtomicRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_duplicate_items (
		id INT PRIMARY KEY,
		v TEXT
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tfirst\n1\tduplicate\n"),
		`COPY copy_stdin_duplicate_items (id, v) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the duplicate key; tag=%s", tag.String())

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_duplicate_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinCheckConstraintIsStatementAtomicRepro guards PostgreSQL
// COPY FROM STDIN atomicity: CHECK constraint errors should roll back all rows
// from the COPY statement, including earlier accepted input rows.
func TestCopyFromStdinCheckConstraintIsStatementAtomicRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_check_items (
		id INT PRIMARY KEY,
		qty INT CHECK (qty > 0)
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\t-1\n"),
		`COPY copy_stdin_check_items (id, qty) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the CHECK violation; tag=%s", tag.String())

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_check_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinDomainConstraintIsStatementAtomicGuard guards PostgreSQL
// COPY FROM STDIN atomicity for domain-typed columns: domain CHECK violations
// should roll back all rows from the COPY statement, including earlier
// accepted input rows.
func TestCopyFromStdinDomainConstraintIsStatementAtomicGuard(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE DOMAIN copy_stdin_positive_domain AS integer
		CONSTRAINT copy_stdin_positive_domain_check CHECK (VALUE > 0);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_stdin_domain_items (
		id INT PRIMARY KEY,
		amount copy_stdin_positive_domain
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\t-1\n"),
		`COPY copy_stdin_domain_items (id, amount) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the domain CHECK violation; tag=%s", tag.String())

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_domain_items;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinForeignKeyIsStatementAtomicRepro guards PostgreSQL COPY
// FROM STDIN atomicity: foreign-key errors should roll back all rows from the
// COPY statement, including earlier accepted input rows.
func TestCopyFromStdinForeignKeyIsStatementAtomicRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_stdin_fk_parents (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_stdin_fk_children (
		id INT PRIMARY KEY,
		parent_id INT REFERENCES copy_stdin_fk_parents(id)
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `INSERT INTO copy_stdin_fk_parents VALUES (1);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t1\n2\t2\n"),
		`COPY copy_stdin_fk_children (id, parent_id) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the foreign-key violation; tag=%s", tag.String())

	var count int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_stdin_fk_children;`,
	).Scan(&count))
	require.Equal(t, int64(0), count)
}

// TestCopyFromStdinBeforeInsertTriggerErrorRollsBackSideEffectsRepro guards
// PostgreSQL COPY FROM STDIN atomicity: side effects from a BEFORE INSERT
// trigger should roll back when that trigger raises an exception.
func TestCopyFromStdinBeforeInsertTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_before_trigger_target (
		id INT PRIMARY KEY,
		label TEXT NOT NULL
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_before_trigger_audit (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_before_trigger_reject() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_before_trigger_audit VALUES (NEW.id);
		RAISE EXCEPTION 'reject copy before trigger';
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_before_trigger
		BEFORE INSERT ON copy_before_trigger_target
		FOR EACH ROW EXECUTE FUNCTION copy_before_trigger_reject();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tnew\n"),
		`COPY copy_before_trigger_target (id, label) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the trigger exception; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_trigger_target;`,
	).Scan(&targetCount))
	require.Equal(t, int64(0), targetCount)

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_trigger_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinAfterInsertTriggerErrorRollsBackStatementRepro guards
// PostgreSQL COPY FROM STDIN atomicity: an AFTER INSERT trigger exception
// should roll back both copied rows and trigger side effects.
func TestCopyFromStdinAfterInsertTriggerErrorRollsBackStatementRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_after_trigger_target (
		id INT PRIMARY KEY,
		label TEXT NOT NULL
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_after_trigger_audit (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_after_trigger_reject() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_after_trigger_audit VALUES (NEW.id);
		RAISE EXCEPTION 'reject copy after trigger';
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_after_trigger
		AFTER INSERT ON copy_after_trigger_target
		FOR EACH ROW EXECUTE FUNCTION copy_after_trigger_reject();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tnew\n"),
		`COPY copy_after_trigger_target (id, label) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the trigger exception; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_trigger_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_trigger_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinBeforeStatementTriggerErrorRollsBackSideEffectsRepro guards
// PostgreSQL COPY FROM STDIN atomicity: side effects from a statement-level
// BEFORE INSERT trigger should roll back when that trigger raises an exception.
func TestCopyFromStdinBeforeStatementTriggerErrorRollsBackSideEffectsRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_before_statement_trigger_target (
		id INT PRIMARY KEY,
		label TEXT NOT NULL
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_before_statement_trigger_audit (
		label TEXT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_before_statement_trigger_reject() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_before_statement_trigger_audit VALUES ('before statement');
		RAISE EXCEPTION 'reject copy before statement trigger';
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_before_statement_trigger
		BEFORE INSERT ON copy_before_statement_trigger_target
		FOR EACH STATEMENT EXECUTE FUNCTION copy_before_statement_trigger_reject();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tnew\n"),
		`COPY copy_before_statement_trigger_target (id, label) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the trigger exception; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_statement_trigger_target;`,
	).Scan(&targetCount))
	require.Equal(t, int64(0), targetCount)

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_statement_trigger_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinAfterStatementTriggerErrorRollsBackStatementRepro guards
// PostgreSQL COPY FROM STDIN atomicity: an AFTER INSERT statement trigger
// exception should roll back both copied rows and trigger side effects.
func TestCopyFromStdinAfterStatementTriggerErrorRollsBackStatementRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_after_statement_trigger_target (
		id INT PRIMARY KEY,
		label TEXT NOT NULL
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_after_statement_trigger_audit (
		label TEXT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_after_statement_trigger_reject() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_after_statement_trigger_audit VALUES ('after statement');
		RAISE EXCEPTION 'reject copy after statement trigger';
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_after_statement_trigger
		AFTER INSERT ON copy_after_statement_trigger_target
		FOR EACH STATEMENT EXECUTE FUNCTION copy_after_statement_trigger_reject();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tnew\n"),
		`COPY copy_after_statement_trigger_target (id, label) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the trigger exception; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_statement_trigger_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_statement_trigger_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinDefaultFunctionSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: side effects from DEFAULT functions used
// by COPY FROM STDIN must roll back if a later copied row fails a constraint.
func TestCopyFromStdinDefaultFunctionSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_default_function_audit (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_default_function_value() RETURNS INT AS $$
	DECLARE
		next_id INT;
	BEGIN
		SELECT COALESCE(MAX(id), 0) + 1 INTO next_id FROM copy_default_function_audit;
		INSERT INTO copy_default_function_audit VALUES (next_id);
		RETURN next_id;
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_default_function_target (
		id INT PRIMARY KEY,
		label TEXT NOT NULL CHECK (label <> 'bad'),
		audit_value INT NOT NULL DEFAULT copy_default_function_value()
	);`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\tgood\n2\tbad\n"),
		`COPY copy_default_function_target (id, label) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the CHECK violation; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_default_function_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_default_function_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: side effects from row-level BEFORE INSERT
// triggers fired by COPY FROM STDIN must roll back if a later copied row fails
// a constraint.
func TestCopyFromStdinBeforeInsertTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_before_trigger_check_target (
		id INT PRIMARY KEY,
		qty INT NOT NULL CHECK (qty > 0)
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_before_trigger_check_audit (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_before_trigger_check_audit() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_before_trigger_check_audit VALUES (NEW.id);
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_before_trigger_check
		BEFORE INSERT ON copy_before_trigger_check_target
		FOR EACH ROW EXECUTE FUNCTION copy_before_trigger_check_audit();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\t-1\n"),
		`COPY copy_before_trigger_check_target (id, qty) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the CHECK violation; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_trigger_check_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_trigger_check_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinAfterInsertTriggerSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: side effects from row-level AFTER INSERT
// triggers fired by COPY FROM STDIN must roll back if a later copied row fails
// a constraint.
func TestCopyFromStdinAfterInsertTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_after_trigger_check_target (
		id INT PRIMARY KEY,
		qty INT NOT NULL CHECK (qty > 0)
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_after_trigger_check_audit (
		id INT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_after_trigger_check_audit() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_after_trigger_check_audit VALUES (NEW.id);
		RETURN NULL;
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_after_trigger_check
		AFTER INSERT ON copy_after_trigger_check_target
		FOR EACH ROW EXECUTE FUNCTION copy_after_trigger_check_audit();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\t-1\n"),
		`COPY copy_after_trigger_check_target (id, qty) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the CHECK violation; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_trigger_check_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_trigger_check_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinBeforeStatementTriggerSideEffectsRollBackOnConstraintErrorRepro
// reproduces a data consistency bug: side effects from statement-level BEFORE
// INSERT triggers fired by COPY FROM STDIN must roll back if a later copied row
// fails a constraint.
func TestCopyFromStdinBeforeStatementTriggerSideEffectsRollBackOnConstraintErrorRepro(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_before_statement_trigger_check_target (
		id INT PRIMARY KEY,
		qty INT NOT NULL CHECK (qty > 0)
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_before_statement_trigger_check_audit (
		label TEXT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_before_statement_trigger_check_audit() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_before_statement_trigger_check_audit VALUES ('before statement');
		RETURN NULL;
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_before_statement_trigger_check
		BEFORE INSERT ON copy_before_statement_trigger_check_target
		FOR EACH STATEMENT EXECUTE FUNCTION copy_before_statement_trigger_check_audit();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\t-1\n"),
		`COPY copy_before_statement_trigger_check_target (id, qty) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the CHECK violation; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_statement_trigger_check_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_before_statement_trigger_check_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}

// TestCopyFromStdinAfterStatementTriggerDoesNotFireOnConstraintErrorGuard
// guards PostgreSQL COPY FROM STDIN atomicity: statement-level AFTER INSERT
// triggers do not fire for a COPY statement that fails a row constraint.
func TestCopyFromStdinAfterStatementTriggerDoesNotFireOnConstraintErrorGuard(t *testing.T) {
	ctx, connection, controller := CreateServer(t, "postgres")
	defer func() {
		connection.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	}()

	_, err := connection.Exec(ctx, `CREATE TABLE copy_after_statement_trigger_check_target (
		id INT PRIMARY KEY,
		qty INT NOT NULL CHECK (qty > 0)
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TABLE copy_after_statement_trigger_check_audit (
		label TEXT PRIMARY KEY
	);`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE FUNCTION copy_after_statement_trigger_check_audit() RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO copy_after_statement_trigger_check_audit VALUES ('after statement');
		RETURN NULL;
	END;
	$$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	_, err = connection.Exec(ctx, `CREATE TRIGGER copy_after_statement_trigger_check
		AFTER INSERT ON copy_after_statement_trigger_check_target
		FOR EACH STATEMENT EXECUTE FUNCTION copy_after_statement_trigger_check_audit();`)
	require.NoError(t, err)

	tag, err := connection.Default.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\t10\n2\t-1\n"),
		`COPY copy_after_statement_trigger_check_target (id, qty) FROM STDIN;`,
	)
	require.Error(t, err, "COPY FROM should reject the CHECK violation; tag=%s", tag.String())

	var targetCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_statement_trigger_check_target;`,
	).Scan(&targetCount))

	var auditCount int64
	require.NoError(t, connection.Default.QueryRow(
		context.Background(),
		`SELECT count(*) FROM copy_after_statement_trigger_check_audit;`,
	).Scan(&auditCount))
	require.Equal(t, []int64{0, 0}, []int64{targetCount, auditCount})
}
