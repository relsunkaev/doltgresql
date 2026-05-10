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
	"os/exec"
	"path/filepath"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestJDBCClientSmoke runs the upstream PostgreSQL JDBC driver against
// Doltgres. It pins the Java client path for startup parameters, prepared
// statements, typed parameters, text[] / JSONB values, multiple connections,
// and transaction commit/rollback behavior.
func TestJDBCClientSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("JDBC harness downloads the PostgreSQL JDBC jar; skipped under -short")
	}
	for _, tool := range []string{"curl", "java", "javac"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not on PATH; install Java tooling to enable this harness", tool)
		}
	}
	for _, check := range []struct {
		tool string
		args []string
	}{
		{tool: "java", args: []string{"-version"}},
		{tool: "javac", args: []string{"-version"}},
	} {
		cmd := exec.Command(check.tool, check.args...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Skipf("%s is present but not usable; install a JDK to enable this harness: %s", check.tool, string(out))
		}
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	work := t.TempDir()
	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jarPath := filepath.Join(work, "postgresql.jar")
	download := exec.CommandContext(cmdCtx, "curl",
		"--fail", "--location", "--silent", "--show-error",
		"--output", jarPath,
		"https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar",
	)
	if out, err := download.CombinedOutput(); err != nil {
		t.Fatalf("download PostgreSQL JDBC jar: %v\n%s", err, string(out))
	}

	source := `import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class JdbcHarness {
  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  private static String accountName(String url, Properties props, int id) throws Exception {
    try (Connection conn = DriverManager.getConnection(url, props);
         PreparedStatement ps = conn.prepareStatement("SELECT email FROM jdbc_accounts WHERE id = ?::int4")) {
      ps.setInt(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        check(rs.next(), "missing account " + id);
        return rs.getString(1);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String url = args[0];
    Properties props = new Properties();
    props.setProperty("user", "postgres");
    props.setProperty("password", "password");
    props.setProperty("ApplicationName", "jdbc-harness");

    try (Connection conn = DriverManager.getConnection(url, props)) {
      try (Statement st = conn.createStatement()) {
        try (ResultSet rs = st.executeQuery("SELECT current_setting('application_name')")) {
          check(rs.next(), "application_name row missing");
          check("jdbc-harness".equals(rs.getString(1)), "application_name mismatch: " + rs.getString(1));
        }
        st.execute("CREATE TABLE jdbc_accounts (id integer PRIMARY KEY, email text NOT NULL UNIQUE, active boolean NOT NULL)");
        st.execute("CREATE TABLE jdbc_items (id integer PRIMARY KEY, account_id integer NOT NULL REFERENCES jdbc_accounts(id), amount numeric(10,2) NOT NULL, tags text[] NOT NULL, payload jsonb NOT NULL)");
      }

      try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_accounts VALUES (?::int4, ?::text, ?::bool), (?::int4, ?::text, ?::bool)")) {
        ps.setInt(1, 1);
        ps.setString(2, "acme@example.com");
        ps.setBoolean(3, true);
        ps.setInt(4, 2);
        ps.setString(5, "beta@example.com");
        ps.setBoolean(6, false);
        check(ps.executeUpdate() == 2, "account insert count mismatch");
      }

      try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_items VALUES (?::int4, ?::int4, ?::text::numeric, ?::text[], ?::jsonb) RETURNING amount, tags[2], payload #>> '{kind}'")) {
        Array tags = conn.createArrayOf("text", new String[] {"red", "blue"});
        ps.setInt(1, 10);
        ps.setInt(2, 1);
        ps.setString(3, "12.34");
        ps.setArray(4, tags);
        ps.setString(5, "{\"kind\":\"invoice\",\"lines\":[1,2]}");
        try (ResultSet rs = ps.executeQuery()) {
          check(rs.next(), "insert returning row missing");
          check(new BigDecimal("12.34").compareTo(rs.getBigDecimal(1)) == 0, "amount mismatch");
          check("blue".equals(rs.getString(2)), "array value mismatch: " + rs.getString(2));
          check("invoice".equals(rs.getString(3)), "jsonb value mismatch: " + rs.getString(3));
        }
      }
    }

    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<String> first = pool.submit(() -> accountName(url, props, 1));
      Future<String> second = pool.submit(() -> accountName(url, props, 2));
      check("acme@example.com".equals(first.get()), "first pooled read mismatch");
      check("beta@example.com".equals(second.get()), "second pooled read mismatch");
    } finally {
      pool.shutdownNow();
    }

    try (Connection conn = DriverManager.getConnection(url, props)) {
      conn.setAutoCommit(false);
      try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_accounts VALUES (?::int4, ?::text, ?::bool)")) {
        ps.setInt(1, 3);
        ps.setString(2, "gamma@example.com");
        ps.setBoolean(3, true);
        ps.executeUpdate();
      }
      conn.commit();

      try {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_accounts VALUES (?::int4, ?::text, ?::bool)")) {
          ps.setInt(1, 4);
          ps.setString(2, "rolled-back@example.com");
          ps.setBoolean(3, true);
          ps.executeUpdate();
        }
        conn.rollback();
      } finally {
        conn.setAutoCommit(true);
      }

      try (Statement st = conn.createStatement();
           ResultSet rs = st.executeQuery("SELECT array_to_string(array_agg(email ORDER BY id), ',') FROM jdbc_accounts")) {
        check(rs.next(), "summary row missing");
        String emails = rs.getString(1);
        check("acme@example.com,beta@example.com,gamma@example.com".equals(emails), "summary mismatch: " + emails);
        System.out.println("{\"ok\":true,\"emails\":\"" + emails + "\"}");
      }
    }
  }
}
`
	sourcePath := filepath.Join(work, "JdbcHarness.java")
	require.NoError(t, os.WriteFile(sourcePath, []byte(source), 0o644))

	compile := exec.CommandContext(cmdCtx, "javac", "-cp", jarPath, sourcePath)
	compile.Dir = work
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile JDBC harness: %v\n%s", err, string(out))
	}

	url := fmt.Sprintf("jdbc:postgresql://127.0.0.1:%d/postgres?sslmode=disable", port)
	run := exec.CommandContext(cmdCtx, "java", "-cp", jarPath+string(os.PathListSeparator)+work, "JdbcHarness", url)
	run.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := run.CombinedOutput()
	require.NoError(t, err, "JDBC probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"emails":"acme@example.com,beta@example.com,gamma@example.com"`)
}
