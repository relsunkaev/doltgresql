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
	"fmt"
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestStartupParameterStatuses asserts that the ParameterStatus
// messages a real PostgreSQL server emits at startup are present.
// Drivers and ORMs key behavior off these:
//
//   - JDBC reads `integer_datetimes` to choose binary date encoding.
//   - node-postgres / pgx use `server_encoding` and `client_encoding`
//     for transcoding.
//   - SQLAlchemy and Hibernate read `DateStyle` and `IntervalStyle`
//     to format / parse temporal literals.
//   - Many tools display `application_name` and `session_authorization`
//     in connection panels.
//
// pgx exposes ParameterStatuses on the underlying *pgconn.PgConn
// after handshake, which lets us verify the wire payload directly.
func TestStartupParameterStatuses(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable&application_name=ptest",
		port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	want := map[string]func(string) bool{
		"server_version":              func(v string) bool { return v != "" },
		"server_encoding":             func(v string) bool { return v == "UTF8" },
		"client_encoding":             func(v string) bool { return v == "UTF8" },
		"standard_conforming_strings": func(v string) bool { return v == "on" },
		"in_hot_standby":              func(v string) bool { return v == "on" || v == "off" },
		"DateStyle":                   func(v string) bool { return v != "" },
		"IntervalStyle":               func(v string) bool { return v != "" },
		"TimeZone":                    func(v string) bool { return v != "" },
		"integer_datetimes":           func(v string) bool { return v == "on" },
		"is_superuser":                func(v string) bool { return v == "on" || v == "off" },
		"session_authorization":       func(v string) bool { return v != "" },
		"application_name":            func(v string) bool { return v == "ptest" },
	}
	for name, check := range want {
		v := conn.PgConn().ParameterStatus(name)
		if v == "" {
			t.Errorf("missing ParameterStatus %q", name)
			continue
		}
		if !check(v) {
			t.Errorf("ParameterStatus %q has unexpected value %q", name, v)
		}
	}
}
