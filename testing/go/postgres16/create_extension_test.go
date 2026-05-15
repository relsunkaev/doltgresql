// Copyright 2025 Dolthub, Inc.
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

	"os"
	"runtime"
	"testing"
)

func TestCreateExtension(t *testing.T) {
	if runtime.GOOS == "windows" && os.Getenv("CI") != "" {
		t.Skip("CI Postgres installation seems to behave weirdly, skipping for now") // TODO: look into this a bit more
	}
	RunScripts(t, []ScriptTest{
		{
			Name: "Extension Test: uuid-ossp",
			SetUpScript: []string{
				`CREATE EXTENSION "uuid-ossp";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT uuid_ns_url();", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0001-select-uuid_ns_url"},
				},
				{
					Skip:  true, // This is returning different results on different platforms for some reason
					Query: "SELECT uuid_generate_v3('00000000-0000-0000-0000-000000000000'::uuid, 'example text');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0002-select-uuid_generate_v3-00000000-0000-0000-0000-000000000000-::uuid-example"},
				},
				{
					Skip:  true, // For some reason, this returns the same result as above
					Query: "SELECT uuid_generate_v3('00000000-0000-0000-0000-000000000001'::uuid, 'example text');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0003-select-uuid_generate_v3-00000000-0000-0000-0000-000000000001-::uuid-example"},
				},
				{
					Skip:  true, // Need to figure out why the result is wrong
					Query: "SELECT uuid_generate_v3(uuid_ns_url(), 'example text');", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0004-select-uuid_generate_v3-uuid_ns_url-example-text"},
				},
				{
					Query: "SELECT uuid_nil();", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0005-select-uuid_nil"},
				},
				{
					Query: "SELECT length(uuid_nil()::text);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0006-select-length-uuid_nil-::text"},
				},
				{
					Query: "SELECT length(uuid_generate_v4()::text);", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0007-select-length-uuid_generate_v4-::text"},
				},
				{
					Query: "SELECT uuid_generate_v4() = uuid_nil();", PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0008-select-uuid_generate_v4-=-uuid_nil"},
				},
				{
					Query: `WITH u1 AS (SELECT uuid_nil() AS id), u2 AS (SELECT uuid_nil() AS id) SELECT (SELECT id FROM u1) = (SELECT id FROM u2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0009-with-u1-as-select-uuid_nil"},
				},
				{
					Query: `WITH u1 AS (SELECT uuid_generate_v4() AS id), u2 AS (SELECT uuid_generate_v4() AS id) SELECT (SELECT id FROM u1) = (SELECT id FROM u2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "create-extension-test-testcreateextension-0010-with-u1-as-select-uuid_generate_v4"},
				},
			},
		},
	})
}
