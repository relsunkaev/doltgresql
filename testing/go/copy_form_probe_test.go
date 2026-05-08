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
)

// TestCopyFormProbe pins how PG `COPY` keyword forms parse today.
// pg_dump emits `COPY (SELECT ...) TO STDOUT WITH (FORMAT text)`
// for filtered exports, and `COPY FROM stdin` for restore. Per the
// Dump/admin/tooling TODO in
// docs/app-compatibility-checklist.md.
func TestCopyFormProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "COPY (SELECT ...) TO STDOUT keyword acceptance",
			SetUpScript: []string{
				`CREATE TABLE t (id INT PRIMARY KEY, v TEXT);`,
				`INSERT INTO t VALUES (1, 'a'), (2, 'b');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					// Today: parser rejection at `(` after COPY.
					// pg_dump emits this for filtered exports, so
					// it must be stripped or rewritten before
					// import. Pin the rejection so the gap stays
					// visible.
					Query:       `COPY (SELECT id, v FROM t ORDER BY id) TO STDOUT WITH (FORMAT text);`,
					ExpectedErr: `at or near "(": syntax error`,
				},
			},
		},
	})
}
