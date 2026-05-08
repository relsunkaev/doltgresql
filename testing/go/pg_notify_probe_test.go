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

// TestPgNotifyProbe pins PG NOTIFY / pg_notify compatibility. Real apps use
// this for cache-invalidation fanout and listener queues; Doltgres accepts
// the call shapes as no-ops so migrations and write paths do not fail, but
// it does not implement listener delivery. Per the Runtime SQL TODO in
// docs/app-compatibility-checklist.md.
func TestPgNotifyProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "NOTIFY statement is accepted as a no-op",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `NOTIFY my_channel, 'payload';`,
				},
				{
					Query: `NOTIFY my_channel;`,
				},
			},
		},
		{
			Name:        "pg_notify returns void",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_notify('my_channel', 'payload');`,
					Expected: []sql.Row{{""}},
				},
			},
		},
	})
}
