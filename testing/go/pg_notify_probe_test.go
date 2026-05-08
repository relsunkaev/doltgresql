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

// TestPgNotifyProbe pins where PG NOTIFY / pg_notify stand today.
// Real apps use this for cache-invalidation fanout and listener
// queues; if the call shape doesn't even parse, every connection
// that issues NOTIFY blows up. Per the Runtime SQL TODO in
// docs/app-compatibility-checklist.md.
func TestPgNotifyProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			// NOTIFY is rejected at the parser today.
			Name:        "NOTIFY is rejected at the parser",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `NOTIFY my_channel, 'payload';`,
					ExpectedErr: `at or near "notify": syntax error`,
				},
			},
		},
		{
			// pg_notify function isn't registered.
			Name:        "pg_notify function is not registered",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:       `SELECT pg_notify('my_channel', 'payload');`,
					ExpectedErr: "function: 'pg_notify' not found",
				},
			},
		},
	})
}
