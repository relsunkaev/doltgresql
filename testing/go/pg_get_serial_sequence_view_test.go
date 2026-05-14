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

func TestPgGetSerialSequenceReturnsNullForViews(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_serial_sequence returns null for view columns",
			SetUpScript: []string{
				`CREATE TABLE pgget_serial_view_base (id INT PRIMARY KEY, label TEXT);`,
				`CREATE VIEW pgget_serial_view AS SELECT id, label FROM pgget_serial_view_base;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT pg_get_serial_sequence('"public"."pgget_serial_view"', 'id') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
				{
					Query:    `SELECT pg_get_serial_sequence('pgget_serial_view', 'label') IS NULL;`,
					Expected: []sql.Row{{"t"}},
				},
			},
		},
	})
}
