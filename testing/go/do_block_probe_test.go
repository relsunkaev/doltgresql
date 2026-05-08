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

// TestDoBlockProbe pins where PG `DO $$ ... $$` anonymous code blocks
// stand in doltgresql today. pg_dump emits these for matview / state
// repair, Alembic upgrade scripts wrap conditional DDL in them, and
// many ORM init scripts use the IF-NOT-EXISTS pattern through DO. The
// keyword is rejected at the parser level (`at or near "do": syntax
// error` SQLSTATE 42601). This pins the rejection so the gap stays
// visible. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestDoBlockProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "DO block with conditional CREATE is rejected at the parser",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'created_by_do') THEN
								CREATE TABLE created_by_do (id INT PRIMARY KEY);
							END IF;
						END $$;`,
					ExpectedErr: `at or near "do": syntax error`,
				},
			},
		},
		{
			Name:        "DO block running RAISE NOTICE is rejected at the parser",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							RAISE NOTICE 'hello from DO block';
						END $$;`,
					ExpectedErr: `at or near "do": syntax error`,
				},
			},
		},
	})
}
