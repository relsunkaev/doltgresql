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

// TestDoBlockProbe pins where PG `DO $$ ... $$` anonymous code blocks
// stand in doltgresql today. pg_dump emits these for matview/state
// repair, Alembic upgrade scripts wrap conditional DDL in them, and
// many ORM init scripts use the IF-NOT-EXISTS pattern through DO. Per
// the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestDoBlockProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "DO block runs conditional CREATE",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'created_by_do') THEN
								CREATE TABLE created_by_do (id INT PRIMARY KEY);
							END IF;
						END;
					$$;`,
				},
				{
					Query: `INSERT INTO created_by_do VALUES (1);`,
				},
				{
					Query:    `SELECT count(*)::text FROM created_by_do;`,
					Expected: []sql.Row{{"1"}},
				},
			},
		},
		{
			Name:        "DO block defaults to plpgsql and can raise notice",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO $$
						BEGIN
							RAISE NOTICE 'hello from DO block';
						END;
					$$;`,
				},
			},
		},
		{
			Name:        "DO block rejects unsupported language",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `DO LANGUAGE sql $$
						SELECT 1;
					$$;`,
					ExpectedErr: `DO only supports LANGUAGE plpgsql`,
				},
			},
		},
	})
}
