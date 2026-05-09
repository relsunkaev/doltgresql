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

func TestRealWorldViewRebuildPath(t *testing.T) {
	const initialView = `CREATE VIEW account_event_summary AS
WITH expanded AS (
	SELECT e.account_id, e.event_type, e.occurred_at,
		tag_expanded.jsonb_array_elements_text AS tag,
		match_expanded.regexp_matches[1] AS status
	FROM app_events e
	JOIN LATERAL jsonb_array_elements_text(e.payload->'tags') AS tag_expanded ON true
	JOIN LATERAL regexp_matches(e.note, 'status=([a-z]+)') AS match_expanded ON true
),
latest_tag AS (
	SELECT DISTINCT ON (account_id, tag)
		account_id, tag, event_type, occurred_at, status
	FROM expanded
	ORDER BY account_id, tag, occurred_at DESC, event_type
),
ranked AS (
	SELECT account_id, tag, event_type, status,
		row_number() OVER (PARTITION BY account_id ORDER BY occurred_at DESC, tag) AS rn,
		count(*) OVER (PARTITION BY account_id) AS tag_count
	FROM latest_tag
)
SELECT a.email, r.tag, r.event_type, r.status, r.rn, r.tag_count
FROM ranked r
JOIN app_accounts a ON a.id = r.account_id
WHERE a.active;`

	const rebuiltView = `CREATE OR REPLACE VIEW account_event_summary AS
WITH expanded AS (
	SELECT e.account_id, e.event_type, e.occurred_at,
		tag_expanded.jsonb_array_elements_text AS tag,
		match_expanded.regexp_matches[1] AS status
	FROM app_events e
	JOIN LATERAL jsonb_array_elements_text(e.payload->'tags') AS tag_expanded ON true
	JOIN LATERAL regexp_matches(e.note, 'status=([a-z]+)') AS match_expanded ON true
),
filtered AS (
	SELECT * FROM expanded WHERE status <> 'failed'
),
latest_tag AS (
	SELECT DISTINCT ON (account_id, tag)
		account_id, tag, event_type, occurred_at, status
	FROM filtered
	ORDER BY account_id, tag, occurred_at DESC, event_type
),
ranked AS (
	SELECT account_id, tag, event_type, status,
		row_number() OVER (PARTITION BY account_id ORDER BY occurred_at DESC, tag) AS rn,
		count(*) OVER (PARTITION BY account_id) AS tag_count
	FROM latest_tag
)
SELECT a.email, r.tag, r.event_type, r.status, r.rn, r.tag_count
FROM ranked r
JOIN app_accounts a ON a.id = r.account_id
WHERE a.active;`

	RunScripts(t, []ScriptTest{
		{
			Name: "real-world analytical view rebuild workload",
			SetUpScript: []string{
				`CREATE TABLE app_accounts (
					id INT PRIMARY KEY,
					email TEXT NOT NULL,
					active BOOLEAN NOT NULL
				);`,
				`CREATE TABLE app_events (
					id INT PRIMARY KEY,
					account_id INT NOT NULL REFERENCES app_accounts(id),
					event_type TEXT NOT NULL,
					occurred_at TIMESTAMP NOT NULL,
					payload JSONB NOT NULL,
					note TEXT NOT NULL
				);`,
				`INSERT INTO app_accounts VALUES
					(1, 'ada@example.com', true),
					(2, 'grace@example.com', true),
					(3, 'inactive@example.com', false);`,
				`INSERT INTO app_events VALUES
					(1, 1, 'login', TIMESTAMP '2026-01-01 00:00:00', '{"tags":["auth","web"]}'::jsonb, 'status=ok id=1'),
					(2, 1, 'purchase', TIMESTAMP '2026-01-02 00:00:00', '{"tags":["billing","web"]}'::jsonb, 'status=paid id=2'),
					(3, 2, 'login', TIMESTAMP '2026-01-03 00:00:00', '{"tags":["auth"]}'::jsonb, 'status=ok id=3'),
					(4, 2, 'sync', TIMESTAMP '2026-01-04 00:00:00', '{"tags":["web"]}'::jsonb, 'status=failed id=4'),
					(5, 3, 'login', TIMESTAMP '2026-01-05 00:00:00', '{"tags":["auth"]}'::jsonb, 'status=ok id=5');`,
				initialView,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT email, tag, event_type, status, rn::text, tag_count::text
FROM account_event_summary
ORDER BY email, rn;`,
					Expected: []sql.Row{
						{"ada@example.com", "billing", "purchase", "paid", "1", "3"},
						{"ada@example.com", "web", "purchase", "paid", "2", "3"},
						{"ada@example.com", "auth", "login", "ok", "3", "3"},
						{"grace@example.com", "web", "sync", "failed", "1", "2"},
						{"grace@example.com", "auth", "login", "ok", "2", "2"},
					},
				},
				{
					Query:    rebuiltView,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT email, tag, event_type, status, rn::text, tag_count::text
FROM account_event_summary
ORDER BY email, rn;`,
					Expected: []sql.Row{
						{"ada@example.com", "billing", "purchase", "paid", "1", "3"},
						{"ada@example.com", "web", "purchase", "paid", "2", "3"},
						{"ada@example.com", "auth", "login", "ok", "3", "3"},
						{"grace@example.com", "auth", "login", "ok", "1", "1"},
					},
				},
			},
		},
	})
}
