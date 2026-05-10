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
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestInsertOnConflictExcluded covers the EXCLUDED pseudo-table that
// every PostgreSQL ORM emits in ON CONFLICT (col) DO UPDATE SET clauses
// to reference the row that would have been inserted. PG-style:
//
//	INSERT INTO t (id, name) VALUES (1, 'a')
//	  ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
//
// Maps internally to MySQL's `values(name)`. Without this, every ORM
// upsert (Drizzle, Prisma, SQLAlchemy.merge, ActiveRecord upsert,
// Sequelize.upsert, Drizzle's onConflictDoUpdate) errors at parse.
func TestInsertOnConflictExcluded(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "EXCLUDED.col copies the new value into the existing row",
			SetUpScript: []string{
				"CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);",
				"INSERT INTO users VALUES (1, 'old', 30);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO users (id, name, age) VALUES (1, 'new', 31)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age;`,
				},
				{
					Query: "SELECT id, name, age FROM users WHERE id = 1;",
					Expected: []gms.Row{
						{1, "new", 31},
					},
				},
			},
		},
		{
			Name: "EXCLUDED in expressions and mixed with existing column refs",
			SetUpScript: []string{
				"CREATE TABLE counters (id INT PRIMARY KEY, hits INT, label TEXT);",
				"INSERT INTO counters VALUES (1, 5, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Increment by EXCLUDED.hits (the proposed new value)
					// and concatenate label with EXCLUDED.label.
					Query: `INSERT INTO counters (id, hits, label) VALUES (1, 3, 'plus')
ON CONFLICT (id) DO UPDATE
SET hits = counters.hits + EXCLUDED.hits,
    label = counters.label || ':' || EXCLUDED.label;`,
				},
				{
					Query: "SELECT id, hits, label FROM counters WHERE id = 1;",
					Expected: []gms.Row{
						{1, 8, "old:plus"},
					},
				},
			},
		},
		{
			Name: "EXCLUDED case-insensitive (lowercase, uppercase, mixed)",
			SetUpScript: []string{
				"CREATE TABLE c_t (id INT PRIMARY KEY, v TEXT);",
				"INSERT INTO c_t VALUES (1, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO c_t (id, v) VALUES (1, 'A')
ON CONFLICT (id) DO UPDATE SET v = excluded.v;`,
				},
				{
					Query:    "SELECT v FROM c_t WHERE id = 1;",
					Expected: []gms.Row{{"A"}},
				},
				{
					Query: `INSERT INTO c_t (id, v) VALUES (1, 'B')
ON CONFLICT (id) DO UPDATE SET v = ExCluDed.v;`,
				},
				{
					Query:    "SELECT v FROM c_t WHERE id = 1;",
					Expected: []gms.Row{{"B"}},
				},
			},
		},
		{
			Name: "EXCLUDED with multi-row VALUES applies the matched row",
			SetUpScript: []string{
				"CREATE TABLE m (id INT PRIMARY KEY, v INT);",
				"INSERT INTO m VALUES (1, 100), (2, 200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Three rows: id=1 conflicts (row 0), id=2 conflicts (row 1),
					// id=3 inserts cleanly (row 2).
					Query: `INSERT INTO m (id, v) VALUES (1, 11), (2, 22), (3, 33)
ON CONFLICT (id) DO UPDATE SET v = m.v + EXCLUDED.v;`,
				},
				{
					Query: "SELECT id, v FROM m ORDER BY id;",
					Expected: []gms.Row{
						{1, 111}, {2, 222}, {3, 33},
					},
				},
			},
		},
	})
}

func TestInsertOnConflictDoNothingAppliesDefaultsToOmittedPrimaryKey(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "Zero permissions singleton initializer",
			SetUpScript: []string{
				`CREATE TABLE zero_permissions_default_probe (
					permissions JSONB,
					hash TEXT,
					lock BOOL PRIMARY KEY DEFAULT true CHECK (lock)
				);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO zero_permissions_default_probe (permissions)
						VALUES (NULL)
						ON CONFLICT DO NOTHING;`,
					SkipResultsCheck: true,
				},
				{
					Query:    `SELECT lock::text, (permissions IS NULL)::text FROM zero_permissions_default_probe;`,
					Expected: []gms.Row{{"true", "true"}},
				},
				{
					Query: `INSERT INTO zero_permissions_default_probe (permissions)
						VALUES ('{"tables":{}}'::jsonb)
						ON CONFLICT DO NOTHING;`,
					SkipResultsCheck: true,
				},
				{
					Query:    `SELECT count(*)::text, bool_and(lock)::text FROM zero_permissions_default_probe;`,
					Expected: []gms.Row{{"1", "true"}},
				},
			},
		},
		{
			Name: "Zero permissions singleton initializer with hash trigger",
			SetUpScript: []string{
				`CREATE TABLE zero_permissions_trigger_probe (
					"permissions" JSONB,
					"hash" TEXT,
					"lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
				);`,
				`CREATE OR REPLACE FUNCTION zero_permissions_trigger_probe_hash()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.hash = md5(NEW.permissions::text);
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE OR REPLACE TRIGGER on_zero_permissions_trigger_probe_hash
					BEFORE INSERT OR UPDATE ON zero_permissions_trigger_probe
					FOR EACH ROW
					EXECUTE FUNCTION zero_permissions_trigger_probe_hash();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO zero_permissions_trigger_probe ("permissions")
						VALUES (NULL)
						ON CONFLICT DO NOTHING;`,
					SkipResultsCheck: true,
				},
				{
					Query:    `SELECT count(*)::text, bool_and("lock")::text, bool_and("hash" IS NULL)::text FROM zero_permissions_trigger_probe;`,
					Expected: []gms.Row{{"1", "true", "true"}},
				},
			},
		},
		{
			Name: "BEFORE INSERT triggers see omitted defaults with explicit column mapping",
			SetUpScript: []string{
				`CREATE TABLE trigger_default_order_probe (
					id INT PRIMARY KEY DEFAULT 10,
					marker TEXT DEFAULT 'from_default',
					supplied TEXT,
					noted TEXT,
					nullable_default TEXT DEFAULT 'fallback'
				);`,
				`CREATE OR REPLACE FUNCTION trigger_default_order_probe_note()
				RETURNS TRIGGER AS $$
				BEGIN
					NEW.noted = COALESCE(NEW.marker, 'missing') || ':' || COALESCE(NEW.supplied, 'none') || ':' || COALESCE(NEW.nullable_default, 'null');
					RETURN NEW;
				END;
				$$ LANGUAGE plpgsql;`,
				`CREATE OR REPLACE TRIGGER on_trigger_default_order_probe_note
					BEFORE INSERT ON trigger_default_order_probe
					FOR EACH ROW
					EXECUTE FUNCTION trigger_default_order_probe_note();`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO trigger_default_order_probe (supplied, nullable_default)
						VALUES ('explicit', NULL);`,
					SkipResultsCheck: true,
				},
				{
					Query: `SELECT id, marker, supplied, nullable_default, noted
						FROM trigger_default_order_probe;`,
					Expected: []gms.Row{{10, "from_default", "explicit", nil, "from_default:explicit:null"}},
				},
			},
		},
	})
}

// TestInsertOnConflictDoUpdateWhere covers the conditional update form
// of ON CONFLICT — `DO UPDATE SET ... WHERE pred`. PG semantics: the
// UPDATE only fires when pred (evaluated against the existing row +
// EXCLUDED proposed row) is true. Otherwise the existing row is kept
// unchanged AND no error is raised.
//
// Real-world example (DDIA / Vitess docs / Drizzle PG):
//
//	INSERT INTO counters (id, hits) VALUES (1, 1)
//	  ON CONFLICT (id) DO UPDATE
//	  SET hits = counters.hits + 1
//	  WHERE counters.hits < 100;
//
// Cap-at-100 idempotent counter increment.
func TestInsertOnConflictDoUpdateWhere(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "WHERE true applies the update",
			SetUpScript: []string{
				"CREATE TABLE w (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO w (id, v) VALUES (1, 99)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE w.v < 100;`,
				},
				{
					Query:    "SELECT v FROM w WHERE id = 1;",
					Expected: []gms.Row{{99}},
				},
			},
		},
		{
			Name: "WHERE false leaves the existing row unchanged, no error",
			SetUpScript: []string{
				"CREATE TABLE w2 (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w2 VALUES (1, 200);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO w2 (id, v) VALUES (1, 99)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE w2.v < 100;`,
				},
				{
					Query:    "SELECT v FROM w2 WHERE id = 1;",
					Expected: []gms.Row{{200}},
				},
			},
		},
		{
			Name: "WHERE referencing EXCLUDED",
			SetUpScript: []string{
				"CREATE TABLE w3 (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w3 VALUES (1, 50);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// New value is bigger -> apply.
					Query: `INSERT INTO w3 (id, v) VALUES (1, 75)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > w3.v;`,
				},
				{
					Query:    "SELECT v FROM w3 WHERE id = 1;",
					Expected: []gms.Row{{75}},
				},
				{
					// New value is smaller -> skip.
					Query: `INSERT INTO w3 (id, v) VALUES (1, 25)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v > w3.v;`,
				},
				{
					Query:    "SELECT v FROM w3 WHERE id = 1;",
					Expected: []gms.Row{{75}},
				},
			},
		},
		{
			Name: "WHERE in mixed multi-row insert: each row checked independently",
			SetUpScript: []string{
				"CREATE TABLE w4 (id INT PRIMARY KEY, v INT);",
				"INSERT INTO w4 VALUES (1, 5), (2, 99);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// id=1 (v=5 < 50) updates, id=2 (v=99 < 50 false) keeps,
					// id=3 inserts cleanly.
					Query: `INSERT INTO w4 (id, v) VALUES (1, 10), (2, 22), (3, 33)
ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE w4.v < 50;`,
				},
				{
					Query: "SELECT id, v FROM w4 ORDER BY id;",
					Expected: []gms.Row{
						{1, 10}, {2, 99}, {3, 33},
					},
				},
			},
		},
	})
}

// TestInsertOnConflictMultiUnique covers the workload pattern that
// real apps with id PK + email UNIQUE (or any second unique constraint)
// hit on every upsert: ON CONFLICT (id) DO UPDATE on a table with
// multiple unique indexes. PG-correct semantics:
//
//   - conflict on the targeted unique (id) -> DO UPDATE fires
//   - conflict on a non-target unique (email) -> raise the unique
//     constraint violation, NOT silently DO UPDATE
//   - no conflict -> INSERT
//
// The previous Doltgres behavior rejected this entire shape with an
// error to avoid MySQL's permissive ON DUPLICATE KEY UPDATE that fires
// for any unique conflict. With a row-by-row pre-check on non-target
// uniques, the targeted upsert pattern works correctly.
func TestInsertOnConflictMultiUnique(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT (pk) on table with email UNIQUE: target conflict updates",
			SetUpScript: []string{
				"CREATE TABLE u (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u VALUES (1, 'a@x.com', 'first'), (2, 'b@x.com', 'second');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Conflict on PK -> DO UPDATE fires.
					Query: `INSERT INTO u (id, email, name) VALUES (1, 'c@x.com', 'updated')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email;`,
				},
				{
					Query:    "SELECT id, email, name FROM u WHERE id = 1;",
					Expected: []gms.Row{{1, "c@x.com", "updated"}},
				},
			},
		},
		{
			Name: "ON CONFLICT (pk): non-target unique conflict raises",
			SetUpScript: []string{
				"CREATE TABLE u2 (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u2 VALUES (1, 'a@x.com', 'first'), (2, 'b@x.com', 'second');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// New id=3 (no PK conflict) but email='a@x.com' (UNIQUE
					// conflict). PG raises duplicate key violation.
					Query: `INSERT INTO u2 (id, email, name) VALUES (3, 'a@x.com', 'wrong')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;`,
					ExpectedErr: "duplicate",
				},
				{
					// State unchanged: no row id=3 was inserted, and
					// id=1's name is still 'first' (not 'wrong').
					Query: "SELECT id, email, name FROM u2 ORDER BY id;",
					Expected: []gms.Row{
						{1, "a@x.com", "first"},
						{2, "b@x.com", "second"},
					},
				},
			},
		},
		{
			Name: "ON CONFLICT (email): same coverage from the other unique direction",
			SetUpScript: []string{
				"CREATE TABLE u3 (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u3 VALUES (1, 'a@x.com', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Email conflict -> DO UPDATE fires.
					Query: `INSERT INTO u3 (id, email, name) VALUES (99, 'a@x.com', 'updated')
ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name;`,
				},
				{
					Query:    "SELECT id, email, name FROM u3;",
					Expected: []gms.Row{{1, "a@x.com", "updated"}},
				},
				{
					// PK conflict (id=1) without email conflict -> raises.
					Query: `INSERT INTO u3 (id, email, name) VALUES (1, 'fresh@x.com', 'wrong')
ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name;`,
					ExpectedErr: "duplicate",
				},
			},
		},
		{
			Name: "ON CONFLICT (pk) DO NOTHING with multi-unique: target conflict ignored",
			SetUpScript: []string{
				"CREATE TABLE u4 (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u4 VALUES (1, 'a@x.com', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// PK conflict on the named target -> ignored. The
					// pre-check inserter wrapper sees email='b@x.com'
					// has no non-target conflict, so the row reaches
					// the underlying inserter, which raises the PK
					// violation that INSERT IGNORE then swallows.
					Query: `INSERT INTO u4 (id, email, name) VALUES (1, 'b@x.com', 'wrong') ON CONFLICT (id) DO NOTHING;`,
				},
				{
					Query:    "SELECT id, email, name FROM u4;",
					Expected: []gms.Row{{1, "a@x.com", "first"}},
				},
			},
		},
		{
			Name: "ON CONFLICT (pk) DO NOTHING raises on non-target unique conflict",
			SetUpScript: []string{
				"CREATE TABLE u4b (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u4b VALUES (1, 'a@x.com', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					// New id, but email already on id=1. Without the
					// pre-check this would be silently swallowed by
					// INSERT IGNORE; with the wrapper, the non-target
					// conflict surfaces.
					Query:       `INSERT INTO u4b (id, email, name) VALUES (2, 'a@x.com', 'wrong') ON CONFLICT (id) DO NOTHING;`,
					ExpectedErr: "duplicate key value violates unique constraint",
				},
				{
					Query:    "SELECT id, email, name FROM u4b ORDER BY id;",
					Expected: []gms.Row{{1, "a@x.com", "first"}},
				},
			},
		},
		{
			Name: "ON CONFLICT (email) on table with id PK + email UNIQUE (2 seed rows)",
			SetUpScript: []string{
				"CREATE TABLE u_two (id INT PRIMARY KEY, email TEXT UNIQUE, name TEXT);",
				"INSERT INTO u_two VALUES (1, 'a@x.com', 'first'), (2, 'b@x.com', 'second');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO u_two VALUES (3, 'a@x.com', 'email update')
ON CONFLICT (email) DO UPDATE SET name = 'email update';`,
				},
				{
					Query: "SELECT id, email, name FROM u_two ORDER BY id;",
					Expected: []gms.Row{
						{1, "a@x.com", "email update"},
						{2, "b@x.com", "second"},
					},
				},
			},
		},
	})
}

// TestInsertOnConflictArbiterPredicate covers the
// `ON CONFLICT (col) WHERE arb_pred` form used to disambiguate
// partial unique indexes. Full-table unique indexes still accept a
// benign arbiter predicate, while partial unique indexes require an
// exact predicate match.
func TestInsertOnConflictArbiterPredicate(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT (col) WHERE pred parses and routes through target",
			SetUpScript: []string{
				"CREATE TABLE arb_t (id INT PRIMARY KEY, v INT);",
				"INSERT INTO arb_t VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					// Arbiter predicate is accepted; the existing
					// target-by-columns path resolves the unique
					// index for `id` and the upsert proceeds.
					Query: "INSERT INTO arb_t VALUES (1, 99) ON CONFLICT (id) WHERE id > 0 DO UPDATE SET v = EXCLUDED.v;",
				},
				{
					Query:    "SELECT v FROM arb_t WHERE id = 1;",
					Expected: []gms.Row{{99}},
				},
				{
					// DO NOTHING shape with arbiter predicate.
					Query: "INSERT INTO arb_t VALUES (1, 1) ON CONFLICT (id) WHERE id IS NOT NULL DO NOTHING;",
				},
				{
					Query:    "SELECT v FROM arb_t WHERE id = 1;",
					Expected: []gms.Row{{99}},
				},
			},
		},
		{
			Name: "ON CONFLICT targets partial unique index predicate",
			SetUpScript: []string{
				"CREATE TABLE partial_arb (id INT PRIMARY KEY, user_id INT, status TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_active_idx ON partial_arb (user_id) WHERE status = 'active';",
				"INSERT INTO partial_arb VALUES (1, 10, 'active', 'old'), (2, 10, 'inactive', 'inactive');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb VALUES (3, 10, 'active', 'updated')
ON CONFLICT (user_id) WHERE status = 'active' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, status, note FROM partial_arb ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active", "updated"},
						{2, 10, "inactive", "inactive"},
					},
				},
				{
					Query: `INSERT INTO partial_arb VALUES (4, 10, 'inactive', 'inactive2')
ON CONFLICT (user_id) WHERE status = 'active' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `INSERT INTO partial_arb VALUES (5, 10, 'active', 'ignored')
ON CONFLICT (user_id) WHERE status = 'active' DO NOTHING;`,
				},
				{
					Query: `INSERT INTO partial_arb VALUES (8, 10, 'active', 'implied')
ON CONFLICT (user_id) WHERE status = 'active' AND note IS NOT NULL DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, status, note FROM partial_arb ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active", "implied"},
						{2, 10, "inactive", "inactive"},
						{4, 10, "inactive", "inactive2"},
					},
				},
				{
					Query: `INSERT INTO partial_arb VALUES (6, 10, 'active', 'wrong-predicate')
ON CONFLICT (user_id) WHERE status = 'inactive' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
				{
					Query: `INSERT INTO partial_arb VALUES (7, 10, 'active', 'wrong-target')
ON CONFLICT (id) DO NOTHING;`,
					ExpectedErr: `duplicate key value violates unique constraint "partial_arb_active_idx"`,
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports stronger inequality predicate",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_score (id INT PRIMARY KEY, user_id INT, score INT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_score_positive_idx ON partial_arb_score (user_id) WHERE score > 0;",
				"INSERT INTO partial_arb_score VALUES (1, 10, 5, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_score VALUES (2, 10, 11, 'stronger')
ON CONFLICT (user_id) WHERE score > 10 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query:    `SELECT id, user_id, score, note FROM partial_arb_score ORDER BY id;`,
					Expected: []gms.Row{{1, 10, 5, "stronger"}},
				},
				{
					Query: `INSERT INTO partial_arb_score VALUES (3, 10, 20, 'stronger-or-equal')
ON CONFLICT (user_id) WHERE score >= 10 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query:    `SELECT id, user_id, score, note FROM partial_arb_score ORDER BY id;`,
					Expected: []gms.Row{{1, 10, 5, "stronger-or-equal"}},
				},
				{
					Query: `INSERT INTO partial_arb_score VALUES (4, 10, 1, 'not-strong-enough')
ON CONFLICT (user_id) WHERE score >= 0 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports bounded numeric subset predicate",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_window (id INT PRIMARY KEY, user_id INT, score INT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_window_idx ON partial_arb_window (user_id) WHERE score > 0 AND score < 100;",
				"INSERT INTO partial_arb_window VALUES (1, 10, 50, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_window VALUES (2, 10, 60, 'bounded')
ON CONFLICT (user_id) WHERE score > 10 AND score < 90 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query:    `SELECT id, user_id, score, note FROM partial_arb_window ORDER BY id;`,
					Expected: []gms.Row{{1, 10, 50, "bounded"}},
				},
				{
					Query: `INSERT INTO partial_arb_window VALUES (3, 10, 80, 'upper-not-subset')
ON CONFLICT (user_id) WHERE score > 10 AND score <= 100 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports BETWEEN range predicate",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_between (id INT PRIMARY KEY, user_id INT, score INT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_between_idx ON partial_arb_between (user_id) WHERE score > 0 AND score < 100;",
				"INSERT INTO partial_arb_between VALUES (1, 10, 50, 'old');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_between VALUES (2, 10, 60, 'between')
ON CONFLICT (user_id) WHERE score BETWEEN 10 AND 90 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query:    `SELECT id, user_id, score, note FROM partial_arb_between ORDER BY id;`,
					Expected: []gms.Row{{1, 10, 50, "between"}},
				},
				{
					Query: `INSERT INTO partial_arb_between VALUES (3, 10, 60, 'not-subset')
ON CONFLICT (user_id) WHERE score BETWEEN -10 AND 90 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports boolean predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_bool (id INT PRIMARY KEY, user_id INT, active BOOL, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_bool_active_idx ON partial_arb_bool (user_id) WHERE active;",
				"CREATE UNIQUE INDEX partial_arb_bool_inactive_idx ON partial_arb_bool (user_id) WHERE NOT active;",
				"INSERT INTO partial_arb_bool VALUES (1, 10, true, 'old-active'), (2, 10, false, 'old-inactive'), (3, 10, NULL, 'unknown');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_bool VALUES (4, 10, true, 'new-active')
ON CONFLICT (user_id) WHERE active = true DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `INSERT INTO partial_arb_bool VALUES (5, 10, false, 'new-inactive')
ON CONFLICT (user_id) WHERE active = false DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, active, note FROM partial_arb_bool ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "t", "new-active"},
						{2, 10, "f", "new-inactive"},
						{3, 10, nil, "unknown"},
					},
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports OR predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_or (id INT PRIMARY KEY, user_id INT, active BOOL, archived BOOL, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_or_visible_idx ON partial_arb_or (user_id) WHERE active OR archived;",
				"INSERT INTO partial_arb_or VALUES (1, 10, true, false, 'old-visible'), (2, 10, false, false, 'hidden');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_or VALUES (3, 10, true, false, 'active-upsert')
ON CONFLICT (user_id) WHERE active DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, active, archived, note FROM partial_arb_or ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "t", "f", "active-upsert"},
						{2, 10, "f", "f", "hidden"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_or VALUES (4, 10, false, true, 'archived-upsert')
ON CONFLICT (user_id) WHERE archived = true DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, active, archived, note FROM partial_arb_or ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "t", "f", "archived-upsert"},
						{2, 10, "f", "f", "hidden"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_or VALUES (5, 10, true, false, 'reordered-or')
ON CONFLICT (user_id) WHERE archived OR active DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, active, archived, note FROM partial_arb_or ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "t", "f", "reordered-or"},
						{2, 10, "f", "f", "hidden"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_or VALUES (6, 10, true, false, 'wrong-predicate')
ON CONFLICT (user_id) WHERE active = false DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports IN-list predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_in (id INT PRIMARY KEY, user_id INT, status TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_in_open_idx ON partial_arb_in (user_id) WHERE status IN ('active', 'pending');",
				"INSERT INTO partial_arb_in VALUES (1, 10, 'active', 'old-open'), (2, 10, 'archived', 'closed');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_in VALUES (3, 10, 'pending', 'pending-upsert')
ON CONFLICT (user_id) WHERE status = 'pending' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, status, note FROM partial_arb_in ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active", "pending-upsert"},
						{2, 10, "archived", "closed"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_in VALUES (4, 10, 'active', 'in-list-upsert')
ON CONFLICT (user_id) WHERE status IN ('pending', 'active') DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, status, note FROM partial_arb_in ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active", "in-list-upsert"},
						{2, 10, "archived", "closed"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_in VALUES (5, 10, 'active', 'wrong-predicate')
ON CONFLICT (user_id) WHERE status IN ('active', 'archived') DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports exclusion-set predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_not_in (id INT PRIMARY KEY, user_id INT, status TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_not_in_user_idx ON partial_arb_not_in (user_id) WHERE status NOT IN ('archived', 'deleted');",
				"INSERT INTO partial_arb_not_in VALUES (1, 10, 'active', 'old-active'), (2, 10, 'archived', 'archived-row');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_not_in VALUES (3, 10, 'active', 'not-in-upsert')
ON CONFLICT (user_id) WHERE status NOT IN ('archived', 'deleted', 'blocked') DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, status, note FROM partial_arb_not_in ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active", "not-in-upsert"},
						{2, 10, "archived", "archived-row"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_not_in VALUES (4, 10, 'active', 'wrong-predicate')
ON CONFLICT (user_id) WHERE status != 'archived' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports expression value-set predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_expr_in (id INT PRIMARY KEY, email TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_expr_in_email_idx ON partial_arb_expr_in (email) WHERE lower(email) IN ('active@example.com', 'admin@example.com');",
				"INSERT INTO partial_arb_expr_in VALUES (1, 'active@example.com', 'old-active'), (2, 'admin@example.com', 'old-admin');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_expr_in VALUES (3, 'active@example.com', 'active-upsert')
ON CONFLICT (email) WHERE lower(email) = 'active@example.com' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, email, note FROM partial_arb_expr_in ORDER BY id;`,
					Expected: []gms.Row{
						{1, "active@example.com", "active-upsert"},
						{2, "admin@example.com", "old-admin"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_expr_in VALUES (4, 'admin@example.com', 'admin-upsert')
ON CONFLICT (email) WHERE lower(email) IN ('admin@example.com', 'active@example.com') DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, email, note FROM partial_arb_expr_in ORDER BY id;`,
					Expected: []gms.Row{
						{1, "active@example.com", "active-upsert"},
						{2, "admin@example.com", "admin-upsert"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_expr_in VALUES (5, 'active@example.com', 'wrong-predicate')
ON CONFLICT (email) WHERE lower(email) IN ('active@example.com', 'other@example.com') DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports equality-chain predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_cross_chain (id INT PRIMARY KEY, user_id INT, tenant INT, workspace_tenant INT, owner_tenant INT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_cross_chain_user_idx ON partial_arb_cross_chain (user_id) WHERE tenant = owner_tenant;",
				"INSERT INTO partial_arb_cross_chain VALUES (1, 10, 1, 1, 1, 'old-same'), (2, 10, NULL, NULL, NULL, 'null-row');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_cross_chain VALUES (3, 10, 1, 1, 1, 'chain-upsert')
ON CONFLICT (user_id) WHERE tenant = workspace_tenant AND workspace_tenant = owner_tenant DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, tenant, workspace_tenant, owner_tenant, note FROM partial_arb_cross_chain ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, 1, 1, 1, "chain-upsert"},
						{2, 10, nil, nil, nil, "null-row"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_cross_chain VALUES (4, 10, 1, 1, 1, 'wrong-predicate')
ON CONFLICT (user_id) WHERE tenant IS NOT DISTINCT FROM workspace_tenant AND workspace_tenant IS NOT DISTINCT FROM owner_tenant DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports coalesce predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_coalesce (id INT PRIMARY KEY, code TEXT, status TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_coalesce_code_idx ON partial_arb_coalesce (code) WHERE coalesce(status, 'inactive') = 'active';",
				"INSERT INTO partial_arb_coalesce VALUES (1, 'A', 'active', 'old-active'), (2, 'A', NULL, 'null-status');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_coalesce VALUES (3, 'A', 'active', 'coalesce-upsert')
ON CONFLICT (code) WHERE coalesce(status, 'inactive') = 'active' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, status, note FROM partial_arb_coalesce ORDER BY id;`,
					Expected: []gms.Row{
						{1, "A", "active", "coalesce-upsert"},
						{2, "A", nil, "null-status"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_coalesce VALUES (4, 'A', 'active', 'wrong-predicate')
ON CONFLICT (code) WHERE status = 'active' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports text-length predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_length (id INT PRIMARY KEY, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_length_code_idx ON partial_arb_length (code) WHERE length(code) = 6;",
				"INSERT INTO partial_arb_length VALUES (1, 'active', 'old-active'), (2, 'archived', 'old-archived');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_length VALUES (3, 'active', 'length-upsert')
ON CONFLICT (code) WHERE char_length(code) = 6 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, note FROM partial_arb_length ORDER BY id;`,
					Expected: []gms.Row{
						{1, "active", "length-upsert"},
						{2, "archived", "old-archived"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_length VALUES (4, 'active', 'wrong-predicate')
ON CONFLICT (code) WHERE length(code) = 7 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports octet_length predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_octet (id INT PRIMARY KEY, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_octet_code_idx ON partial_arb_octet (code) WHERE octet_length(code) = 3;",
				"INSERT INTO partial_arb_octet VALUES (1, 'abc', 'old-abc'), (2, 'de', 'old-de');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_octet VALUES (3, 'abc', 'octet-upsert')
ON CONFLICT (code) WHERE octet_length(code) = 3 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, note FROM partial_arb_octet ORDER BY id;`,
					Expected: []gms.Row{
						{1, "abc", "octet-upsert"},
						{2, "de", "old-de"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_octet VALUES (4, 'abc', 'wrong-predicate')
ON CONFLICT (code) WHERE length(code) = 3 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports bit_length predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_bit (id INT PRIMARY KEY, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_bit_code_idx ON partial_arb_bit (code) WHERE bit_length(code) = 24;",
				"INSERT INTO partial_arb_bit VALUES (1, 'abc', 'old-abc'), (2, 'de', 'old-de');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_bit VALUES (3, 'abc', 'bit-upsert')
ON CONFLICT (code) WHERE bit_length(code) = 24 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, note FROM partial_arb_bit ORDER BY id;`,
					Expected: []gms.Row{
						{1, "abc", "bit-upsert"},
						{2, "de", "old-de"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_bit VALUES (4, 'abc', 'wrong-predicate')
ON CONFLICT (code) WHERE octet_length(code) = 3 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports strpos predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_strpos (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_strpos_user_idx ON partial_arb_strpos (user_id) WHERE strpos(code, 'active') = 1;",
				"INSERT INTO partial_arb_strpos VALUES (1, 10, 'active-a', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_strpos VALUES (3, 10, 'active-b', 'strpos-upsert')
ON CONFLICT (user_id) WHERE strpos(code, 'active') = 1 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_strpos ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active-a", "strpos-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_strpos VALUES (4, 10, 'active-c', 'wrong-predicate')
ON CONFLICT (user_id) WHERE strpos(code, 'pending') = 1 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports starts_with predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_starts_with (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_starts_with_user_idx ON partial_arb_starts_with (user_id) WHERE starts_with(code, 'active');",
				"INSERT INTO partial_arb_starts_with VALUES (1, 10, 'active-a', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_starts_with VALUES (3, 10, 'active-b', 'starts-with-upsert')
ON CONFLICT (user_id) WHERE starts_with(code, 'active') = true DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_starts_with ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active-a", "starts-with-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_starts_with VALUES (4, 10, 'active-c', 'wrong-predicate')
ON CONFLICT (user_id) WHERE starts_with(code, 'pending') DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports prefix LIKE predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_like_prefix (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_like_prefix_user_idx ON partial_arb_like_prefix (user_id) WHERE code LIKE 'active%';",
				"INSERT INTO partial_arb_like_prefix VALUES (1, 10, 'active-a1', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_like_prefix VALUES (3, 10, 'active-a2', 'like-prefix-upsert')
ON CONFLICT (user_id) WHERE code LIKE 'active-a%' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_like_prefix ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active-a1", "like-prefix-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_like_prefix VALUES (4, 10, 'active-b', 'wrong-predicate')
ON CONFLICT (user_id) WHERE code LIKE 'pending%' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports left predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_left (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_left_user_idx ON partial_arb_left (user_id) WHERE left(code, 2) = 'åc';",
				"INSERT INTO partial_arb_left VALUES (1, 10, 'åctive-a', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_left VALUES (3, 10, 'åctor', 'left-upsert')
ON CONFLICT (user_id) WHERE left(code, 2) = 'åc' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_left ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "åctive-a", "left-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_left VALUES (4, 10, 'åction', 'wrong-predicate')
ON CONFLICT (user_id) WHERE left(code, 4) = 'åcti' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports right predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_right (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_right_user_idx ON partial_arb_right (user_id) WHERE right(code, -1) = 'ctive';",
				"INSERT INTO partial_arb_right VALUES (1, 20, 'åctive', 'old-active'), (2, 20, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_right VALUES (3, 20, 'bctive', 'right-upsert')
ON CONFLICT (user_id) WHERE right(code, -1) = 'ctive' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_right ORDER BY id;`,
					Expected: []gms.Row{
						{1, 20, "åctive", "right-upsert"},
						{2, 20, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_right VALUES (4, 20, 'cctive', 'wrong-predicate')
ON CONFLICT (user_id) WHERE right(code, 2) = 've' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports replace predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_replace (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_replace_user_idx ON partial_arb_replace (user_id) WHERE replace(code, '-', '') = 'activea';",
				"INSERT INTO partial_arb_replace VALUES (1, 10, 'active-a', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_replace VALUES (3, 10, 'active--a', 'replace-upsert')
ON CONFLICT (user_id) WHERE replace(code, '-', '') = 'activea' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_replace ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active-a", "replace-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_replace VALUES (4, 10, 'active-a', 'wrong-predicate')
ON CONFLICT (user_id) WHERE replace(code, '_', '') = 'active-a' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports translate predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_translate (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_translate_user_idx ON partial_arb_translate (user_id) WHERE translate(code, '-_', '') = 'activea';",
				"INSERT INTO partial_arb_translate VALUES (1, 10, 'active-a', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_translate VALUES (3, 10, 'active__a', 'translate-upsert')
ON CONFLICT (user_id) WHERE translate(code, '-_', '') = 'activea' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_translate ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active-a", "translate-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_translate VALUES (4, 10, 'active-a', 'wrong-predicate')
ON CONFLICT (user_id) WHERE translate(code, '-.', '') = 'activea' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports md5 predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_md5 (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_md5_user_idx ON partial_arb_md5 (user_id) WHERE md5(code) = 'c76a5e84e4bdee527e274ea30c680d79';",
				"INSERT INTO partial_arb_md5 VALUES (1, 10, 'active', 'old-active'), (2, 10, 'pending', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_md5 VALUES (3, 10, 'active', 'md5-upsert')
ON CONFLICT (user_id) WHERE md5(code) = 'c76a5e84e4bdee527e274ea30c680d79' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_md5 ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "active", "md5-upsert"},
						{2, 10, "pending", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_md5 VALUES (4, 10, 'active', 'wrong-predicate')
ON CONFLICT (user_id) WHERE code = 'active' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports split_part predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_split_part (id INT PRIMARY KEY, user_id INT, email TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_split_part_user_idx ON partial_arb_split_part (user_id) WHERE split_part(email, '@', 2) = 'example.com';",
				"INSERT INTO partial_arb_split_part VALUES (1, 10, 'first@example.com', 'old-active'), (2, 10, 'second@example.org', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_split_part VALUES (3, 10, 'other@example.com', 'split-part-upsert')
ON CONFLICT (user_id) WHERE split_part(email, '@', 2) = 'example.com' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, email, note FROM partial_arb_split_part ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "first@example.com", "split-part-upsert"},
						{2, 10, "second@example.org", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_split_part VALUES (4, 10, 'another@example.com', 'wrong-predicate')
ON CONFLICT (user_id) WHERE split_part(email, '.', 2) = 'com' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports ascii predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_ascii (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_ascii_user_idx ON partial_arb_ascii (user_id) WHERE ascii(code) = 65;",
				"INSERT INTO partial_arb_ascii VALUES (1, 10, 'Alpha', 'old-active'), (2, 10, 'beta', 'old-pending');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_ascii VALUES (3, 10, 'Admin', 'ascii-upsert')
ON CONFLICT (user_id) WHERE ascii(code) = 65 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_ascii ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "Alpha", "ascii-upsert"},
						{2, 10, "beta", "old-pending"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_ascii VALUES (4, 10, 'April', 'wrong-predicate')
ON CONFLICT (user_id) WHERE code = 'April' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports substring predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_substring (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_substring_user_idx ON partial_arb_substring (user_id) WHERE substr(code, 1, 3) = 'Adm';",
				"INSERT INTO partial_arb_substring VALUES (1, 10, 'Admin', 'old-admin'), (2, 10, 'Alpha', 'old-alpha');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_substring VALUES (3, 10, 'Admiral', 'substring-upsert')
ON CONFLICT (user_id) WHERE substring(code, 1, 3) = 'Adm' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_substring ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "Admin", "substring-upsert"},
						{2, 10, "Alpha", "old-alpha"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_substring VALUES (4, 10, 'Admire', 'wrong-predicate')
ON CONFLICT (user_id) WHERE code = 'Admire' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports reverse predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_reverse (id INT PRIMARY KEY, user_id INT, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_reverse_user_idx ON partial_arb_reverse (user_id) WHERE reverse(code) = 'nimdA';",
				"INSERT INTO partial_arb_reverse VALUES (1, 10, 'Admin', 'old-admin'), (2, 10, 'Alpha', 'old-alpha');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_reverse VALUES (3, 10, 'Admin', 'reverse-upsert')
ON CONFLICT (user_id) WHERE reverse(code) = 'nimdA' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, code, note FROM partial_arb_reverse ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "Admin", "reverse-upsert"},
						{2, 10, "Alpha", "old-alpha"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_reverse VALUES (4, 10, 'Admin', 'wrong-predicate')
ON CONFLICT (user_id) WHERE code = 'Admin' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports to_hex predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_to_hex (id INT PRIMARY KEY, user_id INT, account_id INT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_to_hex_user_idx ON partial_arb_to_hex (user_id) WHERE to_hex(account_id) = 'a';",
				"INSERT INTO partial_arb_to_hex VALUES (1, 10, 10, 'old-hex'), (2, 10, 11, 'old-other');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_to_hex VALUES (3, 10, 10, 'hex-upsert')
ON CONFLICT (user_id) WHERE to_hex(account_id) = 'a' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, account_id, note FROM partial_arb_to_hex ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, 10, "hex-upsert"},
						{2, 10, 11, "old-other"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_to_hex VALUES (4, 10, 10, 'wrong-predicate')
ON CONFLICT (user_id) WHERE account_id = 10 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports initcap predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_initcap (id INT PRIMARY KEY, user_id INT, role TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_initcap_user_idx ON partial_arb_initcap (user_id) WHERE initcap(role) = 'Admin User';",
				"INSERT INTO partial_arb_initcap VALUES (1, 10, 'admin user', 'old-admin'), (2, 10, 'regular user', 'old-regular');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_initcap VALUES (3, 10, 'admin user', 'initcap-upsert')
ON CONFLICT (user_id) WHERE initcap(role) = 'Admin User' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, role, note FROM partial_arb_initcap ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "admin user", "initcap-upsert"},
						{2, 10, "regular user", "old-regular"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_initcap VALUES (4, 10, 'admin user', 'wrong-predicate')
ON CONFLICT (user_id) WHERE role = 'admin user' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports quote_literal predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_quote_literal (id INT PRIMARY KEY, user_id INT, role TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_quote_literal_user_idx ON partial_arb_quote_literal (user_id) WHERE quote_literal(role) = '''admin user''';",
				"INSERT INTO partial_arb_quote_literal VALUES (1, 10, 'admin user', 'old-admin'), (2, 10, 'regular user', 'old-regular');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_quote_literal VALUES (3, 10, 'admin user', 'quote-literal-upsert')
ON CONFLICT (user_id) WHERE quote_literal(role) = '''admin user''' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, role, note FROM partial_arb_quote_literal ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "admin user", "quote-literal-upsert"},
						{2, 10, "regular user", "old-regular"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_quote_literal VALUES (4, 10, 'admin user', 'wrong-predicate')
ON CONFLICT (user_id) WHERE role = 'admin user' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports quote_ident predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_quote_ident (id INT PRIMARY KEY, user_id INT, role TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_quote_ident_user_idx ON partial_arb_quote_ident (user_id) WHERE quote_ident(role) = '\"admin user\"';",
				"INSERT INTO partial_arb_quote_ident VALUES (1, 10, 'admin user', 'old-admin'), (2, 10, 'regular user', 'old-regular');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_quote_ident VALUES (3, 10, 'admin user', 'quote-ident-upsert')
ON CONFLICT (user_id) WHERE quote_ident(role) = '"admin user"' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, role, note FROM partial_arb_quote_ident ORDER BY id;`,
					Expected: []gms.Row{
						{1, 10, "admin user", "quote-ident-upsert"},
						{2, 10, "regular user", "old-regular"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_quote_ident VALUES (4, 10, 'admin user', 'wrong-predicate')
ON CONFLICT (user_id) WHERE role = 'admin user' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports trim-function predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_trim (id INT PRIMARY KEY, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_trim_code_idx ON partial_arb_trim (code) WHERE ltrim(code) = 'active';",
				"INSERT INTO partial_arb_trim VALUES (1, ' active', 'old-active'), (2, 'archived', 'old-archived');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_trim VALUES (3, ' active', 'trim-upsert')
ON CONFLICT (code) WHERE ltrim(code) = 'active' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, note FROM partial_arb_trim ORDER BY id;`,
					Expected: []gms.Row{
						{1, " active", "trim-upsert"},
						{2, "archived", "old-archived"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_trim VALUES (4, ' active', 'wrong-predicate')
ON CONFLICT (code) WHERE rtrim(code) = 'active' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports btrim predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_btrim (id INT PRIMARY KEY, code TEXT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_btrim_code_idx ON partial_arb_btrim (code) WHERE btrim(code) = 'active';",
				"INSERT INTO partial_arb_btrim VALUES (1, ' active ', 'old-active'), (2, 'archived', 'old-archived');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_btrim VALUES (3, ' active ', 'btrim-upsert')
ON CONFLICT (code) WHERE btrim(code) = 'active' DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, code, note FROM partial_arb_btrim ORDER BY id;`,
					Expected: []gms.Row{
						{1, " active ", "btrim-upsert"},
						{2, "archived", "old-archived"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_btrim VALUES (4, ' active ', 'wrong-predicate')
ON CONFLICT (code) WHERE btrim(code) = 'archived' DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
		{
			Name: "ON CONFLICT partial unique index supports abs predicate implication",
			SetUpScript: []string{
				"CREATE TABLE partial_arb_abs (id INT PRIMARY KEY, user_id INT, delta BIGINT, note TEXT);",
				"CREATE UNIQUE INDEX partial_arb_abs_user_idx ON partial_arb_abs (user_id) WHERE abs(delta) = 10;",
				"INSERT INTO partial_arb_abs VALUES (1, 10, -10, 'old-active'), (2, 10, 5, 'small-delta');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `INSERT INTO partial_arb_abs VALUES (3, 10, 10, 'abs-upsert')
ON CONFLICT (user_id) WHERE abs(delta) = 10 DO UPDATE SET note = EXCLUDED.note;`,
				},
				{
					Query: `SELECT id, user_id, delta, note FROM partial_arb_abs ORDER BY id;`,
					Expected: []gms.Row{
						{1, int32(10), int64(-10), "abs-upsert"},
						{2, int32(10), int64(5), "small-delta"},
					},
				},
				{
					Query: `INSERT INTO partial_arb_abs VALUES (4, 10, 10, 'wrong-predicate')
ON CONFLICT (user_id) WHERE delta = 10 DO NOTHING;`,
					ExpectedErr: "there is no unique or exclusion constraint matching the ON CONFLICT specification",
				},
			},
		},
	})
}

// TestInsertOnConflictOnConstraint covers the
// `ON CONFLICT ON CONSTRAINT name` syntax. ORM-generated upserts
// (Drizzle .onConflictDoUpdate({target: "constraint_name"}),
// SQLAlchemy.dialects.postgresql.insert(...).on_conflict_do_update
// with constraint=) routinely use the named-constraint form because
// it resolves cleanly even when the constraint columns include
// expressions or are inferred from a table-rename migration.
//
// The implementation looks up the constraint by name, derives its
// column list, and routes through the existing target-by-columns
// pipeline (which already handles the multi-unique target guard
// added earlier).
func TestInsertOnConflictOnConstraint(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ON CONFLICT ON CONSTRAINT named PK index updates",
			SetUpScript: []string{
				"CREATE TABLE oc_pk (id INT PRIMARY KEY, v INT);",
				"INSERT INTO oc_pk VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO oc_pk VALUES (1, 99) ON CONFLICT ON CONSTRAINT oc_pk_pkey DO UPDATE SET v = EXCLUDED.v;",
				},
				{
					Query:    "SELECT v FROM oc_pk WHERE id = 1;",
					Expected: []gms.Row{{99}},
				},
			},
		},
		{
			Name: "ON CONFLICT ON CONSTRAINT named UNIQUE updates",
			SetUpScript: []string{
				"CREATE TABLE oc_uq (id INT PRIMARY KEY, code TEXT, name TEXT);",
				"CREATE UNIQUE INDEX oc_uq_code ON oc_uq (code);",
				"INSERT INTO oc_uq VALUES (1, 'A', 'first');",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO oc_uq VALUES (99, 'A', 'updated') ON CONFLICT ON CONSTRAINT oc_uq_code DO UPDATE SET name = EXCLUDED.name;",
				},
				{
					Query:    "SELECT id, code, name FROM oc_uq;",
					Expected: []gms.Row{{1, "A", "updated"}},
				},
			},
		},
		{
			Name: "ON CONFLICT ON CONSTRAINT DO NOTHING ignores target conflict",
			SetUpScript: []string{
				"CREATE TABLE oc_dn (id INT PRIMARY KEY, v INT);",
				"INSERT INTO oc_dn VALUES (1, 10);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "INSERT INTO oc_dn VALUES (1, 999) ON CONFLICT ON CONSTRAINT oc_dn_pkey DO NOTHING;",
				},
				{
					Query:    "SELECT v FROM oc_dn WHERE id = 1;",
					Expected: []gms.Row{{10}},
				},
			},
		},
		{
			Name: "ON CONFLICT ON CONSTRAINT with unknown name errors",
			SetUpScript: []string{
				"CREATE TABLE oc_bad (id INT PRIMARY KEY);",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query:       "INSERT INTO oc_bad VALUES (1) ON CONFLICT ON CONSTRAINT nope_no_such_constraint DO NOTHING;",
					ExpectedErr: "constraint",
				},
			},
		},
	})
}

// TestInsertOnConflictORMShape exercises the upsert workflow exactly
// as Drizzle / Prisma / SQLAlchemy emit it through the pgx driver.
func TestInsertOnConflictORMShape(t *testing.T) {
	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	dial := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, fmt.Sprintf(
			"postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close(ctx) })
		return conn
	}

	conn := dial(t)
	_, err = conn.Exec(ctx, `CREATE TABLE upserted (
  id INT PRIMARY KEY,
  email TEXT UNIQUE,
  hits INT NOT NULL DEFAULT 0
);`)
	require.NoError(t, err)

	t.Run("Drizzle-shape upsert via parameterized INSERT ON CONFLICT", func(t *testing.T) {
		// Round 1: insert.
		_, err := conn.Exec(ctx,
			`INSERT INTO upserted (id, email, hits) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, hits = upserted.hits + EXCLUDED.hits;`,
			1, "a@x.com", 1)
		require.NoError(t, err)

		// Round 2: conflict on PK -> increment.
		_, err = conn.Exec(ctx,
			`INSERT INTO upserted (id, email, hits) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, hits = upserted.hits + EXCLUDED.hits;`,
			1, "a-2@x.com", 2)
		require.NoError(t, err)

		var hits int
		var email string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT email, hits FROM upserted WHERE id = 1").Scan(&email, &hits))
		require.Equal(t, "a-2@x.com", email)
		require.Equal(t, 3, hits)
	})

	t.Run("non-target unique conflict propagates as a typed error", func(t *testing.T) {
		_, err := conn.Exec(ctx,
			`INSERT INTO upserted (id, email, hits) VALUES (1, 'a-2@x.com', 9)
ON CONFLICT (id) DO UPDATE SET hits = EXCLUDED.hits;`)
		// id=1 conflict -> DO UPDATE applies.
		require.NoError(t, err)

		// New id, but email already on id=1 -> non-target conflict.
		_, err = conn.Exec(ctx, `INSERT INTO upserted (id, email, hits) VALUES (10, 'a-2@x.com', 0)
ON CONFLICT (id) DO UPDATE SET hits = EXCLUDED.hits;`)
		require.Error(t, err)

		// Verify state: id=10 is NOT inserted, id=1 still has the
		// value from the previous successful upsert.
		var count int
		require.NoError(t, conn.QueryRow(context.Background(),
			"SELECT COUNT(*) FROM upserted WHERE id = 10").Scan(&count))
		require.Equal(t, 0, count)
	})
}
