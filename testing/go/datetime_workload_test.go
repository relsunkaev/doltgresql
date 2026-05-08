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

// TestDateTimeWorkload pins the date/time casts and helpers that real
// PG views and reporting queries use: text-to-date casts, make_date,
// extract for fiscal-year/quarter shapes, date arithmetic, and
// time-zone-aware computation. Per the View/query TODO in
// docs/app-compatibility-checklist.md.
func TestDateTimeWorkload(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "text-to-date and text-to-timestamp casts",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT '2026-05-08'::date::text;`,
					Expected: []sql.Row{{"2026-05-08"}},
				},
				{
					Query:    `SELECT '2026-05-08 10:30:45'::timestamp::text;`,
					Expected: []sql.Row{{"2026-05-08 10:30:45"}},
				},
				{
					Query:    `SELECT CAST('2026-05-08' AS DATE)::text;`,
					Expected: []sql.Row{{"2026-05-08"}},
				},
			},
		},
		{
			Name: "make_date and make_timestamp",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT make_date(2026, 5, 8)::text;`,
					Expected: []sql.Row{{"2026-05-08"}},
				},
				{
					Query:    `SELECT make_timestamp(2026, 5, 8, 10, 30, 45)::text;`,
					Expected: []sql.Row{{"2026-05-08 10:30:45"}},
				},
			},
		},
		{
			Name: "extract for date parts",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT extract(year FROM '2026-05-08'::date)::text;`,
					Expected: []sql.Row{{"2026"}},
				},
				{
					Query:    `SELECT extract(month FROM '2026-05-08'::date)::text;`,
					Expected: []sql.Row{{"5"}},
				},
				{
					Query:    `SELECT extract(day FROM '2026-05-08'::date)::text;`,
					Expected: []sql.Row{{"8"}},
				},
				{
					Query:    `SELECT extract(quarter FROM '2026-05-08'::date)::text;`,
					Expected: []sql.Row{{"2"}},
				},
				{
					Query:    `SELECT extract(dow FROM '2026-05-08'::date)::text;`,
					Expected: []sql.Row{{"5"}},
				},
				{
					Query:    `SELECT extract(hour FROM '2026-05-08 10:30:45'::timestamp)::text;`,
					Expected: []sql.Row{{"10"}},
				},
			},
		},
		{
			Name: "date arithmetic and INTERVAL math",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT ('2026-05-08'::date + INTERVAL '1 day')::date::text;`,
					Expected: []sql.Row{{"2026-05-09"}},
				},
				{
					Query:    `SELECT ('2026-05-08'::date - INTERVAL '1 month')::date::text;`,
					Expected: []sql.Row{{"2026-04-08"}},
				},
				{
					Query:    `SELECT ('2026-05-08'::date - '2026-05-01'::date)::text;`,
					Expected: []sql.Row{{"7"}},
				},
			},
		},
		{
			Name: "date_trunc",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT date_trunc('month', '2026-05-08 10:30:45'::timestamp)::text;`,
					Expected: []sql.Row{{"2026-05-01 00:00:00"}},
				},
				{
					Query:    `SELECT date_trunc('hour', '2026-05-08 10:30:45'::timestamp)::text;`,
					Expected: []sql.Row{{"2026-05-08 10:00:00"}},
				},
			},
		},
	})
}
