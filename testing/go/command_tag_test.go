// Copyright 2024 Dolthub, Inc.
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

var CommandTagTests = []ScriptTest{
	{
		Name: "set",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SET extra_float_digits = 3", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0001-set-extra_float_digits-=-3", Compare: "tag"},
			},
		},
	},
	{
		Name: "show",
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW extra_float_digits", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0002-show-extra_float_digits", Compare: "tag"},
			},
		},
	},
	{
		Name: "create database",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE DATABASE mydb", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0003-create-database-mydb", Compare: "tag"},
			},
		},
	},
	{
		Name: "insert",
		SetUpScript: []string{
			"CREATE TABLE table0 (id int, name text)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "INSERT INTO table0 VALUES (1,'Dolt'), (2,'Doltgres'), (3,'DoltHub')", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0004-insert-into-table0-values-1", Compare: "tag"},
			},
			{
				Query: "SELECT * FROM table0 order by id", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0005-select-*-from-table0-order"},
			},
			{
				Query: "SELECT * FROM table0", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0006-select-*-from-table0", Compare: "tag"},
			},
		},
	},
	{
		Name: "update",
		SetUpScript: []string{
			"CREATE TABLE table0 (id int, name text)",
			"INSERT INTO table0 VALUES (1,'Dolt'), (2,'Doltgres'), (3,'DoltHub')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "UPDATE table0 SET id = 4 WHERE name = 'Doltgres'", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0007-update-table0-set-id-=", Compare: "tag"},
			},
			{
				Query: "SELECT * FROM table0 order by id", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0008-select-*-from-table0-order"},
			},
			{
				Query: "SELECT * FROM table0 WHERE name <> 'Dolt'", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0009-select-*-from-table0-where", Compare: "tag"},
			},
		},
	},
	{
		Name: "delete",
		SetUpScript: []string{
			"CREATE TABLE table0 (id int, name text)",
			"INSERT INTO table0 VALUES (1,'Dolt'), (2,'Doltgres'), (3,'DoltHub')",
		},
		Assertions: []ScriptTestAssertion{

			{
				Query: "DELETE FROM table0", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0010-delete-from-table0", Compare: "tag"},
			},
			{
				Query: "SELECT * FROM table0 order by id", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0011-select-*-from-table0-order"},
			},
			{
				Query: "SELECT * FROM table0", PostgresOracle: ScriptTestPostgresOracle{ID: "command-tag-test-testcommandtag-0012-select-*-from-table0", Compare: "tag"},
			},
		},
	},
}

func TestCommandTag(t *testing.T) {
	RunScripts(t, CommandTagTests)
}
