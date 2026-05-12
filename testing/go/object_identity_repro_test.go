package _go

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestPgGetObjectAddressTableRepro documents the PostgreSQL object-address
// helper used by pg_dump, event triggers, dependency introspection, and tooling.
func TestPgGetObjectAddressTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_get_object_address resolves a table name",
			SetUpScript: []string{
				`CREATE TABLE object_address_items (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `
SELECT classid::regclass::TEXT, objid::regclass::TEXT, objsubid::TEXT
FROM pg_catalog.pg_get_object_address(
	'table',
	ARRAY['public', 'object_address_items'],
	ARRAY[]::TEXT[]
);`,
					Expected: []sql.Row{{"pg_class", "object_address_items", "0"}},
				},
			},
		},
	})
}

// TestPgDescribeObjectTableRepro documents the reverse catalog helper that
// turns object addresses into PostgreSQL-compatible user-facing descriptions.
func TestPgDescribeObjectTableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "pg_describe_object describes a table address",
			SetUpScript: []string{
				`CREATE TABLE object_describe_items (id INT PRIMARY KEY);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `
SELECT (pg_catalog.pg_describe_object(
	'pg_class'::regclass,
	'object_describe_items'::regclass,
	0
) LIKE '%object_describe_items%')::TEXT;`,
					Expected: []sql.Row{{"true"}},
				},
			},
		},
	})
}
