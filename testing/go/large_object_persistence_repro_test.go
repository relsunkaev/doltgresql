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

// TestLargeObjectCreatePersistsMetadataRepro reproduces a large-object
// persistence bug: lo_create should create a large object and expose its OID
// through pg_largeobject_metadata.
func TestLargeObjectCreatePersistsMetadataRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "lo_create persists large object metadata",
			Assertions: []ScriptTestAssertion{
				{
					Query:    `SELECT lo_create(424242);`,
					Expected: []sql.Row{{"424242"}},
				},
				{
					Query: `SELECT oid::TEXT
						FROM pg_catalog.pg_largeobject_metadata
						WHERE oid = 424242;`,
					Expected: []sql.Row{{"424242"}},
				},
				{
					Query:    `SELECT lo_unlink(424242);`,
					Expected: []sql.Row{{1}},
				},
			},
		},
	})
}

// TestLargeObjectByteaRoundTripRepro reproduces a large-object persistence
// bug: lo_from_bytea should create a large object whose bytes are readable by
// lo_get through the returned OID.
func TestLargeObjectByteaRoundTripRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "large object bytea round trip",
			SetUpScript: []string{
				`CREATE TABLE large_object_round_trip_stash (loid OID);`,
				`INSERT INTO large_object_round_trip_stash
					SELECT lo_from_bytea(0, decode('deadbeef', 'hex'));`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT encode(lo_get(loid), 'hex')
						FROM large_object_round_trip_stash;`,
					Expected: []sql.Row{{"deadbeef"}},
				},
			},
		},
	})
}
