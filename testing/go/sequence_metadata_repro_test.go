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

// TestCreateSequencePopulatesPgSequencesRepro reproduces a catalog persistence
// bug: Doltgres accepts CREATE SEQUENCE but pg_sequences does not expose the
// created sequence.
func TestCreateSequencePopulatesPgSequencesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SEQUENCE populates pg_sequences",
			SetUpScript: []string{
				`CREATE SEQUENCE sequence_catalog_target START WITH 5 INCREMENT BY 2;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT schemaname, sequencename
						FROM pg_catalog.pg_sequences
						WHERE sequencename = 'sequence_catalog_target';`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-metadata-repro-test-testcreatesequencepopulatespgsequencesrepro-0001-select-schemaname-sequencename-from-pg_catalog.pg_sequences",

						// TestCreateSequencePopulatesPgStatioSequenceViewsRepro reproduces a catalog
						// persistence bug: PostgreSQL exposes created sequences through sequence I/O
						// stats views.
						ColumnModes: []string{"schema"}},
				},
			},
		},
	})
}

func TestCreateSequencePopulatesPgStatioSequenceViewsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE SEQUENCE populates pg_statio sequence views",
			SetUpScript: []string{
				`CREATE SEQUENCE statio_sequence_catalog_target;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT
							EXISTS (
								SELECT 1
								FROM pg_catalog.pg_statio_user_sequences
								WHERE schemaname = 'public'
									AND relname = 'statio_sequence_catalog_target'
									AND blks_read >= 0
							),
							EXISTS (
								SELECT 1
								FROM pg_catalog.pg_statio_all_sequences
								WHERE schemaname = 'public'
									AND relname = 'statio_sequence_catalog_target'
									AND blks_read >= 0
							);`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-metadata-repro-test-testcreatesequencepopulatespgstatiosequenceviewsrepro-0001-select-exists-select-1-from"},
				},
			},
		},
	})
}

// TestCreateTemporarySequenceRepro reproduces a temporary-relation correctness
// gap: PostgreSQL supports session-local temporary sequences.
func TestCreateTemporarySequenceRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEMPORARY SEQUENCE creates a usable temp sequence",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TEMPORARY SEQUENCE temp_sequence_target;`,
				},
				{
					Query: `SELECT nextval('temp_sequence_target');`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-metadata-repro-test-testcreatetemporarysequencerepro-0001-select-nextval-temp_sequence_target"},
				},
			},
		},
	})
}

// TestSequenceRelationTracksIsCalledRepro reproduces a sequence metadata bug:
// PostgreSQL exposes whether a sequence value has been consumed through the
// sequence relation's is_called column.
func TestSequenceRelationTracksIsCalledRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "sequence relation tracks is_called",
			SetUpScript: []string{
				`CREATE SEQUENCE sequence_is_called_target START WITH 10;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT last_value, is_called FROM sequence_is_called_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-metadata-repro-test-testsequencerelationtracksiscalledrepro-0001-select-last_value-is_called-from-sequence_is_called_target"},
				},
				{
					Query: `SELECT nextval('sequence_is_called_target');`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-metadata-repro-test-testsequencerelationtracksiscalledrepro-0002-select-nextval-sequence_is_called_target"},
				},
				{
					Query: `SELECT last_value, is_called FROM sequence_is_called_target;`, PostgresOracle: ScriptTestPostgresOracle{ID: "sequence-metadata-repro-test-testsequencerelationtracksiscalledrepro-0003-select-last_value-is_called-from-sequence_is_called_target"},
				},
			},
		},
	})
}
