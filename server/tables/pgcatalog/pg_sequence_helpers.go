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

package pgcatalog

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/server/functions"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type pgSequenceCatalogEntry struct {
	relID        id.Id
	schemaName   string
	sequenceName string
	sequence     *sequences.Sequence
}

type pgSequenceStatsScope int

const (
	pgSequenceStatsScopeAll pgSequenceStatsScope = iota
	pgSequenceStatsScopeUser
	pgSequenceStatsScopeSystem
)

func pgSequenceCatalogEntries(ctx *sql.Context) ([]pgSequenceCatalogEntry, error) {
	var entries []pgSequenceCatalogEntry
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Sequence: func(ctx *sql.Context, schema functions.ItemSchema, sequence functions.ItemSequence) (cont bool, err error) {
			entries = append(entries, pgSequenceCatalogEntry{
				relID:        sequence.OID.AsId(),
				schemaName:   schema.Item.SchemaName(),
				sequenceName: sequence.Item.Id.SequenceName(),
				sequence:     sequence.Item,
			})
			return true, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func pgSequenceDataTypeName(sequence *sequences.Sequence) string {
	switch sequence.DataTypeID {
	case pgtypes.Int16.ID:
		return "smallint"
	case pgtypes.Int32.ID:
		return "integer"
	case pgtypes.Int64.ID:
		return "bigint"
	default:
		return sequence.DataTypeID.TypeName()
	}
}

func pgSequenceLastValue(sequence *sequences.Sequence) any {
	if !sequence.IsCalled {
		return nil
	}
	return sequence.Current
}

func pgStatioSequenceRows(ctx *sql.Context, scope pgSequenceStatsScope) (sql.RowIter, error) {
	entries, err := pgSequenceCatalogEntries(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]sql.Row, 0, len(entries))
	for _, entry := range entries {
		if !includeSequenceStatsSchema(entry.schemaName, scope) {
			continue
		}
		rows = append(rows, sql.Row{
			entry.relID,        // relid
			entry.schemaName,   // schemaname
			entry.sequenceName, // relname
			int64(0),           // blks_read
			int64(0),           // blks_hit
		})
	}
	return sql.RowsToRowIter(rows...), nil
}

func includeSequenceStatsSchema(schema string, scope pgSequenceStatsScope) bool {
	isSystem := isSequenceStatsSystemSchema(schema)
	switch scope {
	case pgSequenceStatsScopeAll:
		return true
	case pgSequenceStatsScopeUser:
		return !isSystem
	case pgSequenceStatsScopeSystem:
		return isSystem
	default:
		return false
	}
}

func isSequenceStatsSystemSchema(schema string) bool {
	switch strings.ToLower(schema) {
	case PgCatalogName, sql.InformationSchemaDatabaseName:
		return true
	default:
		return false
	}
}
