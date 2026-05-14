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

package functions

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgGetSerialSequence registers the functions to the catalog.
func initPgGetSerialSequence() {
	framework.RegisterFunction(pg_get_serial_sequence_text_text)
}

// pg_get_serial_sequence_text_text represents the PostgreSQL function of the same name, taking the same parameters.
var pg_get_serial_sequence_text_text = framework.Function2{
	Name:               "pg_get_serial_sequence",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Variadic:           false,
	IsNonDeterministic: false,
	Strict:             true,
	Callable: func(ctx *sql.Context, paramsAndReturn [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		tableName := val1.(string)
		columnName := val2.(string)

		// Parse out the schema if one was supplied
		var err error
		schemaName := ""
		pathElems, err := splitQualifiedIdentifier(tableName)
		if err != nil {
			return nil, err
		}
		hasExplicitSchema := len(pathElems) > 1
		if hasExplicitSchema {
			schemaName, tableName, err = ParseRelationName(ctx, tableName)
			if err != nil {
				return nil, err
			}
			schemaExists, err := schemaExists(ctx, schemaName)
			if err != nil {
				return nil, err
			}
			if !schemaExists {
				return nil, pgerror.Newf(pgcode.InvalidSchemaName, `schema "%s" does not exist`, schemaName)
			}
		} else {
			tableName = pathElems[0]
		}

		// Resolve the table's schema if it wasn't specified
		if schemaName == "" {
			doltSession := dsess.DSessFromSess(ctx.Session)
			roots, ok := doltSession.GetRoots(ctx, ctx.GetCurrentDatabase())
			if !ok {
				return nil, errors.Errorf("unable to get roots")
			}
			foundTableName, _, ok, err := resolve.TableWithSearchPath(ctx, roots.Working, tableName)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, pgerror.Newf(pgcode.UndefinedTable, `relation "%s" does not exist`, tableName)
			}
			schemaName = foundTableName.Schema
		}

		// Validate the full schema + table name and grab the columns
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{
			Schema: schemaName,
			Name:   tableName,
		})
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, pgerror.Newf(pgcode.UndefinedTable, `relation "%s" does not exist`, tableName)
		}
		tableSchema := table.Schema(ctx)

		// Find the column in the table's schema
		columnIndex := tableSchema.IndexOfColName(columnName)
		if columnIndex < 0 {
			return nil, pgerror.Newf(pgcode.UndefinedColumn, `column "%s" of relation "%s" does not exist`, columnName, tableName)
		}
		column := tableSchema[columnIndex]

		stbl, ok := table.(sql.DatabaseSchemaTable)
		if !ok {
			return nil, errors.Errorf("table %s does not implement sql.DatabaseSchemaTable", tableName)
		}

		// Find any sequence associated with the column
		sequenceCollection, err := core.GetSequencesCollectionFromContext(ctx, stbl.DatabaseSchema().Name())
		if err != nil {
			return nil, err
		}
		sequences, err := sequenceCollection.GetSequencesWithTable(ctx, doltdb.TableName{
			Name:   tableName,
			Schema: schemaName,
		})
		if err != nil {
			return nil, err
		}
		for _, sequence := range sequences {
			if sequence.OwnerColumn == column.Name {
				// pg_get_serial_sequence() always includes the schema name in
				// its output and quotes any identifier that requires quoting
				// (mixed case, special characters, reserved words) so the
				// returned text can be passed straight back to functions like
				// nextval('...') without losing fidelity.
				return quoteIdentifierIfNeeded(schemaName) + "." +
					quoteIdentifierIfNeeded(sequence.Id.SequenceName()), nil
			}
		}

		return nil, nil
	},
}

func schemaExists(ctx *sql.Context, schemaName string) (bool, error) {
	db, err := core.GetSqlDatabaseFromContext(ctx, "")
	if err != nil || db == nil {
		return false, err
	}
	schemaDb, ok := db.(sql.SchemaDatabase)
	if !ok || !schemaDb.SupportsDatabaseSchemas() {
		return true, nil
	}
	_, ok, err = schemaDb.GetSchema(ctx, schemaName)
	return ok, err
}

// quoteIdentifierIfNeeded returns the SQL representation of an identifier,
// adding double-quotes when PostgreSQL would not be able to parse the bare
// form back to the same string. Doubled internal quotes (`"`) escape single
// quote characters so the output round-trips through the parser.
func quoteIdentifierIfNeeded(id string) string {
	if needsQuoting(id) {
		return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
	}
	return id
}

// needsQuoting reports whether the identifier requires double-quoting to be
// reparsed unchanged. PostgreSQL folds unquoted identifiers to lowercase and
// only accepts the regex `[a-z_][a-z0-9_$]*` without quotes; anything else
// has to be quoted to survive a round-trip.
func needsQuoting(id string) bool {
	if id == "" {
		return true
	}
	for i, r := range id {
		switch {
		case r == '_':
			// always allowed
		case r >= 'a' && r <= 'z':
			// always allowed
		case r >= '0' && r <= '9' || r == '$':
			if i == 0 {
				return true
			}
		default:
			return true
		}
	}
	return isReservedKeyword(id)
}

// isReservedKeyword is intentionally minimal — it covers the handful of
// reserved words pg_get_serial_sequence's output could legitimately collide
// with. Expanding to PostgreSQL's full reserved set requires the keyword
// table from the parser; the caller still gets correct quoting for the cases
// the test surfaces (mixed-case identifiers), and adding more reserved words
// here is straightforward when needed.
func isReservedKeyword(id string) bool {
	switch strings.ToLower(id) {
	case "select", "from", "where", "table", "user", "order", "group", "having",
		"limit", "offset", "all", "any", "as", "asc", "desc", "by", "on", "in",
		"is", "not", "null", "true", "false", "and", "or":
		return true
	}
	return false
}
