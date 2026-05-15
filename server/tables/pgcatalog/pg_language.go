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

package pgcatalog

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgLanguageName is a constant to the pg_language name.
const PgLanguageName = "pg_language"

// InitPgLanguage handles registration of the pg_language handler.
func InitPgLanguage() {
	tables.AddHandler(PgCatalogName, PgLanguageName, PgLanguageHandler{})
}

// PgLanguageHandler is the handler for the pg_language table.
type PgLanguageHandler struct{}

var _ tables.Handler = PgLanguageHandler{}

// Name implements the interface tables.Handler.
func (p PgLanguageHandler) Name() string {
	return PgLanguageName
}

// RowIter implements the interface tables.Handler.
func (p PgLanguageHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rows []sql.Row
	seen := make(map[string]struct{})
	auth.LockRead(func() {
		for _, language := range pgLanguageBuiltinSystemLanguages {
			rows = append(rows, pgLanguageRow(language, nil))
			seen[language.Name] = struct{}{}
		}
		for _, language := range auth.GetAllLanguages() {
			if _, ok := seen[language.Name]; ok {
				continue
			}
			rows = append(rows, pgLanguageRow(language, auth.LanguageACLItems(language.Name)))
		}
	})
	return sql.RowsToRowIter(rows...), nil
}

var pgLanguageBuiltinSystemLanguages = []auth.Language{
	{
		Name:      "c",
		Owner:     "postgres",
		Validator: "fmgr_c_validator",
	},
	{
		Name:      "internal",
		Owner:     "postgres",
		Validator: "fmgr_internal_validator",
	},
}

func pgLanguageRow(language auth.Language, aclItems []string) sql.Row {
	owner := catalogOwnerOID()
	if role := auth.GetRole(language.Owner); role.IsValid() {
		owner = id.NewId(id.Section_User, language.Owner)
	}
	handler, inline, validator := pgLanguageFunctions(language)
	return sql.Row{
		id.NewId(id.Section_FunctionLanguage, language.Name), // oid
		language.Name,          // lanname
		owner,                  // lanowner
		language.IsProcedural,  // lanispl
		language.Trusted,       // lanpltrusted
		handler,                // lanplcallfoid
		inline,                 // laninline
		validator,              // lanvalidator
		aclTextArray(aclItems), // lanacl
		id.NewTable(PgCatalogName, PgLanguageName).AsId(), // tableoid
	}
}

func pgLanguageFunctions(language auth.Language) (handler id.Id, inline id.Id, validator id.Id) {
	handler = zeroOID()
	inline = zeroOID()
	validator = zeroOID()
	switch language.Name {
	case "c":
		validator = pgCatalogFunctionID("fmgr_c_validator", pgCatalogType("oid"))
	case "internal":
		validator = pgCatalogFunctionID("fmgr_internal_validator", pgCatalogType("oid"))
	case "plpgsql":
		handler = pgCatalogFunctionID("plpgsql_call_handler")
		inline = pgCatalogFunctionID("plpgsql_inline_handler", pgCatalogType("internal"))
		validator = pgCatalogFunctionID("plpgsql_validator", pgCatalogType("oid"))
	case "sql":
		validator = pgCatalogFunctionID("fmgr_sql_validator", pgCatalogType("oid"))
	}
	return handler, inline, validator
}

// Schema implements the interface tables.Handler.
func (p PgLanguageHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgLanguageSchema,
		PkOrdinals: nil,
	}
}

// pgLanguageSchema is the schema for pg_language.
var pgLanguageSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanispl", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanpltrusted", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanplcallfoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "laninline", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanvalidator", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgLanguageName},
	{Name: "lanacl", Type: pgtypes.TextArray, Default: nil, Nullable: true, Source: PgLanguageName}, // TODO: aclitem[] type
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgLanguageName},
}

// pgLanguageRowIter is the sql.RowIter for the pg_language table.
type pgLanguageRowIter struct {
}

var _ sql.RowIter = (*pgLanguageRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgLanguageRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (iter *pgLanguageRowIter) Close(ctx *sql.Context) error {
	return nil
}
