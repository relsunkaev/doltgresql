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
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/deferrable"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/settings"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgGetConstraintdef registers the functions to the catalog.
func initPgGetConstraintdef() {
	framework.RegisterFunction(pg_get_constraintdef_oid)
	framework.RegisterFunction(pg_get_constraintdef_oid_bool)
}

// pg_get_constraintdef_oid represents the PostgreSQL function of the same name, taking the same parameters.
var pg_get_constraintdef_oid = framework.Function1{
	Name:       "pg_get_constraintdef",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Oid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		oidVal := val1.(id.Id)
		def, err := getConstraintDef(ctx, oidVal)
		return def, err
	},
}

// pg_get_constraintdef_oid_bool represents the PostgreSQL function of the same name, taking the same parameters.
var pg_get_constraintdef_oid_bool = framework.Function2{
	Name:       "pg_get_constraintdef",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Bool},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		oidVal := val1.(id.Id)
		pretty := val2.(bool)
		if pretty {
			ctx.Warn(0, "pretty printing is not yet supported")
		}
		def, err := getConstraintDef(ctx, oidVal)
		if err != nil {
			return nil, err
		}
		return def, nil
	},
}

// getConstraintDef returns the definition of the constraint for the given OID.
func getConstraintDef(ctx *sql.Context, oidVal id.Id) (string, error) {
	var result string
	err := RunCallback(ctx, oidVal, Callbacks{
		Check: func(ctx *sql.Context, schema ItemSchema, table ItemTable, check ItemCheck) (cont bool, err error) {
			result = fmt.Sprintf("CHECK %s", formatCheckConstraintExpression(check.Item.CheckExpression))
			if !check.Item.Enforced {
				result += " NOT ENFORCED"
			}
			return false, nil
		},
		ForeignKey: func(ctx *sql.Context, schema ItemSchema, table ItemTable, fk ItemForeignKey) (cont bool, err error) {
			parentTableName, err := formatForeignKeyReferencedTable(ctx, schema, fk.Item)
			if err != nil {
				return false, err
			}
			// Note the postgres doesn't include the name of a foreign key when printing it via pg_get_constraintdef
			// The spacing here is also significant, as certain tools (SQLAlchemy) use regex to parse
			result = fmt.Sprintf(
				"FOREIGN KEY (%s) REFERENCES %s(%s)",
				getColumnNamesString(fk.Item.Columns),
				parentTableName,
				getColumnNamesString(fk.Item.ParentColumns),
			)
			matchFull, err := deferrable.ForeignKeyMatchFullForID(ctx, fk.OID, fk.Item)
			if err != nil {
				return false, err
			}
			if matchFull {
				result += " MATCH FULL"
			}
			if action := formatForeignKeyAction("ON UPDATE", fk.Item.OnUpdate); action != "" {
				result += " " + action
			}
			if action := formatForeignKeyAction("ON DELETE", fk.Item.OnDelete); action != "" {
				result += " " + action
			}
			timing, err := deferrable.ForeignKeyTimingForID(ctx, fk.OID, fk.Item)
			if err != nil {
				return false, err
			}
			if timing.Deferrable {
				result += " DEFERRABLE"
			}
			if timing.InitiallyDeferred {
				result += " INITIALLY DEFERRED"
			}
			return false, nil
		},
		Index: func(ctx *sql.Context, schema ItemSchema, table ItemTable, index ItemIndex) (cont bool, err error) {
			colsStr := getColumnNamesString(index.Item.Expressions())
			if strings.ToLower(index.Item.ID()) == "primary" {
				result = fmt.Sprintf("PRIMARY KEY (%s)", colsStr)
			} else {
				result = fmt.Sprintf("UNIQUE (%s)", colsStr)
			}
			return false, nil
		},
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

// getColumnNamesString returns a comma-separated string of column names with
// the table names removed from a list of expressions.
func getColumnNamesString(exprs []string) string {
	colNames := make([]string, len(exprs))
	for i, expr := range exprs {
		split := strings.Split(expr, ".")
		if len(split) == 0 {
			return ""
		}
		if len(split) == 1 {
			colNames[i] = pgQuoteIdentifier(core.DecodePhysicalColumnName(split[0]))
		} else {
			colNames[i] = pgQuoteIdentifier(core.DecodePhysicalColumnName(split[len(split)-1]))
		}
	}
	return strings.Join(colNames, ", ")
}

func formatForeignKeyReferencedTable(ctx *sql.Context, schema ItemSchema, fk sql.ForeignKeyConstraint) (string, error) {
	parentSchema := fk.ParentSchema
	if parentSchema == "" {
		parentSchema = schema.Item.SchemaName()
	}
	searchPath, err := settings.GetCurrentSchemasAsMap(ctx)
	if err != nil {
		return "", err
	}
	if _, ok := searchPath[parentSchema]; ok {
		return pgQuoteIdentifier(fk.ParentTable), nil
	}
	return fmt.Sprintf("%s.%s", pgQuoteIdentifier(parentSchema), pgQuoteIdentifier(fk.ParentTable)), nil
}

func formatForeignKeyAction(prefix string, action sql.ForeignKeyReferentialAction) string {
	switch action {
	case sql.ForeignKeyReferentialAction_Restrict,
		sql.ForeignKeyReferentialAction_Cascade,
		sql.ForeignKeyReferentialAction_SetNull,
		sql.ForeignKeyReferentialAction_SetDefault:
		return fmt.Sprintf("%s %s", prefix, action)
	default:
		return ""
	}
}

func formatCheckConstraintExpression(expr string) string {
	expr = strings.TrimSpace(unquoteSimpleConstraintIdentifiers(expr))
	if expr == "" {
		return "()"
	}
	return fmt.Sprintf("((%s))", stripOuterParens(expr))
}

func stripOuterParens(expr string) string {
	for {
		trimmed := strings.TrimSpace(expr)
		if len(trimmed) < 2 || trimmed[0] != '(' || trimmed[len(trimmed)-1] != ')' {
			return trimmed
		}
		depth := 0
		wrapsWholeExpr := true
		for i := 0; i < len(trimmed); i++ {
			switch trimmed[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 && i < len(trimmed)-1 {
					wrapsWholeExpr = false
				}
			}
			if depth < 0 {
				return trimmed
			}
		}
		if depth != 0 || !wrapsWholeExpr {
			return trimmed
		}
		expr = trimmed[1 : len(trimmed)-1]
	}
}

func unquoteSimpleConstraintIdentifiers(expr string) string {
	var builder strings.Builder
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '\'':
			next := copyConstraintStringLiteral(&builder, expr, i)
			i = next
		case '"':
			next, content, ok := readConstraintQuotedIdentifier(expr, i)
			if ok && canUseBareIdentifier(content) {
				builder.WriteString(content)
			} else {
				builder.WriteString(expr[i:next])
			}
			i = next
		default:
			builder.WriteByte(expr[i])
			i++
		}
	}
	return builder.String()
}

func copyConstraintStringLiteral(builder *strings.Builder, expr string, start int) int {
	builder.WriteByte('\'')
	for i := start + 1; i < len(expr); i++ {
		builder.WriteByte(expr[i])
		if expr[i] == '\'' {
			if i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				builder.WriteByte('\'')
				continue
			}
			return i + 1
		}
	}
	return len(expr)
}

func readConstraintQuotedIdentifier(expr string, start int) (next int, content string, ok bool) {
	var builder strings.Builder
	for i := start + 1; i < len(expr); i++ {
		if expr[i] != '"' {
			builder.WriteByte(expr[i])
			continue
		}
		if i+1 < len(expr) && expr[i+1] == '"' {
			builder.WriteByte('"')
			i++
			continue
		}
		return i + 1, builder.String(), true
	}
	return len(expr), "", false
}
