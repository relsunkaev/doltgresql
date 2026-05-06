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
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgOperatorName is a constant to the pg_operator name.
const PgOperatorName = "pg_operator"

// InitPgOperator handles registration of the pg_operator handler.
func InitPgOperator() {
	tables.AddHandler(PgCatalogName, PgOperatorName, PgOperatorHandler{})
}

// PgOperatorHandler is the handler for the pg_operator table.
type PgOperatorHandler struct{}

var _ tables.Handler = PgOperatorHandler{}

// Name implements the interface tables.Handler.
func (p PgOperatorHandler) Name() string {
	return PgOperatorName
}

// RowIter implements the interface tables.Handler.
func (p PgOperatorHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &pgOperatorRowIter{
		operators: defaultPostgresOperators,
		idx:       0,
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgOperatorHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgOperatorSchema,
		PkOrdinals: nil,
	}
}

// pgOperatorSchema is the schema for pg_operator.
var pgOperatorSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprname", Type: pgtypes.Name, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprnamespace", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprowner", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprkind", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprcanmerge", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprcanhash", Type: pgtypes.Bool, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprleft", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprright", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprresult", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprcom", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprnegate", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprcode", Type: pgtypes.Regproc, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprrest", Type: pgtypes.Regproc, Default: nil, Nullable: false, Source: PgOperatorName},
	{Name: "oprjoin", Type: pgtypes.Regproc, Default: nil, Nullable: false, Source: PgOperatorName},
}

// pgOperatorRowIter is the sql.RowIter for the pg_operator table.
type pgOperatorRowIter struct {
	operators []pgOperator
	idx       int
}

var _ sql.RowIter = (*pgOperatorRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgOperatorRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.operators) {
		return nil, io.EOF
	}
	iter.idx++
	operator := iter.operators[iter.idx-1]

	return sql.Row{
		operator.oid,        // oid
		operator.name,       // oprname
		operator.namespace,  // oprnamespace
		id.Null,             // oprowner
		"b",                 // oprkind
		false,               // oprcanmerge
		false,               // oprcanhash
		operator.leftType,   // oprleft
		operator.rightType,  // oprright
		operator.result,     // oprresult
		operator.commutator, // oprcom
		zeroOID(),           // oprnegate
		operator.code,       // oprcode
		operator.restrict,   // oprrest
		operator.join,       // oprjoin
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgOperatorRowIter) Close(ctx *sql.Context) error {
	return nil
}

type pgOperator struct {
	oid        id.Id
	name       string
	namespace  id.Id
	leftType   id.Id
	rightType  id.Id
	result     id.Id
	commutator id.Id
	code       id.Id
	restrict   id.Id
	join       id.Id
}

var defaultPostgresOperators = func() []pgOperator {
	operators := make([]pgOperator, 0, len(btreeCatalogTypes)*len(btreeComparisonOperators)+5)
	for _, typ := range btreeCatalogTypes {
		for idx, operator := range btreeComparisonOperators {
			operators = append(operators, newBtreeOperator(typ, operator, typ.comparisonFuncs[idx]))
		}
	}
	for _, typ := range btreeIntegerCrossTypeCatalogTypes {
		for idx, operator := range btreeComparisonOperators {
			operators = append(operators, newBtreeCrossTypeOperator(typ, operator, typ.comparisonFuncs[idx]))
		}
	}
	for _, typ := range btreePatternCatalogTypes {
		for idx, operator := range btreePatternComparisonOperators {
			if operator.name == "=" {
				continue
			}
			operators = append(operators, newBtreePatternOperator(typ, operator, typ.comparisonFuncs[idx]))
		}
	}
	operators = append(operators,
		newJsonbOperator("@>", "jsonb", "jsonb", "jsonb_contains", jsonbOperatorID("<@", "jsonb", "jsonb")),
		newJsonbOperator("<@", "jsonb", "jsonb", "jsonb_contained", jsonbOperatorID("@>", "jsonb", "jsonb")),
		newJsonbOperator("?", "jsonb", "text", "jsonb_exists", zeroOID()),
		newJsonbOperator("?|", "jsonb", "_text", "jsonb_exists_any", zeroOID()),
		newJsonbOperator("?&", "jsonb", "_text", "jsonb_exists_all", zeroOID()),
	)
	return operators
}()

func newBtreeOperator(typ btreeCatalogType, operator btreeComparisonOperator, function string) pgOperator {
	functionType := typ.operatorFunctionType()
	return pgOperator{
		oid:        pgCatalogOperatorID(operator.name, typ.typeName, typ.typeName),
		name:       operator.name,
		namespace:  pgCatalogNamespaceID(),
		leftType:   pgCatalogTypeID(typ.typeName),
		rightType:  pgCatalogTypeID(typ.typeName),
		result:     pgCatalogTypeID("bool"),
		commutator: pgCatalogOperatorID(operator.commutator, typ.typeName, typ.typeName),
		code:       pgCatalogFunctionID(function, pgCatalogType(functionType), pgCatalogType(functionType)),
		restrict:   btreeOperatorRestrictFunctionID(operator.name),
		join:       btreeOperatorJoinFunctionID(operator.name),
	}
}

func newBtreePatternOperator(typ btreePatternCatalogType, operator btreeComparisonOperator, function string) pgOperator {
	return pgOperator{
		oid:        pgCatalogOperatorID(operator.name, typ.typeName, typ.typeName),
		name:       operator.name,
		namespace:  pgCatalogNamespaceID(),
		leftType:   pgCatalogTypeID(typ.typeName),
		rightType:  pgCatalogTypeID(typ.typeName),
		result:     pgCatalogTypeID("bool"),
		commutator: pgCatalogOperatorID(operator.commutator, typ.typeName, typ.typeName),
		code:       pgCatalogFunctionID(function, pgCatalogType(typ.typeName), pgCatalogType(typ.typeName)),
		restrict:   btreePatternOperatorRestrictFunctionID(operator.name),
		join:       btreePatternOperatorJoinFunctionID(operator.name),
	}
}

func newBtreeCrossTypeOperator(typ btreeCrossTypeCatalogType, operator btreeComparisonOperator, function string) pgOperator {
	return pgOperator{
		oid:        pgCatalogOperatorID(operator.name, typ.leftType, typ.rightType),
		name:       operator.name,
		namespace:  pgCatalogNamespaceID(),
		leftType:   pgCatalogTypeID(typ.leftType),
		rightType:  pgCatalogTypeID(typ.rightType),
		result:     pgCatalogTypeID("bool"),
		commutator: pgCatalogOperatorID(operator.commutator, typ.rightType, typ.leftType),
		code:       pgCatalogFunctionID(function, pgCatalogType(typ.leftType), pgCatalogType(typ.rightType)),
		restrict:   btreeExactOperatorRestrictFunctionID(operator.name),
		join:       btreeExactOperatorJoinFunctionID(operator.name),
	}
}

func btreeOperatorRestrictFunctionID(name string) id.Id {
	switch name {
	case "=":
		return pgCatalogFunctionID("eqsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "<", "<=":
		return pgCatalogFunctionID("scalarltsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	default:
		return pgCatalogFunctionID("scalargtsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	}
}

func btreeExactOperatorRestrictFunctionID(name string) id.Id {
	switch name {
	case "=":
		return pgCatalogFunctionID("eqsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "<":
		return pgCatalogFunctionID("scalarltsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "<=":
		return pgCatalogFunctionID("scalarlesel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	case ">=":
		return pgCatalogFunctionID("scalargesel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	default:
		return pgCatalogFunctionID("scalargtsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	}
}

func btreePatternOperatorRestrictFunctionID(name string) id.Id {
	switch name {
	case "~<~":
		return pgCatalogFunctionID("scalarltsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "~<=~":
		return pgCatalogFunctionID("scalarlesel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "~>=~":
		return pgCatalogFunctionID("scalargesel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	default:
		return pgCatalogFunctionID("scalargtsel", pgCatalogType("int4"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"))
	}
}

func btreeOperatorJoinFunctionID(name string) id.Id {
	switch name {
	case "=":
		return pgCatalogFunctionID("eqjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "<", "<=":
		return pgCatalogFunctionID("scalarltjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	default:
		return pgCatalogFunctionID("scalargtjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	}
}

func btreeExactOperatorJoinFunctionID(name string) id.Id {
	switch name {
	case "=":
		return pgCatalogFunctionID("eqjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "<":
		return pgCatalogFunctionID("scalarltjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "<=":
		return pgCatalogFunctionID("scalarlejoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	case ">=":
		return pgCatalogFunctionID("scalargejoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	default:
		return pgCatalogFunctionID("scalargtjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	}
}

func btreePatternOperatorJoinFunctionID(name string) id.Id {
	switch name {
	case "~<~":
		return pgCatalogFunctionID("scalarltjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "~<=~":
		return pgCatalogFunctionID("scalarlejoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	case "~>=~":
		return pgCatalogFunctionID("scalargejoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	default:
		return pgCatalogFunctionID("scalargtjoinsel", pgCatalogType("int2"), pgCatalogType("oid"), pgCatalogType("internal"), pgCatalogType("internal"), pgCatalogType("internal"))
	}
}

func newJsonbOperator(name string, leftType string, rightType string, function string, commutator id.Id) pgOperator {
	return pgOperator{
		oid:        jsonbOperatorID(name, leftType, rightType),
		name:       name,
		namespace:  pgCatalogNamespaceID(),
		leftType:   pgCatalogTypeID(leftType),
		rightType:  pgCatalogTypeID(rightType),
		result:     pgCatalogTypeID("bool"),
		commutator: commutator,
		code:       pgCatalogFunctionID(function, pgCatalogType(leftType), pgCatalogType(rightType)),
		restrict:   pgCatalogFunctionID("matchingsel"),
		join:       pgCatalogFunctionID("matchingjoinsel"),
	}
}
