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
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgAmopName is a constant to the pg_amop name.
const PgAmopName = "pg_amop"

// InitPgAmop handles registration of the pg_amop handler.
func InitPgAmop() {
	tables.AddHandler(PgCatalogName, PgAmopName, PgAmopHandler{})
}

// PgAmopHandler is the handler for the pg_amop table.
type PgAmopHandler struct{}

var _ tables.Handler = PgAmopHandler{}

// Name implements the interface tables.Handler.
func (p PgAmopHandler) Name() string {
	return PgAmopName
}

// RowIter implements the interface tables.Handler.
func (p PgAmopHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return &pgAmopRowIter{
		amops: defaultPostgresAmops,
		idx:   0,
	}, nil
}

// Schema implements the interface tables.Handler.
func (p PgAmopHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgAmopSchema,
		PkOrdinals: nil,
	}
}

// pgAmopSchema is the schema for pg_amop.
var pgAmopSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amopfamily", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amoplefttype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amoprighttype", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amopstrategy", Type: pgtypes.Int16, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amoppurpose", Type: pgtypes.InternalChar, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amopopr", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amopmethod", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
	{Name: "amopsortfamily", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgAmopName},
}

// pgAmopRowIter is the sql.RowIter for the pg_amop table.
type pgAmopRowIter struct {
	amops []amop
	idx   int
}

var _ sql.RowIter = (*pgAmopRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgAmopRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.amops) {
		return nil, io.EOF
	}
	iter.idx++
	amop := iter.amops[iter.idx-1]

	return sql.Row{
		amop.oid,       // oid
		amop.family,    // amopfamily
		amop.leftType,  // amoplefttype
		amop.rightType, // amoprighttype
		amop.strategy,  // amopstrategy
		"s",            // amoppurpose
		amop.operator,  // amopopr
		amop.method,    // amopmethod
		zeroOID(),      // amopsortfamily
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgAmopRowIter) Close(ctx *sql.Context) error {
	return nil
}

type amop struct {
	oid       id.Id
	family    id.Id
	leftType  id.Id
	rightType id.Id
	strategy  int16
	operator  id.Id
	method    id.Id
}

var defaultPostgresAmops = func() []amop {
	amops := make([]amop, 0, len(btreeCatalogTypes)*len(btreeComparisonOperators)+5)
	for _, typ := range btreeCatalogTypes {
		for _, operator := range btreeComparisonOperators {
			amops = append(amops, newBtreeAmop(typ, operator))
		}
	}
	for _, typ := range btreeIntegerCrossTypeCatalogTypes {
		for _, operator := range btreeComparisonOperators {
			amops = append(amops, newBtreeCrossTypeAmop(typ, operator))
		}
	}
	for _, typ := range btreeFloatCrossTypeCatalogTypes {
		for _, operator := range btreeComparisonOperators {
			amops = append(amops, newBtreeCrossTypeAmop(typ, operator))
		}
	}
	for _, typ := range btreeTextCrossTypeCatalogTypes {
		for _, operator := range btreeComparisonOperators {
			amops = append(amops, newBtreeCrossTypeAmop(typ, operator))
		}
	}
	for _, typ := range btreeDatetimeCrossTypeCatalogTypes {
		for _, operator := range btreeComparisonOperators {
			amops = append(amops, newBtreeCrossTypeAmop(typ, operator))
		}
	}
	for _, typ := range btreePatternCatalogTypes {
		for _, operator := range btreePatternComparisonOperators {
			amops = append(amops, newBtreePatternAmop(typ, operator))
		}
	}
	amops = append(amops,
		newJsonbGinAmop(indexmetadata.OpClassJsonbOps, "@>", "jsonb", int16(7)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbOps, "?", "text", int16(9)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbOps, "?|", "_text", int16(10)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbOps, "?&", "_text", int16(11)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbPathOps, "@>", "jsonb", int16(7)),
	)
	return amops
}()

func newBtreeAmop(typ btreeCatalogType, operator btreeComparisonOperator) amop {
	return amop{
		oid:       btreeAmopID(typ.opfamily, typ.typeName, operator.strategy),
		family:    btreeOpfamilyID(typ.opfamily),
		leftType:  pgCatalogTypeID(typ.typeName),
		rightType: pgCatalogTypeID(typ.typeName),
		strategy:  operator.strategy,
		operator:  pgCatalogOperatorID(operator.name, typ.typeName, typ.typeName),
		method:    id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
	}
}

func newBtreePatternAmop(typ btreePatternCatalogType, operator btreeComparisonOperator) amop {
	return amop{
		oid:       btreeAmopID(typ.opfamily, typ.typeName, operator.strategy),
		family:    btreeOpfamilyID(typ.opfamily),
		leftType:  pgCatalogTypeID(typ.typeName),
		rightType: pgCatalogTypeID(typ.typeName),
		strategy:  operator.strategy,
		operator:  pgCatalogOperatorID(operator.name, typ.typeName, typ.typeName),
		method:    id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
	}
}

func newBtreeCrossTypeAmop(typ btreeCrossTypeCatalogType, operator btreeComparisonOperator) amop {
	return amop{
		oid:       btreeCrossTypeAmopID(typ.opfamily, typ.leftType, typ.rightType, operator.strategy),
		family:    btreeOpfamilyID(typ.opfamily),
		leftType:  pgCatalogTypeID(typ.leftType),
		rightType: pgCatalogTypeID(typ.rightType),
		strategy:  operator.strategy,
		operator:  pgCatalogOperatorID(operator.name, typ.leftType, typ.rightType),
		method:    id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
	}
}

func newJsonbGinAmop(opclass string, operator string, rightType string, strategy int16) amop {
	return amop{
		oid:       jsonbGinAmopID(opclass, strategy),
		family:    jsonbGinOpfamilyID(opclass),
		leftType:  pgCatalogTypeID("jsonb"),
		rightType: pgCatalogTypeID(rightType),
		strategy:  strategy,
		operator:  jsonbOperatorID(operator, "jsonb", rightType),
		method:    id.NewAccessMethod(indexmetadata.AccessMethodGin).AsId(),
	}
}
