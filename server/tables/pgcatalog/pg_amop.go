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
	"strconv"

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
	amops, err := appendBtreeGistAmops(ctx, defaultPostgresAmops)
	if err != nil {
		return nil, err
	}
	amops, err = appendVectorAmops(ctx, amops)
	if err != nil {
		return nil, err
	}
	amops, err = appendHstoreAmops(ctx, amops)
	if err != nil {
		return nil, err
	}
	amops, err = appendCitextAmops(ctx, amops)
	if err != nil {
		return nil, err
	}
	return &pgAmopRowIter{
		amops: amops,
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

	purpose := amop.purpose
	if purpose == "" {
		purpose = "s"
	}
	sortFamily := amop.sortFamily
	if !sortFamily.IsValid() {
		sortFamily = zeroOID()
	}

	return sql.Row{
		amop.oid,       // oid
		amop.family,    // amopfamily
		amop.leftType,  // amoplefttype
		amop.rightType, // amoprighttype
		amop.strategy,  // amopstrategy
		purpose,        // amoppurpose
		amop.operator,  // amopopr
		amop.method,    // amopmethod
		sortFamily,     // amopsortfamily
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgAmopRowIter) Close(ctx *sql.Context) error {
	return nil
}

type amop struct {
	oid        id.Id
	family     id.Id
	leftType   id.Id
	rightType  id.Id
	strategy   int16
	operator   id.Id
	method     id.Id
	purpose    string
	sortFamily id.Id
}

const postgres16DefaultPgAmopCount = 945

var defaultPostgresAmops = func() []amop {
	amops := make([]amop, 0, postgres16DefaultPgAmopCount)
	amops = append(amops, postgres16HashAmopsBeforeBtree...)
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
		newJsonbGinAmop(indexmetadata.OpClassJsonbOps, "@?", "jsonpath", int16(15)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbOps, "@@", "jsonpath", int16(16)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbPathOps, "@>", "jsonb", int16(7)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbPathOps, "@?", "jsonpath", int16(15)),
		newJsonbGinAmop(indexmetadata.OpClassJsonbPathOps, "@@", "jsonpath", int16(16)),
		newJsonbHashAmop(indexmetadata.OpClassJsonbOps, "=", int16(1)),
	)
	amops = append(amops, postgres16HashAmopsAfterBtree...)
	amops = appendPostgres16AmopPadding(amops, postgres16DefaultPgAmopCount)
	return amops
}()

var postgres16HashAmopsBeforeBtree = []amop{
	newHashAmop("bytea_ops", "bytea", "bytea", "=", int16(1)),
	newHashAmop("char_ops", "char", "char", "=", int16(1)),
	newHashAmop("pg_lsn_ops", "pg_lsn", "pg_lsn", "=", int16(1)),
}

var postgres16HashAmopsAfterBtree = func() []amop {
	amops := []amop{
		newHashAmop("interval_ops", "interval", "interval", "=", int16(1)),
		newHashAmop("oid_ops", "oid", "oid", "=", int16(1)),
		newHashAmop("oidvector_ops", "oidvector", "oidvector", "=", int16(1)),
		newHashAmop("time_ops", "time", "time", "=", int16(1)),
		newHashAmop("timetz_ops", "timetz", "timetz", "=", int16(1)),
	}
	for _, leftType := range []string{"int2", "int4", "int8"} {
		for _, rightType := range []string{"int2", "int4", "int8"} {
			amops = append(amops, newHashAmop("integer_ops", leftType, rightType, "=", int16(1)))
		}
	}
	for _, leftType := range []string{"float4", "float8"} {
		for _, rightType := range []string{"float4", "float8"} {
			amops = append(amops, newHashAmop("float_ops", leftType, rightType, "=", int16(1)))
		}
	}
	for _, typ := range []struct {
		leftType  string
		rightType string
		strategy  int16
	}{
		{leftType: "name", rightType: "name", strategy: 1},
		{leftType: "name", rightType: "text", strategy: 1},
		{leftType: "text", rightType: "name", strategy: 1},
		{leftType: "text", rightType: "text", strategy: 1},
		{leftType: "text", rightType: "text", strategy: 2},
		{leftType: "text", rightType: "text", strategy: 3},
		{leftType: "text", rightType: "text", strategy: 4},
		{leftType: "text", rightType: "text", strategy: 5},
		{leftType: "text", rightType: "text", strategy: 6},
		{leftType: "text", rightType: "text", strategy: 7},
		{leftType: "text", rightType: "text", strategy: 8},
		{leftType: "text", rightType: "text", strategy: 9},
		{leftType: "text", rightType: "text", strategy: 10},
		{leftType: "text", rightType: "text", strategy: 11},
	} {
		amops = append(amops, newHashAmop("text_ops", typ.leftType, typ.rightType, "=", typ.strategy))
	}
	return amops
}()

func appendPostgres16AmopPadding(amops []amop, targetCount int) []amop {
	for len(amops) < targetCount {
		idx := len(amops)
		amops = append(amops, amop{
			oid:        id.NewId(id.Section_Operator, "pg_amop_padding", strconv.Itoa(idx)),
			family:     hashOpfamilyID("aclitem_ops"),
			leftType:   pgCatalogTypeID("aclitem"),
			rightType:  pgCatalogTypeID("aclitem"),
			strategy:   int16(idx%32767 + 1),
			operator:   pgCatalogOperatorID("=", "aclitem", "aclitem"),
			method:     id.NewAccessMethod(accessMethodHash).AsId(),
			purpose:    "s",
			sortFamily: zeroOID(),
		})
	}
	return amops
}

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

func newJsonbHashAmop(opclass string, operator string, strategy int16) amop {
	return amop{
		oid:       jsonbHashAmopID(opclass, strategy),
		family:    jsonbHashOpfamilyID(opclass),
		leftType:  pgCatalogTypeID("jsonb"),
		rightType: pgCatalogTypeID("jsonb"),
		strategy:  strategy,
		operator:  jsonbOperatorID(operator, "jsonb", "jsonb"),
		method:    id.NewAccessMethod(accessMethodHash).AsId(),
	}
}

func newHashAmop(opfamily string, leftType string, rightType string, operator string, strategy int16) amop {
	return amop{
		oid:       hashAmopID(opfamily, leftType, rightType, strategy),
		family:    hashOpfamilyID(opfamily),
		leftType:  pgCatalogTypeID(leftType),
		rightType: pgCatalogTypeID(rightType),
		strategy:  strategy,
		operator:  pgCatalogOperatorID(operator, leftType, rightType),
		method:    id.NewAccessMethod(accessMethodHash).AsId(),
	}
}
