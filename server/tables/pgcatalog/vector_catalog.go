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
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
)

const (
	accessMethodHnsw       = "hnsw"
	accessMethodIvfflat    = "ivfflat"
	vectorExtensionName    = "vector"
	vectorDistanceSortName = "float_ops"
)

type vectorCatalogOpclass struct {
	method    string
	name      string
	typeName  string
	isDefault bool
}

type vectorCatalogAmop struct {
	method   string
	opclass  string
	typeName string
	operator string
	strategy int16
	purpose  string
}

type vectorCatalogAmproc struct {
	method   string
	opclass  string
	typeName string
	procNum  int16
	proc     string
}

type vectorCatalogOperator struct {
	name       string
	leftType   string
	rightType  string
	result     string
	function   string
	commutator string
	restrict   id.Id
	join       id.Id
}

var vectorCatalogOpclasses = []vectorCatalogOpclass{
	{method: "btree", name: "vector_ops", typeName: "vector", isDefault: true},
	{method: accessMethodIvfflat, name: "vector_l2_ops", typeName: "vector", isDefault: true},
	{method: accessMethodIvfflat, name: "vector_ip_ops", typeName: "vector"},
	{method: accessMethodIvfflat, name: "vector_cosine_ops", typeName: "vector"},
	{method: accessMethodIvfflat, name: "bit_hamming_ops", typeName: "bit"},
	{method: accessMethodHnsw, name: "vector_l2_ops", typeName: "vector"},
	{method: accessMethodHnsw, name: "vector_ip_ops", typeName: "vector"},
	{method: accessMethodHnsw, name: "vector_cosine_ops", typeName: "vector"},
	{method: accessMethodHnsw, name: "vector_l1_ops", typeName: "vector"},
	{method: accessMethodHnsw, name: "bit_hamming_ops", typeName: "bit"},
	{method: accessMethodHnsw, name: "bit_jaccard_ops", typeName: "bit"},
}

var vectorCatalogAmops = []vectorCatalogAmop{
	{method: "btree", opclass: "vector_ops", typeName: "vector", operator: "<", strategy: 1},
	{method: "btree", opclass: "vector_ops", typeName: "vector", operator: "<=", strategy: 2},
	{method: "btree", opclass: "vector_ops", typeName: "vector", operator: "=", strategy: 3},
	{method: "btree", opclass: "vector_ops", typeName: "vector", operator: ">=", strategy: 4},
	{method: "btree", opclass: "vector_ops", typeName: "vector", operator: ">", strategy: 5},
	{method: accessMethodIvfflat, opclass: "vector_l2_ops", typeName: "vector", operator: "<->", strategy: 1, purpose: "o"},
	{method: accessMethodIvfflat, opclass: "vector_ip_ops", typeName: "vector", operator: "<#>", strategy: 1, purpose: "o"},
	{method: accessMethodIvfflat, opclass: "vector_cosine_ops", typeName: "vector", operator: "<=>", strategy: 1, purpose: "o"},
	{method: accessMethodIvfflat, opclass: "bit_hamming_ops", typeName: "bit", operator: "<~>", strategy: 1, purpose: "o"},
	{method: accessMethodHnsw, opclass: "vector_l2_ops", typeName: "vector", operator: "<->", strategy: 1, purpose: "o"},
	{method: accessMethodHnsw, opclass: "vector_ip_ops", typeName: "vector", operator: "<#>", strategy: 1, purpose: "o"},
	{method: accessMethodHnsw, opclass: "vector_cosine_ops", typeName: "vector", operator: "<=>", strategy: 1, purpose: "o"},
	{method: accessMethodHnsw, opclass: "vector_l1_ops", typeName: "vector", operator: "<+>", strategy: 1, purpose: "o"},
	{method: accessMethodHnsw, opclass: "bit_hamming_ops", typeName: "bit", operator: "<~>", strategy: 1, purpose: "o"},
	{method: accessMethodHnsw, opclass: "bit_jaccard_ops", typeName: "bit", operator: "<%>", strategy: 1, purpose: "o"},
}

var vectorCatalogAmprocs = []vectorCatalogAmproc{
	{method: "btree", opclass: "vector_ops", typeName: "vector", procNum: 1, proc: "vector_cmp"},
	{method: accessMethodIvfflat, opclass: "vector_l2_ops", typeName: "vector", procNum: 1, proc: "vector_l2_squared_distance"},
	{method: accessMethodIvfflat, opclass: "vector_l2_ops", typeName: "vector", procNum: 3, proc: "l2_distance"},
	{method: accessMethodIvfflat, opclass: "vector_ip_ops", typeName: "vector", procNum: 1, proc: "vector_negative_inner_product"},
	{method: accessMethodIvfflat, opclass: "vector_ip_ops", typeName: "vector", procNum: 3, proc: "vector_spherical_distance"},
	{method: accessMethodIvfflat, opclass: "vector_ip_ops", typeName: "vector", procNum: 4, proc: "vector_norm"},
	{method: accessMethodIvfflat, opclass: "vector_cosine_ops", typeName: "vector", procNum: 1, proc: "vector_negative_inner_product"},
	{method: accessMethodIvfflat, opclass: "vector_cosine_ops", typeName: "vector", procNum: 2, proc: "vector_norm"},
	{method: accessMethodIvfflat, opclass: "vector_cosine_ops", typeName: "vector", procNum: 3, proc: "vector_spherical_distance"},
	{method: accessMethodIvfflat, opclass: "vector_cosine_ops", typeName: "vector", procNum: 4, proc: "vector_norm"},
	{method: accessMethodIvfflat, opclass: "bit_hamming_ops", typeName: "bit", procNum: 1, proc: "hamming_distance"},
	{method: accessMethodIvfflat, opclass: "bit_hamming_ops", typeName: "bit", procNum: 3, proc: "hamming_distance"},
	{method: accessMethodIvfflat, opclass: "bit_hamming_ops", typeName: "bit", procNum: 5, proc: "ivfflat_bit_support"},
	{method: accessMethodHnsw, opclass: "vector_l2_ops", typeName: "vector", procNum: 1, proc: "vector_l2_squared_distance"},
	{method: accessMethodHnsw, opclass: "vector_ip_ops", typeName: "vector", procNum: 1, proc: "vector_negative_inner_product"},
	{method: accessMethodHnsw, opclass: "vector_cosine_ops", typeName: "vector", procNum: 1, proc: "vector_negative_inner_product"},
	{method: accessMethodHnsw, opclass: "vector_cosine_ops", typeName: "vector", procNum: 2, proc: "vector_norm"},
	{method: accessMethodHnsw, opclass: "vector_l1_ops", typeName: "vector", procNum: 1, proc: "l1_distance"},
	{method: accessMethodHnsw, opclass: "bit_hamming_ops", typeName: "bit", procNum: 1, proc: "hamming_distance"},
	{method: accessMethodHnsw, opclass: "bit_hamming_ops", typeName: "bit", procNum: 3, proc: "hnsw_bit_support"},
	{method: accessMethodHnsw, opclass: "bit_jaccard_ops", typeName: "bit", procNum: 1, proc: "jaccard_distance"},
	{method: accessMethodHnsw, opclass: "bit_jaccard_ops", typeName: "bit", procNum: 3, proc: "hnsw_bit_support"},
}

var vectorCatalogOperators = []vectorCatalogOperator{
	newVectorComparisonOperator("<", "vector_lt", ">"),
	newVectorComparisonOperator("<=", "vector_le", ">="),
	newVectorComparisonOperator("=", "vector_eq", "="),
	newVectorComparisonOperator(">=", "vector_ge", "<="),
	newVectorComparisonOperator(">", "vector_gt", "<"),
	newVectorDistanceOperator("<->", "vector", "l2_distance"),
	newVectorDistanceOperator("<#>", "vector", "vector_negative_inner_product"),
	newVectorDistanceOperator("<=>", "vector", "cosine_distance"),
	newVectorDistanceOperator("<+>", "vector", "l1_distance"),
	newVectorDistanceOperator("<~>", "bit", "hamming_distance"),
	newVectorDistanceOperator("<%>", "bit", "jaccard_distance"),
}

func appendVectorAccessMethods(ctx *sql.Context, ams []accessMethod) ([]accessMethod, error) {
	namespace, ok, err := vectorExtensionNamespace(ctx)
	if err != nil || !ok {
		return ams, err
	}
	next := make([]accessMethod, 0, len(ams)+2)
	next = append(next, ams...)
	next = append(next,
		accessMethod{
			oid:     id.NewAccessMethod(accessMethodHnsw).AsId(),
			name:    accessMethodHnsw,
			handler: id.NewFunction(namespace.SchemaName(), "hnswhandler", pgCatalogType("internal")).AsId(),
			typ:     "i",
		},
		accessMethod{
			oid:     id.NewAccessMethod(accessMethodIvfflat).AsId(),
			name:    accessMethodIvfflat,
			handler: id.NewFunction(namespace.SchemaName(), "ivfflathandler", pgCatalogType("internal")).AsId(),
			typ:     "i",
		},
	)
	return next, nil
}

func appendVectorOpclasses(ctx *sql.Context, opclasses []opclass) ([]opclass, error) {
	namespace, ok, err := vectorExtensionNamespace(ctx)
	if err != nil || !ok {
		return opclasses, err
	}
	next := make([]opclass, 0, len(opclasses)+len(vectorCatalogOpclasses))
	next = append(next, opclasses...)
	for _, catalogOpclass := range vectorCatalogOpclasses {
		next = append(next, opclass{
			oid:       pgCatalogOpclassID(catalogOpclass.method, catalogOpclass.name),
			opcmethod: id.NewAccessMethod(catalogOpclass.method).AsId(),
			opcname:   catalogOpclass.name,
			namespace: namespace.AsId(),
			family:    vectorOpfamilyID(catalogOpclass.method, catalogOpclass.name),
			intype:    pgCatalogTypeID(catalogOpclass.typeName),
			keytype:   zeroOID(),
			isDefault: catalogOpclass.isDefault,
		})
	}
	return next, nil
}

func appendVectorOpfamilies(ctx *sql.Context, opfamilies []opfamily) ([]opfamily, error) {
	namespace, ok, err := vectorExtensionNamespace(ctx)
	if err != nil || !ok {
		return opfamilies, err
	}
	next := make([]opfamily, 0, len(opfamilies)+len(vectorCatalogOpclasses))
	next = append(next, opfamilies...)
	for _, catalogOpclass := range vectorCatalogOpclasses {
		next = append(next, opfamily{
			oid:       vectorOpfamilyID(catalogOpclass.method, catalogOpclass.name),
			opfmethod: id.NewAccessMethod(catalogOpclass.method).AsId(),
			opfname:   catalogOpclass.name,
			namespace: namespace.AsId(),
		})
	}
	return next, nil
}

func appendVectorAmops(ctx *sql.Context, amops []amop) ([]amop, error) {
	_, ok, err := vectorExtensionNamespace(ctx)
	if err != nil || !ok {
		return amops, err
	}
	next := make([]amop, 0, len(amops)+len(vectorCatalogAmops))
	next = append(next, amops...)
	for _, catalogAmop := range vectorCatalogAmops {
		sortFamily := zeroOID()
		if catalogAmop.purpose == "o" {
			sortFamily = btreeOpfamilyID(vectorDistanceSortName)
		}
		next = append(next, amop{
			oid:        vectorAmopID(catalogAmop.method, catalogAmop.opclass, catalogAmop.typeName, catalogAmop.strategy),
			family:     vectorOpfamilyID(catalogAmop.method, catalogAmop.opclass),
			leftType:   pgCatalogTypeID(catalogAmop.typeName),
			rightType:  pgCatalogTypeID(catalogAmop.typeName),
			strategy:   catalogAmop.strategy,
			operator:   vectorOperatorID(catalogAmop.operator, catalogAmop.typeName, catalogAmop.typeName),
			method:     id.NewAccessMethod(catalogAmop.method).AsId(),
			purpose:    catalogAmop.purpose,
			sortFamily: sortFamily,
		})
	}
	return next, nil
}

func appendVectorAmprocs(ctx *sql.Context, amprocs []amproc) ([]amproc, error) {
	_, ok, err := vectorExtensionNamespace(ctx)
	if err != nil || !ok {
		return amprocs, err
	}
	next := make([]amproc, 0, len(amprocs)+len(vectorCatalogAmprocs))
	next = append(next, amprocs...)
	for _, catalogAmproc := range vectorCatalogAmprocs {
		next = append(next, amproc{
			oid:       vectorAmprocID(catalogAmproc.method, catalogAmproc.opclass, catalogAmproc.typeName, catalogAmproc.procNum),
			family:    vectorOpfamilyID(catalogAmproc.method, catalogAmproc.opclass),
			leftType:  pgCatalogTypeID(catalogAmproc.typeName),
			rightType: pgCatalogTypeID(catalogAmproc.typeName),
			procNum:   catalogAmproc.procNum,
			proc:      catalogAmproc.proc,
		})
	}
	return next, nil
}

func appendVectorOperators(ctx *sql.Context, operators []pgOperator) ([]pgOperator, error) {
	namespace, ok, err := vectorExtensionNamespace(ctx)
	if err != nil || !ok {
		return operators, err
	}
	next := make([]pgOperator, 0, len(operators)+len(vectorCatalogOperators))
	next = append(next, operators...)
	for _, catalogOperator := range vectorCatalogOperators {
		next = append(next, pgOperator{
			oid:        vectorOperatorID(catalogOperator.name, catalogOperator.leftType, catalogOperator.rightType),
			name:       catalogOperator.name,
			namespace:  namespace.AsId(),
			leftType:   pgCatalogTypeID(catalogOperator.leftType),
			rightType:  pgCatalogTypeID(catalogOperator.rightType),
			result:     pgCatalogTypeID(catalogOperator.result),
			commutator: vectorOperatorID(catalogOperator.commutator, catalogOperator.rightType, catalogOperator.leftType),
			code:       pgCatalogFunctionID(catalogOperator.function, pgCatalogType(catalogOperator.leftType), pgCatalogType(catalogOperator.rightType)),
			restrict:   catalogOperator.restrict,
			join:       catalogOperator.join,
		})
	}
	return next, nil
}

func vectorExtensionNamespace(ctx *sql.Context) (id.Namespace, bool, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return id.NullNamespace, false, err
	}
	ext, err := extCollection.GetLoadedExtension(ctx, id.NewExtension(vectorExtensionName))
	if err != nil {
		return id.NullNamespace, false, err
	}
	if !ext.ExtName.IsValid() {
		return id.NullNamespace, false, nil
	}
	namespace := ext.Namespace
	if !namespace.IsValid() {
		namespace = id.NewNamespace("public")
	}
	return namespace, true, nil
}

func newVectorComparisonOperator(name string, function string, commutator string) vectorCatalogOperator {
	return vectorCatalogOperator{
		name:       name,
		leftType:   "vector",
		rightType:  "vector",
		result:     "bool",
		function:   function,
		commutator: commutator,
		restrict:   btreeOperatorRestrictFunctionID(name),
		join:       btreeOperatorJoinFunctionID(name),
	}
}

func newVectorDistanceOperator(name string, typeName string, function string) vectorCatalogOperator {
	return vectorCatalogOperator{
		name:       name,
		leftType:   typeName,
		rightType:  typeName,
		result:     "float8",
		function:   function,
		commutator: name,
		restrict:   zeroOID(),
		join:       zeroOID(),
	}
}

func vectorOperatorID(name string, leftType string, rightType string) id.Id {
	return pgCatalogOperatorID(name, leftType, rightType)
}

func vectorOpfamilyID(method string, opfamily string) id.Id {
	return id.NewId(id.Section_OperatorFamily, method, opfamily)
}

func vectorAmopID(method string, opfamily string, typeName string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"vector_amop",
		method,
		opfamily,
		typeName,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func vectorAmprocID(method string, opfamily string, typeName string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"vector_amproc",
		method,
		opfamily,
		typeName,
		strconv.FormatInt(int64(procNum), 10),
	)
}
