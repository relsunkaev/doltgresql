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
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

const (
	accessMethodHash    = "hash"
	hstoreExtensionName = "hstore"
)

type hstoreCatalogOpclass struct {
	method    string
	name      string
	typeName  string
	keyType   string
	isDefault bool
}

type hstoreCatalogAmop struct {
	method    string
	opclass   string
	leftType  string
	rightType string
	operator  string
	strategy  int16
}

type hstoreCatalogAmproc struct {
	method    string
	opclass   string
	leftType  string
	rightType string
	procNum   int16
	proc      string
}

type hstoreCatalogOperator struct {
	name       string
	leftType   string
	rightType  string
	function   string
	commutator string
	restrict   id.Id
	join       id.Id
}

var hstoreCatalogOpclasses = []hstoreCatalogOpclass{
	{method: indexmetadata.AccessMethodBtree, name: "btree_hstore_ops", typeName: "hstore", isDefault: true},
	{method: indexmetadata.AccessMethodGin, name: "gin_hstore_ops", typeName: "hstore", keyType: "text", isDefault: true},
	{method: accessMethodGist, name: "gist_hstore_ops", typeName: "hstore", isDefault: true},
	{method: accessMethodHash, name: "hash_hstore_ops", typeName: "hstore", isDefault: true},
}

var hstoreCatalogAmops = []hstoreCatalogAmop{
	{method: indexmetadata.AccessMethodBtree, opclass: "btree_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "#<#", strategy: 1},
	{method: indexmetadata.AccessMethodBtree, opclass: "btree_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "#<=#", strategy: 2},
	{method: indexmetadata.AccessMethodBtree, opclass: "btree_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "=", strategy: 3},
	{method: indexmetadata.AccessMethodBtree, opclass: "btree_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "#>=#", strategy: 4},
	{method: indexmetadata.AccessMethodBtree, opclass: "btree_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "#>#", strategy: 5},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "@>", strategy: 7},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "text", operator: "?", strategy: 9},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "_text", operator: "?|", strategy: 10},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "_text", operator: "?&", strategy: 11},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "@>", strategy: 7},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "text", operator: "?", strategy: 9},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "_text", operator: "?|", strategy: 10},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "_text", operator: "?&", strategy: 11},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "@", strategy: 13},
	{method: accessMethodHash, opclass: "hash_hstore_ops", leftType: "hstore", rightType: "hstore", operator: "=", strategy: 1},
}

var hstoreCatalogAmprocs = []hstoreCatalogAmproc{
	{method: indexmetadata.AccessMethodBtree, opclass: "btree_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 1, proc: "hstore_cmp"},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 1, proc: "bttextcmp"},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 2, proc: "gin_extract_hstore"},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 3, proc: "gin_extract_hstore_query"},
	{method: indexmetadata.AccessMethodGin, opclass: "gin_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 4, proc: "gin_consistent_hstore"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 1, proc: "ghstore_consistent"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 2, proc: "ghstore_union"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 3, proc: "ghstore_compress"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 4, proc: "ghstore_decompress"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 5, proc: "ghstore_penalty"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 6, proc: "ghstore_picksplit"},
	{method: accessMethodGist, opclass: "gist_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 7, proc: "ghstore_same"},
	{method: accessMethodHash, opclass: "hash_hstore_ops", leftType: "hstore", rightType: "hstore", procNum: 1, proc: "hstore_hash"},
}

var hstoreCatalogOperators = []hstoreCatalogOperator{
	newHstoreComparisonOperator("#<#", "hstore_lt", "#>#"),
	newHstoreComparisonOperator("#<=#", "hstore_le", "#>=#"),
	newHstoreComparisonOperator("=", "hstore_eq", "="),
	newHstoreComparisonOperator("#>=#", "hstore_ge", "#<=#"),
	newHstoreComparisonOperator("#>#", "hstore_gt", "#<#"),
	newHstoreMatchingOperator("@>", "hstore", "hstore", "hs_contains", "<@"),
	newHstoreMatchingOperator("<@", "hstore", "hstore", "hs_contained", "@>"),
	newHstoreMatchingOperator("?", "hstore", "text", "exist", ""),
	newHstoreMatchingOperator("?|", "hstore", "_text", "exists_any", ""),
	newHstoreMatchingOperator("?&", "hstore", "_text", "exists_all", ""),
	newHstoreMatchingOperator("@", "hstore", "hstore", "hs_contains", "~"),
	newHstoreMatchingOperator("~", "hstore", "hstore", "hs_contained", "@"),
}

func appendHstoreOpclasses(ctx *sql.Context, opclasses []opclass) ([]opclass, error) {
	namespace, ok, err := hstoreExtensionNamespace(ctx)
	if err != nil || !ok {
		return opclasses, err
	}
	next := make([]opclass, 0, len(opclasses)+len(hstoreCatalogOpclasses))
	next = append(next, opclasses...)
	for _, catalogOpclass := range hstoreCatalogOpclasses {
		keytype := zeroOID()
		if catalogOpclass.keyType != "" {
			keytype = hstoreTypeID(namespace, catalogOpclass.keyType)
		}
		next = append(next, opclass{
			oid:       pgCatalogOpclassID(catalogOpclass.method, catalogOpclass.name),
			opcmethod: id.NewAccessMethod(catalogOpclass.method).AsId(),
			opcname:   catalogOpclass.name,
			namespace: namespace.AsId(),
			family:    hstoreOpfamilyID(catalogOpclass.method, catalogOpclass.name),
			intype:    hstoreTypeID(namespace, catalogOpclass.typeName),
			keytype:   keytype,
			isDefault: catalogOpclass.isDefault,
		})
	}
	return next, nil
}

func appendHstoreOpfamilies(ctx *sql.Context, opfamilies []opfamily) ([]opfamily, error) {
	namespace, ok, err := hstoreExtensionNamespace(ctx)
	if err != nil || !ok {
		return opfamilies, err
	}
	next := make([]opfamily, 0, len(opfamilies)+len(hstoreCatalogOpclasses))
	next = append(next, opfamilies...)
	for _, catalogOpclass := range hstoreCatalogOpclasses {
		next = append(next, opfamily{
			oid:       hstoreOpfamilyID(catalogOpclass.method, catalogOpclass.name),
			opfmethod: id.NewAccessMethod(catalogOpclass.method).AsId(),
			opfname:   catalogOpclass.name,
			namespace: namespace.AsId(),
		})
	}
	return next, nil
}

func appendHstoreAmops(ctx *sql.Context, amops []amop) ([]amop, error) {
	namespace, ok, err := hstoreExtensionNamespace(ctx)
	if err != nil || !ok {
		return amops, err
	}
	next := make([]amop, 0, len(amops)+len(hstoreCatalogAmops))
	next = append(next, amops...)
	for _, catalogAmop := range hstoreCatalogAmops {
		next = append(next, amop{
			oid:       hstoreAmopID(catalogAmop.method, catalogAmop.opclass, catalogAmop.leftType, catalogAmop.rightType, catalogAmop.strategy),
			family:    hstoreOpfamilyID(catalogAmop.method, catalogAmop.opclass),
			leftType:  hstoreTypeID(namespace, catalogAmop.leftType),
			rightType: hstoreTypeID(namespace, catalogAmop.rightType),
			strategy:  catalogAmop.strategy,
			operator:  hstoreOperatorID(namespace, catalogAmop.operator, catalogAmop.leftType, catalogAmop.rightType),
			method:    id.NewAccessMethod(catalogAmop.method).AsId(),
		})
	}
	return next, nil
}

func appendHstoreAmprocs(ctx *sql.Context, amprocs []amproc) ([]amproc, error) {
	namespace, ok, err := hstoreExtensionNamespace(ctx)
	if err != nil || !ok {
		return amprocs, err
	}
	next := make([]amproc, 0, len(amprocs)+len(hstoreCatalogAmprocs))
	next = append(next, amprocs...)
	for _, catalogAmproc := range hstoreCatalogAmprocs {
		next = append(next, amproc{
			oid:       hstoreAmprocID(catalogAmproc.method, catalogAmproc.opclass, catalogAmproc.leftType, catalogAmproc.rightType, catalogAmproc.procNum),
			family:    hstoreOpfamilyID(catalogAmproc.method, catalogAmproc.opclass),
			leftType:  hstoreTypeID(namespace, catalogAmproc.leftType),
			rightType: hstoreTypeID(namespace, catalogAmproc.rightType),
			procNum:   catalogAmproc.procNum,
			proc:      catalogAmproc.proc,
		})
	}
	return next, nil
}

func appendHstoreOperators(ctx *sql.Context, operators []pgOperator) ([]pgOperator, error) {
	namespace, ok, err := hstoreExtensionNamespace(ctx)
	if err != nil || !ok {
		return operators, err
	}
	next := make([]pgOperator, 0, len(operators)+len(hstoreCatalogOperators))
	next = append(next, operators...)
	for _, catalogOperator := range hstoreCatalogOperators {
		commutator := zeroOID()
		if catalogOperator.commutator != "" {
			commutator = hstoreOperatorID(namespace, catalogOperator.commutator, catalogOperator.rightType, catalogOperator.leftType)
		}
		next = append(next, pgOperator{
			oid:        hstoreOperatorID(namespace, catalogOperator.name, catalogOperator.leftType, catalogOperator.rightType),
			name:       catalogOperator.name,
			namespace:  namespace.AsId(),
			leftType:   hstoreTypeID(namespace, catalogOperator.leftType),
			rightType:  hstoreTypeID(namespace, catalogOperator.rightType),
			result:     pgCatalogTypeID("bool"),
			commutator: commutator,
			code:       hstoreFunctionID(namespace, catalogOperator.function, catalogOperator.leftType, catalogOperator.rightType),
			restrict:   catalogOperator.restrict,
			join:       catalogOperator.join,
		})
	}
	return next, nil
}

func hstoreExtensionNamespace(ctx *sql.Context) (id.Namespace, bool, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return id.NullNamespace, false, err
	}
	ext, err := extCollection.GetLoadedExtension(ctx, id.NewExtension(hstoreExtensionName))
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

func newHstoreComparisonOperator(name string, function string, commutator string) hstoreCatalogOperator {
	return hstoreCatalogOperator{
		name:       name,
		leftType:   "hstore",
		rightType:  "hstore",
		function:   function,
		commutator: commutator,
		restrict:   btreeOperatorRestrictFunctionID(name),
		join:       btreeOperatorJoinFunctionID(name),
	}
}

func newHstoreMatchingOperator(name string, leftType string, rightType string, function string, commutator string) hstoreCatalogOperator {
	return hstoreCatalogOperator{
		name:       name,
		leftType:   leftType,
		rightType:  rightType,
		function:   function,
		commutator: commutator,
		restrict:   pgCatalogFunctionID("matchingsel"),
		join:       pgCatalogFunctionID("matchingjoinsel"),
	}
}

func hstoreOpfamilyID(method string, opfamily string) id.Id {
	return id.NewId(id.Section_OperatorFamily, method, opfamily)
}

func hstoreAmopID(method string, opfamily string, leftType string, rightType string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"hstore_amop",
		method,
		opfamily,
		leftType,
		rightType,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func hstoreAmprocID(method string, opfamily string, leftType string, rightType string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"hstore_amproc",
		method,
		opfamily,
		leftType,
		rightType,
		strconv.FormatInt(int64(procNum), 10),
	)
}

func hstoreOperatorID(namespace id.Namespace, name string, leftType string, rightType string) id.Id {
	return id.NewId(
		id.Section_Operator,
		name,
		string(hstoreType(namespace, leftType)),
		string(hstoreType(namespace, rightType)),
	)
}

func hstoreFunctionID(namespace id.Namespace, name string, params ...string) id.Id {
	types := make([]id.Type, len(params))
	for i, param := range params {
		types[i] = hstoreType(namespace, param)
	}
	return id.NewFunction(namespace.SchemaName(), name, types...).AsId()
}

func hstoreTypeID(namespace id.Namespace, typeName string) id.Id {
	return hstoreType(namespace, typeName).AsId()
}

func hstoreType(namespace id.Namespace, typeName string) id.Type {
	switch typeName {
	case "bool", "text", "_text":
		return pgCatalogType(typeName)
	default:
		return id.NewType(namespace.SchemaName(), typeName)
	}
}
