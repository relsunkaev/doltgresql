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

const citextExtensionName = "citext"

var citextComparisonOperators = []btreeComparisonOperator{
	{name: "<", strategy: 1, commutator: ">"},
	{name: "<=", strategy: 2, commutator: ">="},
	{name: "=", strategy: 3, commutator: "="},
	{name: ">=", strategy: 4, commutator: "<="},
	{name: ">", strategy: 5, commutator: "<"},
}

var citextComparisonFunctions = []string{
	"citext_lt",
	"citext_le",
	"citext_eq",
	"citext_ge",
	"citext_gt",
}

func appendCitextOpclasses(ctx *sql.Context, opclasses []opclass) ([]opclass, error) {
	namespace, ok, err := citextExtensionNamespace(ctx)
	if err != nil || !ok {
		return opclasses, err
	}
	next := make([]opclass, 0, len(opclasses)+2)
	next = append(next, opclasses...)
	next = append(next, opclass{
		oid:       pgCatalogOpclassID(indexmetadata.AccessMethodBtree, indexmetadata.OpClassCitextOps),
		opcmethod: id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
		opcname:   indexmetadata.OpClassCitextOps,
		namespace: namespace.AsId(),
		family:    citextOpfamilyID(indexmetadata.AccessMethodBtree),
		intype:    citextTypeID(namespace),
		isDefault: true,
	})
	next = append(next, opclass{
		oid:       pgCatalogOpclassID(accessMethodHash, indexmetadata.OpClassCitextOps),
		opcmethod: id.NewAccessMethod(accessMethodHash).AsId(),
		opcname:   indexmetadata.OpClassCitextOps,
		namespace: namespace.AsId(),
		family:    citextOpfamilyID(accessMethodHash),
		intype:    citextTypeID(namespace),
		isDefault: true,
	})
	return next, nil
}

func appendCitextOpfamilies(ctx *sql.Context, opfamilies []opfamily) ([]opfamily, error) {
	namespace, ok, err := citextExtensionNamespace(ctx)
	if err != nil || !ok {
		return opfamilies, err
	}
	next := make([]opfamily, 0, len(opfamilies)+2)
	next = append(next, opfamilies...)
	next = append(next, opfamily{
		oid:       citextOpfamilyID(indexmetadata.AccessMethodBtree),
		opfmethod: id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
		opfname:   indexmetadata.OpClassCitextOps,
		namespace: namespace.AsId(),
	})
	next = append(next, opfamily{
		oid:       citextOpfamilyID(accessMethodHash),
		opfmethod: id.NewAccessMethod(accessMethodHash).AsId(),
		opfname:   indexmetadata.OpClassCitextOps,
		namespace: namespace.AsId(),
	})
	return next, nil
}

func appendCitextAmops(ctx *sql.Context, amops []amop) ([]amop, error) {
	namespace, ok, err := citextExtensionNamespace(ctx)
	if err != nil || !ok {
		return amops, err
	}
	next := make([]amop, 0, len(amops)+len(citextComparisonOperators)+1)
	next = append(next, amops...)
	for _, operator := range citextComparisonOperators {
		next = append(next, amop{
			oid:       citextAmopID(indexmetadata.AccessMethodBtree, operator.strategy),
			family:    citextOpfamilyID(indexmetadata.AccessMethodBtree),
			leftType:  citextTypeID(namespace),
			rightType: citextTypeID(namespace),
			strategy:  operator.strategy,
			operator:  citextOperatorID(namespace, operator.name),
			method:    id.NewAccessMethod(indexmetadata.AccessMethodBtree).AsId(),
		})
	}
	next = append(next, amop{
		oid:       citextAmopID(accessMethodHash, int16(1)),
		family:    citextOpfamilyID(accessMethodHash),
		leftType:  citextTypeID(namespace),
		rightType: citextTypeID(namespace),
		strategy:  int16(1),
		operator:  citextOperatorID(namespace, "="),
		method:    id.NewAccessMethod(accessMethodHash).AsId(),
	})
	return next, nil
}

func appendCitextAmprocs(ctx *sql.Context, amprocs []amproc) ([]amproc, error) {
	namespace, ok, err := citextExtensionNamespace(ctx)
	if err != nil || !ok {
		return amprocs, err
	}
	next := make([]amproc, 0, len(amprocs)+3)
	next = append(next, amprocs...)
	next = append(next, amproc{
		oid:       citextAmprocID(indexmetadata.AccessMethodBtree, int16(1)),
		family:    citextOpfamilyID(indexmetadata.AccessMethodBtree),
		leftType:  citextTypeID(namespace),
		rightType: citextTypeID(namespace),
		procNum:   1,
		proc:      "citext_cmp",
	})
	next = append(next, amproc{
		oid:       citextAmprocID(accessMethodHash, int16(1)),
		family:    citextOpfamilyID(accessMethodHash),
		leftType:  citextTypeID(namespace),
		rightType: citextTypeID(namespace),
		procNum:   1,
		proc:      "citext_hash",
	})
	next = append(next, amproc{
		oid:       citextAmprocID(accessMethodHash, int16(2)),
		family:    citextOpfamilyID(accessMethodHash),
		leftType:  citextTypeID(namespace),
		rightType: citextTypeID(namespace),
		procNum:   2,
		proc:      "citext_hash_extended",
	})
	return next, nil
}

func appendCitextOperators(ctx *sql.Context, operators []pgOperator) ([]pgOperator, error) {
	namespace, ok, err := citextExtensionNamespace(ctx)
	if err != nil || !ok {
		return operators, err
	}
	next := make([]pgOperator, 0, len(operators)+len(citextComparisonOperators))
	next = append(next, operators...)
	for i, operator := range citextComparisonOperators {
		next = append(next, pgOperator{
			oid:        citextOperatorID(namespace, operator.name),
			name:       operator.name,
			namespace:  namespace.AsId(),
			leftType:   citextTypeID(namespace),
			rightType:  citextTypeID(namespace),
			result:     pgCatalogTypeID("bool"),
			commutator: citextOperatorID(namespace, operator.commutator),
			code:       citextFunctionID(namespace, citextComparisonFunctions[i]),
			restrict:   btreeOperatorRestrictFunctionID(operator.name),
			join:       btreeOperatorJoinFunctionID(operator.name),
		})
	}
	return next, nil
}

func citextExtensionNamespace(ctx *sql.Context) (id.Namespace, bool, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return id.NullNamespace, false, err
	}
	ext, err := extCollection.GetLoadedExtension(ctx, id.NewExtension(citextExtensionName))
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

func citextOpfamilyID(method string) id.Id {
	return id.NewId(id.Section_OperatorFamily, method, indexmetadata.OpClassCitextOps)
}

func citextAmopID(method string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"citext_amop",
		method,
		indexmetadata.OpClassCitextOps,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func citextAmprocID(method string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"citext_amproc",
		method,
		indexmetadata.OpClassCitextOps,
		strconv.FormatInt(int64(procNum), 10),
	)
}

func citextOperatorID(namespace id.Namespace, name string) id.Id {
	return id.NewId(
		id.Section_Operator,
		name,
		string(citextType(namespace)),
		string(citextType(namespace)),
	)
}

func citextFunctionID(namespace id.Namespace, name string) id.Id {
	citextType := citextType(namespace)
	return id.NewFunction(namespace.SchemaName(), name, citextType, citextType).AsId()
}

func citextTypeID(namespace id.Namespace) id.Id {
	return citextType(namespace).AsId()
}

func citextType(namespace id.Namespace) id.Type {
	return id.NewType(namespace.SchemaName(), "citext")
}
