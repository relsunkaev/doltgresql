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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/indexmetadata"
)

func pgCatalogNamespaceID() id.Id {
	return id.NewNamespace("pg_catalog").AsId()
}

func pgCatalogType(name string) id.Type {
	return id.NewType("pg_catalog", name)
}

func pgCatalogTypeID(name string) id.Id {
	return pgCatalogType(name).AsId()
}

func pgCatalogFunctionID(name string, params ...id.Type) id.Id {
	return id.NewFunction("pg_catalog", name, params...).AsId()
}

func pgCatalogOperatorID(name string, leftType string, rightType string) id.Id {
	return id.NewId(
		id.Section_Operator,
		name,
		string(pgCatalogType(leftType)),
		string(pgCatalogType(rightType)),
	)
}

func zeroOID() id.Id {
	return id.NewOID(0).AsId()
}

func pgCatalogOpclassID(method string, opclass string) id.Id {
	return id.NewId(id.Section_OperatorClass, method, opclass)
}

func jsonbGinOpfamilyID(opclass string) id.Id {
	return id.NewId(id.Section_OperatorFamily, indexmetadata.AccessMethodGin, opclass)
}

func jsonbHashOpfamilyID(opclass string) id.Id {
	return id.NewId(id.Section_OperatorFamily, accessMethodHash, opclass)
}

func jsonbOperatorID(name string, leftType string, rightType string) id.Id {
	return pgCatalogOperatorID(name, leftType, rightType)
}

func jsonbGinAmopID(opclass string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"gin_amop",
		opclass,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func jsonbGinAmprocID(opclass string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"gin_amproc",
		opclass,
		strconv.FormatInt(int64(procNum), 10),
	)
}

func jsonbHashAmopID(opclass string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"hash_amop",
		opclass,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func jsonbHashAmprocID(opclass string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"hash_amproc",
		opclass,
		strconv.FormatInt(int64(procNum), 10),
	)
}

func btreeAmopID(opfamily string, typeName string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"btree_amop",
		opfamily,
		typeName,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func btreeCrossTypeAmopID(opfamily string, leftType string, rightType string, strategy int16) id.Id {
	return id.NewId(
		id.Section_Operator,
		"btree_amop",
		opfamily,
		leftType,
		rightType,
		strconv.FormatInt(int64(strategy), 10),
	)
}

func btreeAmprocID(opfamily string, typeName string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"btree_amproc",
		opfamily,
		typeName,
		strconv.FormatInt(int64(procNum), 10),
	)
}

func btreeCrossTypeAmprocID(opfamily string, leftType string, rightType string, procNum int16) id.Id {
	return id.NewId(
		id.Section_OperatorFamily,
		"btree_amproc",
		opfamily,
		leftType,
		rightType,
		strconv.FormatInt(int64(procNum), 10),
	)
}
