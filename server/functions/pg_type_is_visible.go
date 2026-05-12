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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgTableSize registers the functions to the catalog.
func initPgTypeIsVisible() {
	framework.RegisterFunction(pg_type_is_visible)
}

// pg_type_is_visible represents the PostgreSQL function of the same name, taking the same parameters.
var pg_type_is_visible = framework.Function1{
	Name:               "pg_type_is_visible",
	Return:             pgtypes.Bool,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Oid},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		oidVal := val.(id.Id)

		if oidVal.Section() != id.Section_Type {
			return false, nil
		}

		// Get the schema name where the type is defined
		// For type IDs, the first segment contains the schema name
		schemaName := oidVal.Segment(0)

		// Get the current search path
		searchPath, err := core.SearchPath(ctx)
		if err != nil {
			return false, err
		}

		typeName := oidVal.Segment(1)
		typeColl, err := core.GetTypesCollectionFromContext(ctx)
		if err != nil {
			return false, err
		}
		for _, path := range searchPath {
			typ, err := typeColl.GetType(ctx, id.NewType(path, typeName))
			if err != nil {
				return false, err
			}
			if typ != nil {
				return path == schemaName, nil
			}
		}

		return false, nil
	},
}
