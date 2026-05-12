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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initShobjDescription registers the functions to the catalog.
func initShobjDescription() {
	framework.RegisterFunction(shobj_description)
}

// shobj_description represents the PostgreSQL function of the same name, taking the same parameters.
var shobj_description = framework.Function2{
	Name:               "shobj_description",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Name},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		classOID, err := regclassin.Callable(ctx, [2]*pgtypes.DoltgresType{pgtypes.Cstring, pgtypes.Regclass}, val2.(string))
		if err != nil {
			return nil, err
		}
		description, ok := comments.Get(comments.Key{
			ObjOID:   id.Cache().ToOID(val1.(id.Id)),
			ClassOID: id.Cache().ToOID(classOID.(id.Id)),
			ObjSubID: 0,
		})
		if !ok {
			return nil, nil
		}
		return description, nil
	},
}
