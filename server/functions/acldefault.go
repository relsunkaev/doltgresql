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

package functions

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initAclDefault registers the functions to the catalog.
func initAclDefault() {
	framework.RegisterFunction(acldefault_char_oid)
}

// acldefault_char_oid returns the default ACL array for an object type and
// owner. Doltgres does not enforce ACLs yet, so catalog consumers should see
// no explicit privilege delta from the default.
var acldefault_char_oid = framework.Function2{
	Name:       "acldefault",
	Return:     pgtypes.TextArray, // TODO: aclitem[]
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.InternalChar, pgtypes.Oid},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, objectType any, owner any) (any, error) {
		return nil, nil
	},
}
