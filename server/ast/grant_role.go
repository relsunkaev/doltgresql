// Copyright 2023 Dolthub, Inc.
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

package ast

import (
	"strings"

	pgnodes "github.com/dolthub/doltgresql/server/node"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// nodeGrantRole handles *tree.GrantRole nodes.
func nodeGrantRole(ctx *Context, node *tree.GrantRole) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	withAdminOption, withInheritOption, withSetOption := nodeGrantRoleOptions(node.WithOption)
	return vitess.InjectedStatement{
		Statement: &pgnodes.Grant{
			GrantRole: &pgnodes.GrantRole{
				Groups:            node.Roles.ToStrings(),
				WithAdminOption:   withAdminOption,
				WithInheritOption: withInheritOption,
				WithSetOption:     withSetOption,
			},
			ToRoles:   node.Members,
			GrantedBy: node.GrantedBy,
		},
		Children: nil,
	}, nil
}

func nodeGrantRoleOptions(withOption string) (withAdminOption bool, withInheritOption bool, withSetOption bool) {
	withInheritOption = true
	withSetOption = true
	switch strings.ToUpper(withOption) {
	case "ADMIN OPTION", "ADMIN TRUE":
		withAdminOption = true
	case "INHERIT FALSE":
		withInheritOption = false
	case "SET FALSE":
		withSetOption = false
	}
	return withAdminOption, withInheritOption, withSetOption
}
