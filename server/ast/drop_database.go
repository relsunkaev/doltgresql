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
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// nodeDropDatabase handles *tree.DropDatabase nodes.
func nodeDropDatabase(_ *Context, node *tree.DropDatabase) (*vitess.DBDDL, error) {
	if node == nil {
		return nil, nil
	}
	// PostgreSQL's WITH (FORCE) tells the server to terminate any sessions
	// connected to the target database before removing it. Doltgres does not
	// currently model cross-session connection termination, so the keyword is
	// accepted as a pass-through: the underlying DROP DATABASE still fails if
	// the caller's own session is currently connected to the target.
	return &vitess.DBDDL{
		Action:           vitess.DropStr,
		SchemaOrDatabase: "database",
		DBName:           bareIdentifier(node.Name),
		IfExists:         node.IfExists,
	}, nil
}
