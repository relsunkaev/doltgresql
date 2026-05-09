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

package ast

import (
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// nodeLockTable accepts PostgreSQL relation locks. Doltgres does not yet model
// PostgreSQL relation lock conflicts, but pg_dump depends on ACCESS SHARE
// locks succeeding before catalog introspection.
func nodeLockTable(_ *Context, _ *tree.LockTable) (vitess.Statement, error) {
	return NewNoOp("LOCK TABLE is accepted, but PostgreSQL relation lock conflicts are not yet enforced"), nil
}
