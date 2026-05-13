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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
)

func rejectReadOnlyPersistentSequenceWrite(ctx *sql.Context, collection *sequences.Collection, sequenceID id.Sequence) error {
	tx := ctx.GetTransaction()
	if tx == nil || !tx.IsReadOnly() {
		return nil
	}
	sequence, err := collection.GetSequence(ctx, sequenceID)
	if err != nil || sequence == nil {
		return err
	}
	if sequence.Persistence == sequences.Persistence_Temporary {
		return nil
	}
	return pgerror.New(pgcode.ReadOnlySQLTransaction, "cannot execute statement in a read-only transaction (READ ONLY transaction)")
}
