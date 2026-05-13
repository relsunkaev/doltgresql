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

package hook

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
)

func dropSequencesOwnedByColumn(ctx *sql.Context, tableName doltdb.TableName, columnName string) error {
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	seqs, err := collection.GetSequencesWithTable(ctx, tableName)
	if err != nil {
		return err
	}
	for _, seq := range seqs {
		if seq.OwnerColumn != columnName {
			continue
		}
		if err = collection.DropSequence(ctx, seq.Id); err != nil {
			return err
		}
	}
	return nil
}

func renameSequencesOwnedByColumn(ctx *sql.Context, tableName doltdb.TableName, oldColumnName string, newColumnName string) error {
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return err
	}
	seqs, err := collection.GetSequencesWithTable(ctx, tableName)
	if err != nil {
		return err
	}
	for _, seq := range seqs {
		if seq.OwnerColumn == oldColumnName {
			seq.OwnerColumn = newColumnName
		}
	}
	return nil
}
