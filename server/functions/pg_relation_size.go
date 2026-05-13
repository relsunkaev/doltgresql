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
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const estimatedRelationPageSize = int64(8192)

// initPgRelationSize registers the functions to the catalog.
func initPgRelationSize() {
	framework.RegisterFunction(pg_relation_size_regclass)
	framework.RegisterFunction(pg_relation_size_regclass_text)
}

// pg_relation_size_regclass represents the PostgreSQL function of the same name, taking the same parameters.
var pg_relation_size_regclass = framework.Function1{
	Name:               "pg_relation_size",
	Return:             pgtypes.Int64,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Regclass},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return estimatedRelationSizeForRegclass(ctx, val)
	},
}

// pg_relation_size_regclass_text represents the PostgreSQL function of the same name, taking the same parameters.
var pg_relation_size_regclass_text = framework.Function2{
	Name:               "pg_relation_size",
	Return:             pgtypes.Int64,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Regclass, pgtypes.Text},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		switch val2.(string) {
		case "main", "fsm", "vm", "init":
		default:
			return nil, pgerror.New(pgcode.InvalidParameterValue, "invalid fork name")
		}
		return estimatedRelationSizeForRegclass(ctx, val1)
	},
}

func estimatedRelationSizeForRegclass(ctx *sql.Context, val any) (int64, error) {
	relationID := val.(id.Id)
	switch relationID.Section() {
	case id.Section_Table:
		return estimatedTableDataSizeByID(ctx, relationID)
	case id.Section_Index:
		return estimatedIndexSizeByID(ctx, relationID)
	default:
		return 0, nil
	}
}

func estimatedTableDataSizeByID(ctx *sql.Context, relationID id.Id) (int64, error) {
	var size int64
	err := RunCallback(ctx, relationID, Callbacks{
		Table: func(ctx *sql.Context, _ ItemSchema, table ItemTable) (cont bool, err error) {
			size, err = estimatedTableDataSize(ctx, table.Item)
			return false, err
		},
	})
	return size, err
}

func estimatedTableIndexSizeByID(ctx *sql.Context, relationID id.Id) (int64, error) {
	var size int64
	err := RunCallback(ctx, relationID, Callbacks{
		Table: func(ctx *sql.Context, _ ItemSchema, table ItemTable) (cont bool, err error) {
			size, err = estimatedTableIndexSize(ctx, table.Item)
			return false, err
		},
	})
	return size, err
}

func estimatedIndexSizeByID(ctx *sql.Context, relationID id.Id) (int64, error) {
	var size int64
	err := RunCallback(ctx, relationID, Callbacks{
		Index: func(ctx *sql.Context, _ ItemSchema, table ItemTable, _ ItemIndex) (cont bool, err error) {
			size, err = estimatedSingleIndexSize(ctx, table.Item)
			return false, err
		},
	})
	return size, err
}

func estimatedTableDataSize(ctx *sql.Context, table sql.Table) (int64, error) {
	hasRows, err := tableHasRows(ctx, table)
	if err != nil || !hasRows {
		return 0, err
	}
	return estimatedRelationPageSize, nil
}

func estimatedTableIndexSize(ctx *sql.Context, table sql.Table) (int64, error) {
	indexedTable, ok := table.(sql.IndexAddressable)
	if !ok {
		return 0, nil
	}
	indexes, err := indexedTable.GetIndexes(ctx)
	if err != nil || len(indexes) == 0 {
		return 0, err
	}
	singleIndexSize, err := estimatedSingleIndexSize(ctx, table)
	if err != nil || singleIndexSize == 0 {
		return 0, err
	}
	return int64(len(indexes)) * singleIndexSize, nil
}

func estimatedSingleIndexSize(ctx *sql.Context, table sql.Table) (int64, error) {
	hasRows, err := tableHasRows(ctx, table)
	if err != nil || !hasRows {
		return 0, err
	}
	return estimatedRelationPageSize, nil
}

func tableHasRows(ctx *sql.Context, table sql.Table) (bool, error) {
	partitions, err := table.Partitions(ctx)
	if err != nil {
		return false, err
	}
	defer partitions.Close(ctx)
	for {
		partition, err := partitions.Next(ctx)
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		rows, err := table.PartitionRows(ctx, partition)
		if err != nil {
			return false, err
		}
		_, nextErr := rows.Next(ctx)
		closeErr := rows.Close(ctx)
		if nextErr == nil {
			return true, closeErr
		}
		if nextErr != io.EOF {
			return false, nextErr
		}
		if closeErr != nil {
			return false, closeErr
		}
	}
}
