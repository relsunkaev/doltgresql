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
	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initCurrVal registers currval and lastval sequence functions.
func initCurrVal() {
	framework.RegisterFunction(currval_text)
	framework.RegisterFunction(currval_regclass)
	framework.RegisterFunction(lastval)
}

var currval_text = framework.Function1{
	Name:       "currval",
	Return:     pgtypes.Int64,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		database, schema, sequence, err := ResolveSequenceNameWithDatabase(ctx, val.(string))
		if err != nil {
			return nil, err
		}
		return currvalForSequence(ctx, database, id.NewSequence(schema, sequence))
	},
}

var currval_regclass = framework.Function1{
	Name:       "currval",
	Return:     pgtypes.Int64,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regclass},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		relationName, err := pgtypes.Regclass.IoOutput(ctx, val)
		if err != nil {
			return nil, err
		}
		database, schema, sequence, err := ResolveSequenceNameWithDatabase(ctx, relationName)
		if err != nil {
			return nil, err
		}
		return currvalForSequence(ctx, database, id.NewSequence(schema, sequence))
	},
}

var lastval = framework.Function0{
	Name:   "lastval",
	Return: pgtypes.Int64,
	Callable: func(ctx *sql.Context) (any, error) {
		value, ok := sessionSequenceLastValue(ctx)
		if !ok {
			return nil, pgerror.New(pgcode.ObjectNotInPrerequisiteState, "lastval is not yet defined in this session")
		}
		return value.value, nil
	},
}

func currvalForSequence(ctx *sql.Context, database string, sequenceID id.Sequence) (int64, error) {
	collection, err := core.GetSequencesCollectionFromContext(ctx, database)
	if err != nil {
		return 0, err
	}
	seq, err := collection.GetSequence(ctx, sequenceID)
	if err != nil {
		return 0, err
	}
	if seq == nil {
		return 0, errors.Errorf(`relation "%s" does not exist`, sequenceID.SequenceName())
	}
	if value, ok := sessionSequenceCurrentValue(ctx, sequenceID); ok {
		return value, nil
	}
	return 0, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, `currval of sequence "%s" is not yet defined in this session`, sequenceID.SequenceName())
}
