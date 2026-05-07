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

// nodeLockingClause converts a PostgreSQL row-locking clause —
// FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE, with
// optional NOWAIT / SKIP LOCKED and OF table-list — into the
// MySQL-flavored Lock structure GMS understands.
//
// Doltgres does not yet take true row-level pessimistic locks;
// under MVCC + serializable isolation the clause is largely
// advisory. We accept it so ORM read-modify-write workloads
// (SQLAlchemy with_for_update, ActiveRecord lock, Django
// select_for_update, Drizzle .for("update")) succeed.
//
// PostgreSQL allows multiple locking items on the same SELECT
// (e.g. `FOR UPDATE OF a FOR SHARE OF b`). The strongest mode and
// strictest wait-policy across items wins, and OF tables are
// unioned. GMS only validates that each OF table is in scope.
func nodeLockingClause(ctx *Context, node tree.LockingClause) (*vitess.Lock, error) {
	if len(node) == 0 {
		return nil, nil
	}
	var (
		strength   tree.LockingStrength
		waitPolicy tree.LockingWaitPolicy
		targets    tree.TableNames
	)
	for _, item := range node {
		if item == nil {
			continue
		}
		strength = strength.Max(item.Strength)
		if item.WaitPolicy > waitPolicy {
			waitPolicy = item.WaitPolicy
		}
		targets = append(targets, item.Targets...)
	}

	tables := make(vitess.TableNames, 0, len(targets))
	for i := range targets {
		tn, err := nodeTableName(ctx, &targets[i])
		if err != nil {
			return nil, err
		}
		tables = append(tables, tn)
	}

	return &vitess.Lock{
		Type:   lockTypeString(strength, waitPolicy),
		Tables: tables,
	}, nil
}

// lockTypeString maps the (strength, waitPolicy) pair to the
// closest vitess Lock.Type constant. GMS does not implement true
// row locks, so the string is largely cosmetic — buildForUpdateOf
// only inspects Lock.Tables — but we pick a faithful mapping so
// any future formatter / explainer prints the user's intent.
func lockTypeString(strength tree.LockingStrength, waitPolicy tree.LockingWaitPolicy) string {
	updateLike := strength >= tree.ForNoKeyUpdate
	switch waitPolicy {
	case tree.LockWaitSkip:
		// Vitess only defines a SKIP LOCKED string for FOR UPDATE.
		// FOR SHARE SKIP LOCKED reuses it; the precise distinction
		// is moot under the current no-op implementation.
		return vitess.ForUpdateSkipLockedStr
	case tree.LockWaitError:
		return vitess.ForUpdateNowaitStr
	default:
		if updateLike {
			return vitess.ForUpdateStr
		}
		return vitess.ShareModeStr
	}
}
