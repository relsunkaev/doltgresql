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

package node

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/sessionstate"
)

type RelationLockMode int

const (
	RelationLockAccessShare RelationLockMode = iota
	RelationLockRowShare
	RelationLockRowExclusive
	RelationLockShareUpdateExclusive
	RelationLockShare
	RelationLockShareRowExclusive
	RelationLockExclusive
	RelationLockAccessExclusive
)

type RelationLockTarget struct {
	Database string
	Schema   string
	Name     string
}

type LockTable struct {
	targets []RelationLockTarget
	mode    RelationLockMode
	nowait  bool
}

var _ vitess.Injectable = (*LockTable)(nil)
var _ sql.ExecSourceRel = (*LockTable)(nil)

func NewLockTable(targets []RelationLockTarget, mode string, nowait bool) *LockTable {
	return &LockTable{
		targets: targets,
		mode:    ParseRelationLockMode(mode),
		nowait:  nowait,
	}
}

func ParseRelationLockMode(mode string) RelationLockMode {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "ACCESS SHARE":
		return RelationLockAccessShare
	case "ROW SHARE":
		return RelationLockRowShare
	case "ROW EXCLUSIVE":
		return RelationLockRowExclusive
	case "SHARE UPDATE EXCLUSIVE":
		return RelationLockShareUpdateExclusive
	case "SHARE":
		return RelationLockShare
	case "SHARE ROW EXCLUSIVE":
		return RelationLockShareRowExclusive
	case "EXCLUSIVE":
		return RelationLockExclusive
	default:
		return RelationLockAccessExclusive
	}
}

func (m RelationLockMode) String() string {
	switch m {
	case RelationLockAccessShare:
		return "ACCESS SHARE"
	case RelationLockRowShare:
		return "ROW SHARE"
	case RelationLockRowExclusive:
		return "ROW EXCLUSIVE"
	case RelationLockShareUpdateExclusive:
		return "SHARE UPDATE EXCLUSIVE"
	case RelationLockShare:
		return "SHARE"
	case RelationLockShareRowExclusive:
		return "SHARE ROW EXCLUSIVE"
	case RelationLockExclusive:
		return "EXCLUSIVE"
	default:
		return "ACCESS EXCLUSIVE"
	}
}

func (l *LockTable) Children() []sql.Node { return nil }

func (l *LockTable) IsReadOnly() bool { return true }

func (l *LockTable) Resolved() bool { return true }

func (l *LockTable) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	for _, target := range l.targets {
		target = normalizeRelationLockTarget(ctx, target)
		if err := checkLockTablePrivilege(ctx, target); err != nil {
			return nil, err
		}
		if _, err := AcquireTransactionRelationLock(ctx, target, l.mode, l.nowait); err != nil {
			return nil, err
		}
	}
	return lockTableRowIter{}, nil
}

func (l *LockTable) Schema(ctx *sql.Context) sql.Schema { return nil }

func (l *LockTable) String() string {
	return fmt.Sprintf("LOCK TABLE %v IN %s MODE", l.targets, l.mode)
}

func (l *LockTable) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 0)
	}
	return l, nil
}

func (l *LockTable) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return l, nil
}

type lockTableRowIter struct{}

func (l lockTableRowIter) Next(ctx *sql.Context) (sql.Row, error) { return nil, io.EOF }

func (l lockTableRowIter) Close(ctx *sql.Context) error { return nil }

func checkLockTablePrivilege(ctx *sql.Context, target RelationLockTarget) error {
	tableName := doltdb.TableName{Schema: target.Schema, Name: target.Name}
	owner, err := tableOwner(ctx, tableName)
	if err != nil {
		return err
	}
	var allowed bool
	auth.LockRead(func() {
		userRole := auth.GetRole(ctx.Client().User)
		publicRole := auth.GetRole("public")
		if userRole.IsValid() && roleCanOperateAsOwner(userRole, owner) {
			allowed = true
			return
		}
		privileges := []auth.Privilege{
			auth.Privilege_SELECT,
			auth.Privilege_INSERT,
			auth.Privilege_UPDATE,
			auth.Privilege_DELETE,
			auth.Privilege_TRUNCATE,
		}
		for _, privilege := range privileges {
			if userRole.IsValid() && auth.HasTablePrivilege(auth.TablePrivilegeKey{Role: userRole.ID(), Table: tableName}, privilege) {
				allowed = true
				return
			}
			if publicRole.IsValid() && auth.HasTablePrivilege(auth.TablePrivilegeKey{Role: publicRole.ID(), Table: tableName}, privilege) {
				allowed = true
				return
			}
		}
	})
	if allowed {
		return nil
	}
	return fmt.Errorf("permission denied for table %s", target.Name)
}

type RelationLockingNode struct {
	child   sql.Node
	targets []RelationLockTarget
	mode    RelationLockMode
}

var _ sql.ExecBuilderNode = (*RelationLockingNode)(nil)

func NewRelationLockingNode(child sql.Node, targets []RelationLockTarget, mode RelationLockMode) *RelationLockingNode {
	return &RelationLockingNode{
		child:   child,
		targets: targets,
		mode:    mode,
	}
}

func (r *RelationLockingNode) Child() sql.Node { return r.child }

func (r *RelationLockingNode) Children() []sql.Node { return []sql.Node{r.child} }

func (r *RelationLockingNode) IsReadOnly() bool { return r.child.IsReadOnly() }

func (r *RelationLockingNode) Resolved() bool { return r.child.Resolved() }

func (r *RelationLockingNode) BuildRowIter(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	releases := make([]func(), 0, len(r.targets))
	for _, target := range r.targets {
		release, err := AcquireStatementRelationLock(ctx, target, r.mode)
		if err != nil {
			releaseRelationLockFuncs(releases)
			return nil, err
		}
		releases = append(releases, release)
	}
	childIter, err := b.Build(ctx, r.child, row)
	if err != nil {
		releaseRelationLockFuncs(releases)
		return nil, err
	}
	if childIter == nil {
		childIter = sql.RowsToRowIter()
	}
	return &relationLockingNodeIter{inner: childIter, releases: releases}, nil
}

func (r *RelationLockingNode) Schema(ctx *sql.Context) sql.Schema { return r.child.Schema(ctx) }

func (r *RelationLockingNode) String() string { return r.child.String() }

func (r *RelationLockingNode) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, len(children), 1)
	}
	return NewRelationLockingNode(children[0], r.targets, r.mode), nil
}

type relationLockingNodeIter struct {
	inner    sql.RowIter
	releases []func()
}

func (r *relationLockingNodeIter) Next(ctx *sql.Context) (sql.Row, error) {
	return r.inner.Next(ctx)
}

func (r *relationLockingNodeIter) Close(ctx *sql.Context) error {
	err := r.inner.Close(ctx)
	releaseRelationLockFuncs(r.releases)
	r.releases = nil
	return err
}

func releaseRelationLockFuncs(releases []func()) {
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			releases[i]()
		}
	}
}

func AcquireTransactionRelationLock(ctx *sql.Context, target RelationLockTarget, mode RelationLockMode, nowait bool) (func(), error) {
	release, err := acquireRelationLock(ctx, target, mode, nowait)
	if err != nil {
		return nil, err
	}
	connectionID := uint32(ctx.Session.ID())
	sessionstate.RegisterCommitAction(connectionID, "relation-locks", func() (bool, error) {
		ReleaseSessionRelationLocks(connectionID)
		return false, nil
	})
	sessionstate.RegisterRollbackAction(connectionID, "relation-locks", func() error {
		ReleaseSessionRelationLocks(connectionID)
		return nil
	})
	return release, nil
}

func AcquireStatementRelationLock(ctx *sql.Context, target RelationLockTarget, mode RelationLockMode) (func(), error) {
	return acquireRelationLock(ctx, target, mode, false)
}

type heldRelationLock struct {
	key       string
	target    RelationLockTarget
	sessionID uint32
	mode      RelationLockMode
}

var relationLocks = struct {
	mu    sync.Mutex
	cond  *sync.Cond
	locks map[string][]*heldRelationLock
}{
	locks: make(map[string][]*heldRelationLock),
}

func init() {
	relationLocks.cond = sync.NewCond(&relationLocks.mu)
}

func acquireRelationLock(ctx *sql.Context, target RelationLockTarget, mode RelationLockMode, nowait bool) (func(), error) {
	sessionID := uint32(ctx.Session.ID())
	target = normalizeRelationLockTarget(ctx, target)
	lock := &heldRelationLock{
		key:       target.key(),
		target:    target,
		sessionID: sessionID,
		mode:      mode,
	}

	relationLocks.mu.Lock()
	defer relationLocks.mu.Unlock()
	for relationLockConflicts(lock.key, sessionID, mode) {
		if nowait {
			return nil, pgerror.Newf(pgcode.LockNotAvailable, "could not obtain lock on relation \"%s\"", target.Name)
		}
		relationLocks.cond.Wait()
	}
	relationLocks.locks[lock.key] = append(relationLocks.locks[lock.key], lock)
	return func() {
		releaseRelationLock(lock)
	}, nil
}

func relationLockConflicts(key string, sessionID uint32, requested RelationLockMode) bool {
	for _, held := range relationLocks.locks[key] {
		if held.sessionID == sessionID {
			continue
		}
		if relationLockModesConflict(held.mode, requested) {
			return true
		}
	}
	return false
}

func relationLockModesConflict(a RelationLockMode, b RelationLockMode) bool {
	if a == RelationLockAccessExclusive || b == RelationLockAccessExclusive {
		return true
	}
	switch a {
	case RelationLockAccessShare:
		return false
	case RelationLockRowShare:
		return b == RelationLockExclusive
	case RelationLockRowExclusive:
		return b == RelationLockShare || b == RelationLockShareRowExclusive || b == RelationLockExclusive
	case RelationLockShareUpdateExclusive:
		return b == RelationLockShareUpdateExclusive || b == RelationLockShare || b == RelationLockShareRowExclusive || b == RelationLockExclusive
	case RelationLockShare:
		return b == RelationLockRowExclusive || b == RelationLockShareUpdateExclusive || b == RelationLockShareRowExclusive || b == RelationLockExclusive
	case RelationLockShareRowExclusive:
		return b == RelationLockRowExclusive || b == RelationLockShareUpdateExclusive || b == RelationLockShare || b == RelationLockShareRowExclusive || b == RelationLockExclusive
	case RelationLockExclusive:
		return b == RelationLockRowShare || b == RelationLockRowExclusive || b == RelationLockShareUpdateExclusive || b == RelationLockShare || b == RelationLockShareRowExclusive || b == RelationLockExclusive
	default:
		return true
	}
}

func releaseRelationLock(lock *heldRelationLock) {
	relationLocks.mu.Lock()
	defer relationLocks.mu.Unlock()
	locks := relationLocks.locks[lock.key]
	for i, held := range locks {
		if held == lock {
			locks = append(locks[:i], locks[i+1:]...)
			break
		}
	}
	if len(locks) == 0 {
		delete(relationLocks.locks, lock.key)
	} else {
		relationLocks.locks[lock.key] = locks
	}
	relationLocks.cond.Broadcast()
}

func ReleaseSessionRelationLocks(sessionID uint32) {
	relationLocks.mu.Lock()
	defer relationLocks.mu.Unlock()
	for key, locks := range relationLocks.locks {
		kept := locks[:0]
		for _, held := range locks {
			if held.sessionID != sessionID {
				kept = append(kept, held)
			}
		}
		if len(kept) == 0 {
			delete(relationLocks.locks, key)
		} else {
			relationLocks.locks[key] = kept
		}
	}
	relationLocks.cond.Broadcast()
}

func normalizeRelationLockTarget(ctx *sql.Context, target RelationLockTarget) RelationLockTarget {
	if target.Database == "" && ctx != nil {
		target.Database = ctx.GetCurrentDatabase()
	}
	if target.Schema == "" {
		target.Schema = "public"
	}
	target.Database = strings.ToLower(target.Database)
	target.Schema = strings.ToLower(target.Schema)
	target.Name = strings.ToLower(target.Name)
	return target
}

func (t RelationLockTarget) key() string {
	return t.Database + "." + t.Schema + "." + t.Name
}
