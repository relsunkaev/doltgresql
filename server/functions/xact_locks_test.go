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
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestReleaseSessionXactLocksUsesProvidedLockSubsystem(t *testing.T) {
	const sessionID = uint32(4242)
	const lockName = "8:cleanup"

	session := sql.NewBaseSession()
	session.SetConnectionId(sessionID)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ls := sql.NewLockSubsystem()

	require.NoError(t, ls.Lock(ctx, lockName, 0))
	recordXactLock(sessionID, lockName)

	require.NoError(t, ReleaseSessionXactLocksWithSubsystem(ctx, ls))

	state, _ := ls.GetLockState(lockName)
	require.Equal(t, sql.LockFree, state)
	require.False(t, HasSessionXactLocks(sessionID))
}

func TestReleaseSessionXactLocksToleratesMissingRunningServer(t *testing.T) {
	const sessionID = uint32(4343)

	session := sql.NewBaseSession()
	session.SetConnectionId(sessionID)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	recordXactLock(sessionID, "8:stale")

	require.NoError(t, ReleaseSessionXactLocks(ctx))
	require.False(t, HasSessionXactLocks(sessionID))
}
