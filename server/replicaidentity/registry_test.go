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

package replicaidentity

import (
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/require"
)

func TestConfigureStoragePersistsReplicaIdentity(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/replica_identity.json"
	require.NoError(t, ConfigureStorage(fs, path))
	require.NoError(t, Set("postgres", "public", "items", IdentityUsingIndex, "items_label_idx"))

	setting := Get("postgres", "public", "items")
	require.Equal(t, IdentityUsingIndex, setting.Identity)
	require.Equal(t, "items_label_idx", setting.IndexName)

	require.NoError(t, ConfigureStorage(fs, path))
	setting = Get("postgres", "public", "items")
	require.Equal(t, IdentityUsingIndex, setting.Identity)
	require.Equal(t, "items_label_idx", setting.IndexName)

	require.NoError(t, Set("postgres", "public", "items", IdentityDefault, ""))
	setting = Get("postgres", "public", "items")
	require.Equal(t, IdentityDefault, setting.Identity)
	require.Empty(t, setting.IndexName)

	exists, isDir := fs.Exists(path)
	require.False(t, exists)
	require.False(t, isDir)
}

func TestDropTableRemovesPersistedReplicaIdentity(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/replica_identity.json"
	require.NoError(t, ConfigureStorage(fs, path))
	require.NoError(t, Set("postgres", "public", "items", IdentityFull, ""))
	require.NoError(t, Set("postgres", "public", "other", IdentityNothing, ""))

	require.NoError(t, DropTable("postgres", "public", "items"))

	setting := Get("postgres", "public", "items")
	require.Equal(t, IdentityDefault, setting.Identity)
	require.Empty(t, setting.IndexName)

	setting = Get("postgres", "public", "other")
	require.Equal(t, IdentityNothing, setting.Identity)

	require.NoError(t, ConfigureStorage(fs, path))
	setting = Get("postgres", "public", "items")
	require.Equal(t, IdentityDefault, setting.Identity)
	setting = Get("postgres", "public", "other")
	require.Equal(t, IdentityNothing, setting.Identity)
}
