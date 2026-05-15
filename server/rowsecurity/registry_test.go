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

package rowsecurity

import (
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/require"
)

func TestConfigureStoragePersistsState(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/row_security.json"
	require.NoError(t, ConfigureStorage(fs, path))

	SetTableMode(1, "postgres", "public", "docs", boolPtr(true), nil)
	require.True(t, AddPolicy(1, "postgres", "public", "docs", Policy{
		Name:        "docs_owner",
		Command:     "select",
		UsingColumn: "owner_name",
		Roles:       []string{"reader"},
	}))

	require.NoError(t, ConfigureStorage(fs, path))
	state, ok := Get("postgres", "public", "docs")
	require.True(t, ok)
	require.True(t, state.Enabled)
	require.False(t, state.Forced)
	require.Len(t, state.Policies, 1)
	require.Equal(t, "docs_owner", state.Policies[0].Name)
	require.Equal(t, "owner_name", state.Policies[0].UsingColumn)
}

func TestRollbackPersistsRestoredState(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/row_security.json"
	require.NoError(t, ConfigureStorage(fs, path))

	SetTableMode(1, "postgres", "public", "docs", boolPtr(true), nil)
	BeginTransaction(2)
	SetTableMode(2, "postgres", "public", "docs", boolPtr(false), nil)
	RollbackTransaction(2)

	require.NoError(t, ConfigureStorage(fs, path))
	state, ok := Get("postgres", "public", "docs")
	require.True(t, ok)
	require.True(t, state.Enabled)
}

func boolPtr(v bool) *bool {
	return &v
}
