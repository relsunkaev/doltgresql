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

package accessmethod

import (
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/require"
)

func TestConfigureStoragePersistsAccessMethods(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/access_methods.json"
	require.NoError(t, ConfigureStorage(fs, path))
	require.NoError(t, Register("persisted_am", "heap_tableam_handler", "t"))

	require.Equal(t, []Entry{{
		Name:    "persisted_am",
		Handler: "heap_tableam_handler",
		Type:    "t",
	}}, Snapshot())

	require.NoError(t, ConfigureStorage(fs, path))
	require.Equal(t, []Entry{{
		Name:    "persisted_am",
		Handler: "heap_tableam_handler",
		Type:    "t",
	}}, Snapshot())

	dropped, err := Drop("persisted_am")
	require.NoError(t, err)
	require.True(t, dropped)

	exists, isDir := fs.Exists(path)
	require.False(t, exists)
	require.False(t, isDir)
}
