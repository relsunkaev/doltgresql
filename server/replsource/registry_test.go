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

package replsource

import (
	"os"
	"testing"

	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/stretchr/testify/require"
)

func TestConfigureStorageRejectsCorruptState(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/logical_replication_source.json"
	require.NoError(t, fs.WriteFile(path, []byte("{not-json"), os.ModePerm))

	err := ConfigureStorage(fs, path)
	require.Error(t, err)
	require.Empty(t, ListSlots())
}

func TestConfigureStorageRejectsDirectoryStatePath(t *testing.T) {
	defer ResetForTests()

	fs := filesys.EmptyInMemFS("")
	path := ".doltcfg/logical_replication_source.json"
	require.NoError(t, fs.MkDirs(path))

	err := ConfigureStorage(fs, path)
	require.ErrorContains(t, err, "is a directory")
	require.Empty(t, ListSlots())
}
