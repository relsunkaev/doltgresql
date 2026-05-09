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

package _go

import (
	"testing"

	gms "github.com/dolthub/go-mysql-server/sql"
	_ "unsafe"
)

// The local Dolt pause point is deliberately unexported; linkname keeps its
// installer in this _test binary without reopening a production API.
//
//go:linkname creationTestHookBeforeBuildSecondaryIndex github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation.testHookBeforeBuildSecondaryIndex
var creationTestHookBeforeBuildSecondaryIndex func(ctx *gms.Context)

func setCreationTestHookBeforeBuildSecondaryIndex(t *testing.T, hook func(ctx *gms.Context)) {
	t.Helper()
	previousHook := creationTestHookBeforeBuildSecondaryIndex
	creationTestHookBeforeBuildSecondaryIndex = hook
	t.Cleanup(func() {
		creationTestHookBeforeBuildSecondaryIndex = previousHook
	})
}
