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

package auth

import (
	"sync"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestPrivilegeSetLayerConcurrentRoleWrites(t *testing.T) {
	Init(nil, nil)
	t.Cleanup(func() {
		Init(nil, nil)
	})

	ctx := sql.NewEmptyContext().WithClient(sql.Client{User: "postgres"})
	start := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			LockWrite(func() {
				role := GetRole("postgres")
				role.CanCreateDB = i%2 == 0
				SetRole(role)
			})
		}
	}()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				privileges := NewPrivilegeSetLayer(ctx)
				_ = privileges.HasPrivileges()
				_ = privileges.Count()
				_ = privileges.ToSlice()
				dbPrivileges := privileges.Database("postgres")
				_ = dbPrivileges.HasPrivileges()
				_ = dbPrivileges.Count()
				_ = dbPrivileges.ToSlice()
			}
		}()
	}

	close(start)
	wg.Wait()
}
