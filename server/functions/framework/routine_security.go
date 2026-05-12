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

package framework

import "github.com/dolthub/go-mysql-server/sql"

type routineSecurityContext interface {
	RoutineSecurityDefiner() (owner string, enabled bool)
}

func applyRoutineSecurityDefiner(ctx *sql.Context, routine routineSecurityContext) func() {
	if ctx == nil || ctx.Session == nil || routine == nil {
		return func() {}
	}
	owner, enabled := routine.RoutineSecurityDefiner()
	if !enabled || owner == "" {
		return func() {}
	}
	client := ctx.Client()
	if client.User == owner {
		return func() {}
	}
	definerClient := client
	definerClient.User = owner
	ctx.Session.SetClient(definerClient)
	return func() {
		ctx.Session.SetClient(client)
	}
}
