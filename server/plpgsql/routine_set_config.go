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

package plpgsql

import "github.com/dolthub/go-mysql-server/sql"

func applyRoutineSetConfig(ctx *sql.Context, setConfig map[string]string) (func(), error) {
	if ctx == nil || len(setConfig) == 0 {
		return func() {}, nil
	}
	previousValues := make(map[string]any, len(setConfig))
	for name, value := range setConfig {
		previousValue, err := ctx.GetSessionVariable(ctx, name)
		if err != nil {
			restoreRoutineSetConfig(ctx, previousValues)
			return nil, err
		}
		previousValues[name] = previousValue
		if err = ctx.SetSessionVariable(ctx, name, value); err != nil {
			restoreRoutineSetConfig(ctx, previousValues)
			return nil, err
		}
	}
	return func() {
		restoreRoutineSetConfig(ctx, previousValues)
	}, nil
}

func restoreRoutineSetConfig(ctx *sql.Context, values map[string]any) {
	for name, value := range values {
		_ = ctx.SetSessionVariable(ctx, name, value)
	}
}
