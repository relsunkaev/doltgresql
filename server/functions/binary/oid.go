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

package binary

import (
	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/core/id"
)

func oidValue(val any) (uint32, error) {
	switch val := val.(type) {
	case id.Id:
		return id.Cache().ToOID(val), nil
	case uint32:
		return val, nil
	case int32:
		return uint32(val), nil
	case uint64:
		return uint32(val), nil
	case int64:
		return uint32(val), nil
	case uint:
		return uint32(val), nil
	case int:
		return uint32(val), nil
	default:
		return 0, errors.Errorf("expected oid value, got %T", val)
	}
}
