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

package ast

import (
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

func nodeTableRelOptions(params tree.StorageParams) ([]string, error) {
	if len(params) == 0 {
		return nil, nil
	}
	relOptions := make([]string, 0, len(params))
	seen := make(map[string]struct{}, len(params))
	for _, param := range params {
		key := strings.ToLower(strings.TrimSpace(string(param.Key)))
		if key == "" {
			return nil, errors.Errorf("storage parameter name cannot be empty")
		}
		if _, ok := seen[key]; ok {
			return nil, errors.Errorf("storage parameter %s is specified more than once", key)
		}
		seen[key] = struct{}{}
		value, err := nodeStorageParamValue(param.Value)
		if err != nil {
			return nil, errors.Errorf("storage parameter %s requires a value", key)
		}
		relOptions = append(relOptions, key+"="+value)
	}
	return relOptions, nil
}

func nodeStorageParamValue(expr tree.Expr) (string, error) {
	if expr == nil {
		return "", errors.Errorf("missing value")
	}
	return strings.Trim(tree.AsString(expr), "'"), nil
}
