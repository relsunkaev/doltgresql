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

package node

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/jsonbgin"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func jsonbGinExtractEncodedTokensFromSQLValue(ctx *sql.Context, value any, opClass string) ([]string, error) {
	unwrapped, err := sql.UnwrapAny(ctx, value)
	if err != nil {
		return nil, err
	}
	switch value := unwrapped.(type) {
	case pgtypes.JsonDocument:
		return jsonbgin.ExtractValueEncoded(value.Value, opClass)
	case pgtypes.JsonValue:
		return jsonbgin.ExtractValueEncoded(value, opClass)
	case nil:
		return jsonbgin.ExtractValueEncoded(pgtypes.JsonValueNull(0), opClass)
	default:
		doc, err := pgtypes.JsonDocumentFromSQLValue(ctx, pgtypes.JsonB, unwrapped)
		if err != nil {
			return nil, err
		}
		return jsonbgin.ExtractValueEncoded(doc.Value, opClass)
	}
}
