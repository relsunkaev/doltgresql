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

package functions

import (
	"encoding/hex"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initEncodeDecode registers the functions to the catalog.
func initEncodeDecode() {
	framework.RegisterFunction(decode_text_text)
	framework.RegisterFunction(encode_bytea_text)
}

var decode_text_text = framework.Function2{
	Name:       "decode",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, value any, format any) (any, error) {
		formatText := strings.ToLower(format.(string))
		if formatText != "hex" {
			return nil, errors.Errorf("decode format %q is not supported", formatText)
		}
		return hex.DecodeString(value.(string))
	},
}

var encode_bytea_text = framework.Function2{
	Name:       "encode",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, value any, format any) (any, error) {
		formatText := strings.ToLower(format.(string))
		if formatText != "hex" {
			return nil, errors.Errorf("encode format %q is not supported", formatText)
		}
		data, ok, err := sql.Unwrap[[]byte](ctx, value)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("expected bytea, got %T", value)
		}
		return hex.EncodeToString(data), nil
	},
}
