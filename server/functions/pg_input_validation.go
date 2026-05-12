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
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initPgInputValidation() {
	framework.RegisterFunction(pg_input_is_valid_text_text)
	framework.RegisterFunction(pg_input_error_info_sql_error_code_text_text)
}

var pg_input_is_valid_text_text = framework.Function2{
	Name:       "pg_input_is_valid",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, input any, typ any) (any, error) {
		return pgInputErrorCode(input.(string), typ.(string)) == "", nil
	},
}

var pg_input_error_info_sql_error_code_text_text = framework.Function2{
	Name:       "pg_input_error_info_sql_error_code",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, input any, typ any) (any, error) {
		code := pgInputErrorCode(input.(string), typ.(string))
		if code == "" {
			return nil, nil
		}
		return code, nil
	},
}

func pgInputErrorCode(input string, typ string) string {
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "integer", "int", "int4", "pg_catalog.integer", "pg_catalog.int4":
		if _, err := strconv.ParseInt(strings.TrimSpace(input), 10, 32); err != nil {
			if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
				return "22003"
			}
			return "22P02"
		}
		return ""
	default:
		return "42804"
	}
}
