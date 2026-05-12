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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initStringToTable() {
	framework.RegisterFunction(string_to_table_text_text)
	framework.RegisterFunction(string_to_table_text_text_text)
}

var string_to_table_text_text = framework.Function2{
	Name:       "string_to_table",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     false,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return stringToTableIter(val1, val2, nil), nil
	},
}

var string_to_table_text_text_text = framework.Function3{
	Name:       "string_to_table",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Text),
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     false,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, val1, val2, val3 any) (any, error) {
		return stringToTableIter(val1, val2, val3), nil
	},
}

func stringToTableIter(input, delimiter, nullString any) *pgtypes.SetReturningFunctionRowIter {
	var rows []any
	if input != nil {
		rows = stringToTableRows(input.(string), delimiter, nullString)
	}
	idx := 0
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if idx >= len(rows) {
			return nil, io.EOF
		}
		row := sql.Row{rows[idx]}
		idx++
		return row, nil
	})
}

func stringToTableRows(input string, delimiter, nullString any) []any {
	var parts []string
	switch delim := delimiter.(type) {
	case nil:
		parts = make([]string, 0, len(input))
		for _, r := range input {
			parts = append(parts, string(r))
		}
	case string:
		if delim == "" {
			parts = []string{input}
		} else {
			parts = strings.Split(input, delim)
		}
	}

	rows := make([]any, len(parts))
	nullText, replaceWithNull := nullString.(string)
	for i, part := range parts {
		if replaceWithNull && part == nullText {
			rows[i] = nil
		} else {
			rows[i] = part
		}
	}
	return rows
}
