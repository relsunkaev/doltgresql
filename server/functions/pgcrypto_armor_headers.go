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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

type pgcryptoArmorHeader struct {
	key   string
	value string
}

var pgcrypto_pgp_armor_headers = framework.Function1{
	Name:       "pgp_armor_headers",
	Return:     pgtypes.RowTypeWithReturnType(pgtypes.Record),
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	SRF:        true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, armored any) (any, error) {
		headers, err := pgcryptoArmorHeaders(armored.(string))
		if err != nil {
			return nil, err
		}
		return pgcryptoArmorHeadersRecordRowIter(headers), nil
	},
}

func pgcryptoArmorHeaders(armored string) ([]pgcryptoArmorHeader, error) {
	lines := pgcryptoArmorLines(armored)
	begin := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "-----BEGIN") {
			begin = i
			break
		}
	}
	if begin < 0 || !pgcryptoArmorHeaderLine(lines[begin], "BEGIN") {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}

	end := -1
	for i := begin + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "-----END") {
			end = i
			break
		}
	}
	if end < 0 || !pgcryptoArmorHeaderLine(lines[end], "END") {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}

	headers := make([]pgcryptoArmorHeader, 0)
	for i := begin + 1; i < end; i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			return headers, nil
		}
		split := strings.Index(line, ": ")
		if split <= 0 {
			return nil, errors.Errorf("Corrupt ascii-armor")
		}
		headers = append(headers, pgcryptoArmorHeader{
			key:   line[:split],
			value: line[split+2:],
		})
	}
	return nil, errors.Errorf("Corrupt ascii-armor")
}

func pgcryptoArmorHeadersRecordRowIter(headers []pgcryptoArmorHeader) *pgtypes.SetReturningFunctionRowIter {
	var idx int
	return pgtypes.NewSetReturningFunctionRowIter(func(ctx *sql.Context) (sql.Row, error) {
		if idx >= len(headers) {
			return nil, io.EOF
		}
		header := headers[idx]
		idx++
		return sql.Row{[]pgtypes.RecordValue{
			{Type: pgtypes.Text, Value: header.key},
			{Type: pgtypes.Text, Value: header.value},
		}}, nil
	})
}
