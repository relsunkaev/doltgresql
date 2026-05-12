// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgEncodingToChar registers the functions to the catalog.
func initPgEncodingToChar() {
	framework.RegisterFunction(pg_encoding_to_char_int)
}

// pg_encoding_to_char_int represents the PostgreSQL system catalog information function.
var pg_encoding_to_char_int = framework.Function1{
	Name:               "pg_encoding_to_char",
	Return:             pgtypes.Name,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		if encoding, ok := pgEncodingNames[val.(int32)]; ok {
			return encoding, nil
		}
		return "", nil
	},
}

var pgEncodingNames = map[int32]string{
	0:  "SQL_ASCII",
	1:  "EUC_JP",
	2:  "EUC_CN",
	3:  "EUC_KR",
	4:  "EUC_TW",
	5:  "EUC_JIS_2004",
	6:  "UTF8",
	7:  "MULE_INTERNAL",
	8:  "LATIN1",
	9:  "LATIN2",
	10: "LATIN3",
	11: "LATIN4",
	12: "LATIN5",
	13: "LATIN6",
	14: "LATIN7",
	15: "LATIN8",
	16: "LATIN9",
	17: "LATIN10",
	18: "WIN1256",
	19: "WIN1258",
	20: "WIN866",
	21: "WIN874",
	22: "KOI8R",
	23: "WIN1251",
	24: "WIN1252",
	25: "ISO_8859_5",
	26: "ISO_8859_6",
	27: "ISO_8859_7",
	28: "ISO_8859_8",
	29: "WIN1250",
	30: "WIN1253",
	31: "WIN1254",
	32: "WIN1255",
	33: "WIN1257",
	34: "KOI8U",
	35: "SJIS",
	36: "BIG5",
	37: "GBK",
	38: "UHC",
	39: "GB18030",
	40: "JOHAB",
	41: "SHIFT_JIS_2004",
}
