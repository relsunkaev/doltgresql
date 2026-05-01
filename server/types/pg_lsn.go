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

package types

import (
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

// PgLsn is the PostgreSQL write-ahead log location type. It is represented as a 64-bit byte offset.
var PgLsn = &DoltgresType{
	ID:                  toInternal("pg_lsn"),
	TypLength:           int16(8),
	PassedByVal:         true,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_pg_lsn"),
	InputFunc:           toFuncID("pg_lsn_in", toInternal("cstring")),
	OutputFunc:          toFuncID("pg_lsn_out", toInternal("pg_lsn")),
	ReceiveFunc:         toFuncID("pg_lsn_recv", toInternal("internal")),
	SendFunc:            toFuncID("pg_lsn_send", toInternal("pg_lsn")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Double,
	Storage:             TypeStorage_Plain,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("pg_lsn_cmp", toInternal("pg_lsn"), toInternal("pg_lsn")),
	SerializationFunc:   serializeTypePgLsn,
	DeserializationFunc: deserializeTypePgLsn,
}

// ParsePgLsn converts PostgreSQL's X/Y hexadecimal LSN syntax to its 64-bit representation.
func ParsePgLsn(input string) (uint64, error) {
	hi, lo, ok := strings.Cut(input, "/")
	if !ok || hi == "" || lo == "" || strings.Contains(lo, "/") {
		return 0, ErrInvalidSyntaxForType.New("pg_lsn", input)
	}
	if strings.HasPrefix(hi, "+") || strings.HasPrefix(hi, "-") ||
		strings.HasPrefix(lo, "+") || strings.HasPrefix(lo, "-") {
		return 0, ErrInvalidSyntaxForType.New("pg_lsn", input)
	}
	hiVal, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, ErrInvalidSyntaxForType.New("pg_lsn", input)
	}
	loVal, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, ErrInvalidSyntaxForType.New("pg_lsn", input)
	}
	return (hiVal << 32) | loVal, nil
}

// FormatPgLsn converts an LSN value to PostgreSQL's canonical uppercase hexadecimal X/Y syntax.
func FormatPgLsn(value uint64) string {
	return strings.ToUpper(strconv.FormatUint(value>>32, 16) + "/" + strconv.FormatUint(value&0xffffffff, 16))
}

// serializeTypePgLsn handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypePgLsn(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	retVal := make([]byte, 8)
	binary.BigEndian.PutUint64(retVal, val.(uint64))
	return retVal, nil
}

// deserializeTypePgLsn handles deserialization from the Dolt serialized format to our standard representation used by
// expressions and nodes.
func deserializeTypePgLsn(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return binary.BigEndian.Uint64(data), nil
}
