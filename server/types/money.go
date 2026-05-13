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

type MoneyValue = int64

const maxMoneyCents = int64(^uint64(0) >> 1)

// Money is PostgreSQL's currency type. The value is stored as cents, matching PostgreSQL's binary representation.
var Money = &DoltgresType{
	ID:                  toInternal("money"),
	TypLength:           int16(8),
	PassedByVal:         true,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_NumericTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_money"),
	InputFunc:           toFuncID("cash_in", toInternal("cstring")),
	OutputFunc:          toFuncID("cash_out", toInternal("money")),
	ReceiveFunc:         toFuncID("cash_recv", toInternal("internal")),
	SendFunc:            toFuncID("cash_send", toInternal("money")),
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
	CompareFunc:         toFuncID("cash_cmp", toInternal("money"), toInternal("money")),
	SerializationFunc:   serializeTypeMoney,
	DeserializationFunc: deserializeTypeMoney,
}

func ParseMoney(input string) (MoneyValue, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return 0, ErrInvalidSyntaxForType.New("money", input)
	}

	negative := false
	if strings.HasPrefix(trimmed, "-") || strings.HasPrefix(trimmed, "+") {
		negative = trimmed[0] == '-'
		trimmed = strings.TrimSpace(trimmed[1:])
	}
	if strings.HasPrefix(trimmed, "$") {
		trimmed = strings.TrimSpace(trimmed[1:])
	}
	if strings.HasPrefix(trimmed, "-") || strings.HasPrefix(trimmed, "+") {
		negative = trimmed[0] == '-'
		trimmed = strings.TrimSpace(trimmed[1:])
	}

	trimmed = strings.ReplaceAll(trimmed, ",", "")
	dollarsPart, centsPart, found := strings.Cut(trimmed, ".")
	if strings.Contains(centsPart, ".") || dollarsPart == "" {
		return 0, ErrInvalidSyntaxForType.New("money", input)
	}
	if !found {
		centsPart = "00"
	} else if centsPart == "" {
		centsPart = "00"
	} else if len(centsPart) == 1 {
		centsPart += "0"
	} else if len(centsPart) > 2 {
		return 0, ErrInvalidSyntaxForType.New("money", input)
	}

	dollars, err := strconv.ParseInt(dollarsPart, 10, 64)
	if err != nil || dollars < 0 {
		return 0, ErrInvalidSyntaxForType.New("money", input)
	}
	cents, err := strconv.ParseInt(centsPart, 10, 64)
	if err != nil || cents < 0 || cents > 99 {
		return 0, ErrInvalidSyntaxForType.New("money", input)
	}
	if dollars > (maxMoneyCents-cents)/100 {
		return 0, ErrInvalidSyntaxForType.New("money", input)
	}

	total := dollars*100 + cents
	if negative {
		total = -total
	}
	return MoneyValue(total), nil
}

func FormatMoney(value MoneyValue) string {
	sign := ""
	abs := uint64(value)
	if value < 0 {
		sign = "-"
		abs = uint64(-(value + 1)) + 1
	}
	dollars := abs / 100
	cents := abs % 100
	formatted := sign + "$" + strconv.FormatUint(dollars, 10) + "."
	if cents < 10 {
		formatted += "0"
	}
	return formatted + strconv.FormatUint(cents, 10)
}

func serializeTypeMoney(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	retVal := make([]byte, 8)
	binary.BigEndian.PutUint64(retVal, uint64(val.(MoneyValue))+(1<<63))
	return retVal, nil
}

func deserializeTypeMoney(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return MoneyValue(int64(binary.BigEndian.Uint64(data) - (1 << 63))), nil
}
