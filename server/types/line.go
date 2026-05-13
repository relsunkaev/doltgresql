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
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
)

// LineValue is the in-memory representation of PostgreSQL's geometric line type.
type LineValue struct {
	A float64
	B float64
	C float64
}

// Line is PostgreSQL's geometric line type.
var Line = &DoltgresType{
	ID:                  toInternal("line"),
	TypLength:           int16(24),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_GeometricTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_line"),
	InputFunc:           toFuncID("line_in", toInternal("cstring")),
	OutputFunc:          toFuncID("line_out", toInternal("line")),
	ReceiveFunc:         toFuncID("line_recv", toInternal("internal")),
	SendFunc:            toFuncID("line_send", toInternal("line")),
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
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeLine,
	DeserializationFunc: deserializeTypeLine,
}

// ParseLine converts PostgreSQL line text input into a LineValue.
func ParseLine(input string) (LineValue, error) {
	replacer := strings.NewReplacer("{", " ", "}", " ", "[", " ", "]", " ", ",", " ")
	parts := strings.Fields(replacer.Replace(input))
	if len(parts) != 3 {
		return LineValue{}, ErrInvalidSyntaxForType.New("line", input)
	}
	a, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return LineValue{}, ErrInvalidSyntaxForType.New("line", input)
	}
	b, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return LineValue{}, ErrInvalidSyntaxForType.New("line", input)
	}
	c, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return LineValue{}, ErrInvalidSyntaxForType.New("line", input)
	}
	return LineValue{A: a, B: b, C: c}, nil
}

// FormatLine converts a LineValue to PostgreSQL's canonical text form.
func FormatLine(value LineValue) string {
	return "{" +
		strconv.FormatFloat(value.A, 'g', -1, 64) + "," +
		strconv.FormatFloat(value.B, 'g', -1, 64) + "," +
		strconv.FormatFloat(value.C, 'g', -1, 64) + "}"
}

func serializeTypeLine(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	line := val.(LineValue)
	data := make([]byte, 24)
	binary.BigEndian.PutUint64(data[:8], math.Float64bits(line.A))
	binary.BigEndian.PutUint64(data[8:16], math.Float64bits(line.B))
	binary.BigEndian.PutUint64(data[16:], math.Float64bits(line.C))
	return data, nil
}

func deserializeTypeLine(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 24 {
		return nil, errors.Errorf("invalid line data length: %d", len(data))
	}
	return LineValue{
		A: math.Float64frombits(binary.BigEndian.Uint64(data[:8])),
		B: math.Float64frombits(binary.BigEndian.Uint64(data[8:16])),
		C: math.Float64frombits(binary.BigEndian.Uint64(data[16:])),
	}, nil
}
