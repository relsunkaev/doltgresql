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

// BoxValue is the in-memory representation of PostgreSQL's geometric box type.
type BoxValue struct {
	High PointValue
	Low  PointValue
}

// Box is PostgreSQL's geometric box type.
var Box = &DoltgresType{
	ID:                  toInternal("box"),
	TypLength:           int16(32),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_GeometricTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_box"),
	InputFunc:           toFuncID("box_in", toInternal("cstring")),
	OutputFunc:          toFuncID("box_out", toInternal("box")),
	ReceiveFunc:         toFuncID("box_recv", toInternal("internal")),
	SendFunc:            toFuncID("box_send", toInternal("box")),
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
	SerializationFunc:   serializeTypeBox,
	DeserializationFunc: deserializeTypeBox,
}

// ParseBox converts PostgreSQL box text input into a BoxValue.
func ParseBox(input string) (BoxValue, error) {
	replacer := strings.NewReplacer("(", " ", ")", " ", "[", " ", "]", " ", ",", " ")
	parts := strings.Fields(replacer.Replace(input))
	if len(parts) != 4 {
		return BoxValue{}, ErrInvalidSyntaxForType.New("box", input)
	}
	x1, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return BoxValue{}, ErrInvalidSyntaxForType.New("box", input)
	}
	y1, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return BoxValue{}, ErrInvalidSyntaxForType.New("box", input)
	}
	x2, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return BoxValue{}, ErrInvalidSyntaxForType.New("box", input)
	}
	y2, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return BoxValue{}, ErrInvalidSyntaxForType.New("box", input)
	}
	return NewBoxValue(PointValue{X: x1, Y: y1}, PointValue{X: x2, Y: y2}), nil
}

func NewBoxValue(p1, p2 PointValue) BoxValue {
	return BoxValue{
		High: PointValue{X: math.Max(p1.X, p2.X), Y: math.Max(p1.Y, p2.Y)},
		Low:  PointValue{X: math.Min(p1.X, p2.X), Y: math.Min(p1.Y, p2.Y)},
	}
}

// FormatBox converts a BoxValue to PostgreSQL's canonical text form.
func FormatBox(value BoxValue) string {
	return FormatPoint(value.High) + "," + FormatPoint(value.Low)
}

func serializeTypeBox(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	box := val.(BoxValue)
	data := make([]byte, 32)
	binary.BigEndian.PutUint64(data[:8], math.Float64bits(box.High.X))
	binary.BigEndian.PutUint64(data[8:16], math.Float64bits(box.High.Y))
	binary.BigEndian.PutUint64(data[16:24], math.Float64bits(box.Low.X))
	binary.BigEndian.PutUint64(data[24:], math.Float64bits(box.Low.Y))
	return data, nil
}

func deserializeTypeBox(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 32 {
		return nil, errors.Errorf("invalid box data length: %d", len(data))
	}
	return BoxValue{
		High: PointValue{
			X: math.Float64frombits(binary.BigEndian.Uint64(data[:8])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(data[8:16])),
		},
		Low: PointValue{
			X: math.Float64frombits(binary.BigEndian.Uint64(data[16:24])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(data[24:])),
		},
	}, nil
}
