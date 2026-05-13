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

// CircleValue is the in-memory representation of PostgreSQL's geometric circle type.
type CircleValue struct {
	Center PointValue
	Radius float64
}

// Circle is PostgreSQL's geometric circle type.
var Circle = &DoltgresType{
	ID:                  toInternal("circle"),
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
	Array:               toInternal("_circle"),
	InputFunc:           toFuncID("circle_in", toInternal("cstring")),
	OutputFunc:          toFuncID("circle_out", toInternal("circle")),
	ReceiveFunc:         toFuncID("circle_recv", toInternal("internal")),
	SendFunc:            toFuncID("circle_send", toInternal("circle")),
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
	SerializationFunc:   serializeTypeCircle,
	DeserializationFunc: deserializeTypeCircle,
}

// ParseCircle converts PostgreSQL circle text input into a CircleValue.
func ParseCircle(input string) (CircleValue, error) {
	replacer := strings.NewReplacer("<", " ", ">", " ", "(", " ", ")", " ", ",", " ")
	parts := strings.Fields(replacer.Replace(input))
	if len(parts) != 3 {
		return CircleValue{}, ErrInvalidSyntaxForType.New("circle", input)
	}
	x, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return CircleValue{}, ErrInvalidSyntaxForType.New("circle", input)
	}
	y, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return CircleValue{}, ErrInvalidSyntaxForType.New("circle", input)
	}
	radius, err := strconv.ParseFloat(parts[2], 64)
	if err != nil || radius < 0 {
		return CircleValue{}, ErrInvalidSyntaxForType.New("circle", input)
	}
	return CircleValue{Center: PointValue{X: x, Y: y}, Radius: radius}, nil
}

// FormatCircle converts a CircleValue to PostgreSQL's canonical text form.
func FormatCircle(value CircleValue) string {
	return "<" + FormatPoint(value.Center) + "," + strconv.FormatFloat(value.Radius, 'g', -1, 64) + ">"
}

func serializeTypeCircle(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	circle := val.(CircleValue)
	data := make([]byte, 24)
	binary.BigEndian.PutUint64(data[:8], math.Float64bits(circle.Center.X))
	binary.BigEndian.PutUint64(data[8:16], math.Float64bits(circle.Center.Y))
	binary.BigEndian.PutUint64(data[16:], math.Float64bits(circle.Radius))
	return data, nil
}

func deserializeTypeCircle(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 24 {
		return nil, errors.Errorf("invalid circle data length: %d", len(data))
	}
	return CircleValue{
		Center: PointValue{
			X: math.Float64frombits(binary.BigEndian.Uint64(data[:8])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(data[8:16])),
		},
		Radius: math.Float64frombits(binary.BigEndian.Uint64(data[16:])),
	}, nil
}
