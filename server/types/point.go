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

// PointValue is the in-memory representation of PostgreSQL's geometric point type.
type PointValue struct {
	X float64
	Y float64
}

// Point is PostgreSQL's geometric point type.
var Point = &DoltgresType{
	ID:                  toInternal("point"),
	TypLength:           int16(16),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_GeometricTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_point"),
	InputFunc:           toFuncID("point_in", toInternal("cstring")),
	OutputFunc:          toFuncID("point_out", toInternal("point")),
	ReceiveFunc:         toFuncID("point_recv", toInternal("internal")),
	SendFunc:            toFuncID("point_send", toInternal("point")),
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
	SerializationFunc:   serializeTypePoint,
	DeserializationFunc: deserializeTypePoint,
}

// ParsePoint converts PostgreSQL point text input into a PointValue.
func ParsePoint(input string) (PointValue, error) {
	trimmed := strings.TrimSpace(input)
	trimmed = strings.TrimPrefix(trimmed, "(")
	trimmed = strings.TrimSuffix(trimmed, ")")
	parts := strings.Split(trimmed, ",")
	if len(parts) != 2 {
		return PointValue{}, ErrInvalidSyntaxForType.New("point", input)
	}
	x, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return PointValue{}, ErrInvalidSyntaxForType.New("point", input)
	}
	y, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return PointValue{}, ErrInvalidSyntaxForType.New("point", input)
	}
	return PointValue{X: x, Y: y}, nil
}

// FormatPoint converts a PointValue to PostgreSQL's canonical text form.
func FormatPoint(value PointValue) string {
	return "(" + strconv.FormatFloat(value.X, 'g', -1, 64) + "," + strconv.FormatFloat(value.Y, 'g', -1, 64) + ")"
}

func serializeTypePoint(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	point := val.(PointValue)
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[:8], math.Float64bits(point.X))
	binary.BigEndian.PutUint64(data[8:], math.Float64bits(point.Y))
	return data, nil
}

func deserializeTypePoint(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 16 {
		return nil, errors.Errorf("invalid point data length: %d", len(data))
	}
	return PointValue{
		X: math.Float64frombits(binary.BigEndian.Uint64(data[:8])),
		Y: math.Float64frombits(binary.BigEndian.Uint64(data[8:])),
	}, nil
}
