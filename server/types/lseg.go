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

// LsegValue is the in-memory representation of PostgreSQL's geometric lseg type.
type LsegValue struct {
	P1 PointValue
	P2 PointValue
}

// Lseg is PostgreSQL's geometric line segment type.
var Lseg = &DoltgresType{
	ID:                  toInternal("lseg"),
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
	Array:               toInternal("_lseg"),
	InputFunc:           toFuncID("lseg_in", toInternal("cstring")),
	OutputFunc:          toFuncID("lseg_out", toInternal("lseg")),
	ReceiveFunc:         toFuncID("lseg_recv", toInternal("internal")),
	SendFunc:            toFuncID("lseg_send", toInternal("lseg")),
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
	SerializationFunc:   serializeTypeLseg,
	DeserializationFunc: deserializeTypeLseg,
}

// ParseLseg converts PostgreSQL lseg text input into an LsegValue.
func ParseLseg(input string) (LsegValue, error) {
	replacer := strings.NewReplacer("(", " ", ")", " ", "[", " ", "]", " ", ",", " ")
	parts := strings.Fields(replacer.Replace(input))
	if len(parts) != 4 {
		return LsegValue{}, ErrInvalidSyntaxForType.New("lseg", input)
	}
	p1, p2, err := parseFourPointParts(parts)
	if err != nil {
		return LsegValue{}, ErrInvalidSyntaxForType.New("lseg", input)
	}
	return LsegValue{P1: p1, P2: p2}, nil
}

// FormatLseg converts an LsegValue to PostgreSQL's canonical text form.
func FormatLseg(value LsegValue) string {
	return "[" + FormatPoint(value.P1) + "," + FormatPoint(value.P2) + "]"
}

func parseFourPointParts(parts []string) (PointValue, PointValue, error) {
	x1, err := parseFloatPart(parts[0])
	if err != nil {
		return PointValue{}, PointValue{}, err
	}
	y1, err := parseFloatPart(parts[1])
	if err != nil {
		return PointValue{}, PointValue{}, err
	}
	x2, err := parseFloatPart(parts[2])
	if err != nil {
		return PointValue{}, PointValue{}, err
	}
	y2, err := parseFloatPart(parts[3])
	if err != nil {
		return PointValue{}, PointValue{}, err
	}
	return PointValue{X: x1, Y: y1}, PointValue{X: x2, Y: y2}, nil
}

func parseFloatPart(part string) (float64, error) {
	return strconv.ParseFloat(part, 64)
}

func serializeTypeLseg(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	lseg := val.(LsegValue)
	data := make([]byte, 32)
	binary.BigEndian.PutUint64(data[:8], math.Float64bits(lseg.P1.X))
	binary.BigEndian.PutUint64(data[8:16], math.Float64bits(lseg.P1.Y))
	binary.BigEndian.PutUint64(data[16:24], math.Float64bits(lseg.P2.X))
	binary.BigEndian.PutUint64(data[24:], math.Float64bits(lseg.P2.Y))
	return data, nil
}

func deserializeTypeLseg(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 32 {
		return nil, errors.Errorf("invalid lseg data length: %d", len(data))
	}
	return LsegValue{
		P1: PointValue{
			X: math.Float64frombits(binary.BigEndian.Uint64(data[:8])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(data[8:16])),
		},
		P2: PointValue{
			X: math.Float64frombits(binary.BigEndian.Uint64(data[16:24])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(data[24:])),
		},
	}, nil
}
