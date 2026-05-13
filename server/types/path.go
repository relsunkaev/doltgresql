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

// PathValue is the in-memory representation of PostgreSQL's geometric path type.
type PathValue struct {
	Points []PointValue
	Closed bool
}

// Path is PostgreSQL's geometric path type.
var Path = &DoltgresType{
	ID:                  toInternal("path"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_GeometricTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_path"),
	InputFunc:           toFuncID("path_in", toInternal("cstring")),
	OutputFunc:          toFuncID("path_out", toInternal("path")),
	ReceiveFunc:         toFuncID("path_recv", toInternal("internal")),
	SendFunc:            toFuncID("path_send", toInternal("path")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Double,
	Storage:             TypeStorage_Extended,
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
	SerializationFunc:   serializeTypePath,
	DeserializationFunc: deserializeTypePath,
}

// ParsePath converts PostgreSQL path text input into a PathValue.
func ParsePath(input string) (PathValue, error) {
	trimmed := strings.TrimSpace(input)
	if len(trimmed) < 2 {
		return PathValue{}, ErrInvalidSyntaxForType.New("path", input)
	}
	closed := trimmed[0] == '('
	if !closed && trimmed[0] != '[' {
		return PathValue{}, ErrInvalidSyntaxForType.New("path", input)
	}

	replacer := strings.NewReplacer("(", " ", ")", " ", "[", " ", "]", " ", ",", " ")
	parts := strings.Fields(replacer.Replace(trimmed))
	if len(parts) == 0 || len(parts)%2 != 0 {
		return PathValue{}, ErrInvalidSyntaxForType.New("path", input)
	}
	points := make([]PointValue, 0, len(parts)/2)
	for i := 0; i < len(parts); i += 2 {
		x, err := strconv.ParseFloat(parts[i], 64)
		if err != nil {
			return PathValue{}, ErrInvalidSyntaxForType.New("path", input)
		}
		y, err := strconv.ParseFloat(parts[i+1], 64)
		if err != nil {
			return PathValue{}, ErrInvalidSyntaxForType.New("path", input)
		}
		points = append(points, PointValue{X: x, Y: y})
	}
	return PathValue{Points: points, Closed: closed}, nil
}

// FormatPath converts a PathValue to PostgreSQL's canonical text form.
func FormatPath(value PathValue) string {
	start, end := "[", "]"
	if value.Closed {
		start, end = "(", ")"
	}
	var builder strings.Builder
	builder.WriteString(start)
	for i, point := range value.Points {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(FormatPoint(point))
	}
	builder.WriteString(end)
	return builder.String()
}

func serializeTypePath(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	path := val.(PathValue)
	data := make([]byte, 5+len(path.Points)*16)
	if path.Closed {
		data[0] = 1
	}
	binary.BigEndian.PutUint32(data[1:5], uint32(len(path.Points)))
	offset := 5
	for _, point := range path.Points {
		binary.BigEndian.PutUint64(data[offset:offset+8], math.Float64bits(point.X))
		binary.BigEndian.PutUint64(data[offset+8:offset+16], math.Float64bits(point.Y))
		offset += 16
	}
	return data, nil
}

func deserializeTypePath(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 5 {
		return nil, errors.Errorf("invalid path data length: %d", len(data))
	}
	pointCount := int(binary.BigEndian.Uint32(data[1:5]))
	if len(data) != 5+pointCount*16 {
		return nil, errors.Errorf("invalid path data length for %d points: %d", pointCount, len(data))
	}
	points := make([]PointValue, pointCount)
	offset := 5
	for i := range points {
		points[i] = PointValue{
			X: math.Float64frombits(binary.BigEndian.Uint64(data[offset : offset+8])),
			Y: math.Float64frombits(binary.BigEndian.Uint64(data[offset+8 : offset+16])),
		}
		offset += 16
	}
	return PathValue{Points: points, Closed: data[0] == 1}, nil
}
