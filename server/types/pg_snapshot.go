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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// PgSnapshotValue stores PostgreSQL snapshot visibility bounds.
type PgSnapshotValue struct {
	Xmin uint64
	Xmax uint64
	Xip  []uint64
}

// PgSnapshot is PostgreSQL's transaction visibility snapshot type.
var PgSnapshot = &DoltgresType{
	ID:                  toInternal("pg_snapshot"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_pg_snapshot"),
	InputFunc:           toFuncID("pg_snapshot_in", toInternal("cstring")),
	OutputFunc:          toFuncID("pg_snapshot_out", toInternal("pg_snapshot")),
	ReceiveFunc:         toFuncID("pg_snapshot_recv", toInternal("internal")),
	SendFunc:            toFuncID("pg_snapshot_send", toInternal("pg_snapshot")),
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
	SerializationFunc:   serializeTypePgSnapshot,
	DeserializationFunc: deserializeTypePgSnapshot,
}

// ParsePgSnapshot parses PostgreSQL's xmin:xmax:xip-list snapshot syntax.
func ParsePgSnapshot(input string) (PgSnapshotValue, error) {
	originalInput := input
	input = strings.TrimSpace(input)
	xminText, rest, ok := strings.Cut(input, ":")
	if !ok {
		return PgSnapshotValue{}, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
	}
	xmaxText, xipText, ok := strings.Cut(rest, ":")
	if !ok || strings.Contains(xipText, ":") {
		return PgSnapshotValue{}, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
	}

	xmin, err := parsePgSnapshotUint(xminText, originalInput)
	if err != nil {
		return PgSnapshotValue{}, err
	}
	xmax, err := parsePgSnapshotUint(xmaxText, originalInput)
	if err != nil {
		return PgSnapshotValue{}, err
	}
	if xmin > xmax {
		return PgSnapshotValue{}, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
	}

	var xip []uint64
	if xipText != "" {
		xipParts := strings.Split(xipText, ",")
		xip = make([]uint64, len(xipParts))
		for i, part := range xipParts {
			xid, err := parsePgSnapshotUint(part, originalInput)
			if err != nil {
				return PgSnapshotValue{}, err
			}
			if xid < xmin || xid >= xmax {
				return PgSnapshotValue{}, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
			}
			if i > 0 && xid <= xip[i-1] {
				return PgSnapshotValue{}, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
			}
			xip[i] = xid
		}
	}
	return PgSnapshotValue{Xmin: xmin, Xmax: xmax, Xip: xip}, nil
}

func parsePgSnapshotUint(input string, originalInput string) (uint64, error) {
	if input == "" || strings.HasPrefix(input, "+") || strings.HasPrefix(input, "-") {
		return 0, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
	}
	value, err := strconv.ParseUint(input, 10, 64)
	if err != nil {
		return 0, ErrInvalidSyntaxForType.New("pg_snapshot", originalInput)
	}
	return value, nil
}

// FormatPgSnapshot formats a snapshot in PostgreSQL's xmin:xmax:xip-list syntax.
func FormatPgSnapshot(snapshot PgSnapshotValue) string {
	var builder strings.Builder
	builder.WriteString(strconv.FormatUint(snapshot.Xmin, 10))
	builder.WriteByte(':')
	builder.WriteString(strconv.FormatUint(snapshot.Xmax, 10))
	builder.WriteByte(':')
	for i, xid := range snapshot.Xip {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatUint(xid, 10))
	}
	return builder.String()
}

// EncodePgSnapshotBinary encodes a pg_snapshot in PostgreSQL's binary wire format.
func EncodePgSnapshotBinary(snapshot PgSnapshotValue) []byte {
	writer := utils.NewWireWriter()
	writer.Reserve(uint64(20 + (len(snapshot.Xip) * 8)))
	writer.WriteUint32(uint32(len(snapshot.Xip)))
	writer.WriteUint64(snapshot.Xmin)
	writer.WriteUint64(snapshot.Xmax)
	for _, xid := range snapshot.Xip {
		writer.WriteUint64(xid)
	}
	return writer.BufferData()
}

// DecodePgSnapshotBinary decodes a pg_snapshot from PostgreSQL's binary wire format.
func DecodePgSnapshotBinary(data []byte) (PgSnapshotValue, error) {
	if len(data) < 20 {
		return PgSnapshotValue{}, errors.Errorf("invalid pg_snapshot binary length %d", len(data))
	}
	nxip := binary.BigEndian.Uint32(data[0:4])
	expectedLength := 20 + (int(nxip) * 8)
	if len(data) != expectedLength {
		return PgSnapshotValue{}, errors.Errorf("invalid pg_snapshot binary length %d", len(data))
	}
	xip := make([]uint64, nxip)
	for i := range xip {
		offset := 20 + (i * 8)
		xip[i] = binary.BigEndian.Uint64(data[offset : offset+8])
	}
	return PgSnapshotValue{
		Xmin: binary.BigEndian.Uint64(data[4:12]),
		Xmax: binary.BigEndian.Uint64(data[12:20]),
		Xip:  xip,
	}, nil
}

// serializeTypePgSnapshot handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypePgSnapshot(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	return EncodePgSnapshotBinary(val.(PgSnapshotValue)), nil
}

// deserializeTypePgSnapshot handles deserialization from the Dolt serialized format to our standard representation used
// by expressions and nodes.
func deserializeTypePgSnapshot(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	return DecodePgSnapshotBinary(data)
}
