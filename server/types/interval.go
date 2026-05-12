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

package types

import (
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/duration"
	postgresTypes "github.com/dolthub/doltgresql/postgres/parser/types"
	"github.com/dolthub/doltgresql/utils"
)

// Interval is the interval type.
var Interval = &DoltgresType{
	ID:                  toInternal("interval"),
	TypLength:           int16(16),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_TimespanTypes,
	IsPreferred:         true,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_interval"),
	InputFunc:           toFuncID("interval_in", toInternal("cstring"), toInternal("oid"), toInternal("int4")),
	OutputFunc:          toFuncID("interval_out", toInternal("interval")),
	ReceiveFunc:         toFuncID("interval_recv", toInternal("internal"), toInternal("oid"), toInternal("int4")),
	SendFunc:            toFuncID("interval_send", toInternal("interval")),
	ModInFunc:           toFuncID("intervaltypmodin", toInternal("_cstring")),
	ModOutFunc:          toFuncID("intervaltypmodout", toInternal("int4")),
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
	CompareFunc:         toFuncID("interval_cmp", toInternal("interval"), toInternal("interval")),
	SerializationFunc:   serializeTypeInterval,
	DeserializationFunc: deserializeTypeInterval,
}

const (
	intervalFullRange     int32 = 0x7FFF
	intervalFullPrecision int32 = 0xFFFF
)

// NewIntervalType returns an interval type with PostgreSQL interval typmod metadata.
func NewIntervalType(metadata postgresTypes.IntervalTypeMetadata) (*DoltgresType, error) {
	typmod, err := GetTypmodFromIntervalTypeMetadata(metadata)
	if err != nil {
		return nil, err
	}
	if typmod == -1 {
		return Interval, nil
	}
	newType := *Interval.WithAttTypMod(typmod)
	return &newType, nil
}

func IntervalTypeMetadataHasTypmod(metadata postgresTypes.IntervalTypeMetadata) bool {
	return metadata.PrecisionIsSet ||
		metadata.DurationField.DurationType != postgresTypes.IntervalDurationType_UNSET ||
		metadata.DurationField.FromDurationType != postgresTypes.IntervalDurationType_UNSET
}

func GetTypmodFromIntervalTypeMetadata(metadata postgresTypes.IntervalTypeMetadata) (int32, error) {
	if !IntervalTypeMetadataHasTypmod(metadata) {
		return -1, nil
	}
	if metadata.Precision < 0 || metadata.Precision > 6 {
		return 0, fmt.Errorf("INTERVAL(%v) precision must be between 0 and 6", metadata.Precision)
	}
	precision := intervalFullPrecision
	if metadata.PrecisionIsSet {
		precision = metadata.Precision
	}
	return (intervalRangeMask(metadata.DurationField) << 16) | precision, nil
}

func GetIntervalTypeMetadataFromTypmod(typmod int32) postgresTypes.IntervalTypeMetadata {
	if typmod == -1 {
		return postgresTypes.DefaultIntervalTypeMetadata
	}
	precision := typmod & intervalFullPrecision
	metadata := postgresTypes.IntervalTypeMetadata{
		DurationField: intervalDurationFieldFromRangeMask(int32(uint32(typmod) >> 16)),
	}
	if precision != intervalFullPrecision {
		metadata.Precision = precision
		metadata.PrecisionIsSet = true
	}
	return metadata
}

func ApplyIntervalTypmod(val duration.Duration, typmod int32) (duration.Duration, error) {
	if typmod == -1 {
		return val, nil
	}
	metadata := GetIntervalTypeMetadataFromTypmod(typmod)
	switch metadata.DurationField.DurationType {
	case postgresTypes.IntervalDurationType_YEAR:
		val.Months = val.Months - val.Months%12
		val.Days = 0
		val.SetNanos(0)
	case postgresTypes.IntervalDurationType_MONTH:
		val.Days = 0
		val.SetNanos(0)
	case postgresTypes.IntervalDurationType_DAY:
		val.SetNanos(0)
	case postgresTypes.IntervalDurationType_HOUR:
		val.SetNanos(val.Nanos() - val.Nanos()%time.Hour.Nanoseconds())
	case postgresTypes.IntervalDurationType_MINUTE:
		val.SetNanos(val.Nanos() - val.Nanos()%time.Minute.Nanoseconds())
	case postgresTypes.IntervalDurationType_SECOND, postgresTypes.IntervalDurationType_UNSET:
		if metadata.PrecisionIsSet || metadata.Precision > 0 {
			precision, err := timePrecisionToRoundDuration(metadata.Precision)
			if err != nil {
				return duration.Duration{}, err
			}
			val.SetNanos(time.Duration(val.Nanos()).Round(precision).Nanoseconds())
		}
	}
	return val, nil
}

func IntervalTypmodOut(typmod int32) string {
	if typmod == -1 {
		return ""
	}
	metadata := GetIntervalTypeMetadataFromTypmod(typmod)
	parts := make([]string, 0, 4)
	if metadata.DurationField.FromDurationType != postgresTypes.IntervalDurationType_UNSET {
		parts = append(parts, strings.ToLower(metadata.DurationField.FromDurationType.String()), "to")
	}
	if metadata.DurationField.DurationType != postgresTypes.IntervalDurationType_UNSET {
		parts = append(parts, strings.ToLower(metadata.DurationField.DurationType.String()))
	}

	precision := ""
	if metadata.PrecisionIsSet || metadata.Precision > 0 {
		precision = fmt.Sprintf("(%d)", metadata.Precision)
	}
	if len(parts) == 0 {
		return precision
	}
	return " " + strings.Join(parts, " ") + precision
}

func intervalRangeMask(field postgresTypes.IntervalDurationField) int32 {
	if field.DurationType == postgresTypes.IntervalDurationType_UNSET {
		return intervalFullRange
	}
	if field.FromDurationType == postgresTypes.IntervalDurationType_UNSET {
		return intervalDurationBit(field.DurationType)
	}
	return intervalDurationRangeMask(field.FromDurationType, field.DurationType)
}

func intervalDurationRangeMask(from postgresTypes.IntervalDurationType, to postgresTypes.IntervalDurationType) int32 {
	order := []postgresTypes.IntervalDurationType{
		postgresTypes.IntervalDurationType_YEAR,
		postgresTypes.IntervalDurationType_MONTH,
		postgresTypes.IntervalDurationType_DAY,
		postgresTypes.IntervalDurationType_HOUR,
		postgresTypes.IntervalDurationType_MINUTE,
		postgresTypes.IntervalDurationType_SECOND,
	}
	mask := int32(0)
	inRange := false
	for _, typ := range order {
		if typ == from {
			inRange = true
		}
		if inRange {
			mask |= intervalDurationBit(typ)
		}
		if typ == to {
			break
		}
	}
	return mask
}

func intervalDurationBit(typ postgresTypes.IntervalDurationType) int32 {
	switch typ {
	case postgresTypes.IntervalDurationType_YEAR:
		return 1 << 1
	case postgresTypes.IntervalDurationType_MONTH:
		return 1 << 2
	case postgresTypes.IntervalDurationType_DAY:
		return 1 << 3
	case postgresTypes.IntervalDurationType_HOUR:
		return 1 << 10
	case postgresTypes.IntervalDurationType_MINUTE:
		return 1 << 11
	case postgresTypes.IntervalDurationType_SECOND:
		return 1 << 12
	default:
		return intervalFullRange
	}
}

func intervalDurationFieldFromRangeMask(mask int32) postgresTypes.IntervalDurationField {
	switch mask {
	case intervalFullRange:
		return postgresTypes.IntervalDurationField{}
	case intervalDurationBit(postgresTypes.IntervalDurationType_YEAR):
		return postgresTypes.IntervalDurationField{DurationType: postgresTypes.IntervalDurationType_YEAR}
	case intervalDurationBit(postgresTypes.IntervalDurationType_MONTH):
		return postgresTypes.IntervalDurationField{DurationType: postgresTypes.IntervalDurationType_MONTH}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_YEAR, postgresTypes.IntervalDurationType_MONTH):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_YEAR,
			DurationType:     postgresTypes.IntervalDurationType_MONTH,
		}
	case intervalDurationBit(postgresTypes.IntervalDurationType_DAY):
		return postgresTypes.IntervalDurationField{DurationType: postgresTypes.IntervalDurationType_DAY}
	case intervalDurationBit(postgresTypes.IntervalDurationType_HOUR):
		return postgresTypes.IntervalDurationField{DurationType: postgresTypes.IntervalDurationType_HOUR}
	case intervalDurationBit(postgresTypes.IntervalDurationType_MINUTE):
		return postgresTypes.IntervalDurationField{DurationType: postgresTypes.IntervalDurationType_MINUTE}
	case intervalDurationBit(postgresTypes.IntervalDurationType_SECOND):
		return postgresTypes.IntervalDurationField{DurationType: postgresTypes.IntervalDurationType_SECOND}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_DAY, postgresTypes.IntervalDurationType_HOUR):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_DAY,
			DurationType:     postgresTypes.IntervalDurationType_HOUR,
		}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_DAY, postgresTypes.IntervalDurationType_MINUTE):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_DAY,
			DurationType:     postgresTypes.IntervalDurationType_MINUTE,
		}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_DAY, postgresTypes.IntervalDurationType_SECOND):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_DAY,
			DurationType:     postgresTypes.IntervalDurationType_SECOND,
		}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_HOUR, postgresTypes.IntervalDurationType_MINUTE):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_HOUR,
			DurationType:     postgresTypes.IntervalDurationType_MINUTE,
		}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_HOUR, postgresTypes.IntervalDurationType_SECOND):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_HOUR,
			DurationType:     postgresTypes.IntervalDurationType_SECOND,
		}
	case intervalDurationRangeMask(postgresTypes.IntervalDurationType_MINUTE, postgresTypes.IntervalDurationType_SECOND):
		return postgresTypes.IntervalDurationField{
			FromDurationType: postgresTypes.IntervalDurationType_MINUTE,
			DurationType:     postgresTypes.IntervalDurationType_SECOND,
		}
	default:
		return postgresTypes.IntervalDurationField{}
	}
}

// serializeTypeInterval handles serialization from the standard representation to our serialized representation that is
// written in Dolt.
func serializeTypeInterval(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	sortNanos, months, days, err := val.(duration.Duration).Encode()
	if err != nil {
		return nil, err
	}
	writer := utils.NewWriter(0)
	writer.Int64(sortNanos)
	writer.Int32(int32(months))
	writer.Int32(int32(days))
	return writer.Data(), nil
}

// deserializeTypeInterval handles deserialization from the Dolt serialized format to our standard representation used by
// expressions and nodes.
func deserializeTypeInterval(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader := utils.NewReader(data)
	sortNanos := reader.Int64()
	months := reader.Int32()
	days := reader.Int32()
	return duration.Decode(sortNanos, int64(months), int64(days))
}
