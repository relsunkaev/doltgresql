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

import "github.com/dolthub/doltgresql/core/id"

// Int4Range is the int4range type.
var Int4Range = NewRangeType(toInternal("_int4range"), toInternal("int4range"))

// Int4RangeArray is the array variant of Int4Range.
var Int4RangeArray = CreateArrayTypeFromBaseType(Int4Range)

// Int8Range is the int8range type.
var Int8Range = NewRangeType(toInternal("_int8range"), toInternal("int8range"))

// Int8RangeArray is the array variant of Int8Range.
var Int8RangeArray = CreateArrayTypeFromBaseType(Int8Range)

// NumRange is the numrange type.
var NumRange = NewRangeType(toInternal("_numrange"), toInternal("numrange"))

// NumRangeArray is the array variant of NumRange.
var NumRangeArray = CreateArrayTypeFromBaseType(NumRange)

// DateRange is the daterange type.
var DateRange = NewRangeType(toInternal("_daterange"), toInternal("daterange"))

// DateRangeArray is the array variant of DateRange.
var DateRangeArray = CreateArrayTypeFromBaseType(DateRange)

// TsRange is the tsrange type.
var TsRange = NewRangeType(toInternal("_tsrange"), toInternal("tsrange"))

// TsRangeArray is the array variant of TsRange.
var TsRangeArray = CreateArrayTypeFromBaseType(TsRange)

// TstzRange is the tstzrange type.
var TstzRange = NewRangeType(toInternal("_tstzrange"), toInternal("tstzrange"))

// TstzRangeArray is the array variant of TstzRange.
var TstzRangeArray = CreateArrayTypeFromBaseType(TstzRange)

// Int4MultiRange is the int4multirange type.
var Int4MultiRange = NewMultiRangeType(toInternal("_int4multirange"), toInternal("int4multirange"))

// Int4MultiRangeArray is the array variant of Int4MultiRange.
var Int4MultiRangeArray = CreateArrayTypeFromBaseType(Int4MultiRange)

// Int8MultiRange is the int8multirange type.
var Int8MultiRange = NewMultiRangeType(toInternal("_int8multirange"), toInternal("int8multirange"))

// Int8MultiRangeArray is the array variant of Int8MultiRange.
var Int8MultiRangeArray = CreateArrayTypeFromBaseType(Int8MultiRange)

// NumMultiRange is the nummultirange type.
var NumMultiRange = NewMultiRangeType(toInternal("_nummultirange"), toInternal("nummultirange"))

// NumMultiRangeArray is the array variant of NumMultiRange.
var NumMultiRangeArray = CreateArrayTypeFromBaseType(NumMultiRange)

// DateMultiRange is the datemultirange type.
var DateMultiRange = NewMultiRangeType(toInternal("_datemultirange"), toInternal("datemultirange"))

// DateMultiRangeArray is the array variant of DateMultiRange.
var DateMultiRangeArray = CreateArrayTypeFromBaseType(DateMultiRange)

// TsMultiRange is the tsmultirange type.
var TsMultiRange = NewMultiRangeType(toInternal("_tsmultirange"), toInternal("tsmultirange"))

// TsMultiRangeArray is the array variant of TsMultiRange.
var TsMultiRangeArray = CreateArrayTypeFromBaseType(TsMultiRange)

// TstzMultiRange is the tstzmultirange type.
var TstzMultiRange = NewMultiRangeType(toInternal("_tstzmultirange"), toInternal("tstzmultirange"))

// TstzMultiRangeArray is the array variant of TstzMultiRange.
var TstzMultiRangeArray = CreateArrayTypeFromBaseType(TstzMultiRange)

// NewRangeType returns a text-compatible range type.
func NewRangeType(arrayID, typeID id.Type) *DoltgresType {
	rangeType := *Text
	rangeType.ID = typeID
	rangeType.TypType = TypeType_Range
	rangeType.TypCategory = TypeCategory_RangeTypes
	rangeType.IsPreferred = false
	rangeType.Elem = id.NullType
	rangeType.Array = arrayID
	rangeType.TypCollation = id.NullCollation
	rangeType.InternalName = ""
	return &rangeType
}

// NewMultiRangeType returns a text-compatible multirange type.
func NewMultiRangeType(arrayID, typeID id.Type) *DoltgresType {
	multirangeType := *Text
	multirangeType.ID = typeID
	multirangeType.TypType = TypeType_MultiRange
	multirangeType.TypCategory = TypeCategory_RangeTypes
	multirangeType.IsPreferred = false
	multirangeType.Elem = id.NullType
	multirangeType.Array = arrayID
	multirangeType.TypCollation = id.NullCollation
	multirangeType.InternalName = ""
	return &multirangeType
}
