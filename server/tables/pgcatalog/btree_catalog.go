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

package pgcatalog

type btreeCatalogType struct {
	typeName        string
	functionType    string
	opfamily        string
	compareProc     string
	comparisonFuncs [5]string
}

func (typ btreeCatalogType) operatorFunctionType() string {
	if typ.functionType != "" {
		return typ.functionType
	}
	return typ.typeName
}

type btreeComparisonOperator struct {
	name       string
	strategy   int16
	commutator string
}

type btreePatternCatalogType struct {
	typeName        string
	opfamily        string
	compareProc     string
	sortSupportProc string
	comparisonFuncs [5]string
}

type btreeCrossTypeCatalogType struct {
	leftType        string
	rightType       string
	opfamily        string
	comparisonFuncs [5]string
}

type btreeSupportProc struct {
	leftType  string
	rightType string
	opfamily  string
	procNum   int16
	proc      string
}

var btreeComparisonOperators = []btreeComparisonOperator{
	{name: "<", strategy: 1, commutator: ">"},
	{name: "<=", strategy: 2, commutator: ">="},
	{name: "=", strategy: 3, commutator: "="},
	{name: ">=", strategy: 4, commutator: "<="},
	{name: ">", strategy: 5, commutator: "<"},
}

var btreePatternComparisonOperators = []btreeComparisonOperator{
	{name: "~<~", strategy: 1, commutator: "~>~"},
	{name: "~<=~", strategy: 2, commutator: "~>=~"},
	{name: "=", strategy: 3, commutator: "="},
	{name: "~>=~", strategy: 4, commutator: "~<=~"},
	{name: "~>~", strategy: 5, commutator: "~<~"},
}

var btreePatternCatalogTypes = []btreePatternCatalogType{
	{
		typeName:        "text",
		opfamily:        "text_pattern_ops",
		compareProc:     "bttext_pattern_cmp",
		sortSupportProc: "bttext_pattern_sortsupport",
		comparisonFuncs: [5]string{"text_pattern_lt", "text_pattern_le", "texteq", "text_pattern_ge", "text_pattern_gt"},
	},
	{
		typeName:        "bpchar",
		opfamily:        "bpchar_pattern_ops",
		compareProc:     "btbpchar_pattern_cmp",
		sortSupportProc: "btbpchar_pattern_sortsupport",
		comparisonFuncs: [5]string{"bpchar_pattern_lt", "bpchar_pattern_le", "bpchareq", "bpchar_pattern_ge", "bpchar_pattern_gt"},
	},
}

var btreeIntegerCrossTypeCatalogTypes = []btreeCrossTypeCatalogType{
	{
		leftType:        "int2",
		rightType:       "int4",
		opfamily:        "integer_ops",
		comparisonFuncs: [5]string{"int24lt", "int24le", "int24eq", "int24ge", "int24gt"},
	},
	{
		leftType:        "int2",
		rightType:       "int8",
		opfamily:        "integer_ops",
		comparisonFuncs: [5]string{"int28lt", "int28le", "int28eq", "int28ge", "int28gt"},
	},
	{
		leftType:        "int4",
		rightType:       "int2",
		opfamily:        "integer_ops",
		comparisonFuncs: [5]string{"int42lt", "int42le", "int42eq", "int42ge", "int42gt"},
	},
	{
		leftType:        "int4",
		rightType:       "int8",
		opfamily:        "integer_ops",
		comparisonFuncs: [5]string{"int48lt", "int48le", "int48eq", "int48ge", "int48gt"},
	},
	{
		leftType:        "int8",
		rightType:       "int2",
		opfamily:        "integer_ops",
		comparisonFuncs: [5]string{"int82lt", "int82le", "int82eq", "int82ge", "int82gt"},
	},
	{
		leftType:        "int8",
		rightType:       "int4",
		opfamily:        "integer_ops",
		comparisonFuncs: [5]string{"int84lt", "int84le", "int84eq", "int84ge", "int84gt"},
	},
}

var btreeIntegerSupportProcs = []btreeSupportProc{
	{leftType: "int2", rightType: "int2", opfamily: "integer_ops", procNum: 2, proc: "btint2sortsupport"},
	{leftType: "int2", rightType: "int2", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int2", rightType: "int2", opfamily: "integer_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "int2", rightType: "int4", opfamily: "integer_ops", procNum: 1, proc: "btint24cmp"},
	{leftType: "int2", rightType: "int4", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int2", rightType: "int8", opfamily: "integer_ops", procNum: 1, proc: "btint28cmp"},
	{leftType: "int2", rightType: "int8", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int4", rightType: "int2", opfamily: "integer_ops", procNum: 1, proc: "btint42cmp"},
	{leftType: "int4", rightType: "int2", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int4", rightType: "int4", opfamily: "integer_ops", procNum: 2, proc: "btint4sortsupport"},
	{leftType: "int4", rightType: "int4", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int4", rightType: "int4", opfamily: "integer_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "int4", rightType: "int8", opfamily: "integer_ops", procNum: 1, proc: "btint48cmp"},
	{leftType: "int4", rightType: "int8", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int8", rightType: "int2", opfamily: "integer_ops", procNum: 1, proc: "btint82cmp"},
	{leftType: "int8", rightType: "int4", opfamily: "integer_ops", procNum: 1, proc: "btint84cmp"},
	{leftType: "int8", rightType: "int8", opfamily: "integer_ops", procNum: 2, proc: "btint8sortsupport"},
	{leftType: "int8", rightType: "int8", opfamily: "integer_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "int8", rightType: "int8", opfamily: "integer_ops", procNum: 4, proc: "btequalimage"},
}

var btreeFloatCrossTypeCatalogTypes = []btreeCrossTypeCatalogType{
	{
		leftType:        "float4",
		rightType:       "float8",
		opfamily:        "float_ops",
		comparisonFuncs: [5]string{"float48lt", "float48le", "float48eq", "float48ge", "float48gt"},
	},
	{
		leftType:        "float8",
		rightType:       "float4",
		opfamily:        "float_ops",
		comparisonFuncs: [5]string{"float84lt", "float84le", "float84eq", "float84ge", "float84gt"},
	},
}

var btreeFloatSupportProcs = []btreeSupportProc{
	{leftType: "float4", rightType: "float4", opfamily: "float_ops", procNum: 2, proc: "btfloat4sortsupport"},
	{leftType: "float4", rightType: "float8", opfamily: "float_ops", procNum: 1, proc: "btfloat48cmp"},
	{leftType: "float4", rightType: "float8", opfamily: "float_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "float8", rightType: "float4", opfamily: "float_ops", procNum: 1, proc: "btfloat84cmp"},
	{leftType: "float8", rightType: "float8", opfamily: "float_ops", procNum: 2, proc: "btfloat8sortsupport"},
	{leftType: "float8", rightType: "float8", opfamily: "float_ops", procNum: 3, proc: "pg_catalog.in_range"},
}

var btreeTextCrossTypeCatalogTypes = []btreeCrossTypeCatalogType{
	{
		leftType:        "name",
		rightType:       "text",
		opfamily:        "text_ops",
		comparisonFuncs: [5]string{"namelttext", "nameletext", "nameeqtext", "namegetext", "namegttext"},
	},
	{
		leftType:        "text",
		rightType:       "name",
		opfamily:        "text_ops",
		comparisonFuncs: [5]string{"textltname", "textlename", "texteqname", "textgename", "textgtname"},
	},
}

var btreeTextSupportProcs = []btreeSupportProc{
	{leftType: "name", rightType: "name", opfamily: "text_ops", procNum: 2, proc: "btnamesortsupport"},
	{leftType: "name", rightType: "name", opfamily: "text_ops", procNum: 4, proc: "btvarstrequalimage"},
	{leftType: "name", rightType: "text", opfamily: "text_ops", procNum: 1, proc: "btnametextcmp"},
	{leftType: "text", rightType: "name", opfamily: "text_ops", procNum: 1, proc: "bttextnamecmp"},
	{leftType: "text", rightType: "text", opfamily: "text_ops", procNum: 2, proc: "bttextsortsupport"},
	{leftType: "text", rightType: "text", opfamily: "text_ops", procNum: 4, proc: "btvarstrequalimage"},
}

var btreeScalarSupportProcs = []btreeSupportProc{
	{leftType: "anyenum", rightType: "anyenum", opfamily: "enum_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "bit", rightType: "bit", opfamily: "bit_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "bool", rightType: "bool", opfamily: "bool_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "bpchar", rightType: "bpchar", opfamily: "bpchar_ops", procNum: 2, proc: "bpchar_sortsupport"},
	{leftType: "bpchar", rightType: "bpchar", opfamily: "bpchar_ops", procNum: 4, proc: "btvarstrequalimage"},
	{leftType: "bytea", rightType: "bytea", opfamily: "bytea_ops", procNum: 2, proc: "bytea_sortsupport"},
	{leftType: "bytea", rightType: "bytea", opfamily: "bytea_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "char", rightType: "char", opfamily: "char_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "interval", rightType: "interval", opfamily: "interval_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "macaddr", rightType: "macaddr", opfamily: "macaddr_ops", procNum: 2, proc: "macaddr_sortsupport"},
	{leftType: "macaddr", rightType: "macaddr", opfamily: "macaddr_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "macaddr8", rightType: "macaddr8", opfamily: "macaddr8_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "money", rightType: "money", opfamily: "money_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "inet", rightType: "inet", opfamily: "network_ops", procNum: 2, proc: "network_sortsupport"},
	{leftType: "inet", rightType: "inet", opfamily: "network_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "numeric", rightType: "numeric", opfamily: "numeric_ops", procNum: 2, proc: "numeric_sortsupport"},
	{leftType: "numeric", rightType: "numeric", opfamily: "numeric_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "oid", rightType: "oid", opfamily: "oid_ops", procNum: 2, proc: "btoidsortsupport"},
	{leftType: "oid", rightType: "oid", opfamily: "oid_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "oidvector", rightType: "oidvector", opfamily: "oidvector_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "pg_lsn", rightType: "pg_lsn", opfamily: "pg_lsn_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "tid", rightType: "tid", opfamily: "tid_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "time", rightType: "interval", opfamily: "time_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "time", rightType: "time", opfamily: "time_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "timetz", rightType: "interval", opfamily: "timetz_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "timetz", rightType: "timetz", opfamily: "timetz_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "uuid", rightType: "uuid", opfamily: "uuid_ops", procNum: 2, proc: "uuid_sortsupport"},
	{leftType: "uuid", rightType: "uuid", opfamily: "uuid_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "varbit", rightType: "varbit", opfamily: "varbit_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "xid8", rightType: "xid8", opfamily: "xid8_ops", procNum: 4, proc: "btequalimage"},
}

var btreeDatetimeCrossTypeCatalogTypes = []btreeCrossTypeCatalogType{
	{
		leftType:        "date",
		rightType:       "timestamp",
		opfamily:        "datetime_ops",
		comparisonFuncs: [5]string{"date_lt_timestamp", "date_le_timestamp", "date_eq_timestamp", "date_ge_timestamp", "date_gt_timestamp"},
	},
	{
		leftType:        "date",
		rightType:       "timestamptz",
		opfamily:        "datetime_ops",
		comparisonFuncs: [5]string{"date_lt_timestamptz", "date_le_timestamptz", "date_eq_timestamptz", "date_ge_timestamptz", "date_gt_timestamptz"},
	},
	{
		leftType:        "timestamp",
		rightType:       "date",
		opfamily:        "datetime_ops",
		comparisonFuncs: [5]string{"timestamp_lt_date", "timestamp_le_date", "timestamp_eq_date", "timestamp_ge_date", "timestamp_gt_date"},
	},
	{
		leftType:        "timestamp",
		rightType:       "timestamptz",
		opfamily:        "datetime_ops",
		comparisonFuncs: [5]string{"timestamp_lt_timestamptz", "timestamp_le_timestamptz", "timestamp_eq_timestamptz", "timestamp_ge_timestamptz", "timestamp_gt_timestamptz"},
	},
	{
		leftType:        "timestamptz",
		rightType:       "date",
		opfamily:        "datetime_ops",
		comparisonFuncs: [5]string{"timestamptz_lt_date", "timestamptz_le_date", "timestamptz_eq_date", "timestamptz_ge_date", "timestamptz_gt_date"},
	},
	{
		leftType:        "timestamptz",
		rightType:       "timestamp",
		opfamily:        "datetime_ops",
		comparisonFuncs: [5]string{"timestamptz_lt_timestamp", "timestamptz_le_timestamp", "timestamptz_eq_timestamp", "timestamptz_ge_timestamp", "timestamptz_gt_timestamp"},
	},
}

var btreeDatetimeSupportProcs = []btreeSupportProc{
	{leftType: "date", rightType: "date", opfamily: "datetime_ops", procNum: 2, proc: "date_sortsupport"},
	{leftType: "date", rightType: "date", opfamily: "datetime_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "date", rightType: "interval", opfamily: "datetime_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "date", rightType: "timestamp", opfamily: "datetime_ops", procNum: 1, proc: "date_cmp_timestamp"},
	{leftType: "date", rightType: "timestamptz", opfamily: "datetime_ops", procNum: 1, proc: "date_cmp_timestamptz"},
	{leftType: "timestamp", rightType: "date", opfamily: "datetime_ops", procNum: 1, proc: "timestamp_cmp_date"},
	{leftType: "timestamp", rightType: "interval", opfamily: "datetime_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "timestamp", rightType: "timestamp", opfamily: "datetime_ops", procNum: 2, proc: "timestamp_sortsupport"},
	{leftType: "timestamp", rightType: "timestamp", opfamily: "datetime_ops", procNum: 4, proc: "btequalimage"},
	{leftType: "timestamp", rightType: "timestamptz", opfamily: "datetime_ops", procNum: 1, proc: "timestamp_cmp_timestamptz"},
	{leftType: "timestamptz", rightType: "date", opfamily: "datetime_ops", procNum: 1, proc: "timestamptz_cmp_date"},
	{leftType: "timestamptz", rightType: "interval", opfamily: "datetime_ops", procNum: 3, proc: "pg_catalog.in_range"},
	{leftType: "timestamptz", rightType: "timestamp", opfamily: "datetime_ops", procNum: 1, proc: "timestamptz_cmp_timestamp"},
	{leftType: "timestamptz", rightType: "timestamptz", opfamily: "datetime_ops", procNum: 2, proc: "timestamp_sortsupport"},
	{leftType: "timestamptz", rightType: "timestamptz", opfamily: "datetime_ops", procNum: 4, proc: "btequalimage"},
}

var btreeCatalogTypes = []btreeCatalogType{
	{
		typeName:        "bit",
		opfamily:        "bit_ops",
		compareProc:     "bitcmp",
		comparisonFuncs: [5]string{"bitlt", "bitle", "biteq", "bitge", "bitgt"},
	},
	{
		typeName:        "bool",
		opfamily:        "bool_ops",
		compareProc:     "btboolcmp",
		comparisonFuncs: [5]string{"boollt", "boolle", "booleq", "boolge", "boolgt"},
	},
	{
		typeName:        "anyarray",
		opfamily:        "array_ops",
		compareProc:     "btarraycmp",
		comparisonFuncs: [5]string{"array_lt", "array_le", "array_eq", "array_ge", "array_gt"},
	},
	{
		typeName:        "anyenum",
		opfamily:        "enum_ops",
		compareProc:     "enum_cmp",
		comparisonFuncs: [5]string{"enum_lt", "enum_le", "enum_eq", "enum_ge", "enum_gt"},
	},
	{
		typeName:        "int2",
		opfamily:        "integer_ops",
		compareProc:     "btint2cmp",
		comparisonFuncs: [5]string{"int2lt", "int2le", "int2eq", "int2ge", "int2gt"},
	},
	{
		typeName:        "int4",
		opfamily:        "integer_ops",
		compareProc:     "btint4cmp",
		comparisonFuncs: [5]string{"int4lt", "int4le", "int4eq", "int4ge", "int4gt"},
	},
	{
		typeName:        "int8",
		opfamily:        "integer_ops",
		compareProc:     "btint8cmp",
		comparisonFuncs: [5]string{"int8lt", "int8le", "int8eq", "int8ge", "int8gt"},
	},
	{
		typeName:        "float4",
		opfamily:        "float_ops",
		compareProc:     "btfloat4cmp",
		comparisonFuncs: [5]string{"float4lt", "float4le", "float4eq", "float4ge", "float4gt"},
	},
	{
		typeName:        "float8",
		opfamily:        "float_ops",
		compareProc:     "btfloat8cmp",
		comparisonFuncs: [5]string{"float8lt", "float8le", "float8eq", "float8ge", "float8gt"},
	},
	{
		typeName:        "numeric",
		opfamily:        "numeric_ops",
		compareProc:     "numeric_cmp",
		comparisonFuncs: [5]string{"numeric_lt", "numeric_le", "numeric_eq", "numeric_ge", "numeric_gt"},
	},
	{
		typeName:        "text",
		opfamily:        "text_ops",
		compareProc:     "bttextcmp",
		comparisonFuncs: [5]string{"text_lt", "text_le", "text_eq", "text_ge", "text_gt"},
	},
	{
		typeName:        "name",
		opfamily:        "text_ops",
		compareProc:     "btnamecmp",
		comparisonFuncs: [5]string{"namelt", "namele", "nameeq", "namege", "namegt"},
	},
	{
		typeName:        "bpchar",
		opfamily:        "bpchar_ops",
		compareProc:     "bpcharcmp",
		comparisonFuncs: [5]string{"bpcharlt", "bpcharle", "bpchareq", "bpcharge", "bpchargt"},
	},
	{
		typeName:        "bytea",
		opfamily:        "bytea_ops",
		compareProc:     "byteacmp",
		comparisonFuncs: [5]string{"bytealt", "byteale", "byteaeq", "byteage", "byteagt"},
	},
	{
		typeName:        "char",
		opfamily:        "char_ops",
		compareProc:     "btcharcmp",
		comparisonFuncs: [5]string{"charlt", "charle", "chareq", "charge", "chargt"},
	},
	{
		typeName:        "date",
		opfamily:        "datetime_ops",
		compareProc:     "date_cmp",
		comparisonFuncs: [5]string{"date_lt", "date_le", "date_eq", "date_ge", "date_gt"},
	},
	{
		typeName:        "interval",
		opfamily:        "interval_ops",
		compareProc:     "interval_cmp",
		comparisonFuncs: [5]string{"interval_lt", "interval_le", "interval_eq", "interval_ge", "interval_gt"},
	},
	{
		typeName:        "jsonb",
		opfamily:        "jsonb_ops",
		compareProc:     "jsonb_cmp",
		comparisonFuncs: [5]string{"jsonb_lt", "jsonb_le", "jsonb_eq", "jsonb_ge", "jsonb_gt"},
	},
	{
		typeName:        "macaddr",
		opfamily:        "macaddr_ops",
		compareProc:     "macaddr_cmp",
		comparisonFuncs: [5]string{"macaddr_lt", "macaddr_le", "macaddr_eq", "macaddr_ge", "macaddr_gt"},
	},
	{
		typeName:        "macaddr8",
		opfamily:        "macaddr8_ops",
		compareProc:     "macaddr8_cmp",
		comparisonFuncs: [5]string{"macaddr8_lt", "macaddr8_le", "macaddr8_eq", "macaddr8_ge", "macaddr8_gt"},
	},
	{
		typeName:        "money",
		opfamily:        "money_ops",
		compareProc:     "cash_cmp",
		comparisonFuncs: [5]string{"cash_lt", "cash_le", "cash_eq", "cash_ge", "cash_gt"},
	},
	{
		typeName:        "anymultirange",
		opfamily:        "multirange_ops",
		compareProc:     "multirange_cmp",
		comparisonFuncs: [5]string{"multirange_lt", "multirange_le", "multirange_eq", "multirange_ge", "multirange_gt"},
	},
	{
		typeName:        "inet",
		opfamily:        "network_ops",
		compareProc:     "network_cmp",
		comparisonFuncs: [5]string{"network_lt", "network_le", "network_eq", "network_ge", "network_gt"},
	},
	{
		typeName:        "oid",
		opfamily:        "oid_ops",
		compareProc:     "btoidcmp",
		comparisonFuncs: [5]string{"oidlt", "oidle", "oideq", "oidge", "oidgt"},
	},
	{
		typeName:        "oidvector",
		opfamily:        "oidvector_ops",
		compareProc:     "btoidvectorcmp",
		comparisonFuncs: [5]string{"oidvectorlt", "oidvectorle", "oidvectoreq", "oidvectorge", "oidvectorgt"},
	},
	{
		typeName:        "pg_lsn",
		opfamily:        "pg_lsn_ops",
		compareProc:     "pg_lsn_cmp",
		comparisonFuncs: [5]string{"pg_lsn_lt", "pg_lsn_le", "pg_lsn_eq", "pg_lsn_ge", "pg_lsn_gt"},
	},
	{
		typeName:        "anyrange",
		opfamily:        "range_ops",
		compareProc:     "range_cmp",
		comparisonFuncs: [5]string{"range_lt", "range_le", "range_eq", "range_ge", "range_gt"},
	},
	{
		typeName:        "record",
		opfamily:        "record_image_ops",
		compareProc:     "btrecordimagecmp",
		comparisonFuncs: [5]string{"record_image_lt", "record_image_le", "record_image_eq", "record_image_ge", "record_image_gt"},
	},
	{
		typeName:        "record",
		opfamily:        "record_ops",
		compareProc:     "btrecordcmp",
		comparisonFuncs: [5]string{"record_lt", "record_le", "record_eq", "record_ge", "record_gt"},
	},
	{
		typeName:        "tid",
		opfamily:        "tid_ops",
		compareProc:     "bttidcmp",
		comparisonFuncs: [5]string{"tidlt", "tidle", "tideq", "tidge", "tidgt"},
	},
	{
		typeName:        "time",
		opfamily:        "time_ops",
		compareProc:     "time_cmp",
		comparisonFuncs: [5]string{"time_lt", "time_le", "time_eq", "time_ge", "time_gt"},
	},
	{
		typeName:        "timestamp",
		opfamily:        "datetime_ops",
		compareProc:     "timestamp_cmp",
		comparisonFuncs: [5]string{"timestamp_lt", "timestamp_le", "timestamp_eq", "timestamp_ge", "timestamp_gt"},
	},
	{
		typeName:        "timestamptz",
		opfamily:        "datetime_ops",
		compareProc:     "timestamptz_cmp",
		comparisonFuncs: [5]string{"timestamptz_lt", "timestamptz_le", "timestamptz_eq", "timestamptz_ge", "timestamptz_gt"},
	},
	{
		typeName:        "timetz",
		opfamily:        "timetz_ops",
		compareProc:     "timetz_cmp",
		comparisonFuncs: [5]string{"timetz_lt", "timetz_le", "timetz_eq", "timetz_ge", "timetz_gt"},
	},
	{
		typeName:        "tsquery",
		opfamily:        "tsquery_ops",
		compareProc:     "tsquery_cmp",
		comparisonFuncs: [5]string{"tsquery_lt", "tsquery_le", "tsquery_eq", "tsquery_ge", "tsquery_gt"},
	},
	{
		typeName:        "tsvector",
		opfamily:        "tsvector_ops",
		compareProc:     "tsvector_cmp",
		comparisonFuncs: [5]string{"tsvector_lt", "tsvector_le", "tsvector_eq", "tsvector_ge", "tsvector_gt"},
	},
	{
		typeName:        "uuid",
		opfamily:        "uuid_ops",
		compareProc:     "uuid_cmp",
		comparisonFuncs: [5]string{"uuid_lt", "uuid_le", "uuid_eq", "uuid_ge", "uuid_gt"},
	},
	{
		typeName:        "varbit",
		opfamily:        "varbit_ops",
		compareProc:     "varbitcmp",
		comparisonFuncs: [5]string{"varbitlt", "varbitle", "varbiteq", "varbitge", "varbitgt"},
	},
	{
		typeName:        "xid8",
		opfamily:        "xid8_ops",
		compareProc:     "xid8cmp",
		comparisonFuncs: [5]string{"xid8lt", "xid8le", "xid8eq", "xid8ge", "xid8gt"},
	},
}
