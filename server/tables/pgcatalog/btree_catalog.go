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

var btreeCatalogTypes = []btreeCatalogType{
	{
		typeName:        "bool",
		opfamily:        "bool_ops",
		compareProc:     "btboolcmp",
		comparisonFuncs: [5]string{"boollt", "boolle", "booleq", "boolge", "boolgt"},
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
		typeName:        "date",
		opfamily:        "datetime_ops",
		compareProc:     "date_cmp",
		comparisonFuncs: [5]string{"date_lt", "date_le", "date_eq", "date_ge", "date_gt"},
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
		typeName:        "uuid",
		opfamily:        "uuid_ops",
		compareProc:     "uuid_cmp",
		comparisonFuncs: [5]string{"uuid_lt", "uuid_le", "uuid_eq", "uuid_ge", "uuid_gt"},
	},
}
