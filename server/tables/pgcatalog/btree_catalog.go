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
		typeName:        "varchar",
		functionType:    "text",
		opfamily:        "text_ops",
		compareProc:     "bttextcmp",
		comparisonFuncs: [5]string{"text_lt", "text_le", "text_eq", "text_ge", "text_gt"},
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
