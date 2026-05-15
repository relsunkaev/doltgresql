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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

// TestJsonAggDistinctRequiresJsonEqualityOperatorRepro reproduces a query
// correctness bug: PostgreSQL rejects DISTINCT over json values because json
// does not have an equality operator.
func TestJsonAggDistinctRequiresJsonEqualityOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_agg DISTINCT rejects json inputs without equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_agg(DISTINCT doc)::text
						FROM (VALUES ('{"a":1}'::json), ('{"a":1}'::json)) AS v(doc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonaggdistinctrequiresjsonequalityoperatorrepro-0001-select-json_agg-distinct-doc-::text",

						// TestJsonObjectAggDistinctRequiresJsonEqualityOperatorRepro reproduces a
						// query correctness bug: PostgreSQL rejects DISTINCT rows containing json
						// values because json does not have an equality operator.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonObjectAggDistinctRequiresJsonEqualityOperatorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_object_agg DISTINCT rejects json inputs without equality",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_object_agg(DISTINCT key, doc)::text
						FROM (VALUES ('a', '{"x":1}'::json), ('a', '{"x":1}'::json)) AS v(key, doc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectaggdistinctrequiresjsonequalityoperatorrepro-0001-select-json_object_agg-distinct-key-doc",

						// TestJsonbAggDistinctDeduplicatesJsonbValuesRepro reproduces a query
						// correctness bug: DISTINCT jsonb aggregate inputs are not deduplicated.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbAggDistinctDeduplicatesJsonbValuesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_agg DISTINCT deduplicates jsonb inputs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_agg(DISTINCT doc)::text
						FROM (VALUES ('{"a":1}'::jsonb), ('{"a":1}'::jsonb)) AS v(doc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbaggdistinctdeduplicatesjsonbvaluesrepro-0001-select-jsonb_agg-distinct-doc-::text"},
				},
			},
		},
	})
}

// TestJsonExtractionPreservesJsonSubdocumentTextRepro reproduces a JSON
// correctness bug: PostgreSQL's json extraction operators preserve the
// extracted subdocument's original lexical representation.
func TestJsonExtractionPreservesJsonSubdocumentTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json extraction preserves subdocument text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ('{"a": { "b" : 1, "c" : 2 }}'::json -> 'a')::text,
						('{"a": { "b" : 1, "c" : 2 }}'::json #> ARRAY['a'])::text,
						('[{ "b" : 1, "c" : 2 }]'::json -> 0)::text,
						json_extract_path('{"a": { "b" : 1, "c" : 2 }}'::json, 'a')::text,
						json_extract_path_text('{"a": { "b" : 1, "c" : 2 }}'::json, 'a');`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonextractionpreservesjsonsubdocumenttextrepro-0001-select-{-a-:-{"},
				},
			},
		},
	})
}

// TestJsonExtractionPreservesJsonObjectKeyOrderRepro reproduces a JSON
// correctness bug: PostgreSQL's json extraction operators preserve object key
// order inside extracted JSON subdocuments.
func TestJsonExtractionPreservesJsonObjectKeyOrderRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json extraction preserves object key order",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ('{"z":0,"a":{"b":1,"a":2}}'::json -> 'a')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonextractionpreservesjsonobjectkeyorderrepro-0001-select-{-z-:0-a"},
				},
			},
		},
	})
}

// TestJsonEachPreservesJsonObjectOrderAndDuplicatesRepro reproduces a JSON
// correctness bug: PostgreSQL's json_each preserves plain json object order and
// duplicate keys.
func TestJsonEachPreservesJsonObjectOrderAndDuplicatesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_each preserves plain json object order and duplicate keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT string_agg(key || ':' || value::text, ',')
						FROM json_each('{"b":1,"a":2}'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsoneachpreservesjsonobjectorderandduplicatesrepro-0001-select-string_agg-key-||-:"},
				},
				{
					Query: `SELECT string_agg(key || ':' || value::text, ',')
						FROM json_each('{"a":1,"a":2}'::json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsoneachpreservesjsonobjectorderandduplicatesrepro-0002-select-string_agg-key-||-:"},
				},
			},
		},
	})
}

// TestJsonObjectKeysPreservesJsonObjectOrderAndDuplicatesRepro reproduces a
// JSON correctness bug: PostgreSQL's json_object_keys preserves plain json
// object order and duplicate keys.
func TestJsonObjectKeysPreservesJsonObjectOrderAndDuplicatesRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_object_keys preserves plain json object order and duplicate keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT string_agg(key, ',')
						FROM json_object_keys('{"b":1,"a":2}'::json) AS key;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectkeyspreservesjsonobjectorderandduplicatesrepro-0001-select-string_agg-key-from-json_object_keys"},
				},
				{
					Query: `SELECT string_agg(key, ',')
						FROM json_object_keys('{"a":1,"a":2}'::json) AS key;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectkeyspreservesjsonobjectorderandduplicatesrepro-0002-select-string_agg-key-from-json_object_keys"},
				},
			},
		},
	})
}

// TestJsonArrayElementsPreservesJsonElementTextRepro reproduces a JSON
// correctness bug: PostgreSQL's json_array_elements functions preserve plain
// json element text, including object order, duplicate keys, and whitespace.
func TestJsonArrayElementsPreservesJsonElementTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_array_elements preserves plain json element text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT string_agg(value::text, '|')
						FROM json_array_elements('[{"b":1,"a":2},{"a":1,"a":2},{ "c" : 3 }]'::json) AS value;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonarrayelementspreservesjsonelementtextrepro-0001-select-string_agg-value::text-|-from"},
				},
				{
					Query: `SELECT string_agg(value, '|')
						FROM json_array_elements_text('[{"b":1,"a":2},{"a":1,"a":2},{ "c" : 3 }]'::json) AS value;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonarrayelementspreservesjsonelementtextrepro-0002-select-string_agg-value-|-from"},
				},
			},
		},
	})
}

// TestJsonBuildObjectRejectsNonScalarKeysRepro reproduces a JSON correctness
// bug: PostgreSQL rejects array, composite, and JSON values used as
// json_build_object keys.
func TestJsonBuildObjectRejectsNonScalarKeysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_build_object rejects non-scalar keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_build_object(json '{"a":1}', 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbuildobjectrejectsnonscalarkeysrepro-0001-select-json_build_object-json-{-a", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_build_object(ARRAY[1,2,3], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbuildobjectrejectsnonscalarkeysrepro-0002-select-json_build_object-array[1-2-3]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_build_object(r, 2) FROM (SELECT 1 AS a, 2 AS b) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbuildobjectrejectsnonscalarkeysrepro-0003-select-json_build_object-r-2-from",

						// TestJsonbBuildObjectRejectsNonScalarKeysRepro reproduces a JSONB correctness
						// bug: PostgreSQL rejects array, composite, and JSON values used as
						// jsonb_build_object keys.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbBuildObjectRejectsNonScalarKeysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_build_object rejects non-scalar keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_build_object(json '{"a":1}', 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbbuildobjectrejectsnonscalarkeysrepro-0001-select-jsonb_build_object-json-{-a", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_build_object(ARRAY[1,2,3], 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbbuildobjectrejectsnonscalarkeysrepro-0002-select-jsonb_build_object-array[1-2-3]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_build_object(r, 2) FROM (SELECT 1 AS a, 2 AS b) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbbuildobjectrejectsnonscalarkeysrepro-0003-select-jsonb_build_object-r-2-from",

						// TestJsonObjectAggRejectsNonScalarKeysRepro reproduces a JSON correctness bug:
						// PostgreSQL rejects array, composite, and JSON values used as json_object_agg
						// keys.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonObjectAggRejectsNonScalarKeysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_object_agg rejects non-scalar keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_object_agg(k, 2) FROM (VALUES (json '{"a":1}')) AS v(k);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectaggrejectsnonscalarkeysrepro-0001-select-json_object_agg-k-2-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_object_agg(k, 2) FROM (VALUES (ARRAY[1,2,3])) AS v(k);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectaggrejectsnonscalarkeysrepro-0002-select-json_object_agg-k-2-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_object_agg(r, 2) FROM (SELECT 1 AS a, 2 AS b) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectaggrejectsnonscalarkeysrepro-0003-select-json_object_agg-r-2-from",

						// TestJsonAggregatesPreserveJsonInputTextRepro reproduces a JSON correctness
						// bug: PostgreSQL's plain json aggregate outputs preserve input json object
						// text, including object order, duplicate keys, and whitespace.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonAggregatesPreserveJsonInputTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json aggregates preserve plain json input text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_agg(doc)::text
						FROM (VALUES ('{"b":1,"a":2}'::json),
							('{"a":1,"a":2}'::json),
							('{ "c" : 3 }'::json)) AS v(doc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonaggregatespreservejsoninputtextrepro-0001-select-json_agg-doc-::text-from"},
				},
				{
					Query: `SELECT json_object_agg(k, doc)::text
						FROM (VALUES ('x', '{"b":1,"a":2}'::json),
							('y', '{"a":1,"a":2}'::json)) AS v(k, doc);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonaggregatespreservejsoninputtextrepro-0002-select-json_object_agg-k-doc-::text"},
				},
			},
		},
	})
}

// TestJsonToRecordPreservesNestedJsonTextRepro reproduces a JSON correctness
// bug: PostgreSQL's json_to_record preserves nested plain json field text.
func TestJsonToRecordPreservesNestedJsonTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_to_record preserves nested plain json field text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT nested::text
						FROM json_to_record('{"nested":{"b":1,"a":2}}'::json) AS r(nested json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsontorecordpreservesnestedjsontextrepro-0001-select-nested::text-from-json_to_record-{"},
				},
				{
					Query: `SELECT nested::text
						FROM json_to_record('{"nested":{"a":1,"a":2}}'::json) AS r(nested json);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsontorecordpreservesnestedjsontextrepro-0002-select-nested::text-from-json_to_record-{"},
				},
			},
		},
	})
}

// TestJsonPopulateRecordPreservesNestedJsonTextRepro reproduces a JSON
// correctness bug: PostgreSQL's json_populate_record preserves nested plain
// json field text.
func TestJsonPopulateRecordPreservesNestedJsonTextRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_populate_record preserves nested plain json field text",
			SetUpScript: []string{
				`CREATE TYPE json_populate_plain_row AS (nested JSON);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT (json_populate_record(NULL::json_populate_plain_row,
						'{"nested":{"b":1,"a":2}}'::json)).nested::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonpopulaterecordpreservesnestedjsontextrepro-0001-select-json_populate_record-null::json_populate_plain_row-{-nested"},
				},
				{
					Query: `SELECT (json_populate_record(NULL::json_populate_plain_row,
						'{"nested":{"a":1,"a":2}}'::json)).nested::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonpopulaterecordpreservesnestedjsontextrepro-0002-select-json_populate_record-null::json_populate_plain_row-{-nested"},
				},
			},
		},
	})
}

// TestJsonbObjectAggRejectsNonScalarKeysRepro reproduces a JSONB correctness
// bug: PostgreSQL rejects array, composite, and JSON values used as
// jsonb_object_agg keys.
func TestJsonbObjectAggRejectsNonScalarKeysRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_object_agg rejects non-scalar keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_object_agg(k, 2) FROM (VALUES (json '{"a":1}')) AS v(k);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbobjectaggrejectsnonscalarkeysrepro-0001-select-jsonb_object_agg-k-2-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object_agg(k, 2) FROM (VALUES (ARRAY[1,2,3])) AS v(k);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbobjectaggrejectsnonscalarkeysrepro-0002-select-jsonb_object_agg-k-2-from", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object_agg(r, 2) FROM (SELECT 1 AS a, 2 AS b) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbobjectaggrejectsnonscalarkeysrepro-0003-select-jsonb_object_agg-r-2-from",

						// TestJsonObjectAcceptsTwoDimensionalTextArrayRepro reproduces a JSON
						// correctness bug: PostgreSQL accepts json_object(text[]) inputs shaped as a
						// two-dimensional array with two columns.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonObjectAcceptsTwoDimensionalTextArrayRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_object accepts two-dimensional text array",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}'::text[])::jsonb::text,
						jsonb_object('{{a,1},{b,2},{3,NULL},{"d e f","a b c"}}'::text[])::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjectacceptstwodimensionaltextarrayrepro-0001-select-json_object-{{a-1}-{b"},
				},
			},
		},
	})
}

// TestJsonObjectTwoDimensionalArrayRequiresTwoColumnsRepro reproduces a JSON
// correctness bug: PostgreSQL rejects two-dimensional json_object(text[])
// inputs unless each row has exactly two columns.
func TestJsonObjectTwoDimensionalArrayRequiresTwoColumnsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_object two-dimensional text array requires two columns",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_object('{{a},{b}}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwodimensionalarrayrequirestwocolumnsrepro-0001-select-json_object-{{a}-{b}}-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object('{{a},{b}}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwodimensionalarrayrequirestwocolumnsrepro-0002-select-jsonb_object-{{a}-{b}}-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_object('{{a,b,c},{b,c,d}}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwodimensionalarrayrequirestwocolumnsrepro-0003-select-json_object-{{a-b-c}", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object('{{a,b,c},{b,c,d}}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwodimensionalarrayrequirestwocolumnsrepro-0004-select-jsonb_object-{{a-b-c}",

						// TestJsonObjectTwoArrayFormMultidimensionalInputsErrorRepro reproduces a JSON
						// correctness bug: PostgreSQL rejects multidimensional arrays in the two-array
						// json_object(text[], text[]) form with a specific subscript error.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonObjectTwoArrayFormMultidimensionalInputsErrorRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_object two-array form rejects multidimensional inputs",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_object('{{a,1},{b,2}}'::text[], '{1,2}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwoarrayformmultidimensionalinputserrorrepro-0001-select-json_object-{{a-1}-{b", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object('{{a,1},{b,2}}'::text[], '{1,2}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwoarrayformmultidimensionalinputserrorrepro-0002-select-jsonb_object-{{a-1}-{b", Compare: "sqlstate"},
				},
				{
					Query: `SELECT json_object('{a,b}'::text[], '{{1,2},{3,4}}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwoarrayformmultidimensionalinputserrorrepro-0003-select-json_object-{a-b}-::text[]", Compare: "sqlstate"},
				},
				{
					Query: `SELECT jsonb_object('{a,b}'::text[], '{{1,2},{3,4}}'::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonobjecttwoarrayformmultidimensionalinputserrorrepro-0004-select-jsonb_object-{a-b}-::text[]",

						// TestToJsonFloatNonFiniteValuesBecomeStringsRepro reproduces a JSON
						// correctness bug: PostgreSQL converts non-finite float values to JSON strings
						// rather than JSON numbers.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestToJsonFloatNonFiniteValuesBecomeStringsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_json non-finite floats become strings",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_json('NaN'::float8)::text,
						to_json('Infinity'::float8)::text,
						to_json('-Infinity'::float8)::text,
						to_jsonb('NaN'::float8)::text,
						to_jsonb('Infinity'::float8)::text,
						to_jsonb('-Infinity'::float8)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testtojsonfloatnonfinitevaluesbecomestringsrepro-0001-select-to_json-nan-::float8-::text"},
				},
			},
		},
	})
}

// TestJsonBuildersFloatNonFiniteValuesBecomeStringsRepro reproduces a JSON
// correctness bug: PostgreSQL converts non-finite float values to JSON strings
// inside JSON builders.
func TestJsonBuildersFloatNonFiniteValuesBecomeStringsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json builders convert non-finite floats to strings",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_build_array('NaN'::float8, 'Infinity'::float8)::text,
						jsonb_build_array('NaN'::float8, '-Infinity'::float8)::text,
						json_build_object('x', 'NaN'::float8)::text,
						jsonb_build_object('x', 'Infinity'::float8)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbuildersfloatnonfinitevaluesbecomestringsrepro-0001-select-json_build_array-nan-::float8-infinity"},
				},
			},
		},
	})
}

// TestJsonFloatNegativeZeroPreservesJsonSpellingRepro reproduces a JSON
// correctness bug: PostgreSQL preserves negative zero for float8 conversion to
// plain json, while jsonb canonicalizes it.
func TestJsonFloatNegativeZeroPreservesJsonSpellingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "plain json float negative zero preserves spelling",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_json('-0'::float8)::text,
						to_jsonb('-0'::float8)::text,
						json_build_array('-0'::float8)::text,
						jsonb_build_array('-0'::float8)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonfloatnegativezeropreservesjsonspellingrepro-0001-select-to_json-0-::float8-::text"},
				},
			},
		},
	})
}

// TestToJsonMultidimensionalArrayPreservesNestingRepro reproduces a JSON
// correctness bug: PostgreSQL preserves SQL array dimensions when converting
// arrays to JSON.
func TestToJsonMultidimensionalArrayPreservesNestingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_json multidimensional array preserves nesting",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_json('{{1,2},{3,4}}'::int[])::text,
						to_jsonb('{{1,2},{3,4}}'::int[])::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testtojsonmultidimensionalarraypreservesnestingrepro-0001-select-to_json-{{1-2}-{3"},
				},
			},
		},
	})
}

// TestToJsonDateTimestampUsePostgresFormattingRepro reproduces a JSON
// correctness bug: PostgreSQL converts date and timestamp values using their
// type-specific JSON string formats.
func TestToJsonDateTimestampUsePostgresFormattingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_json date and timestamp values use PostgreSQL formatting",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_json('2020-01-02'::date)::text,
						to_jsonb('2020-01-02'::date)::text,
						to_json('2020-01-02 03:04:05.123456'::timestamp)::text,
						to_jsonb('2020-01-02 03:04:05.123456'::timestamp)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testtojsondatetimestampusepostgresformattingrepro-0001-select-to_json-2020-01-02-::date-::text"},
				},
			},
		},
	})
}

// TestJsonBuildersDateTimestampUsePostgresFormattingRepro reproduces a JSON
// correctness bug: PostgreSQL converts date and timestamp values inside JSON
// builders using their type-specific JSON string formats.
func TestJsonBuildersDateTimestampUsePostgresFormattingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json builders date and timestamp values use PostgreSQL formatting",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_build_array('2020-01-02'::date,
							'2020-01-02 03:04:05.123456'::timestamp)::text,
						jsonb_build_array('2020-01-02'::date,
							'2020-01-02 03:04:05.123456'::timestamp)::text,
						json_build_object('d', '2020-01-02'::date,
							'ts', '2020-01-02 03:04:05.123456'::timestamp)::text,
						jsonb_build_object('d', '2020-01-02'::date,
							'ts', '2020-01-02 03:04:05.123456'::timestamp)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbuildersdatetimestampusepostgresformattingrepro-0001-select-json_build_array-2020-01-02-::date-2020-01-02"},
				},
			},
		},
	})
}

// TestToJsonRecordPreservesFieldOrderRepro reproduces a JSON correctness bug:
// PostgreSQL's to_json(record) preserves record field order, unlike to_jsonb.
func TestToJsonRecordPreservesFieldOrderRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "to_json record conversion preserves field order",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_json(r)::text,
						row_to_json(r)::text,
						to_jsonb(r)::text
						FROM (SELECT 1 AS b, 2 AS a) AS r;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testtojsonrecordpreservesfieldorderrepro-0001-select-to_json-r-::text-row_to_json"},
				},
			},
		},
	})
}

// TestJsonbComparisonTypePrecedenceMatchesPostgresRepro reproduces a JSONB
// correctness bug: PostgreSQL's JSONB cross-type ordering differs from
// Doltgres' internal JSON value type precedence.
func TestJsonbComparisonTypePrecedenceMatchesPostgresRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb comparison uses PostgreSQL type precedence",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '[]'::jsonb < 'null'::jsonb,
						'null'::jsonb < '"a"'::jsonb,
						'"a"'::jsonb < '1'::jsonb,
						'1'::jsonb < 'false'::jsonb,
						'false'::jsonb < '{}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbcomparisontypeprecedencematchespostgresrepro-0001-select-[]-::jsonb-<-null"},
				},
			},
		},
	})
}

// TestJsonbPathMatchJsonNullReturnsSqlNullRepro reproduces a jsonpath
// correctness bug: PostgreSQL returns SQL NULL when jsonb_path_match evaluates
// to the JSON null value.
func TestJsonbPathMatchJsonNullReturnsSqlNullRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_path_match maps JSON null to SQL null",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_path_match('null'::jsonb, '$');`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbpathmatchjsonnullreturnssqlnullrepro-0001-select-jsonb_path_match-null-::jsonb-$"},
				},
			},
		},
	})
}

// TestJsonbPathMatchRequiresSingleBooleanResultRepro reproduces a jsonpath
// correctness bug: PostgreSQL rejects path expressions that do not produce a
// single boolean result.
func TestJsonbPathMatchRequiresSingleBooleanResultRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_path_match rejects non-boolean results",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_path_match('[true]'::jsonb, '$');`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbpathmatchrequiressinglebooleanresultrepro-0001-select-jsonb_path_match-[true]-::jsonb-$",

						// TestJsonbPathQueryArrayFilterPredicateRepro reproduces a jsonpath
						// correctness bug: PostgreSQL applies filter predicates inside jsonpath
						// queries, but Doltgres rejects the accepted expression.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbPathQueryArrayFilterPredicateRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_path_query_array applies filter predicates",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_path_query_array('[{"a": 1}, {"a": 2}]'::jsonb,
						'$[*].a ? (@ > 1)')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbpathqueryarrayfilterpredicaterepro-0001-select-jsonb_path_query_array-[{-a-:"},
				},
			},
		},
	})
}

// TestJsonpathPostgres16NumericLiteralSyntaxRepro reproduces a PostgreSQL 16
// compatibility gap: JSONPath numeric literals support underscore separators
// and non-decimal integer prefixes.
func TestJsonpathPostgres16NumericLiteralSyntaxRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "JSONPath accepts PostgreSQL 16 numeric literal syntax",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_path_match('1000'::jsonb, '$ == 1_000'),
						jsonb_path_match('16'::jsonb, '$ == 0x10'),
						jsonb_path_match('8'::jsonb, '$ == 0o10'),
						jsonb_path_match('10'::jsonb, '$ == 0b1010');`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonpathpostgres16numericliteralsyntaxrepro-0001-select-jsonb_path_match-1000-::jsonb-$"},
				},
			},
		},
	})
}

// TestJsonbNestedArrayContainmentGuard guards PostgreSQL JSONB containment
// semantics for nested array values.
func TestJsonbNestedArrayContainmentGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb containment matches nested array elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '[1,[2,3],4]'::jsonb @> '[[2,3]]'::jsonb,
						'[[2,3]]'::jsonb <@ '[1,[2,3],4]'::jsonb,
						'{"a":[1,[2,3]]}'::jsonb @> '{"a":[[2,3]]}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbnestedarraycontainmentguard-0001-select-[1-[2-3]-4]"},
				},
			},
		},
	})
}

// TestJsonbArrayContainmentSemanticsGuard guards PostgreSQL JSONB array
// containment semantics for scalar membership, order-insensitive array
// containment, and duplicate array elements.
func TestJsonbArrayContainmentSemanticsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb array containment uses PostgreSQL containment semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '[1,2,3]'::jsonb @> '2'::jsonb,
						'2'::jsonb <@ '[1,2,3]'::jsonb,
						'[1]'::jsonb @> '[1,1]'::jsonb,
						'[1,2,3]'::jsonb @> '[3,1]'::jsonb,
						'[1,2,3]'::jsonb @> '[[1]]'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbarraycontainmentsemanticsguard-0001-select-[1-2-3]-::jsonb"},
				},
			},
		},
	})
}

// TestJsonbObjectContainmentSemanticsGuard guards PostgreSQL JSONB object
// containment semantics for top-level object keys.
func TestJsonbObjectContainmentSemanticsGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb object containment uses PostgreSQL containment semantics",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '{"a":1, "b":2}'::jsonb @> '{"b":2}'::jsonb,
						'{"b":2}'::jsonb <@ '{"a":1, "b":2}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbobjectcontainmentsemanticsguard-0001-select-{-a-:1-b"},
				},
			},
		},
	})
}

// TestJsonbSetEmptyPathReplacesWholeDocumentRepro reproduces a JSONB
// correctness bug: PostgreSQL treats an empty jsonb_set path as the whole target
// document and replaces it with the new value.
func TestJsonbSetEmptyPathReplacesWholeDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_set empty path replaces whole document",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_set('{"a":1}'::jsonb, '{}', '2'::jsonb)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsetemptypathreplaceswholedocumentrepro-0001-select-jsonb_set-{-a-:1}"},
				},
			},
		},
	})
}

// TestJsonbSetEmptyPathUpdateReplacesStoredDocumentRepro reproduces a JSONB
// persistence bug: jsonb_set with an empty path should replace the stored JSONB
// document when used in UPDATE expressions.
func TestJsonbSetEmptyPathUpdateReplacesStoredDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_set empty path update replaces stored document",
			SetUpScript: []string{
				`CREATE TABLE jsonb_set_empty_path_items (
					id INT PRIMARY KEY,
					doc JSONB
				);`,
				`INSERT INTO jsonb_set_empty_path_items VALUES (1, '{"a":1}'::jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE jsonb_set_empty_path_items
						SET doc = jsonb_set(doc, '{}', '2'::jsonb)
						WHERE id = 1;`,
				},
				{
					Query: `SELECT doc::text FROM jsonb_set_empty_path_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsetemptypathupdatereplacesstoreddocumentrepro-0001-select-doc::text-from-jsonb_set_empty_path_items"},
				},
			},
		},
	})
}

// TestJsonbSetLaxEmptyPathReplacesWholeDocumentRepro reproduces a JSONB
// correctness bug: jsonb_set_lax with a non-null replacement value should share
// jsonb_set's empty-path whole-document replacement semantics.
func TestJsonbSetLaxEmptyPathReplacesWholeDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_set_lax empty path replaces whole document",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_set_lax('{"a":1}'::jsonb, '{}', '2'::jsonb)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsetlaxemptypathreplaceswholedocumentrepro-0001-select-jsonb_set_lax-{-a-:1}"},
				},
			},
		},
	})
}

// TestJsonbExistsDecodesEscapedStringElementsRepro reproduces a JSONB
// correctness bug: PostgreSQL JSONB existence operators compare decoded JSON
// string values against the SQL text probe.
func TestJsonbExistsDecodesEscapedStringElementsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb existence operators decode escaped string values",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '["a\nb"]'::jsonb ? E'a\nb',
						'"a\nb"'::jsonb ? E'a\nb',
						'["a\nb","c"]'::jsonb ?| ARRAY[E'a\nb'],
						'["a\nb","c"]'::jsonb ?& ARRAY[E'a\nb','c'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbexistsdecodesescapedstringelementsrepro-0001-select-[-a\\nb-]-::jsonb"},
				},
			},
		},
	})
}

// TestJsonbSetRejectsScalarTargetRepro reproduces a JSONB correctness bug:
// PostgreSQL rejects attempts to set a nested path inside a scalar JSONB value.
func TestJsonbSetRejectsScalarTargetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_set rejects scalar targets",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_set('"a"'::jsonb, '{a}', '"b"'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsetrejectsscalartargetrepro-0001-select-jsonb_set-a-::jsonb-{a}",

						// TestJsonbSetArrayPathRequiresIntegerRepro reproduces a JSONB correctness bug:
						// PostgreSQL rejects non-integer path elements when the path descends through a
						// JSONB array.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbSetArrayPathRequiresIntegerRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_set array path elements must be integers",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_set('{"a":[1,2,3]}'::jsonb,
						'{a,not_an_int}', '"new_value"'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsetarraypathrequiresintegerrepro-0001-select-jsonb_set-{-a-:[1",

						// TestJsonbInsertRejectsScalarTargetRepro reproduces a JSONB correctness bug:
						// PostgreSQL rejects attempts to insert a nested path inside a scalar JSONB
						// value.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbInsertRejectsScalarTargetRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_insert rejects scalar targets",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_insert('"a"'::jsonb, '{a}', '"b"'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbinsertrejectsscalartargetrepro-0001-select-jsonb_insert-a-::jsonb-{a}",

						// TestJsonbInsertRejectsExistingObjectKeyRepro reproduces a JSONB correctness
						// bug: PostgreSQL rejects jsonb_insert when the target object key already
						// exists.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbInsertRejectsExistingObjectKeyRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_insert rejects existing object keys",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT jsonb_insert('{"a":1}'::jsonb, '{a}', '2'::jsonb);`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbinsertrejectsexistingobjectkeyrepro-0001-select-jsonb_insert-{-a-:1}",

						// TestJsonbExtractPathNullElementReturnsNullGuard guards PostgreSQL JSONB path
						// extraction semantics: a NULL path element returns SQL NULL for both JSONB and
						// text extraction operators.
						Compare: "sqlstate"},
				},
			},
		},
	})
}

func TestJsonbExtractPathNullElementReturnsNullGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb path extraction null path element returns null",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '{"a":{"b":{"c":"foo"}}}'::jsonb #> ARRAY['a', NULL]::text[],
						'{"a":{"b":{"c":"foo"}}}'::jsonb #>> ARRAY['a', NULL]::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbextractpathnullelementreturnsnullguard-0001-select-{-a-:{-b"},
				},
			},
		},
	})
}

// TestJsonbSubscriptUpdatePersistsNestedDocumentRepro reproduces JSONB
// query and persistence bugs: PostgreSQL jsonb subscripting reads nested values,
// and subscripting assignment rewrites the stored document at the addressed
// path.
func TestJsonbSubscriptUpdatePersistsNestedDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb subscript reads and updates stored document",
			SetUpScript: []string{
				`CREATE TABLE jsonb_subscript_update_items (
					id INT PRIMARY KEY,
					doc JSONB
				);`,
				`INSERT INTO jsonb_subscript_update_items VALUES
					(1, '{"a":{"b":1},"keep":true}'::jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT doc['a']['b']::text FROM jsonb_subscript_update_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsubscriptupdatepersistsnesteddocumentrepro-0001-select-doc[-a-][-b"},
				},
				{
					Query: `UPDATE jsonb_subscript_update_items
						SET doc['a']['b'] = '2'::jsonb
						WHERE id = 1;`,
				},
				{
					Query: `SELECT doc::text FROM jsonb_subscript_update_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsubscriptupdatepersistsnesteddocumentrepro-0002-select-doc::text-from-jsonb_subscript_update_items"},
				},
			},
		},
	})
}

// TestJsonbDeleteOperatorPersistsStoredDocumentRepro guards PostgreSQL JSONB
// persistence semantics: the jsonb delete operator removes an object key and
// persists the rewritten document when used in UPDATE.
func TestJsonbDeleteOperatorPersistsStoredDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb delete operator updates stored document",
			SetUpScript: []string{
				`CREATE TABLE jsonb_delete_operator_items (
					id INT PRIMARY KEY,
					doc JSONB
				);`,
				`INSERT INTO jsonb_delete_operator_items VALUES
					(1, '{"keep":true,"remove":{"nested":1}}'::jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE jsonb_delete_operator_items
						SET doc = doc - 'remove'
						WHERE id = 1;`,
				},
				{
					Query: `SELECT doc::text FROM jsonb_delete_operator_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbdeleteoperatorpersistsstoreddocumentrepro-0001-select-doc::text-from-jsonb_delete_operator_items"},
				},
			},
		},
	})
}

// TestJsonbInsertPersistsNestedArrayUpdateGuard guards PostgreSQL JSONB
// persistence semantics: jsonb_insert rewrites nested arrays at the requested
// insertion position when used in UPDATE.
func TestJsonbInsertPersistsNestedArrayUpdateGuard(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_insert updates nested arrays",
			SetUpScript: []string{
				`CREATE TABLE jsonb_insert_update_items (
					id INT PRIMARY KEY,
					doc JSONB
				);`,
				`INSERT INTO jsonb_insert_update_items VALUES
					(1, '{"a":[1,3],"keep":true}'::jsonb),
					(2, '{"a":[1,3],"keep":true}'::jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE jsonb_insert_update_items
						SET doc = jsonb_insert(doc, '{a,1}', '2'::jsonb)
						WHERE id = 1;`,
				},
				{
					Query: `UPDATE jsonb_insert_update_items
						SET doc = jsonb_insert(doc, '{a,1}', '2'::jsonb, true)
						WHERE id = 2;`,
				},
				{
					Query: `SELECT id, doc::text
						FROM jsonb_insert_update_items
						ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbinsertpersistsnestedarrayupdateguard-0001-select-id-doc::text-from-jsonb_insert_update_items"},
				},
			},
		},
	})
}

// TestJsonbSetLaxDeleteKeyPersistsStoredDocumentRepro guards PostgreSQL JSONB
// persistence semantics: jsonb_set_lax with null_value_treatment =>
// 'delete_key' should remove the addressed key when used in an UPDATE.
func TestJsonbSetLaxDeleteKeyPersistsStoredDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "jsonb_set_lax delete_key updates stored document",
			SetUpScript: []string{
				`CREATE TABLE jsonb_set_lax_delete_key_items (
					id INT PRIMARY KEY,
					doc JSONB
				);`,
				`INSERT INTO jsonb_set_lax_delete_key_items VALUES
					(1, '{"keep":true,"remove":{"nested":1}}'::jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `UPDATE jsonb_set_lax_delete_key_items
						SET doc = jsonb_set_lax(doc, '{remove}', NULL, true, 'delete_key')
						WHERE id = 1;`,
				},
				{
					Query: `SELECT doc::text FROM jsonb_set_lax_delete_key_items;`, PostgresOracle: ScriptTestPostgresOracle{ID: "json-correctness-repro-test-testjsonbsetlaxdeletekeypersistsstoreddocumentrepro-0001-select-doc::text-from-jsonb_set_lax_delete_key_items"},
				},
			},
		},
	})
}
