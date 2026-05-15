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

// PostgreSQL's built-in text-search functions can parse documents and queries
// using the built-in simple configuration. Doltgres has catalog OIDs for these
// functions but does not currently execute them.
func TestBuiltInTextSearchFunctionsMatchTermsRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "built-in text-search functions match terms",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT to_tsvector('simple'::regconfig, 'jumped cats')::text,
							to_tsquery('simple'::regconfig, 'jumped & cats')::text,
							to_tsvector('simple'::regconfig, 'jumped cats') @@
								to_tsquery('simple'::regconfig, 'cats');`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testbuiltintextsearchfunctionsmatchtermsrepro-0001-select-to_tsvector-simple-::regconfig-jumped"},
				},
			},
		},
	})
}

// TestPlainToTsQueryFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes plainto_tsquery for turning plain text into a tsquery.
func TestPlainToTsQueryFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "plainto_tsquery parses plain text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT plainto_tsquery('simple'::regconfig, 'fat cats')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testplaintotsqueryfunctionrepro-0001-select-plainto_tsquery-simple-::regconfig-fat"},
				},
			},
		},
	})
}

// TestPhraseToTsQueryFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes phraseto_tsquery for turning plain text phrases into
// phrase-distance tsquery values.
func TestPhraseToTsQueryFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "phraseto_tsquery parses phrase text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT phraseto_tsquery('simple'::regconfig, 'fat cats')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testphrasetotsqueryfunctionrepro-0001-select-phraseto_tsquery-simple-::regconfig-fat"},
				},
			},
		},
	})
}

// TestWebsearchToTsQueryFunctionRepro reproduces a full-text search
// compatibility gap: PostgreSQL exposes websearch_to_tsquery for accepting
// web-search-style query text.
func TestWebsearchToTsQueryFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "websearch_to_tsquery parses web search text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT websearch_to_tsquery('simple'::regconfig, 'fat cat')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testwebsearchtotsqueryfunctionrepro-0001-select-websearch_to_tsquery-simple-::regconfig-fat"},
				},
			},
		},
	})
}

// TestTsHeadlineFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes ts_headline for highlighting text fragments that match a
// tsquery.
func TestTsHeadlineFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ts_headline highlights matching text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_headline('simple'::regconfig, 'fat cats ate rats', to_tsquery('simple'::regconfig, 'cats'))::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsheadlinefunctionrepro-0001-select-ts_headline-simple-::regconfig-fat"},
				},
			},
		},
	})
}

// TestTsRankFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes ts_rank for scoring a tsvector against a tsquery.
func TestTsRankFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ts_rank scores matching documents",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_rank(
						to_tsvector('simple'::regconfig, 'fat cats ate rats'),
						to_tsquery('simple'::regconfig, 'cats')
					) > 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsrankfunctionrepro-0001-select-ts_rank-to_tsvector-simple-::regconfig"},
				},
			},
		},
	})
}

// TestTsRankCdFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes ts_rank_cd for cover-density ranking of a tsvector against
// a tsquery.
func TestTsRankCdFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ts_rank_cd scores matching documents",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_rank_cd(
						to_tsvector('simple'::regconfig, 'fat cats ate rats'),
						to_tsquery('simple'::regconfig, 'cats')
					) > 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsrankcdfunctionrepro-0001-select-ts_rank_cd-to_tsvector-simple-::regconfig"},
				},
			},
		},
	})
}

// TestTsVectorToArrayFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes tsvector_to_array for extracting lexemes from a
// tsvector.
func TestTsVectorToArrayFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "tsvector_to_array extracts lexemes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tsvector_to_array(to_tsvector('simple'::regconfig, 'fat cats'))::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsvectortoarrayfunctionrepro-0001-select-tsvector_to_array-to_tsvector-simple-::regconfig"},
				},
			},
		},
	})
}

// TestArrayToTsVectorFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes array_to_tsvector for constructing a tsvector from
// lexeme arrays.
func TestArrayToTsVectorFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "array_to_tsvector constructs sorted lexemes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT array_to_tsvector(ARRAY['foo', 'bar', 'baz', 'bar'])::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testarraytotsvectorfunctionrepro-0001-select-array_to_tsvector-array[-foo-bar"},
				},
			},
		},
	})
}

// TestTsDeleteFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes ts_delete for removing lexemes from a tsvector.
func TestTsDeleteFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ts_delete removes a lexeme",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_delete(to_tsvector('simple'::regconfig, 'fat cats'), 'cats')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsdeletefunctionrepro-0001-select-ts_delete-to_tsvector-simple-::regconfig"},
				},
			},
		},
	})
}

// TestSetWeightFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes setweight for assigning weights to tsvector lexemes.
func TestSetWeightFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "setweight assigns lexeme weights",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT setweight(to_tsvector('simple'::regconfig, 'fat cats'), 'A')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testsetweightfunctionrepro-0001-select-setweight-to_tsvector-simple-::regconfig"},
				},
			},
		},
	})
}

// TestStripTsVectorFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes strip for removing position and weight data from a
// tsvector.
func TestStripTsVectorFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "strip removes tsvector positions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT strip(to_tsvector('simple'::regconfig, 'fat cats'))::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-teststriptsvectorfunctionrepro-0001-select-strip-to_tsvector-simple-::regconfig"},
				},
			},
		},
	})
}

// TestNumNodeFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes numnode for counting lexeme and operator nodes in a
// tsquery.
func TestNumNodeFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "numnode counts tsquery nodes",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT numnode(to_tsquery('simple'::regconfig, 'fat & cats'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testnumnodefunctionrepro-0001-select-numnode-to_tsquery-simple-::regconfig"},
				},
			},
		},
	})
}

// TestQueryTreeFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes querytree for extracting the indexable portion of a
// tsquery.
func TestQueryTreeFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "querytree returns indexable tsquery text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT querytree(to_tsquery('simple'::regconfig, 'fat & cats'));`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testquerytreefunctionrepro-0001-select-querytree-to_tsquery-simple-::regconfig"},
				},
			},
		},
	})
}

// TestTsQueryPhraseFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes tsquery_phrase for building phrase-distance tsquery
// values from two tsquery inputs.
func TestTsQueryPhraseFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "tsquery_phrase builds phrase query",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT tsquery_phrase(
						to_tsquery('simple'::regconfig, 'fat'),
						to_tsquery('simple'::regconfig, 'cats')
					)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsqueryphrasefunctionrepro-0001-select-tsquery_phrase-to_tsquery-simple-::regconfig"},
				},
			},
		},
	})
}

// TestTsRewriteFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes ts_rewrite for substituting parts of a tsquery.
func TestTsRewriteFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ts_rewrite substitutes tsquery terms",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_rewrite(
						to_tsquery('simple'::regconfig, 'fat'),
						to_tsquery('simple'::regconfig, 'fat'),
						to_tsquery('simple'::regconfig, 'cat')
					)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsrewritefunctionrepro-0001-select-ts_rewrite-to_tsquery-simple-::regconfig"},
				},
			},
		},
	})
}

// TestTsFilterFunctionRepro reproduces a full-text search compatibility gap:
// PostgreSQL exposes ts_filter for keeping only selected tsvector weights.
func TestTsFilterFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ts_filter keeps selected tsvector weights",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT ts_filter('base:1A hidden:2B rebel:3A'::tsvector, '{a}')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testtsfilterfunctionrepro-0001-select-ts_filter-base:1a-hidden:2b-rebel:3a"},
				},
			},
		},
	})
}

// TestJsonToTsVectorFunctionRepro reproduces a full-text search compatibility
// gap: PostgreSQL exposes json_to_tsvector for indexing JSON documents with a
// selectable token filter.
func TestJsonToTsVectorFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "json_to_tsvector extracts JSON string tokens",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT json_to_tsvector('english'::regconfig, '{"a": "aaa in bbb"}'::json, '"string"')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testjsontotsvectorfunctionrepro-0001-select-json_to_tsvector-english-::regconfig-{"},
				},
			},
		},
	})
}

// PostgreSQL allows user-defined text-search configurations and makes them
// available to full-text functions. Doltgres currently rejects the definition
// syntax before it can persist the catalog object.
func TestCreateTextSearchConfigurationCopyIsUsableRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "CREATE TEXT SEARCH CONFIGURATION copy is usable",
			SetUpScript: []string{
				`CREATE TEXT SEARCH CONFIGURATION copied_simple_config (COPY = pg_catalog.simple);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT cfgname
						FROM pg_catalog.pg_ts_config
						WHERE cfgname = 'copied_simple_config';`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testcreatetextsearchconfigurationcopyisusablerepro-0001-select-cfgname-from-pg_catalog.pg_ts_config-where"},
				},
				{
					Query: `SELECT to_tsvector('copied_simple_config', 'jumped cats');`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testcreatetextsearchconfigurationcopyisusablerepro-0002-select-to_tsvector-copied_simple_config-jumped-cats"},
				},
			},
		},
	})
}

// TestDropTextSearchConfigurationIfExistsMissingRepro reproduces a
// compatibility gap: PostgreSQL accepts DROP TEXT SEARCH CONFIGURATION IF
// EXISTS for absent configurations.
func TestDropTextSearchConfigurationIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TEXT SEARCH CONFIGURATION IF EXISTS missing config succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TEXT SEARCH CONFIGURATION IF EXISTS missing_ts_config_repro;`,
				},
			},
		},
	})
}

// TestDropTextSearchConfigurationIfExistsDropsExistingRepro reproduces a
// catalog consistency bug: DROP TEXT SEARCH CONFIGURATION IF EXISTS is
// currently converted to a no-op, so an existing configuration remains visible.
func TestDropTextSearchConfigurationIfExistsDropsExistingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TEXT SEARCH CONFIGURATION IF EXISTS removes existing config",
			SetUpScript: []string{
				`CREATE TEXT SEARCH CONFIGURATION drop_existing_ts_config_repro
					(COPY = pg_catalog.simple);`,
				`DROP TEXT SEARCH CONFIGURATION IF EXISTS drop_existing_ts_config_repro;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT COUNT(*)
						FROM pg_catalog.pg_ts_config
						WHERE cfgname = 'drop_existing_ts_config_repro'
						  AND cfgnamespace = 'public'::regnamespace;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testdroptextsearchconfigurationifexistsdropsexistingrepro-0001-select-count-*-from-pg_catalog.pg_ts_config"},
				},
			},
		},
	})
}

// TestDropTextSearchDictionaryIfExistsMissingRepro reproduces a compatibility
// gap: PostgreSQL accepts DROP TEXT SEARCH DICTIONARY IF EXISTS for absent
// dictionaries.
func TestDropTextSearchDictionaryIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TEXT SEARCH DICTIONARY IF EXISTS missing dictionary succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TEXT SEARCH DICTIONARY IF EXISTS missing_ts_dictionary_repro;`,
				},
			},
		},
	})
}

// TestDropTextSearchParserIfExistsMissingRepro reproduces a compatibility gap:
// PostgreSQL accepts DROP TEXT SEARCH PARSER IF EXISTS for absent parsers.
func TestDropTextSearchParserIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TEXT SEARCH PARSER IF EXISTS missing parser succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TEXT SEARCH PARSER IF EXISTS missing_ts_parser_repro;`,
				},
			},
		},
	})
}

// TestDropTextSearchTemplateIfExistsMissingRepro reproduces a compatibility
// gap: PostgreSQL accepts DROP TEXT SEARCH TEMPLATE IF EXISTS for absent
// templates.
func TestDropTextSearchTemplateIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP TEXT SEARCH TEMPLATE IF EXISTS missing template succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP TEXT SEARCH TEMPLATE IF EXISTS missing_ts_template_repro;`,
				},
			},
		},
	})
}

// TestAlterTextSearchObjectsReachMissingObjectValidationRepro reproduces a
// compatibility gap: PostgreSQL supports ALTER TEXT SEARCH object statements
// and validates that the target object exists.
func TestAlterTextSearchObjectsReachMissingObjectValidationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "ALTER TEXT SEARCH objects validate missing targets",
			Assertions: []ScriptTestAssertion{
				{
					Query: `ALTER TEXT SEARCH CONFIGURATION missing_ts_config_repro RENAME TO renamed_ts_config_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testaltertextsearchobjectsreachmissingobjectvalidationrepro-0001-alter-text-search-configuration-missing_ts_config_repro", Compare: "sqlstate"},
				},
				{
					Query: `ALTER TEXT SEARCH DICTIONARY missing_ts_dictionary_repro RENAME TO renamed_ts_dictionary_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testaltertextsearchobjectsreachmissingobjectvalidationrepro-0002-alter-text-search-dictionary-missing_ts_dictionary_repro", Compare: "sqlstate"},
				},
				{
					Query: `ALTER TEXT SEARCH PARSER missing_ts_parser_repro RENAME TO renamed_ts_parser_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testaltertextsearchobjectsreachmissingobjectvalidationrepro-0003-alter-text-search-parser-missing_ts_parser_repro", Compare: "sqlstate"},
				},
				{
					Query: `ALTER TEXT SEARCH TEMPLATE missing_ts_template_repro RENAME TO renamed_ts_template_repro;`, PostgresOracle: ScriptTestPostgresOracle{ID: "text-search-definition-repro-test-testaltertextsearchobjectsreachmissingobjectvalidationrepro-0004-alter-text-search-template-missing_ts_template_repro", Compare: "sqlstate"},
				},
			},
		},
	})
}
