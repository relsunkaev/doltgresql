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

// TestXmlTypeColumnCreationRepro reproduces an XML type compatibility gap:
// PostgreSQL exposes XML as a built-in type that can be used in table
// definitions.
func TestXmlTypeColumnCreationRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "XML type can be used in table definitions",
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE TABLE xml_type_items (
						id INT PRIMARY KEY,
						doc XML
					);`,
				},
			},
		},
	})
}

// TestXmlParseDocumentRepro reproduces an XML function compatibility gap:
// PostgreSQL supports SQL-standard XMLPARSE for constructing XML values.
func TestXmlParseDocumentRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "XMLPARSE document returns XML text",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT XMLPARSE(DOCUMENT '<doc/>')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "xml-correctness-repro-test-testxmlparsedocumentrepro-0001-select-xmlparse-document-<doc/>-::text"},
				},
			},
		},
	})
}

// TestXmlTextFunctionRepro reproduces an XML function compatibility gap:
// PostgreSQL exposes xmltext for constructing an XML text node with predefined
// entities escaped.
func TestXmlTextFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "xmltext escapes XML text-node content",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT xmltext('< foo & bar >')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "xml-correctness-repro-test-testxmltextfunctionrepro-0001-select-xmltext-<-foo-&"},
				},
			},
		},
	})
}

// TestXmlCommentFunctionRepro reproduces an XML function compatibility gap:
// PostgreSQL exposes xmlcomment for constructing XML comment nodes.
func TestXmlCommentFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "xmlcomment constructs XML comments",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT xmlcomment('hello')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "xml-correctness-repro-test-testxmlcommentfunctionrepro-0001-select-xmlcomment-hello-::text"},
				},
			},
		},
	})
}

// TestXmlElementFunctionRepro reproduces an XML function compatibility gap:
// PostgreSQL exposes xmlelement for constructing XML elements.
func TestXmlElementFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "xmlelement constructs XML elements",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT xmlelement(name foo)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "xml-correctness-repro-test-testxmlelementfunctionrepro-0001-select-xmlelement-name-foo-::text"},
				},
			},
		},
	})
}

// TestXmlForestFunctionRepro reproduces an XML function compatibility gap:
// PostgreSQL exposes xmlforest for constructing XML content fragments from
// scalar values.
func TestXmlForestFunctionRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "xmlforest constructs XML fragments",
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT xmlforest('abc' AS foo, 123 AS bar)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "xml-correctness-repro-test-testxmlforestfunctionrepro-0001-select-xmlforest-abc-as-foo"},
				},
			},
		},
	})
}
