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

package _go

import "testing"

// TestDropOperatorClassIfExistsMissingRepro reproduces a compatibility gap:
// PostgreSQL accepts DROP OPERATOR CLASS IF EXISTS for absent operator classes.
func TestDropOperatorClassIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP OPERATOR CLASS IF EXISTS missing class succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP OPERATOR CLASS IF EXISTS missing_operator_class_repro USING btree;`,
				},
			},
		},
	})
}

// TestDropOperatorFamilyIfExistsMissingRepro reproduces a compatibility gap:
// PostgreSQL accepts DROP OPERATOR FAMILY IF EXISTS for absent operator
// families.
func TestDropOperatorFamilyIfExistsMissingRepro(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name: "DROP OPERATOR FAMILY IF EXISTS missing family succeeds",
			Assertions: []ScriptTestAssertion{
				{
					Query: `DROP OPERATOR FAMILY IF EXISTS missing_operator_family_repro USING btree;`,
				},
			},
		},
	})
}
