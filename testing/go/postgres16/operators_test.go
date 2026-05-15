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

package postgres16

import (
	. "github.com/dolthub/doltgresql/testing/go"

	"testing"
)

func TestOperators(t *testing.T) {
	RunScriptsWithoutNormalization(t, []ScriptTest{
		{
			Name: "Addition",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1::float4 + 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0001-select-1::float4-+-2::float4"},
				},
				{
					Query: `SELECT 1::float4 + 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0002-select-1::float4-+-2::float8"},
				},
				{
					Query: `SELECT 1::float4 + 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0003-select-1::float4-+-2::int2"},
				},
				{
					Query: `SELECT 1::float4 + 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0004-select-1::float4-+-2::int4"},
				},
				{
					Query: `SELECT 1::float4 + 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0005-select-1::float4-+-2::int8"},
				},
				{
					Query: `SELECT 1::float4 + 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0006-select-1::float4-+-2::numeric"},
				},
				{
					Query: `SELECT 1::float8 + 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0007-select-1::float8-+-2::float4"},
				},
				{
					Query: `SELECT 1::float8 + 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0008-select-1::float8-+-2::float8"},
				},
				{
					Query: `SELECT 1::float8 + 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0009-select-1::float8-+-2::int2"},
				},
				{
					Query: `SELECT 1::float8 + 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0010-select-1::float8-+-2::int4"},
				},
				{
					Query: `SELECT 1::float8 + 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0011-select-1::float8-+-2::int8"},
				},
				{
					Query: `SELECT 1::float8 + 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0012-select-1::float8-+-2::numeric"},
				},
				{
					Query: `SELECT 1::int2 + 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0013-select-1::int2-+-2::float4"},
				},
				{
					Query: `SELECT 1::int2 + 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0014-select-1::int2-+-2::float8"},
				},
				{
					Query: `SELECT 1::int2 + 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0015-select-1::int2-+-2::int2"},
				},
				{
					Query: `SELECT 1::int2 + 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0016-select-1::int2-+-2::int4"},
				},
				{
					Query: `SELECT 1::int2 + 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0017-select-1::int2-+-2::int8"},
				},
				{
					Query: `SELECT 1::int2 + 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0018-select-1::int2-+-2::numeric"},
				},
				{
					Query: `SELECT 1::int4 + 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0019-select-1::int4-+-2::float4"},
				},
				{
					Query: `SELECT 1::int4 + 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0020-select-1::int4-+-2::float8"},
				},
				{
					Query: `SELECT 1::int4 + 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0021-select-1::int4-+-2::int2"},
				},
				{
					Query: `SELECT 1::int4 + 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0022-select-1::int4-+-2::int4"},
				},
				{
					Query: `SELECT 1::int4 + 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0023-select-1::int4-+-2::int8"},
				},
				{
					Query: `SELECT 1::int4 + 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0024-select-1::int4-+-2::numeric"},
				},
				{
					Query: `SELECT 1::int8 + 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0025-select-1::int8-+-2::float4"},
				},
				{
					Query: `SELECT 1::int8 + 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0026-select-1::int8-+-2::float8"},
				},
				{
					Query: `SELECT 1::int8 + 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0027-select-1::int8-+-2::int2"},
				},
				{
					Query: `SELECT 1::int8 + 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0028-select-1::int8-+-2::int4"},
				},
				{
					Query: `SELECT 1::int8 + 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0029-select-1::int8-+-2::int8"},
				},
				{
					Query: `SELECT 1::int8 + 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0030-select-1::int8-+-2::numeric"},
				},
				{
					Query: `SELECT 1::numeric + 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0031-select-1::numeric-+-2::float4"},
				},
				{
					Query: `SELECT 1::numeric + 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0032-select-1::numeric-+-2::float8"},
				},
				{
					Query: `SELECT 1::numeric + 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0033-select-1::numeric-+-2::int2"},
				},
				{
					Query: `SELECT 1::numeric + 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0034-select-1::numeric-+-2::int4"},
				},
				{
					Query: `SELECT 1::numeric + 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0035-select-1::numeric-+-2::int8"},
				},
				{
					Query: `SELECT 1::numeric + 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0036-select-1::numeric-+-2::numeric"},
				},
				{
					Query: `select interval '2 days' + interval '1.5 days';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0037-select-interval-2-days-+"},
				},
				{
					Query: `select interval '2 days' + time '12:23:34';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0038-select-interval-2-days-+"},
				},
				{
					Query: `select interval '2 days' + date '2022-2-5';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0039-select-interval-2-days-+"},
				},
				{
					Query: `select interval '2 days' + time with time zone '12:23:45-0700';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0040-select-interval-2-days-+"},
				},
				{
					Query: `select interval '2 days' + timestamp '2021-04-08 12:23:45';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0041-select-interval-2-days-+"},
				},
				{
					Query: `select interval '2 days' + timestamp with time zone '2021-04-08 12:23:45-0700';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0042-select-interval-2-days-+"},
				},
			},
		},
		{
			Name: "Subtraction",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1::float4 - 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0043-select-1::float4-2::float4"},
				},
				{
					Query: `SELECT 1::float4 - 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0044-select-1::float4-2::float8"},
				},
				{
					Query: `SELECT 1::float4 - 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0045-select-1::float4-2::int2"},
				},
				{
					Query: `SELECT 1::float4 - 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0046-select-1::float4-2::int4"},
				},
				{
					Query: `SELECT 1::float4 - 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0047-select-1::float4-2::int8"},
				},
				{
					Query: `SELECT 1::float4 - 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0048-select-1::float4-2::numeric"},
				},
				{
					Query: `SELECT 1::float8 - 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0049-select-1::float8-2::float4"},
				},
				{
					Query: `SELECT 1::float8 - 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0050-select-1::float8-2::float8"},
				},
				{
					Query: `SELECT 1::float8 - 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0051-select-1::float8-2::int2"},
				},
				{
					Query: `SELECT 1::float8 - 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0052-select-1::float8-2::int4"},
				},
				{
					Query: `SELECT 1::float8 - 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0053-select-1::float8-2::int8"},
				},
				{
					Query: `SELECT 1::float8 - 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0054-select-1::float8-2::numeric"},
				},
				{
					Query: `SELECT 1::int2 - 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0055-select-1::int2-2::float4"},
				},
				{
					Query: `SELECT 1::int2 - 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0056-select-1::int2-2::float8"},
				},
				{
					Query: `SELECT 1::int2 - 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0057-select-1::int2-2::int2"},
				},
				{
					Query: `SELECT 1::int2 - 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0058-select-1::int2-2::int4"},
				},
				{
					Query: `SELECT 1::int2 - 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0059-select-1::int2-2::int8"},
				},
				{
					Query: `SELECT 1::int2 - 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0060-select-1::int2-2::numeric"},
				},
				{
					Query: `SELECT 1::int4 - 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0061-select-1::int4-2::float4"},
				},
				{
					Query: `SELECT 1::int4 - 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0062-select-1::int4-2::float8"},
				},
				{
					Query: `SELECT 1::int4 - 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0063-select-1::int4-2::int2"},
				},
				{
					Query: `SELECT 1::int4 - 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0064-select-1::int4-2::int4"},
				},
				{
					Query: `SELECT 1::int4 - 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0065-select-1::int4-2::int8"},
				},
				{
					Query: `SELECT 1::int4 - 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0066-select-1::int4-2::numeric"},
				},
				{
					Query: `SELECT 1::int8 - 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0067-select-1::int8-2::float4"},
				},
				{
					Query: `SELECT 1::int8 - 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0068-select-1::int8-2::float8"},
				},
				{
					Query: `SELECT 1::int8 - 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0069-select-1::int8-2::int2"},
				},
				{
					Query: `SELECT 1::int8 - 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0070-select-1::int8-2::int4"},
				},
				{
					Query: `SELECT 1::int8 - 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0071-select-1::int8-2::int8"},
				},
				{
					Query: `SELECT 1::int8 - 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0072-select-1::int8-2::numeric"},
				},
				{
					Query: `SELECT 1::numeric - 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0073-select-1::numeric-2::float4"},
				},
				{
					Query: `SELECT 1::numeric - 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0074-select-1::numeric-2::float8"},
				},
				{
					Query: `SELECT 1::numeric - 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0075-select-1::numeric-2::int2"},
				},
				{
					Query: `SELECT 1::numeric - 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0076-select-1::numeric-2::int4"},
				},
				{
					Query: `SELECT 1::numeric - 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0077-select-1::numeric-2::int8"},
				},
				{
					Query: `SELECT 1::numeric - 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0078-select-1::numeric-2::numeric"},
				},
				{
					Query: `select interval '2 days' - interval '1.5 days';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0079-select-interval-2-days-interval"},
				},
			},
		},
		{
			Name: "Multiplication",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 1::float4 * 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0080-select-1::float4-*-2::float4"},
				},
				{
					Query: `SELECT 1::float4 * 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0081-select-1::float4-*-2::float8"},
				},
				{
					Query: `SELECT 1::float4 * 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0082-select-1::float4-*-2::int2"},
				},
				{
					Query: `SELECT 1::float4 * 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0083-select-1::float4-*-2::int4"},
				},
				{
					Query: `SELECT 1::float4 * 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0084-select-1::float4-*-2::int8"},
				},
				{
					Query: `SELECT 1::float4 * 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0085-select-1::float4-*-2::numeric"},
				},
				{
					Query: `SELECT 1::float8 * 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0086-select-1::float8-*-2::float4"},
				},
				{
					Query: `SELECT 1::float8 * 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0087-select-1::float8-*-2::float8"},
				},
				{
					Query: `SELECT 1::float8 * 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0088-select-1::float8-*-2::int2"},
				},
				{
					Query: `SELECT 1::float8 * 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0089-select-1::float8-*-2::int4"},
				},
				{
					Query: `SELECT 1::float8 * 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0090-select-1::float8-*-2::int8"},
				},
				{
					Query: `SELECT 1::float8 * 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0091-select-1::float8-*-2::numeric"},
				},
				{
					Query: `SELECT 1::int2 * 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0092-select-1::int2-*-2::float4"},
				},
				{
					Query: `SELECT 1::int2 * 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0093-select-1::int2-*-2::float8"},
				},
				{
					Query: `SELECT 1::int2 * 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0094-select-1::int2-*-2::int2"},
				},
				{
					Query: `SELECT 1::int2 * 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0095-select-1::int2-*-2::int4"},
				},
				{
					Query: `SELECT 1::int2 * 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0096-select-1::int2-*-2::int8"},
				},
				{
					Query: `SELECT 1::int2 * 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0097-select-1::int2-*-2::numeric"},
				},
				{
					Query: `SELECT 1::int4 * 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0098-select-1::int4-*-2::float4"},
				},
				{
					Query: `SELECT 1::int4 * 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0099-select-1::int4-*-2::float8"},
				},
				{
					Query: `SELECT 1::int4 * 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0100-select-1::int4-*-2::int2"},
				},
				{
					Query: `SELECT 1::int4 * 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0101-select-1::int4-*-2::int4"},
				},
				{
					Query: `SELECT 1::int4 * 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0102-select-1::int4-*-2::int8"},
				},
				{
					Query: `SELECT 1::int4 * 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0103-select-1::int4-*-2::numeric"},
				},
				{
					Query: `SELECT 1::int8 * 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0104-select-1::int8-*-2::float4"},
				},
				{
					Query: `SELECT 1::int8 * 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0105-select-1::int8-*-2::float8"},
				},
				{
					Query: `SELECT 1::int8 * 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0106-select-1::int8-*-2::int2"},
				},
				{
					Query: `SELECT 1::int8 * 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0107-select-1::int8-*-2::int4"},
				},
				{
					Query: `SELECT 1::int8 * 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0108-select-1::int8-*-2::int8"},
				},
				{
					Query: `SELECT 1::int8 * 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0109-select-1::int8-*-2::numeric"},
				},
				{
					Query: `SELECT 1::numeric * 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0110-select-1::numeric-*-2::float4"},
				},
				{
					Query: `SELECT 1::numeric * 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0111-select-1::numeric-*-2::float8"},
				},
				{
					Query: `SELECT 1::numeric * 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0112-select-1::numeric-*-2::int2"},
				},
				{
					Query: `SELECT 1::numeric * 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0113-select-1::numeric-*-2::int4"},
				},
				{
					Query: `SELECT 1::numeric * 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0114-select-1::numeric-*-2::int8"},
				},
				{
					Query: `SELECT 1::numeric * 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0115-select-1::numeric-*-2::numeric"},
				},
				{
					Query: `select interval '20 days' * 2.3`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0116-select-interval-20-days-*"},
				},
			},
		},
		{
			Name: "Division",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 8::float4 / 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0117-select-8::float4-/-2::float4"},
				},
				{
					Query: `SELECT 8::float4 / 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0118-select-8::float4-/-2::float8"},
				},
				{
					Query: `SELECT 8::float4 / 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0119-select-8::float4-/-2::int2"},
				},
				{
					Query: `SELECT 8::float4 / 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0120-select-8::float4-/-2::int4"},
				},
				{
					Query: `SELECT 8::float4 / 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0121-select-8::float4-/-2::int8"},
				},
				{
					Query: `SELECT 8::float4 / 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0122-select-8::float4-/-2::numeric"},
				},
				{
					Query: `SELECT 8::float8 / 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0123-select-8::float8-/-2::float4"},
				},
				{
					Query: `SELECT 8::float8 / 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0124-select-8::float8-/-2::float8"},
				},
				{
					Query: `SELECT 8::float8 / 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0125-select-8::float8-/-2::int2"},
				},
				{
					Query: `SELECT 8::float8 / 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0126-select-8::float8-/-2::int4"},
				},
				{
					Query: `SELECT 8::float8 / 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0127-select-8::float8-/-2::int8"},
				},
				{
					Query: `SELECT 8::float8 / 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0128-select-8::float8-/-2::numeric"},
				},
				{
					Query: `SELECT 8::int2 / 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0129-select-8::int2-/-2::float4"},
				},
				{
					Query: `SELECT 8::int2 / 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0130-select-8::int2-/-2::float8"},
				},
				{
					Query: `SELECT 8::int2 / 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0131-select-8::int2-/-2::int2"},
				},
				{
					Query: `SELECT 8::int2 / 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0132-select-8::int2-/-2::int4"},
				},
				{
					Query: `SELECT 8::int2 / 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0133-select-8::int2-/-2::int8"},
				},
				{
					Query: `SELECT 8::int2 / 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0134-select-8::int2-/-2::numeric"},
				},
				{
					Query: `SELECT 8::int4 / 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0135-select-8::int4-/-2::float4"},
				},
				{
					Query: `SELECT 8::int4 / 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0136-select-8::int4-/-2::float8"},
				},
				{
					Query: `SELECT 8::int4 / 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0137-select-8::int4-/-2::int2"},
				},
				{
					Query: `SELECT 8::int4 / 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0138-select-8::int4-/-2::int4"},
				},
				{
					Query: `SELECT 8::int4 / 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0139-select-8::int4-/-2::int8"},
				},
				{
					Query: `SELECT 8::int4 / 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0140-select-8::int4-/-2::numeric"},
				},
				{
					Query: `SELECT 8::int8 / 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0141-select-8::int8-/-2::float4"},
				},
				{
					Query: `SELECT 8::int8 / 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0142-select-8::int8-/-2::float8"},
				},
				{
					Query: `SELECT 8::int8 / 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0143-select-8::int8-/-2::int2"},
				},
				{
					Query: `SELECT 8::int8 / 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0144-select-8::int8-/-2::int4"},
				},
				{
					Query: `SELECT 8::int8 / 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0145-select-8::int8-/-2::int8"},
				},
				{
					Query: `SELECT 8::int8 / 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0146-select-8::int8-/-2::numeric"},
				},
				{
					Query: `SELECT 8::numeric / 2::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0147-select-8::numeric-/-2::float4"},
				},
				{
					Query: `SELECT 8::numeric / 2::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0148-select-8::numeric-/-2::float8"},
				},
				{
					Query: `SELECT 8::numeric / 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0149-select-8::numeric-/-2::int2"},
				},
				{
					Query: `SELECT 8::numeric / 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0150-select-8::numeric-/-2::int4"},
				},
				{
					Query: `SELECT 8::numeric / 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0151-select-8::numeric-/-2::int8"},
				},
				{
					Query: `SELECT 8::numeric / 2::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0152-select-8::numeric-/-2::numeric"},
				},
				{
					Query: `select interval '20 days' / 2.3`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0153-select-interval-20-days-/"},
				},
			},
		},
		{
			Name: "Mod",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 11::int2 % 3::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0154-select-11::int2-%-3::int2"},
				},
				{
					Query: `SELECT 11::int2 % 3::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0155-select-11::int2-%-3::int4"},
				},
				{
					Query: `SELECT 11::int2 % 3::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0156-select-11::int2-%-3::int8"},
				},
				{
					Query: `SELECT 11::int2 % 3::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0157-select-11::int2-%-3::numeric"},
				},
				{
					Query: `SELECT 11::int4 % 3::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0158-select-11::int4-%-3::int2"},
				},
				{
					Query: `SELECT 11::int4 % 3::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0159-select-11::int4-%-3::int4"},
				},
				{
					Query: `SELECT 11::int4 % 3::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0160-select-11::int4-%-3::int8"},
				},
				{
					Query: `SELECT 11::int4 % 3::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0161-select-11::int4-%-3::numeric"},
				},
				{
					Query: `SELECT 11::int8 % 3::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0162-select-11::int8-%-3::int2"},
				},
				{
					Query: `SELECT 11::int8 % 3::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0163-select-11::int8-%-3::int4"},
				},
				{
					Query: `SELECT 11::int8 % 3::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0164-select-11::int8-%-3::int8"},
				},
				{
					Query: `SELECT 11::int8 % 3::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0165-select-11::int8-%-3::numeric"},
				},
				{
					Query: `SELECT 11::numeric % 3::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0166-select-11::numeric-%-3::int2"},
				},
				{
					Query: `SELECT 11::numeric % 3::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0167-select-11::numeric-%-3::int4"},
				},
				{
					Query: `SELECT 11::numeric % 3::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0168-select-11::numeric-%-3::int8"},
				},
				{
					Query: `SELECT 11::numeric % 3::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0169-select-11::numeric-%-3::numeric"},
				},
			},
		},
		{
			Name: "Shift Left",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 5::int2 << 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0170-select-5::int2-<<-2::int2"},
				},
				{
					Query: `SELECT 5::int2 << 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0171-select-5::int2-<<-2::int4"},
				},
				{
					Query: `SELECT 5::int2 << 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0172-select-5::int2-<<-2::int8", Compare: "sqlstate"},
				},
				{
					Query: `SELECT 5::int4 << 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0173-select-5::int4-<<-2::int2"},
				},
				{
					Query: `SELECT 5::int4 << 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0174-select-5::int4-<<-2::int4"},
				},
				{
					Query: `SELECT 5::int4 << 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0175-select-5::int4-<<-2::int8", Compare: "sqlstate"},
				},
				{
					Query: `SELECT 5::int8 << 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0176-select-5::int8-<<-2::int2"},
				},
				{
					Query: `SELECT 5::int8 << 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0177-select-5::int8-<<-2::int4"},
				},
				{
					Query: `SELECT 5::int8 << 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0178-select-5::int8-<<-2::int8", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Shift Right",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 17::int2 >> 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0179-select-17::int2->>-2::int2"},
				},
				{
					Query: `SELECT 17::int2 >> 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0180-select-17::int2->>-2::int4"},
				},
				{
					Query: `SELECT 17::int2 >> 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0181-select-17::int2->>-2::int8", Compare: "sqlstate"},
				},
				{
					Query: `SELECT 17::int4 >> 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0182-select-17::int4->>-2::int2"},
				},
				{
					Query: `SELECT 17::int4 >> 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0183-select-17::int4->>-2::int4"},
				},
				{
					Query: `SELECT 17::int4 >> 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0184-select-17::int4->>-2::int8", Compare: "sqlstate"},
				},
				{
					Query: `SELECT 17::int8 >> 2::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0185-select-17::int8->>-2::int2"},
				},
				{
					Query: `SELECT 17::int8 >> 2::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0186-select-17::int8->>-2::int4"},
				},
				{
					Query: `SELECT 17::int8 >> 2::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0187-select-17::int8->>-2::int8", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "Less Than",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT false < true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0188-select-false-<-true"},
				},
				{
					Query: `SELECT true < false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0189-select-true-<-false"},
				},
				{
					Query: `SELECT 'abc'::bpchar < 'def'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0190-select-abc-::bpchar-<-def"},
				},
				{
					Query: `SELECT 'def'::bpchar < 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0191-select-def-::bpchar-<-abc"},
				},
				{
					Query: `SELECT 'abc'::"char" < 'def'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0192-select-abc-::-char-<"},
				},
				{
					Query: `SELECT 'abc'::"char" < 'aef';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0193-select-abc-::-char-<"},
				},
				{
					Query: `SELECT E'\\x01'::bytea < E'\\x02'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0194-select-e-\\\\x01-::bytea-<"},
				},
				{
					Query: `SELECT E'\\x02'::bytea < E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0195-select-e-\\\\x02-::bytea-<"},
				},
				{
					Query: `SELECT '2019-01-03'::date < '2020-07-15'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0196-select-2019-01-03-::date-<-2020-07-15"},
				},
				{
					Query: `SELECT '2020-02-05'::date < '2019-08-17'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0197-select-2020-02-05-::date-<-2019-08-17"},
				},
				{
					Query: `SELECT '2021-03-07'::date < '2022-09-19 04:19:19'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0198-select-2021-03-07-::date-<-2022-09-19"},
				},
				{
					Query: `SELECT '2022-04-09'::date < '2021-10-21 08:27:40'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0199-select-2022-04-09-::date-<-2021-10-21"},
				},
				{
					Query: `SELECT '2023-05-11'::date < '2024-11-23 12:35:54+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0200-select-2023-05-11-::date-<-2024-11-23"},
				},
				{
					Query: `SELECT '2024-06-13'::date < '2023-12-25 16:43:55+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0201-select-2024-06-13-::date-<-2023-12-25"},
				},
				{
					Query: `SELECT 1.23::float4 < 4.56::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0202-select-1.23::float4-<-4.56::float4"},
				},
				{
					Query: `SELECT 4.56::float4 < 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0203-select-4.56::float4-<-1.23::float4"},
				},
				{
					Query: `SELECT 7.89::float4 < 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0204-select-7.89::float4-<-9.01::float8"},
				},
				{
					Query: `SELECT 9.01::float4 < 7.89::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0205-select-9.01::float4-<-7.89::float8"},
				},
				{
					Query: `SELECT 2.34::float8 < 5.67::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0206-select-2.34::float8-<-5.67::float4"},
				},
				{
					Query: `SELECT 5.67::float8 < 2.34::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0207-select-5.67::float8-<-2.34::float4"},
				},
				{
					Query: `SELECT 8.99::float8 < 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0208-select-8.99::float8-<-9.01::float8"},
				},
				{
					Query: `SELECT 9.01::float8 < 8.99::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0209-select-9.01::float8-<-8.99::float8"},
				},
				{
					Query: `SELECT 10::int2 < 29::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0210-select-10::int2-<-29::int2"},
				},
				{
					Query: `SELECT 29::int2 < 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0211-select-29::int2-<-10::int2"},
				},
				{
					Query: `SELECT 11::int2 < 28::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0212-select-11::int2-<-28::int4"},
				},
				{
					Query: `SELECT 28::int2 < 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0213-select-28::int2-<-11::int4"},
				},
				{
					Query: `SELECT 12::int2 < 27::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0214-select-12::int2-<-27::int8"},
				},
				{
					Query: `SELECT 27::int2 < 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0215-select-27::int2-<-12::int8"},
				},
				{
					Query: `SELECT 13::int4 < 26::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0216-select-13::int4-<-26::int2"},
				},
				{
					Query: `SELECT 26::int4 < 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0217-select-26::int4-<-13::int2"},
				},
				{
					Query: `SELECT 14::int4 < 25::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0218-select-14::int4-<-25::int4"},
				},
				{
					Query: `SELECT 25::int4 < 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0219-select-25::int4-<-14::int4"},
				},
				{
					Query: `SELECT 15::int4 < 24::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0220-select-15::int4-<-24::int8"},
				},
				{
					Query: `SELECT 24::int4 < 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0221-select-24::int4-<-15::int8"},
				},
				{
					Query: `SELECT 16::int8 < 23::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0222-select-16::int8-<-23::int2"},
				},
				{
					Query: `SELECT 23::int8 < 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0223-select-23::int8-<-16::int2"},
				},
				{
					Query: `SELECT 17::int8 < 22::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0224-select-17::int8-<-22::int4"},
				},
				{
					Query: `SELECT 22::int8 < 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0225-select-22::int8-<-17::int4"},
				},
				{
					Query: `SELECT 18::int8 < 21::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0226-select-18::int8-<-21::int8"},
				},
				{
					Query: `SELECT 21::int8 < 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0227-select-21::int8-<-18::int8"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb < '{"b":2}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0228-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb < '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0229-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT 'and'::name < 'then'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0230-select-and-::name-<-then"},
				},
				{
					Query: `SELECT 'then'::name < 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0231-select-then-::name-<-and"},
				},
				{
					Query: `SELECT 'cold'::name < 'dance'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0232-select-cold-::name-<-dance"},
				},
				{
					Query: `SELECT 'dance'::name < 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0233-select-dance-::name-<-cold"},
				},
				{
					Query: `SELECT 10.20::numeric < 20.10::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0234-select-10.20::numeric-<-20.10::numeric"},
				},
				{
					Query: `SELECT 20.10::numeric < 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0235-select-20.10::numeric-<-10.20::numeric"},
				},
				{
					Query: `SELECT 101::oid < 202::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0236-select-101::oid-<-202::oid"},
				},
				{
					Query: `SELECT 202::oid < 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0237-select-202::oid-<-101::oid"},
				},
				{
					Query: `SELECT 'dog'::text < 'good'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0238-select-dog-::text-<-good"},
				},
				{
					Query: `SELECT 'good'::text < 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0239-select-good-::text-<-dog"},
				},
				{
					Query: `SELECT 'hello'::text < 'world'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0240-select-hello-::text-<-world"},
				},
				{
					Query: `SELECT 'world'::text < 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0241-select-world-::text-<-hello"},
				},
				{
					Query: `SELECT '12:12:12'::time < '14:15:16'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0242-select-12:12:12-::time-<-14:15:16"},
				},
				{
					Query: `SELECT '14:15:16'::time < '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0243-select-14:15:16-::time-<-12:12:12"},
				},
				{
					Query: `SELECT '2019-01-03 10:21:00'::timestamp < '2020-02-05'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0244-select-2019-01-03-10:21:00-::timestamp-<"},
				},
				{
					Query: `SELECT '2020-02-05 10:21:00'::timestamp < '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0245-select-2020-02-05-10:21:00-::timestamp-<"},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp < '2021-03-07 12:43:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0246-select-2020-02-05-11:32:00-::timestamp-<"},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp < '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0247-select-2021-03-07-12:43:00-::timestamp-<"},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp < '2022-04-09 13:54:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0248-select-2021-03-07-12:43:00-::timestamp-<"},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00'::timestamp < '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0249-select-2022-04-09-13:54:00-::timestamp-<"},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00+00'::timestamptz < '2023-05-11'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0250-select-2022-04-09-13:54:00+00-::timestamptz-<"},
				},
				{
					Query: `SELECT '2023-05-11 13:54:00+00'::timestamptz < '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0251-select-2023-05-11-13:54:00+00-::timestamptz-<"},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz < '2024-06-13 13:54:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0252-select-2023-05-11-14:15:00+00-::timestamptz-<"},
				},
				{
					Query: `SELECT '2024-06-13 13:54:00+00'::timestamptz < '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0253-select-2024-06-13-13:54:00+00-::timestamptz-<"},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz < '2025-07-15 14:15:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0254-select-2024-06-13-15:36:00+00-::timestamptz-<"},
				},
				{
					Query: `SELECT '2025-07-15 14:15:00+00'::timestamptz < '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0255-select-2025-07-15-14:15:00+00-::timestamptz-<"},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz < '13:17:21+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0256-select-12:16:20+00-::timetz-<-13:17:21+00"},
				},
				{
					Query: `SELECT '13:17:21+00'::timetz < '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0257-select-13:17:21+00-::timetz-<-12:16:20+00"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid < '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0258-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid-<-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid < '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0259-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6-::uuid-<-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `select '27:00:24'::interval < '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0260-select-27:00:24-::interval-<-1"},
				},
				{
					Query: `select '27:01:24'::interval < '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0261-select-27:01:24-::interval-<-1"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector < '1234 5678 9012'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0262-select-1234-5678-::oidvector-<"},
				},
				{
					Query: `SELECT '1234 5678 9012'::oidvector < '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0263-select-1234-5678-9012-::oidvector"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector < '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0264-select-1234-5678-::oidvector-<"},
				},
				{
					Query: `SELECT '1234 5677'::oidvector < '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0265-select-1234-5677-::oidvector-<"},
				},
			},
		},
		{
			Name: "Greater Than",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT false > true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0266-select-false->-true"},
				},
				{
					Query: `SELECT true > false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0267-select-true->-false"},
				},
				{
					Query: `SELECT 'abc'::bpchar > 'def'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0268-select-abc-::bpchar->-def"},
				},
				{
					Query: `SELECT 'def'::bpchar > 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0269-select-def-::bpchar->-abc"},
				},
				{
					Query: `SELECT 'abc'::"char" > 'def'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0270-select-abc-::-char->"},
				},
				{
					Query: `SELECT 'def'::"char" > 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0271-select-def-::-char->"},
				},
				{
					Query: `SELECT 'aef' > 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0272-select-aef->-abc-::"},
				},
				{
					Query: `SELECT E'\\x01'::bytea > E'\\x02'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0273-select-e-\\\\x01-::bytea->"},
				},
				{
					Query: `SELECT E'\\x02'::bytea > E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0274-select-e-\\\\x02-::bytea->"},
				},
				{
					Query: `SELECT '2019-01-03'::date > '2020-07-15'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0275-select-2019-01-03-::date->-2020-07-15"},
				},
				{
					Query: `SELECT '2020-02-05'::date > '2019-08-17'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0276-select-2020-02-05-::date->-2019-08-17"},
				},
				{
					Query: `SELECT '2021-03-07'::date > '2022-09-19 04:19:19'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0277-select-2021-03-07-::date->-2022-09-19"},
				},
				{
					Query: `SELECT '2022-04-09'::date > '2021-10-21 08:27:40'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0278-select-2022-04-09-::date->-2021-10-21"},
				},
				{
					Query: `SELECT '2023-05-11'::date > '2024-11-23 12:35:54+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0279-select-2023-05-11-::date->-2024-11-23"},
				},
				{
					Query: `SELECT '2024-06-13'::date > '2023-12-25 16:43:55+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0280-select-2024-06-13-::date->-2023-12-25"},
				},
				{
					Query: `SELECT 1.23::float4 > 4.56::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0281-select-1.23::float4->-4.56::float4"},
				},
				{
					Query: `SELECT 4.56::float4 > 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0282-select-4.56::float4->-1.23::float4"},
				},
				{
					Query: `SELECT 7.89::float4 > 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0283-select-7.89::float4->-9.01::float8"},
				},
				{
					Query: `SELECT 9.01::float4 > 7.89::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0284-select-9.01::float4->-7.89::float8"},
				},
				{
					Query: `SELECT 2.34::float8 > 5.67::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0285-select-2.34::float8->-5.67::float4"},
				},
				{
					Query: `SELECT 5.67::float8 > 2.34::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0286-select-5.67::float8->-2.34::float4"},
				},
				{
					Query: `SELECT 8.99::float8 > 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0287-select-8.99::float8->-9.01::float8"},
				},
				{
					Query: `SELECT 9.01::float8 > 8.99::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0288-select-9.01::float8->-8.99::float8"},
				},
				{
					Query: `SELECT 10::int2 > 29::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0289-select-10::int2->-29::int2"},
				},
				{
					Query: `SELECT 29::int2 > 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0290-select-29::int2->-10::int2"},
				},
				{
					Query: `SELECT 11::int2 > 28::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0291-select-11::int2->-28::int4"},
				},
				{
					Query: `SELECT 28::int2 > 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0292-select-28::int2->-11::int4"},
				},
				{
					Query: `SELECT 12::int2 > 27::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0293-select-12::int2->-27::int8"},
				},
				{
					Query: `SELECT 27::int2 > 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0294-select-27::int2->-12::int8"},
				},
				{
					Query: `SELECT 13::int4 > 26::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0295-select-13::int4->-26::int2"},
				},
				{
					Query: `SELECT 26::int4 > 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0296-select-26::int4->-13::int2"},
				},
				{
					Query: `SELECT 14::int4 > 25::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0297-select-14::int4->-25::int4"},
				},
				{
					Query: `SELECT 25::int4 > 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0298-select-25::int4->-14::int4"},
				},
				{
					Query: `SELECT 15::int4 > 24::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0299-select-15::int4->-24::int8"},
				},
				{
					Query: `SELECT 24::int4 > 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0300-select-24::int4->-15::int8"},
				},
				{
					Query: `SELECT 16::int8 > 23::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0301-select-16::int8->-23::int2"},
				},
				{
					Query: `SELECT 23::int8 > 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0302-select-23::int8->-16::int2"},
				},
				{
					Query: `SELECT 17::int8 > 22::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0303-select-17::int8->-22::int4"},
				},
				{
					Query: `SELECT 22::int8 > 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0304-select-22::int8->-17::int4"},
				},
				{
					Query: `SELECT 18::int8 > 21::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0305-select-18::int8->-21::int8"},
				},
				{
					Query: `SELECT 21::int8 > 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0306-select-21::int8->-18::int8"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb > '{"b":2}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0307-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb > '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0308-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT 'and'::name > 'then'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0309-select-and-::name->-then"},
				},
				{
					Query: `SELECT 'then'::name > 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0310-select-then-::name->-and"},
				},
				{
					Query: `SELECT 'cold'::name > 'dance'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0311-select-cold-::name->-dance"},
				},
				{
					Query: `SELECT 'dance'::name > 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0312-select-dance-::name->-cold"},
				},
				{
					Query: `SELECT 10.20::numeric > 20.10::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0313-select-10.20::numeric->-20.10::numeric"},
				},
				{
					Query: `SELECT 20.10::numeric > 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0314-select-20.10::numeric->-10.20::numeric"},
				},
				{
					Query: `SELECT 101::oid > 202::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0315-select-101::oid->-202::oid"},
				},
				{
					Query: `SELECT 202::oid > 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0316-select-202::oid->-101::oid"},
				},
				{
					Query: `SELECT 'dog'::text > 'good'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0317-select-dog-::text->-good"},
				},
				{
					Query: `SELECT 'good'::text > 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0318-select-good-::text->-dog"},
				},
				{
					Query: `SELECT 'hello'::text > 'world'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0319-select-hello-::text->-world"},
				},
				{
					Query: `SELECT 'world'::text > 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0320-select-world-::text->-hello"},
				},
				{
					Query: `SELECT '12:12:12'::time > '14:15:16'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0321-select-12:12:12-::time->-14:15:16"},
				},
				{
					Query: `SELECT '14:15:16'::time > '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0322-select-14:15:16-::time->-12:12:12"},
				},
				{
					Query: `SELECT '2019-01-03 10:21:00'::timestamp > '2020-02-05'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0323-select-2019-01-03-10:21:00-::timestamp->"},
				},
				{
					Query: `SELECT '2020-02-05 10:21:00'::timestamp > '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0324-select-2020-02-05-10:21:00-::timestamp->"},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp > '2021-03-07 12:43:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0325-select-2020-02-05-11:32:00-::timestamp->"},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp > '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0326-select-2021-03-07-12:43:00-::timestamp->"},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp > '2022-04-09 13:54:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0327-select-2021-03-07-12:43:00-::timestamp->"},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00'::timestamp > '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0328-select-2022-04-09-13:54:00-::timestamp->"},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00+00'::timestamptz > '2023-05-11'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0329-select-2022-04-09-13:54:00+00-::timestamptz->"},
				},
				{
					Query: `SELECT '2023-05-11 13:54:00+00'::timestamptz > '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0330-select-2023-05-11-13:54:00+00-::timestamptz->"},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz > '2024-06-13 13:54:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0331-select-2023-05-11-14:15:00+00-::timestamptz->"},
				},
				{
					Query: `SELECT '2024-06-13 13:54:00+00'::timestamptz > '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0332-select-2024-06-13-13:54:00+00-::timestamptz->"},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz > '2025-07-15 14:15:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0333-select-2024-06-13-15:36:00+00-::timestamptz->"},
				},
				{
					Query: `SELECT '2025-07-15 14:15:00+00'::timestamptz > '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0334-select-2025-07-15-14:15:00+00-::timestamptz->"},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz > '13:17:21+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0335-select-12:16:20+00-::timetz->-13:17:21+00"},
				},
				{
					Query: `SELECT '13:17:21+00'::timetz > '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0336-select-13:17:21+00-::timetz->-12:16:20+00"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid > '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0337-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid->-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid > '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0338-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6-::uuid->-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `select '28:22:24'::interval > '1 day 03:00:00'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0339-select-28:22:24-::interval->-1"},
				},
				{
					Query: `select '23:22:24'::interval > '1 day 03:00:00'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0340-select-23:22:24-::interval->-1"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector > '1234 5678 9012'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0341-select-1234-5678-::oidvector->"},
				},
				{
					Query: `SELECT '1234 5678 9012'::oidvector > '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0342-select-1234-5678-9012-::oidvector"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector > '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0343-select-1234-5678-::oidvector->"},
				},
				{
					Query: `SELECT '1234 5679'::oidvector > '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0344-select-1234-5679-::oidvector->"},
				},
			},
		},
		{
			Name: "Less Or Equal",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT false <= true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0345-select-false-<=-true"},
				},
				{
					Query: `SELECT true <= true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0346-select-true-<=-true"},
				},
				{
					Query: `SELECT true <= false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0347-select-true-<=-false"},
				},
				{
					Query: `SELECT 'abc'::bpchar <= 'def'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0348-select-abc-::bpchar-<=-def"},
				},
				{
					Query: `SELECT 'abc'::bpchar <= 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0349-select-abc-::bpchar-<=-abc"},
				},
				{
					Query: `SELECT 'def'::bpchar <= 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0350-select-def-::bpchar-<=-abc"},
				},
				{
					Query: `SELECT 'abc'::"char" <= 'def'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0351-select-abc-::-char-<="},
				},
				{
					Query: `SELECT 'def'::"char" <= 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0352-select-def-::-char-<="},
				},
				{
					Query: `SELECT 'abc' <= 'aef'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0353-select-abc-<=-aef-::"},
				},
				{
					Query: `SELECT E'\\x01'::bytea <= E'\\x02'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0354-select-e-\\\\x01-::bytea-<="},
				},
				{
					Query: `SELECT E'\\x01'::bytea <= E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0355-select-e-\\\\x01-::bytea-<="},
				},
				{
					Query: `SELECT E'\\x02'::bytea <= E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0356-select-e-\\\\x02-::bytea-<="},
				},
				{
					Query: `SELECT '2019-01-03'::date <= '2020-07-15'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0357-select-2019-01-03-::date-<=-2020-07-15"},
				},
				{
					Query: `SELECT '2019-01-03'::date <= '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0358-select-2019-01-03-::date-<=-2019-01-03"},
				},
				{
					Query: `SELECT '2020-02-05'::date <= '2019-08-17'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0359-select-2020-02-05-::date-<=-2019-08-17"},
				},
				{
					Query: `SELECT '2021-03-07'::date <= '2022-09-19 04:19:19'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0360-select-2021-03-07-::date-<=-2022-09-19"},
				},
				{
					Query: `SELECT '2021-03-07'::date <= '2021-03-07 00:00:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0361-select-2021-03-07-::date-<=-2021-03-07"},
				},
				{
					Query: `SELECT '2022-04-09'::date <= '2021-10-21 08:27:40'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0362-select-2022-04-09-::date-<=-2021-10-21"},
				},
				{
					Query: `SELECT '2023-05-11'::date <= '2024-11-23 12:35:54+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0363-select-2023-05-11-::date-<=-2024-11-23"},
				},
				{
					Query: `SELECT '2023-05-11'::date <= '2023-05-11 00:00:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0364-select-2023-05-11-::date-<=-2023-05-11"},
				},
				{
					Query: `SELECT '2024-06-13'::date <= '2023-12-25 16:43:55+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0365-select-2024-06-13-::date-<=-2023-12-25"},
				},
				{
					Query: `SELECT 1.23::float4 <= 4.56::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0366-select-1.23::float4-<=-4.56::float4"},
				},
				{
					Query: `SELECT 1.23::float4 <= 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0367-select-1.23::float4-<=-1.23::float4"},
				},
				{
					Query: `SELECT 4.56::float4 <= 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0368-select-4.56::float4-<=-1.23::float4"},
				},
				{
					Query: `SELECT 7.89::float4 <= 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0369-select-7.89::float4-<=-9.01::float8"},
				},
				{
					Query: `SELECT 7.75::float4 <= 7.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0370-select-7.75::float4-<=-7.75::float8"},
				},
				{
					Query: `SELECT 9.01::float4 <= 7.89::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0371-select-9.01::float4-<=-7.89::float8"},
				},
				{
					Query: `SELECT 2.34::float8 <= 5.67::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0372-select-2.34::float8-<=-5.67::float4"},
				},
				{
					Query: `SELECT 2.25::float8 <= 2.25::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0373-select-2.25::float8-<=-2.25::float4"},
				},
				{
					Query: `SELECT 5.67::float8 <= 2.34::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0374-select-5.67::float8-<=-2.34::float4"},
				},
				{
					Query: `SELECT 8.99::float8 <= 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0375-select-8.99::float8-<=-9.01::float8"},
				},
				{
					Query: `SELECT 8.75::float8 <= 8.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0376-select-8.75::float8-<=-8.75::float8"},
				},
				{
					Query: `SELECT 9.01::float8 <= 8.99::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0377-select-9.01::float8-<=-8.99::float8"},
				},
				{
					Query: `SELECT 10::int2 <= 29::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0378-select-10::int2-<=-29::int2"},
				},
				{
					Query: `SELECT 10::int2 <= 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0379-select-10::int2-<=-10::int2"},
				},
				{
					Query: `SELECT 29::int2 <= 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0380-select-29::int2-<=-10::int2"},
				},
				{
					Query: `SELECT 11::int2 <= 28::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0381-select-11::int2-<=-28::int4"},
				},
				{
					Query: `SELECT 11::int2 <= 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0382-select-11::int2-<=-11::int4"},
				},
				{
					Query: `SELECT 28::int2 <= 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0383-select-28::int2-<=-11::int4"},
				},
				{
					Query: `SELECT 12::int2 <= 27::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0384-select-12::int2-<=-27::int8"},
				},
				{
					Query: `SELECT 12::int2 <= 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0385-select-12::int2-<=-12::int8"},
				},
				{
					Query: `SELECT 27::int2 <= 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0386-select-27::int2-<=-12::int8"},
				},
				{
					Query: `SELECT 13::int4 <= 26::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0387-select-13::int4-<=-26::int2"},
				},
				{
					Query: `SELECT 13::int4 <= 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0388-select-13::int4-<=-13::int2"},
				},
				{
					Query: `SELECT 26::int4 <= 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0389-select-26::int4-<=-13::int2"},
				},
				{
					Query: `SELECT 14::int4 <= 25::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0390-select-14::int4-<=-25::int4"},
				},
				{
					Query: `SELECT 14::int4 <= 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0391-select-14::int4-<=-14::int4"},
				},
				{
					Query: `SELECT 25::int4 <= 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0392-select-25::int4-<=-14::int4"},
				},
				{
					Query: `SELECT 15::int4 <= 24::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0393-select-15::int4-<=-24::int8"},
				},
				{
					Query: `SELECT 15::int4 <= 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0394-select-15::int4-<=-15::int8"},
				},
				{
					Query: `SELECT 24::int4 <= 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0395-select-24::int4-<=-15::int8"},
				},
				{
					Query: `SELECT 16::int8 <= 23::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0396-select-16::int8-<=-23::int2"},
				},
				{
					Query: `SELECT 16::int8 <= 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0397-select-16::int8-<=-16::int2"},
				},
				{
					Query: `SELECT 23::int8 <= 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0398-select-23::int8-<=-16::int2"},
				},
				{
					Query: `SELECT 17::int8 <= 22::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0399-select-17::int8-<=-22::int4"},
				},
				{
					Query: `SELECT 17::int8 <= 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0400-select-17::int8-<=-17::int4"},
				},
				{
					Query: `SELECT 22::int8 <= 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0401-select-22::int8-<=-17::int4"},
				},
				{
					Query: `SELECT 18::int8 <= 21::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0402-select-18::int8-<=-21::int8"},
				},
				{
					Query: `SELECT 18::int8 <= 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0403-select-18::int8-<=-18::int8"},
				},
				{
					Query: `SELECT 21::int8 <= 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0404-select-21::int8-<=-18::int8"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb <= '{"b":2}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0405-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb <= '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0406-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb <= '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0407-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT 'and'::name <= 'then'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0408-select-and-::name-<=-then"},
				},
				{
					Query: `SELECT 'and'::name <= 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0409-select-and-::name-<=-and"},
				},
				{
					Query: `SELECT 'then'::name <= 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0410-select-then-::name-<=-and"},
				},
				{
					Query: `SELECT 'cold'::name <= 'dance'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0411-select-cold-::name-<=-dance"},
				},
				{
					Query: `SELECT 'cold'::name <= 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0412-select-cold-::name-<=-cold"},
				},
				{
					Query: `SELECT 'dance'::name <= 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0413-select-dance-::name-<=-cold"},
				},
				{
					Query: `SELECT 10.20::numeric <= 20.10::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0414-select-10.20::numeric-<=-20.10::numeric"},
				},
				{
					Query: `SELECT 10.20::numeric <= 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0415-select-10.20::numeric-<=-10.20::numeric"},
				},
				{
					Query: `SELECT 20.10::numeric <= 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0416-select-20.10::numeric-<=-10.20::numeric"},
				},
				{
					Query: `SELECT 101::oid <= 202::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0417-select-101::oid-<=-202::oid"},
				},
				{
					Query: `SELECT 101::oid <= 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0418-select-101::oid-<=-101::oid"},
				},
				{
					Query: `SELECT 202::oid <= 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0419-select-202::oid-<=-101::oid"},
				},
				{
					Query: `SELECT 'dog'::text <= 'good'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0420-select-dog-::text-<=-good"},
				},
				{
					Query: `SELECT 'dog'::text <= 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0421-select-dog-::text-<=-dog"},
				},
				{
					Query: `SELECT 'good'::text <= 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0422-select-good-::text-<=-dog"},
				},
				{
					Query: `SELECT 'hello'::text <= 'world'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0423-select-hello-::text-<=-world"},
				},
				{
					Query: `SELECT 'hello'::text <= 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0424-select-hello-::text-<=-hello"},
				},
				{
					Query: `SELECT 'world'::text <= 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0425-select-world-::text-<=-hello"},
				},
				{
					Query: `SELECT '12:12:12'::time <= '14:15:16'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0426-select-12:12:12-::time-<=-14:15:16"},
				},
				{
					Query: `SELECT '12:12:12'::time <= '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0427-select-12:12:12-::time-<=-12:12:12"},
				},
				{
					Query: `SELECT '14:15:16'::time <= '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0428-select-14:15:16-::time-<=-12:12:12"},
				},
				{
					Query: `SELECT '2019-01-03 10:21:00'::timestamp <= '2020-02-05'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0429-select-2019-01-03-10:21:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2019-01-03 00:00:00'::timestamp <= '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0430-select-2019-01-03-00:00:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2020-02-05 10:21:00'::timestamp <= '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0431-select-2020-02-05-10:21:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp <= '2021-03-07 12:43:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0432-select-2020-02-05-11:32:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp <= '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0433-select-2020-02-05-11:32:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp <= '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0434-select-2021-03-07-12:43:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp <= '2022-04-09 13:54:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0435-select-2021-03-07-12:43:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp <= '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0436-select-2021-03-07-12:43:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00'::timestamp <= '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0437-select-2022-04-09-13:54:00-::timestamp-<="},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00+00'::timestamptz <= '2023-05-11'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0438-select-2022-04-09-13:54:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2022-04-09 00:00:00+00'::timestamptz <= '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0439-select-2022-04-09-00:00:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2023-05-11 13:54:00+00'::timestamptz <= '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0440-select-2023-05-11-13:54:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz <= '2024-06-13 13:54:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0441-select-2023-05-11-14:15:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz <= '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0442-select-2023-05-11-14:15:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2024-06-13 13:54:00+00'::timestamptz <= '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0443-select-2024-06-13-13:54:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz <= '2025-07-15 14:15:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0444-select-2024-06-13-15:36:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz <= '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0445-select-2024-06-13-15:36:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '2025-07-15 14:15:00+00'::timestamptz <= '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0446-select-2025-07-15-14:15:00+00-::timestamptz-<="},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz <= '13:17:21+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0447-select-12:16:20+00-::timetz-<=-13:17:21+00"},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz <= '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0448-select-12:16:20+00-::timetz-<=-12:16:20+00"},
				},
				{
					Query: `SELECT '13:17:21+00'::timetz <= '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0449-select-13:17:21+00-::timetz-<=-12:16:20+00"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid <= '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0450-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid-<=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid <= '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0451-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid-<=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid <= '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0452-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6-::uuid-<=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `select '27:00:24.5'::interval <= '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0453-select-27:00:24.5-::interval-<=-1"},
				},
				{
					Query: `select '25:00:24.5'::interval <= '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0454-select-25:00:24.5-::interval-<=-1"},
				},
				{
					Query: `select '2 days 27:00:24.5'::interval <= '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0455-select-2-days-27:00:24.5-::interval"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector <= '1234 5678 9012'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0456-select-1234-5678-::oidvector-<="},
				},
				{
					Query: `SELECT '1234 5678 9012'::oidvector <= '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0457-select-1234-5678-9012-::oidvector"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector <= '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0458-select-1234-5678-::oidvector-<="},
				},
				{
					Query: `SELECT '1234 5677'::oidvector <= '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0459-select-1234-5677-::oidvector-<="},
				},
			},
		},
		{
			Name: "Greater Or Equal",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT false >= true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0460-select-false->=-true"},
				},
				{
					Query: `SELECT true >= true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0461-select-true->=-true"},
				},
				{
					Query: `SELECT true >= false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0462-select-true->=-false"},
				},
				{
					Query: `SELECT 'abc'::bpchar >= 'def'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0463-select-abc-::bpchar->=-def"},
				},
				{
					Query: `SELECT 'abc'::bpchar >= 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0464-select-abc-::bpchar->=-abc"},
				},
				{
					Query: `SELECT 'def'::bpchar >= 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0465-select-def-::bpchar->=-abc"},
				},
				{
					Query: `SELECT 'abc'::"char" >= 'def'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0466-select-abc-::-char->="},
				},
				{
					Query: `SELECT 'def'::"char" >= 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0467-select-def-::-char->="},
				},
				{
					Query: `SELECT 'aef'::"char" >= 'abc';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0468-select-aef-::-char->="},
				},
				{
					Query: `SELECT E'\\x01'::bytea >= E'\\x02'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0469-select-e-\\\\x01-::bytea->="},
				},
				{
					Query: `SELECT E'\\x01'::bytea >= E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0470-select-e-\\\\x01-::bytea->="},
				},
				{
					Query: `SELECT E'\\x02'::bytea >= E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0471-select-e-\\\\x02-::bytea->="},
				},
				{
					Query: `SELECT '2019-01-03'::date >= '2020-07-15'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0472-select-2019-01-03-::date->=-2020-07-15"},
				},
				{
					Query: `SELECT '2019-01-03'::date >= '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0473-select-2019-01-03-::date->=-2019-01-03"},
				},
				{
					Query: `SELECT '2020-02-05'::date >= '2019-08-17'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0474-select-2020-02-05-::date->=-2019-08-17"},
				},
				{
					Query: `SELECT '2021-03-07'::date >= '2022-09-19 04:19:19'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0475-select-2021-03-07-::date->=-2022-09-19"},
				},
				{
					Query: `SELECT '2021-03-07'::date >= '2021-03-07 00:00:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0476-select-2021-03-07-::date->=-2021-03-07"},
				},
				{
					Query: `SELECT '2022-04-09'::date >= '2021-10-21 08:27:40'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0477-select-2022-04-09-::date->=-2021-10-21"},
				},
				{
					Query: `SELECT '2023-05-11'::date >= '2024-11-23 12:35:54+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0478-select-2023-05-11-::date->=-2024-11-23"},
				},
				{
					Query: `SELECT '2023-05-11'::date >= '2023-05-11 00:00:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0479-select-2023-05-11-::date->=-2023-05-11"},
				},
				{
					Query: `SELECT '2024-06-13'::date >= '2023-12-25 16:43:55+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0480-select-2024-06-13-::date->=-2023-12-25"},
				},
				{
					Query: `SELECT 1.23::float4 >= 4.56::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0481-select-1.23::float4->=-4.56::float4"},
				},
				{
					Query: `SELECT 1.23::float4 >= 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0482-select-1.23::float4->=-1.23::float4"},
				},
				{
					Query: `SELECT 4.56::float4 >= 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0483-select-4.56::float4->=-1.23::float4"},
				},
				{
					Query: `SELECT 7.89::float4 >= 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0484-select-7.89::float4->=-9.01::float8"},
				},
				{
					Query: `SELECT 7.75::float4 >= 7.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0485-select-7.75::float4->=-7.75::float8"},
				},
				{
					Query: `SELECT 9.01::float4 >= 7.89::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0486-select-9.01::float4->=-7.89::float8"},
				},
				{
					Query: `SELECT 2.34::float8 >= 5.67::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0487-select-2.34::float8->=-5.67::float4"},
				},
				{
					Query: `SELECT 2.25::float8 >= 2.25::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0488-select-2.25::float8->=-2.25::float4"},
				},
				{
					Query: `SELECT 5.67::float8 >= 2.34::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0489-select-5.67::float8->=-2.34::float4"},
				},
				{
					Query: `SELECT 8.99::float8 >= 9.01::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0490-select-8.99::float8->=-9.01::float8"},
				},
				{
					Query: `SELECT 8.75::float8 >= 8.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0491-select-8.75::float8->=-8.75::float8"},
				},
				{
					Query: `SELECT 9.01::float8 >= 8.99::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0492-select-9.01::float8->=-8.99::float8"},
				},
				{
					Query: `SELECT 10::int2 >= 29::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0493-select-10::int2->=-29::int2"},
				},
				{
					Query: `SELECT 10::int2 >= 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0494-select-10::int2->=-10::int2"},
				},
				{
					Query: `SELECT 29::int2 >= 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0495-select-29::int2->=-10::int2"},
				},
				{
					Query: `SELECT 11::int2 >= 28::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0496-select-11::int2->=-28::int4"},
				},
				{
					Query: `SELECT 11::int2 >= 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0497-select-11::int2->=-11::int4"},
				},
				{
					Query: `SELECT 28::int2 >= 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0498-select-28::int2->=-11::int4"},
				},
				{
					Query: `SELECT 12::int2 >= 27::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0499-select-12::int2->=-27::int8"},
				},
				{
					Query: `SELECT 12::int2 >= 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0500-select-12::int2->=-12::int8"},
				},
				{
					Query: `SELECT 27::int2 >= 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0501-select-27::int2->=-12::int8"},
				},
				{
					Query: `SELECT 13::int4 >= 26::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0502-select-13::int4->=-26::int2"},
				},
				{
					Query: `SELECT 13::int4 >= 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0503-select-13::int4->=-13::int2"},
				},
				{
					Query: `SELECT 26::int4 >= 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0504-select-26::int4->=-13::int2"},
				},
				{
					Query: `SELECT 14::int4 >= 25::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0505-select-14::int4->=-25::int4"},
				},
				{
					Query: `SELECT 14::int4 >= 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0506-select-14::int4->=-14::int4"},
				},
				{
					Query: `SELECT 25::int4 >= 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0507-select-25::int4->=-14::int4"},
				},
				{
					Query: `SELECT 15::int4 >= 24::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0508-select-15::int4->=-24::int8"},
				},
				{
					Query: `SELECT 15::int4 >= 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0509-select-15::int4->=-15::int8"},
				},
				{
					Query: `SELECT 24::int4 >= 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0510-select-24::int4->=-15::int8"},
				},
				{
					Query: `SELECT 16::int8 >= 23::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0511-select-16::int8->=-23::int2"},
				},
				{
					Query: `SELECT 16::int8 >= 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0512-select-16::int8->=-16::int2"},
				},
				{
					Query: `SELECT 23::int8 >= 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0513-select-23::int8->=-16::int2"},
				},
				{
					Query: `SELECT 17::int8 >= 22::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0514-select-17::int8->=-22::int4"},
				},
				{
					Query: `SELECT 17::int8 >= 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0515-select-17::int8->=-17::int4"},
				},
				{
					Query: `SELECT 22::int8 >= 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0516-select-22::int8->=-17::int4"},
				},
				{
					Query: `SELECT 18::int8 >= 21::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0517-select-18::int8->=-21::int8"},
				},
				{
					Query: `SELECT 18::int8 >= 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0518-select-18::int8->=-18::int8"},
				},
				{
					Query: `SELECT 21::int8 >= 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0519-select-21::int8->=-18::int8"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb >= '{"b":2}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0520-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb >= '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0521-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb >= '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0522-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT 'and'::name >= 'then'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0523-select-and-::name->=-then"},
				},
				{
					Query: `SELECT 'and'::name >= 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0524-select-and-::name->=-and"},
				},
				{
					Query: `SELECT 'then'::name >= 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0525-select-then-::name->=-and"},
				},
				{
					Query: `SELECT 'cold'::name >= 'dance'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0526-select-cold-::name->=-dance"},
				},
				{
					Query: `SELECT 'cold'::name >= 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0527-select-cold-::name->=-cold"},
				},
				{
					Query: `SELECT 'dance'::name >= 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0528-select-dance-::name->=-cold"},
				},
				{
					Query: `SELECT 10.20::numeric >= 20.10::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0529-select-10.20::numeric->=-20.10::numeric"},
				},
				{
					Query: `SELECT 10.20::numeric >= 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0530-select-10.20::numeric->=-10.20::numeric"},
				},
				{
					Query: `SELECT 20.10::numeric >= 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0531-select-20.10::numeric->=-10.20::numeric"},
				},
				{
					Query: `SELECT 101::oid >= 202::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0532-select-101::oid->=-202::oid"},
				},
				{
					Query: `SELECT 101::oid >= 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0533-select-101::oid->=-101::oid"},
				},
				{
					Query: `SELECT 202::oid >= 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0534-select-202::oid->=-101::oid"},
				},
				{
					Query: `SELECT 'dog'::text >= 'good'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0535-select-dog-::text->=-good"},
				},
				{
					Query: `SELECT 'dog'::text >= 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0536-select-dog-::text->=-dog"},
				},
				{
					Query: `SELECT 'good'::text >= 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0537-select-good-::text->=-dog"},
				},
				{
					Query: `SELECT 'hello'::text >= 'world'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0538-select-hello-::text->=-world"},
				},
				{
					Query: `SELECT 'hello'::text >= 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0539-select-hello-::text->=-hello"},
				},
				{
					Query: `SELECT 'world'::text >= 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0540-select-world-::text->=-hello"},
				},
				{
					Query: `SELECT '12:12:12'::time >= '14:15:16'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0541-select-12:12:12-::time->=-14:15:16"},
				},
				{
					Query: `SELECT '12:12:12'::time >= '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0542-select-12:12:12-::time->=-12:12:12"},
				},
				{
					Query: `SELECT '14:15:16'::time >= '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0543-select-14:15:16-::time->=-12:12:12"},
				},
				{
					Query: `SELECT '2019-01-03 10:21:00'::timestamp >= '2020-02-05'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0544-select-2019-01-03-10:21:00-::timestamp->="},
				},
				{
					Query: `SELECT '2019-01-03 00:00:00'::timestamp >= '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0545-select-2019-01-03-00:00:00-::timestamp->="},
				},
				{
					Query: `SELECT '2020-02-05 10:21:00'::timestamp >= '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0546-select-2020-02-05-10:21:00-::timestamp->="},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp >= '2021-03-07 12:43:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0547-select-2020-02-05-11:32:00-::timestamp->="},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp >= '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0548-select-2020-02-05-11:32:00-::timestamp->="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp >= '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0549-select-2021-03-07-12:43:00-::timestamp->="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp >= '2022-04-09 13:54:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0550-select-2021-03-07-12:43:00-::timestamp->="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp >= '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0551-select-2021-03-07-12:43:00-::timestamp->="},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00'::timestamp >= '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0552-select-2022-04-09-13:54:00-::timestamp->="},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00+00'::timestamptz >= '2023-05-11'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0553-select-2022-04-09-13:54:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2022-04-09 00:00:00+00'::timestamptz >= '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0554-select-2022-04-09-00:00:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2023-05-11 13:54:00+00'::timestamptz >= '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0555-select-2023-05-11-13:54:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz >= '2024-06-13 13:54:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0556-select-2023-05-11-14:15:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz >= '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0557-select-2023-05-11-14:15:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2024-06-13 13:54:00+00'::timestamptz >= '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0558-select-2024-06-13-13:54:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz >= '2025-07-15 14:15:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0559-select-2024-06-13-15:36:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz >= '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0560-select-2024-06-13-15:36:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '2025-07-15 14:15:00+00'::timestamptz >= '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0561-select-2025-07-15-14:15:00+00-::timestamptz->="},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz >= '13:17:21+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0562-select-12:16:20+00-::timetz->=-13:17:21+00"},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz >= '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0563-select-12:16:20+00-::timetz->=-12:16:20+00"},
				},
				{
					Query: `SELECT '13:17:21+00'::timetz >= '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0564-select-13:17:21+00-::timetz->=-12:16:20+00"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid >= '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0565-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid->=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid >= '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0566-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid->=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid >= '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0567-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6-::uuid->=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `select '1 month 00:00:24'::interval >= '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0568-select-1-month-00:00:24-::interval"},
				},
				{
					Query: `select '2 days 00:00:24'::interval >= '48:00:24'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0569-select-2-days-00:00:24-::interval"},
				},
				{
					Query: `select '27:00:24'::interval >= '1 day 03:00:24.5'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0570-select-27:00:24-::interval->=-1"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector >= '1234 5678 9012'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0571-select-1234-5678-::oidvector->="},
				},
				{
					Query: `SELECT '1234 5678 9012'::oidvector >= '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0572-select-1234-5678-9012-::oidvector"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector >= '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0573-select-1234-5678-::oidvector->="},
				},
				{
					Query: `SELECT '1234 5679'::oidvector >= '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0574-select-1234-5679-::oidvector->="},
				},
			},
		},
		{
			Name: "Equal",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT true = true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0575-select-true-=-true"},
				},
				{
					Query: `SELECT true = false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0576-select-true-=-false"},
				},
				{
					Query: `SELECT 'abc'::bpchar = 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0577-select-abc-::bpchar-=-abc"},
				},
				{
					Query: `SELECT 'def'::bpchar = 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0578-select-def-::bpchar-=-abc"},
				},
				{
					Query: `SELECT 'abc'::"char" = 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0579-select-abc-::-char-="},
				},
				{
					Query: `SELECT 'def'::"char" = 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0580-select-def-::-char-="},
				},
				{
					Query: `SELECT 'abc'::"char" = 'aef';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0581-select-abc-::-char-="},
				},
				{
					Query: `SELECT E'\\x01'::bytea = E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0582-select-e-\\\\x01-::bytea-="},
				},
				{
					Query: `SELECT E'\\x02'::bytea = E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0583-select-e-\\\\x02-::bytea-="},
				},
				{
					Query: `SELECT '2019-01-03'::date = '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0584-select-2019-01-03-::date-=-2019-01-03"},
				},
				{
					Query: `SELECT '2020-02-05'::date = '2019-08-17'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0585-select-2020-02-05-::date-=-2019-08-17"},
				},
				{
					Query: `SELECT '2021-03-07'::date = '2021-03-07 00:00:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0586-select-2021-03-07-::date-=-2021-03-07"},
				},
				{
					Query: `SELECT '2022-04-09'::date = '2021-10-21 08:27:40'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0587-select-2022-04-09-::date-=-2021-10-21"},
				},
				{
					Query: `SELECT '2023-05-11'::date = '2023-05-11 00:00:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0588-select-2023-05-11-::date-=-2023-05-11"},
				},
				{
					Query: `SELECT '2024-06-13'::date = '2023-12-25 16:43:55+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0589-select-2024-06-13-::date-=-2023-12-25"},
				},
				{
					Query: `SELECT 1.23::float4 = 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0590-select-1.23::float4-=-1.23::float4"},
				},
				{
					Query: `SELECT 4.56::float4 = 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0591-select-4.56::float4-=-1.23::float4"},
				},
				{
					Query: `SELECT 7.75::float4 = 7.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0592-select-7.75::float4-=-7.75::float8"},
				},
				{
					Query: `SELECT 9.01::float4 = 7.89::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0593-select-9.01::float4-=-7.89::float8"},
				},
				{
					Query: `SELECT 2.25::float8 = 2.25::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0594-select-2.25::float8-=-2.25::float4"},
				},
				{
					Query: `SELECT 5.67::float8 = 2.34::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0595-select-5.67::float8-=-2.34::float4"},
				},
				{
					Query: `SELECT 8.75::float8 = 8.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0596-select-8.75::float8-=-8.75::float8"},
				},
				{
					Query: `SELECT 9.01::float8 = 8.99::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0597-select-9.01::float8-=-8.99::float8"},
				},
				{
					Query: `SELECT 10::int2 = 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0598-select-10::int2-=-10::int2"},
				},
				{
					Query: `SELECT 29::int2 = 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0599-select-29::int2-=-10::int2"},
				},
				{
					Query: `SELECT 11::int2 = 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0600-select-11::int2-=-11::int4"},
				},
				{
					Query: `SELECT 28::int2 = 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0601-select-28::int2-=-11::int4"},
				},
				{
					Query: `SELECT 12::int2 = 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0602-select-12::int2-=-12::int8"},
				},
				{
					Query: `SELECT 27::int2 = 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0603-select-27::int2-=-12::int8"},
				},
				{
					Query: `SELECT 13::int4 = 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0604-select-13::int4-=-13::int2"},
				},
				{
					Query: `SELECT 26::int4 = 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0605-select-26::int4-=-13::int2"},
				},
				{
					Query: `SELECT 14::int4 = 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0606-select-14::int4-=-14::int4"},
				},
				{
					Query: `SELECT 25::int4 = 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0607-select-25::int4-=-14::int4"},
				},
				{
					Query: `SELECT 15::int4 = 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0608-select-15::int4-=-15::int8"},
				},
				{
					Query: `SELECT 24::int4 = 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0609-select-24::int4-=-15::int8"},
				},
				{
					Query: `SELECT 16::int8 = 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0610-select-16::int8-=-16::int2"},
				},
				{
					Query: `SELECT 23::int8 = 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0611-select-23::int8-=-16::int2"},
				},
				{
					Query: `SELECT 17::int8 = 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0612-select-17::int8-=-17::int4"},
				},
				{
					Query: `SELECT 22::int8 = 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0613-select-22::int8-=-17::int4"},
				},
				{
					Query: `SELECT 18::int8 = 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0614-select-18::int8-=-18::int8"},
				},
				{
					Query: `SELECT 21::int8 = 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0615-select-21::int8-=-18::int8"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb = '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0616-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb = '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0617-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT 'and'::name = 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0618-select-and-::name-=-and"},
				},
				{
					Query: `SELECT 'then'::name = 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0619-select-then-::name-=-and"},
				},
				{
					Query: `SELECT 'cold'::name = 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0620-select-cold-::name-=-cold"},
				},
				{
					Query: `SELECT 'dance'::name = 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0621-select-dance-::name-=-cold"},
				},
				{
					Query: `SELECT 10.20::numeric = 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0622-select-10.20::numeric-=-10.20::numeric"},
				},
				{
					Query: `SELECT 20.10::numeric = 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0623-select-20.10::numeric-=-10.20::numeric"},
				},
				{
					Query: `SELECT 101::oid = 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0624-select-101::oid-=-101::oid"},
				},
				{
					Query: `SELECT 202::oid = 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0625-select-202::oid-=-101::oid"},
				},
				{
					Query: `SELECT 'dog'::text = 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0626-select-dog-::text-=-dog"},
				},
				{
					Query: `SELECT 'good'::text = 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0627-select-good-::text-=-dog"},
				},
				{
					Query: `SELECT 'hello'::text = 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0628-select-hello-::text-=-hello"},
				},
				{
					Query: `SELECT 'world'::text = 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0629-select-world-::text-=-hello"},
				},
				{
					Query: `SELECT '12:12:12'::time = '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0630-select-12:12:12-::time-=-12:12:12"},
				},
				{
					Query: `SELECT '14:15:16'::time = '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0631-select-14:15:16-::time-=-12:12:12"},
				},
				{
					Query: `SELECT '2019-01-03 00:00:00'::timestamp = '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0632-select-2019-01-03-00:00:00-::timestamp-="},
				},
				{
					Query: `SELECT '2020-02-05 10:21:00'::timestamp = '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0633-select-2020-02-05-10:21:00-::timestamp-="},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp = '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0634-select-2020-02-05-11:32:00-::timestamp-="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp = '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0635-select-2021-03-07-12:43:00-::timestamp-="},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp = '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0636-select-2021-03-07-12:43:00-::timestamp-="},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00'::timestamp = '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0637-select-2022-04-09-13:54:00-::timestamp-="},
				},
				{
					Query: `SELECT '2022-04-09 00:00:00+00'::timestamptz = '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0638-select-2022-04-09-00:00:00+00-::timestamptz-="},
				},
				{
					Query: `SELECT '2023-05-11 13:54:00+00'::timestamptz = '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0639-select-2023-05-11-13:54:00+00-::timestamptz-="},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz = '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0640-select-2023-05-11-14:15:00+00-::timestamptz-="},
				},
				{
					Query: `SELECT '2024-06-13 13:54:00+00'::timestamptz = '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0641-select-2024-06-13-13:54:00+00-::timestamptz-="},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz = '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0642-select-2024-06-13-15:36:00+00-::timestamptz-="},
				},
				{
					Query: `SELECT '2025-07-15 14:15:00+00'::timestamptz = '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0643-select-2025-07-15-14:15:00+00-::timestamptz-="},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz = '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0644-select-12:16:20+00-::timetz-=-12:16:20+00"},
				},
				{
					Query: `SELECT '13:17:21+00'::timetz = '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0645-select-13:17:21+00-::timetz-=-12:16:20+00"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid = '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0646-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid-=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid = '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0647-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6-::uuid-=-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `select '27:00:24'::interval = '1 day 03:00:24'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0648-select-27:00:24-::interval-=-1"},
				},
				{
					Query: `select '1 day'::interval = '1 day 03:00:24'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0649-select-1-day-::interval-="},
				},
				{
					Query: `SELECT '1234 5678'::oidvector = '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0650-select-1234-5678-::oidvector-="},
				},
				{
					Query: `SELECT '1234 5677'::oidvector = '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0651-select-1234-5677-::oidvector-="},
				},
			},
		},
		{
			Name: "Not Equal Standard Syntax (<>)",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT true <> true;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0652-select-true-<>-true"},
				},
				{
					Query: `SELECT true <> false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0653-select-true-<>-false"},
				},
				{
					Query: `SELECT 'abc'::bpchar <> 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0654-select-abc-::bpchar-<>-abc"},
				},
				{
					Query: `SELECT 'def'::bpchar <> 'abc'::bpchar;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0655-select-def-::bpchar-<>-abc"},
				},
				{
					Query: `SELECT 'abc'::"char" <> 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0656-select-abc-::-char-<>"},
				},
				{
					Query: `SELECT 'def'::"char" <> 'abc'::"char";`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0657-select-def-::-char-<>"},
				},
				{
					Query: `SELECT E'\\x01'::bytea <> E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0658-select-e-\\\\x01-::bytea-<>"},
				},
				{
					Query: `SELECT E'\\x02'::bytea <> E'\\x01'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0659-select-e-\\\\x02-::bytea-<>"},
				},
				{
					Query: `SELECT '2019-01-03'::date <> '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0660-select-2019-01-03-::date-<>-2019-01-03"},
				},
				{
					Query: `SELECT '2020-02-05'::date <> '2019-08-17'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0661-select-2020-02-05-::date-<>-2019-08-17"},
				},
				{
					Query: `SELECT '2021-03-07'::date <> '2021-03-07 00:00:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0662-select-2021-03-07-::date-<>-2021-03-07"},
				},
				{
					Query: `SELECT '2022-04-09'::date <> '2021-10-21 08:27:40'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0663-select-2022-04-09-::date-<>-2021-10-21"},
				},
				{
					Query: `SELECT '2023-05-11'::date <> '2023-05-11 00:00:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0664-select-2023-05-11-::date-<>-2023-05-11"},
				},
				{
					Query: `SELECT '2024-06-13'::date <> '2023-12-25 16:43:55+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0665-select-2024-06-13-::date-<>-2023-12-25"},
				},
				{
					Query: `SELECT 1.23::float4 <> 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0666-select-1.23::float4-<>-1.23::float4"},
				},
				{
					Query: `SELECT 4.56::float4 <> 1.23::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0667-select-4.56::float4-<>-1.23::float4"},
				},
				{
					Query: `SELECT 7.75::float4 <> 7.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0668-select-7.75::float4-<>-7.75::float8"},
				},
				{
					Query: `SELECT 9.01::float4 <> 7.89::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0669-select-9.01::float4-<>-7.89::float8"},
				},
				{
					Query: `SELECT 2.25::float8 <> 2.25::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0670-select-2.25::float8-<>-2.25::float4"},
				},
				{
					Query: `SELECT 5.67::float8 <> 2.34::float4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0671-select-5.67::float8-<>-2.34::float4"},
				},
				{
					Query: `SELECT 8.75::float8 <> 8.75::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0672-select-8.75::float8-<>-8.75::float8"},
				},
				{
					Query: `SELECT 9.01::float8 <> 8.99::float8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0673-select-9.01::float8-<>-8.99::float8"},
				},
				{
					Query: `SELECT 10::int2 <> 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0674-select-10::int2-<>-10::int2"},
				},
				{
					Query: `SELECT 29::int2 <> 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0675-select-29::int2-<>-10::int2"},
				},
				{
					Query: `SELECT 11::int2 <> 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0676-select-11::int2-<>-11::int4"},
				},
				{
					Query: `SELECT 28::int2 <> 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0677-select-28::int2-<>-11::int4"},
				},
				{
					Query: `SELECT 12::int2 <> 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0678-select-12::int2-<>-12::int8"},
				},
				{
					Query: `SELECT 27::int2 <> 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0679-select-27::int2-<>-12::int8"},
				},
				{
					Query: `SELECT 13::int4 <> 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0680-select-13::int4-<>-13::int2"},
				},
				{
					Query: `SELECT 26::int4 <> 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0681-select-26::int4-<>-13::int2"},
				},
				{
					Query: `SELECT 14::int4 <> 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0682-select-14::int4-<>-14::int4"},
				},
				{
					Query: `SELECT 25::int4 <> 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0683-select-25::int4-<>-14::int4"},
				},
				{
					Query: `SELECT 15::int4 <> 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0684-select-15::int4-<>-15::int8"},
				},
				{
					Query: `SELECT 24::int4 <> 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0685-select-24::int4-<>-15::int8"},
				},
				{
					Query: `SELECT 16::int8 <> 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0686-select-16::int8-<>-16::int2"},
				},
				{
					Query: `SELECT 23::int8 <> 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0687-select-23::int8-<>-16::int2"},
				},
				{
					Query: `SELECT 17::int8 <> 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0688-select-17::int8-<>-17::int4"},
				},
				{
					Query: `SELECT 22::int8 <> 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0689-select-22::int8-<>-17::int4"},
				},
				{
					Query: `SELECT 18::int8 <> 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0690-select-18::int8-<>-18::int8"},
				},
				{
					Query: `SELECT 21::int8 <> 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0691-select-21::int8-<>-18::int8"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb <> '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0692-select-{-a-:1}-::jsonb"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb <> '{"a":1}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0693-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT 'and'::name <> 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0694-select-and-::name-<>-and"},
				},
				{
					Query: `SELECT 'then'::name <> 'and'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0695-select-then-::name-<>-and"},
				},
				{
					Query: `SELECT 'cold'::name <> 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0696-select-cold-::name-<>-cold"},
				},
				{
					Query: `SELECT 'dance'::name <> 'cold'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0697-select-dance-::name-<>-cold"},
				},
				{
					Query: `SELECT 10.20::numeric <> 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0698-select-10.20::numeric-<>-10.20::numeric"},
				},
				{
					Query: `SELECT 20.10::numeric <> 10.20::numeric;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0699-select-20.10::numeric-<>-10.20::numeric"},
				},
				{
					Query: `SELECT 101::oid <> 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0700-select-101::oid-<>-101::oid"},
				},
				{
					Query: `SELECT 202::oid <> 101::oid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0701-select-202::oid-<>-101::oid"},
				},
				{
					Query: `SELECT 'dog'::text <> 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0702-select-dog-::text-<>-dog"},
				},
				{
					Query: `SELECT 'good'::text <> 'dog'::name;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0703-select-good-::text-<>-dog"},
				},
				{
					Query: `SELECT 'hello'::text <> 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0704-select-hello-::text-<>-hello"},
				},
				{
					Query: `SELECT 'world'::text <> 'hello'::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0705-select-world-::text-<>-hello"},
				},
				{
					Query: `SELECT '12:12:12'::time <> '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0706-select-12:12:12-::time-<>-12:12:12"},
				},
				{
					Query: `SELECT '14:15:16'::time <> '12:12:12'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0707-select-14:15:16-::time-<>-12:12:12"},
				},
				{
					Query: `SELECT '2019-01-03 00:00:00'::timestamp <> '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0708-select-2019-01-03-00:00:00-::timestamp-<>"},
				},
				{
					Query: `SELECT '2020-02-05 10:21:00'::timestamp <> '2019-01-03'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0709-select-2020-02-05-10:21:00-::timestamp-<>"},
				},
				{
					Query: `SELECT '2020-02-05 11:32:00'::timestamp <> '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0710-select-2020-02-05-11:32:00-::timestamp-<>"},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp <> '2020-02-05 11:32:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0711-select-2021-03-07-12:43:00-::timestamp-<>"},
				},
				{
					Query: `SELECT '2021-03-07 12:43:00'::timestamp <> '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0712-select-2021-03-07-12:43:00-::timestamp-<>"},
				},
				{
					Query: `SELECT '2022-04-09 13:54:00'::timestamp <> '2021-03-07 12:43:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0713-select-2022-04-09-13:54:00-::timestamp-<>"},
				},
				{
					Query: `SELECT '2022-04-09 00:00:00+00'::timestamptz <> '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0714-select-2022-04-09-00:00:00+00-::timestamptz-<>"},
				},
				{
					Query: `SELECT '2023-05-11 13:54:00+00'::timestamptz <> '2022-04-09'::date;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0715-select-2023-05-11-13:54:00+00-::timestamptz-<>"},
				},
				{
					Query: `SELECT '2023-05-11 14:15:00+00'::timestamptz <> '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0716-select-2023-05-11-14:15:00+00-::timestamptz-<>"},
				},
				{
					Query: `SELECT '2024-06-13 13:54:00+00'::timestamptz <> '2023-05-11 14:15:00'::timestamp;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0717-select-2024-06-13-13:54:00+00-::timestamptz-<>"},
				},
				{
					Query: `SELECT '2024-06-13 15:36:00+00'::timestamptz <> '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0718-select-2024-06-13-15:36:00+00-::timestamptz-<>"},
				},
				{
					Query: `SELECT '2025-07-15 14:15:00+00'::timestamptz <> '2024-06-13 15:36:00+00'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0719-select-2025-07-15-14:15:00+00-::timestamptz-<>"},
				},
				{
					Query: `SELECT '12:16:20+00'::timetz <> '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0720-select-12:16:20+00-::timetz-<>-12:16:20+00"},
				},
				{
					Query: `SELECT '13:17:21+00'::timetz <> '12:16:20+00'::timetz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0721-select-13:17:21+00-::timetz-<>-12:16:20+00"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid <> '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0722-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5-::uuid-<>-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `SELECT '64b67ba1-e368-4cfd-ae6f-0c3e77716fb6'::uuid <> '64b67ba1-e368-4cfd-ae6f-0c3e77716fb5'::uuid;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0723-select-64b67ba1-e368-4cfd-ae6f-0c3e77716fb6-::uuid-<>-64b67ba1-e368-4cfd-ae6f-0c3e77716fb5"},
				},
				{
					Query: `select '3 hours 24 seconds'::interval <> '1 day 03:00:24'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0724-select-3-hours-24-seconds"},
				},
				{
					Query: `select '27:00:24'::interval <> '1 day 03:00:24'::interval;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0725-select-27:00:24-::interval-<>-1"},
				},
				{
					Query: `SELECT '1234 5678'::oidvector <> '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0726-select-1234-5678-::oidvector-<>"},
				},
				{
					Query: `SELECT '1234 5677'::oidvector <> '1234 5678'::oidvector;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0727-select-1234-5677-::oidvector-<>"},
				},
			},
		},
		{
			Name: "Not Equal Alternate Syntax (!=)", // This should be exactly equivalent to <>, so this is only a subset
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 10::int2 != 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0728-select-10::int2-!=-10::int2"},
				},
				{
					Query: `SELECT 29::int2 != 10::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0729-select-29::int2-!=-10::int2"},
				},
				{
					Query: `SELECT 11::int2 != 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0730-select-11::int2-!=-11::int4"},
				},
				{
					Query: `SELECT 28::int2 != 11::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0731-select-28::int2-!=-11::int4"},
				},
				{
					Query: `SELECT 12::int2 != 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0732-select-12::int2-!=-12::int8"},
				},
				{
					Query: `SELECT 27::int2 != 12::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0733-select-27::int2-!=-12::int8"},
				},
				{
					Query: `SELECT 13::int4 != 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0734-select-13::int4-!=-13::int2"},
				},
				{
					Query: `SELECT 26::int4 != 13::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0735-select-26::int4-!=-13::int2"},
				},
				{
					Query: `SELECT 14::int4 != 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0736-select-14::int4-!=-14::int4"},
				},
				{
					Query: `SELECT 25::int4 != 14::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0737-select-25::int4-!=-14::int4"},
				},
				{
					Query: `SELECT 15::int4 != 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0738-select-15::int4-!=-15::int8"},
				},
				{
					Query: `SELECT 24::int4 != 15::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0739-select-24::int4-!=-15::int8"},
				},
				{
					Query: `SELECT 16::int8 != 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0740-select-16::int8-!=-16::int2"},
				},
				{
					Query: `SELECT 23::int8 != 16::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0741-select-23::int8-!=-16::int2"},
				},
				{
					Query: `SELECT 17::int8 != 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0742-select-17::int8-!=-17::int4"},
				},
				{
					Query: `SELECT 22::int8 != 17::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0743-select-22::int8-!=-17::int4"},
				},
				{
					Query: `SELECT 18::int8 != 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0744-select-18::int8-!=-18::int8"},
				},
				{
					Query: `SELECT 21::int8 != 18::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0745-select-21::int8-!=-18::int8"},
				},
			},
		},
		{
			Name: "Bit And",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 13::int2 & 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0746-select-13::int2-&-7::int2"},
				},
				{
					Query: `SELECT 13::int2 & 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0747-select-13::int2-&-7::int4"},
				},
				{
					Query: `SELECT 13::int2 & 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0748-select-13::int2-&-7::int8"},
				},
				{
					Query: `SELECT 13::int4 & 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0749-select-13::int4-&-7::int2"},
				},
				{
					Query: `SELECT 13::int4 & 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0750-select-13::int4-&-7::int4"},
				},
				{
					Query: `SELECT 13::int4 & 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0751-select-13::int4-&-7::int8"},
				},
				{
					Query: `SELECT 13::int8 & 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0752-select-13::int8-&-7::int2"},
				},
				{
					Query: `SELECT 13::int8 & 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0753-select-13::int8-&-7::int4"},
				},
				{
					Query: `SELECT 13::int8 & 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0754-select-13::int8-&-7::int8"},
				},
			},
		},
		{
			Name: "Bit Or",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 13::int2 | 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0755-select-13::int2-|-7::int2"},
				},
				{
					Query: `SELECT 13::int2 | 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0756-select-13::int2-|-7::int4"},
				},
				{
					Query: `SELECT 13::int2 | 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0757-select-13::int2-|-7::int8"},
				},
				{
					Query: `SELECT 13::int4 | 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0758-select-13::int4-|-7::int2"},
				},
				{
					Query: `SELECT 13::int4 | 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0759-select-13::int4-|-7::int4"},
				},
				{
					Query: `SELECT 13::int4 | 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0760-select-13::int4-|-7::int8"},
				},
				{
					Query: `SELECT 13::int8 | 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0761-select-13::int8-|-7::int2"},
				},
				{
					Query: `SELECT 13::int8 | 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0762-select-13::int8-|-7::int4"},
				},
				{
					Query: `SELECT 13::int8 | 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0763-select-13::int8-|-7::int8"},
				},
			},
		},
		{
			Name: "Bit Xor",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 13::int2 # 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0764-select-13::int2-#-7::int2"},
				},
				{
					Query: `SELECT 13::int2 # 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0765-select-13::int2-#-7::int4"},
				},
				{
					Query: `SELECT 13::int2 # 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0766-select-13::int2-#-7::int8"},
				},
				{
					Query: `SELECT 13::int4 # 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0767-select-13::int4-#-7::int2"},
				},
				{
					Query: `SELECT 13::int4 # 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0768-select-13::int4-#-7::int4"},
				},
				{
					Query: `SELECT 13::int4 # 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0769-select-13::int4-#-7::int8"},
				},
				{
					Query: `SELECT 13::int8 # 7::int2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0770-select-13::int8-#-7::int2"},
				},
				{
					Query: `SELECT 13::int8 # 7::int4;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0771-select-13::int8-#-7::int4"},
				},
				{
					Query: `SELECT 13::int8 # 7::int8;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0772-select-13::int8-#-7::int8"},
				},
			},
		},
		{
			Name: "Negate",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT -(7::float4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0773-select-7::float4"},
				},
				{
					Query: `SELECT -(7::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0774-select-7::float8"},
				},
				{
					Query: `SELECT -(7::int2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0775-select-7::int2"},
				},
				{
					Query: `SELECT -(7::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0776-select-7::int4"},
				},
				{
					Query: `SELECT -(7::int8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0777-select-7::int8"},
				},
				{
					Query: `SELECT -(7::numeric);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0778-select-7::numeric"},
				},
				{
					Query: `select - interval '20 days -11:00:00';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0779-select-interval-20-days-11:00:00"},
				},
			},
		},
		{
			Name: "Unary Plus",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT +(7::float4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0780-select-+-7::float4"},
				},
				{
					Query: `SELECT +(7::float8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0781-select-+-7::float8"},
				},
				{
					Query: `SELECT +(7::int2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0782-select-+-7::int2"},
				},
				{
					Query: `SELECT +(7::int4);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0783-select-+-7::int4"},
				},
				{
					Query: `SELECT +(7::int8);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0784-select-+-7::int8"},
				},
				{
					Query: `SELECT +(7::numeric);`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0785-select-+-7::numeric"},
				},
			},
		},
		{
			Name: "Binary JSON",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json -> 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0786-select-[{-a-:-foo"},
				},
				{
					Query: `SELECT '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::jsonb -> 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0787-select-[{-a-:-foo"},
				},
				{
					Query: `SELECT '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json -> -3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0788-select-[{-a-:-foo"},
				},
				{
					Query: `SELECT '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::jsonb -> -3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0789-select-[{-a-:-foo"},
				},
				{
					Query: `SELECT '{"a": {"b":"foo"}}'::json -> 'a';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0790-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b":"foo"}}'::jsonb -> 'a';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0791-select-{-a-:-{"},
				},
				{
					Query: `SELECT '[1,2,3]'::json ->> 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0792-select-[1-2-3]-::json"},
				},
				{
					Query: `SELECT '[1,2,3]'::jsonb ->> 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0793-select-[1-2-3]-::jsonb"},
				},
				{
					Query: `SELECT '{"a":1,"b":2}'::json ->> 'b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0794-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '{"a":1,"b":2}'::jsonb ->> 'b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0795-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::json #> ARRAY['a','b','1']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0796-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::json #> ARRAY['a','b','1'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0797-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::json #> '{a,b,1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0798-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::jsonb #> ARRAY['a','b','1']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0799-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::jsonb #> ARRAY['a','b','1'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0800-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::jsonb #> '{a,b,1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0801-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::json #>> ARRAY['a','b','1']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0802-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::json #>> ARRAY['a','b','1'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0803-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::json #>> '{a,b,1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0804-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::jsonb #>> ARRAY['a','b','1']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0805-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::jsonb #>> ARRAY['a','b','1'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0806-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a": {"b": ["foo","bar"]}}'::jsonb #>> '{a,b,1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0807-select-{-a-:-{"},
				},
				{
					Query: `SELECT '{"a":1, "b":2}'::jsonb @> '{"b":2}'::jsonb;`,
					Skip:  true, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0808-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '{"b":2}'::jsonb <@ '{"a":1, "b":2}'::jsonb;`,
					Skip:  true, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0809-select-{-b-:2}-::jsonb"},
				},
				{
					Query: `SELECT '{"a":1, "b":2}'::jsonb ? 'b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0810-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb ? 'b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0811-select-[-a-b-c"},
				},
				{
					Query: `SELECT '{"a":1, "b":2, "c":3}'::jsonb ?| ARRAY['b','d']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0812-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '{"a":1, "b":2, "c":3}'::jsonb ?| ARRAY['b','d'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0813-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb ?& ARRAY['a','b']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0814-select-[-a-b-c"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb ?& ARRAY['a','b'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0815-select-[-a-b-c"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb ?& ARRAY['d','b']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0816-select-[-a-b-c"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb ?& ARRAY['d','b'];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0817-select-[-a-b-c"},
				},
				{
					Query: `SELECT '{"a":1, "b":2, "c":3}'::jsonb - 'b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0818-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '{"a":1, "b":2, "c":3}'::jsonb - ARRAY['c','a']::text[];`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0819-select-{-a-:1-b"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb - 'b';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0820-select-[-a-b-c"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb - 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0821-select-[-a-b-c"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb - -1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0822-select-[-a-b-c"},
				},
				{
					Query: `SELECT '["a", "b", "c"]'::jsonb - 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0823-select-[-a-b-c"},
				},
				{
					Query: `SELECT '{"n":null, "a":1, "b":[1,2], "d":{"1":[2,3]}}'::jsonb #- '{d,1,0}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0824-select-{-n-:null-a"},
				},
				{
					Query: `SELECT '{"n":null, "a":1, "b":[1,2], "d":{"1":[2,3]}}'::jsonb #- '{b,-1}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0825-select-{-n-:null-a"},
				},
				{
					Query: `SELECT '{"n":null, "a":1, "b":[1,2], "d":{"1":[2,3]}}'::jsonb #- '{b,not_an_int}';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0826-select-{-n-:null-a", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '"a"'::jsonb - 'a';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0827-select-a-::jsonb-a", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '{"a":1}'::jsonb - 0;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0828-select-{-a-:1}-::jsonb", Compare: "sqlstate"},
				},
				{
					Query: `SELECT '["a", "b"]'::jsonb || '["a", "d"]'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0829-select-[-a-b-]"},
				},
				{
					Query: `SELECT '{"a": "b"}'::jsonb || '{"c": "d"}'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0830-select-{-a-:-b"},
				},
				{
					Query: `SELECT '[1, 2]'::jsonb || '3'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0831-select-[1-2]-::jsonb-||"},
				},
				{
					Query: `SELECT '{"a": "b"}'::jsonb || '42'::jsonb;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0832-select-{-a-:-b"},
				},
			},
		},
		{
			Name: "Table Columns",
			SetUpScript: []string{
				`DROP TABLE IF EXISTS table_col_checks;`,
				`CREATE TABLE table_col_checks (v1 INT4, v2 INT8, v3 FLOAT4);`,
				`INSERT INTO table_col_checks VALUES (1, 2, 3), (4, 5, 6);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT v1 + v2 FROM table_col_checks ORDER BY v1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0833-select-v1-+-v2-from"},
				},
				{
					Query: `SELECT v2 - v1 FROM table_col_checks ORDER BY v1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0834-select-v2-v1-from-table_col_checks"},
				},
				{
					Query: `SELECT v3 * v3 FROM table_col_checks ORDER BY v1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0835-select-v3-*-v3-from"},
				},
				{
					Query: `SELECT v1 / 2::int4, v2 / 2::int8 FROM table_col_checks ORDER BY v1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0836-select-v1-/-2::int4-v2"},
				},
			},
		},
		{
			Name: "Concatenate",
			SetUpScript: []string{
				"SET timezone TO 'UTC';",
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT 'Hello, ' || 'World!';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0837-select-hello-||-world!"},
				},
				{
					Query: `SELECT '123' || '456';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0838-select-123-||-456"},
				},
				{
					Query: `SELECT 'foo' || 'bar' || 'baz';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0839-select-foo-||-bar-||"},
				},
				{
					Query: `SELECT 123 || '456';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0840-select-123-||-456"},
				},
				{
					Query: `SELECT '123' || 456;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0841-select-123-||-456"},
				},
				{
					Query: `SELECT '123' || 4.56;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0842-select-123-||-4.56"},
				},
				{
					Query: `SELECT 12.3 || '4.56';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0843-select-12.3-||-4.56"},
				},
				{
					Query: `SELECT true || 'bar' || false;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0844-select-true-||-bar-||"},
				},
				{
					Query: `SELECT '2000-01-01 00:00:00'::timestamp || ' happy new year';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0845-select-2000-01-01-00:00:00-::timestamp-||"},
				},
				{
					Query: `SELECT 'hello ' || '2000-01-01 00:00:00'::timestamp ;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0846-select-hello-||-2000-01-01-00:00:00"},
				},
				{
					Query: `SELECT '2000-01-01'::timestamp || ' happy new year';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0847-select-2000-01-01-::timestamp-||-happy"},
				},
				{
					Query: `SELECT '2000-01-01 00:00:00-08'::timestamptz || ' happy new year';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0848-select-2000-01-01-00:00:00-08-::timestamptz-||"},
				},
				{
					Query: `SELECT 'hello ' || '2000-01-01 00:00:00-08'::timestamptz;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0849-select-hello-||-2000-01-01-00:00:00-08"},
				},
				{
					Query: `SELECT '00:00:00'::time || ' midnight';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0850-select-00:00:00-::time-||-midnight"},
				},
				{
					Query: `SELECT 'midnight ' || '00:00:00'::time;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0851-select-midnight-||-00:00:00-::time"},
				},
				{
					Query: `SELECT '00:00:00-07'::timetz || ' midnight';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0852-select-00:00:00-07-::timetz-||-midnight"},
				},
				{
					Query: `SELECT 'midnight ' || '00:00:00-07'::timetz ;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0853-select-midnight-||-00:00:00-07-::timetz"},
				},
				{
					Query: `SELECT 'foo'::bytea || 'bar';`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0854-select-foo-::bytea-||-bar", ColumnModes: []string{"bytea"}},
				},
				{
					Query: `SELECT 'bar' || 'foo'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0855-select-bar-||-foo-::bytea", ColumnModes: []string{"bytea"}},
				},
				{
					Query: `SELECT '\xDEADBEEF'::bytea || '\xCAFEBABE'::bytea;`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0856-select-\\xdeadbeef-::bytea-||-\\xcafebabe", ColumnModes: []string{"bytea"}},
				},
			},
		},
		{
			Name: "ARRAY",
			Assertions: []ScriptTestAssertion{
				{
					Query: "SELECT ARRAY[1,2,3] @> ARRAY[2,2], ARRAY[2] <@ ARRAY[1,2,3], ARRAY[1,2] && ARRAY[2,3], ARRAY[1,2] && ARRAY[3,4];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0857-select-array[1-2-3]-@>"},
				},
				{
					Query: "SELECT ARRAY[1,NULL]::int[] @> ARRAY[NULL]::int[], ARRAY[NULL]::int[] && ARRAY[NULL]::int[];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0858-select-array[1-null]::int[]-@>-array[null]::int[]"},
				},
				{
					Query: "SELECT ARRAY['label','category']::name[] <@ ARRAY['id','label','category']::name[];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0859-select-array[-label-category-]::name[]"},
				},
				{
					Query: "SELECT (ARRAY[10,20])[1::bigint];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0860-select-array[10-20]-[1::bigint]"},
				},
				{
					Query: "SELECT (ARRAY[10,20])[2147483648::bigint];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0861-select-array[10-20]-[2147483648::bigint]", Compare: "sqlstate"},
				},
				{
					Query: "SELECT ARRAY[4] || 20;", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0862-select-array[4]-||-20"},
				},
				{
					Query: "SELECT 20 || ARRAY[4];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0863-select-20-||-array[4]"},
				},
				{
					Query: "SELECT ARRAY[4] || ARRAY[5,6];", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0864-select-array[4]-||-array[5-6]"},
				},
			},
		},
		{
			Name: "json extract text operator used in generated column default",
			SetUpScript: []string{
				`CREATE TABLE users_sync (
		raw_json jsonb NOT NULL,
		id text GENERATED ALWAYS AS ((raw_json ->> 'id'::text)) STORED NOT NULL
);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: "ALTER TABLE ONLY users_sync ADD CONSTRAINT users_sync_pkey PRIMARY KEY (id);", PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0865-alter-table-only-users_sync-add"},
				},
				{
					Query: `INSERT INTO users_sync (raw_json) VALUES ('{"id":2}')`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0866-insert-into-users_sync-raw_json-values"},
				},
				{
					Query: `SELECT * FROM users_sync`, PostgresOracle: ScriptTestPostgresOracle{ID: "operators-test-testoperators-0867-select-*-from-users_sync"},
				},
			},
		},
	})
}
