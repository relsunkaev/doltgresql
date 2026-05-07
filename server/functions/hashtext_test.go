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

package functions

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPgHashBytes asserts the byte-level lookup3-style hash agrees with values
// produced by upstream PostgreSQL 16's hashtext().
func TestPgHashBytes(t *testing.T) {
	cases := []struct {
		in   string
		want uint32
	}{
		{"", 0xa7ea466d},
		{"abc", 0xd12feb97},
		{"electric_slot_default", 0x9695b86d},
		{"PostgreSQL", 0x9ae1fe84},
		{"ümlaut", 0x54f154af},
		{"the quick brown fox jumps over the lazy dog", 0x08ba7fd1},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := pgHashBytes([]byte(tc.in))
			assert.Equal(t, tc.want, got, "pgHashBytes(%q)", tc.in)
		})
	}
}

// TestPgHashBytesExtended asserts the seeded 64-bit hash matches values
// produced by upstream PostgreSQL 16's hashtextextended().
func TestPgHashBytesExtended(t *testing.T) {
	cases := []struct {
		in   string
		seed uint64
		want uint64
	}{
		{"", 0, 0x9fb1b765a7ea466d},
		{"PostgreSQL", 0, 0xafea81699ae1fe84},
		{"PostgreSQL", 1, 0xed24688f82697658},
		{"abc", 0, 0xa25b273dd12feb97},
		{"abc", 1234567890123, 0x971663f2e16f14e0},
		{"abc", 0xffffffffffffffff, 0x1a86146d4d561077}, // -1 as uint64
		{"ümlaut", 0, 0xa659f97554f154af},
		{"electric_slot_default", 0, 0x2aba591c9695b86d},
		{"a", 0, 0x31d94c66401370b1},
		{"ab", 0, 0x2751f60c666effbd},
		{"abcd", 0, 0xf196c20ce885082c},
		{"abcdefghijkl", 0, 0x97216e83a1763ad4},
		{"abcdefghijklm", 0, 0x7b1e572e1830c6d0},
		{"the quick brown fox jumps over the lazy dog", 0, 0xa5dfa74c08ba7fd1},
		{"the quick brown fox jumps over the lazy dog", 0x7fffffffffffffff, 0xe401ad05a5a33147},
		{"the quick brown fox jumps over the lazy dog", 0x8000000000000000, 0x26c402efddec3689},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := pgHashBytesExtended([]byte(tc.in), tc.seed)
			assert.Equal(t, tc.want, got, "pgHashBytesExtended(%q, %#x)", tc.in, tc.seed)
		})
	}
}

// TestPgHashBytesExtendedSeedZero asserts the documented invariant that the
// low 32 bits of hashtextextended(v, 0) equal hashtext(v) for any input.
func TestPgHashBytesExtendedSeedZero(t *testing.T) {
	inputs := []string{
		"", "x", "ab", "abc", "abcd", "abcdefghijk", "abcdefghijkl",
		"abcdefghijklm", "abcdefghijklmnopqrst", "PostgreSQL",
		"electric_slot_default", "ümlaut", "日本語",
		strings.Repeat("z", 32), strings.Repeat("\x00", 12),
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			extended := pgHashBytesExtended([]byte(in), 0)
			standard := pgHashBytes([]byte(in))
			assert.Equal(t, uint64(standard), extended&0xffffffff,
				"low 32 bits of hashtextextended(%q, 0) must equal hashtext(%q)", in, in)
		})
	}
}

// TestPgHashBytesExtendedDifferentSeeds asserts that any non-zero seed
// perturbs the result so it is distinct from the unseeded form.
func TestPgHashBytesExtendedDifferentSeeds(t *testing.T) {
	in := []byte("PostgreSQL")
	zero := pgHashBytesExtended(in, 0)
	for _, seed := range []uint64{1, 2, 0xdeadbeef, 0xffffffff, 0xffffffffffffffff} {
		got := pgHashBytesExtended(in, seed)
		assert.NotEqual(t, zero, got, "seed %#x must perturb the result", seed)
	}
}
