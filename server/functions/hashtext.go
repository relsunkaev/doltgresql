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
	"encoding/binary"
	"math/bits"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initHashText registers the functions to the catalog.
func initHashText() {
	framework.RegisterFunction(hashtext_text)
	framework.RegisterFunction(hashtextextended_text_int8)
}

// hashtext_text represents the PostgreSQL function hashtext(text).
var hashtext_text = framework.Function1{
	Name:       "hashtext",
	Return:     pgtypes.Int32,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val1 any) (any, error) {
		return int32(PgHashBytes([]byte(val1.(string)))), nil
	},
}

// hashtextextended_text_int8 represents the PostgreSQL function hashtextextended(text, bigint).
// It must agree with hashtext on its low 32 bits when the seed is zero.
var hashtextextended_text_int8 = framework.Function2{
	Name:       "hashtextextended",
	Return:     pgtypes.Int64,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Int64},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1 any, val2 any) (any, error) {
		return int64(PgHashBytesExtended([]byte(val1.(string)), uint64(val2.(int64)))), nil
	},
}

// PgHashBytes returns PostgreSQL's hash_bytes result for the given raw bytes.
func PgHashBytes(k []byte) uint32 {
	a, b, c := pgHashInit(len(k))
	a, b, c = pgHashConsume(a, b, c, k)
	_, _, c = pgHashFinal(a, b, c)
	return c
}

func pgHashBytes(k []byte) uint32 {
	return PgHashBytes(k)
}

// PgHashBytesExtended is the 64-bit seeded variant matching PostgreSQL's
// hash_bytes_extended. With seed == 0 it produces the same low 32 bits as
// PgHashBytes; with any non-zero seed it perturbs the initial mixer state.
func PgHashBytesExtended(k []byte, seed uint64) uint64 {
	a, b, c := pgHashInit(len(k))
	if seed != 0 {
		a += uint32(seed >> 32)
		b += uint32(seed)
		a, b, c = pgHashMix(a, b, c)
	}
	a, b, c = pgHashConsume(a, b, c, k)
	_, b, c = pgHashFinal(a, b, c)
	return uint64(b)<<32 | uint64(c)
}

func pgHashBytesExtended(k []byte, seed uint64) uint64 {
	return PgHashBytesExtended(k, seed)
}

func pgHashInit(keylen int) (uint32, uint32, uint32) {
	x := uint32(0x9e3779b9 + keylen + 3923095)
	return x, x, x
}

func pgHashConsume(a, b, c uint32, k []byte) (uint32, uint32, uint32) {
	for len(k) >= 12 {
		a += binary.LittleEndian.Uint32(k[0:4])
		b += binary.LittleEndian.Uint32(k[4:8])
		c += binary.LittleEndian.Uint32(k[8:12])
		a, b, c = pgHashMix(a, b, c)
		k = k[12:]
	}

	switch len(k) {
	case 11:
		c += uint32(k[10]) << 24
		fallthrough
	case 10:
		c += uint32(k[9]) << 16
		fallthrough
	case 9:
		c += uint32(k[8]) << 8
		fallthrough
	case 8:
		b += uint32(k[7]) << 24
		fallthrough
	case 7:
		b += uint32(k[6]) << 16
		fallthrough
	case 6:
		b += uint32(k[5]) << 8
		fallthrough
	case 5:
		b += uint32(k[4])
		fallthrough
	case 4:
		a += uint32(k[3]) << 24
		fallthrough
	case 3:
		a += uint32(k[2]) << 16
		fallthrough
	case 2:
		a += uint32(k[1]) << 8
		fallthrough
	case 1:
		a += uint32(k[0])
	}

	return a, b, c
}

func pgHashMix(a uint32, b uint32, c uint32) (uint32, uint32, uint32) {
	a -= c
	a ^= bits.RotateLeft32(c, 4)
	c += b
	b -= a
	b ^= bits.RotateLeft32(a, 6)
	a += c
	c -= b
	c ^= bits.RotateLeft32(b, 8)
	b += a
	a -= c
	a ^= bits.RotateLeft32(c, 16)
	c += b
	b -= a
	b ^= bits.RotateLeft32(a, 19)
	a += c
	c -= b
	c ^= bits.RotateLeft32(b, 4)
	b += a
	return a, b, c
}

func pgHashFinal(a uint32, b uint32, c uint32) (uint32, uint32, uint32) {
	c ^= b
	c -= bits.RotateLeft32(b, 14)
	a ^= c
	a -= bits.RotateLeft32(c, 11)
	b ^= a
	b -= bits.RotateLeft32(a, 25)
	c ^= b
	c -= bits.RotateLeft32(b, 16)
	a ^= c
	a -= bits.RotateLeft32(c, 4)
	b ^= a
	b -= bits.RotateLeft32(a, 14)
	c ^= b
	c -= bits.RotateLeft32(b, 24)
	return a, b, c
}
