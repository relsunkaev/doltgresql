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
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initPgCrypto registers the pgcrypto-compatible functions to the catalog.
func initPgCrypto() {
	framework.RegisterFunction(pgcrypto_digest_text)
	framework.RegisterFunction(pgcrypto_digest_bytea)
	framework.RegisterFunction(pgcrypto_gen_random_bytes)
	framework.RegisterFunction(pgcrypto_hmac_text)
	framework.RegisterFunction(pgcrypto_hmac_bytea)
}

var pgcrypto_digest_text = framework.Function2{
	Name:       "digest",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, data any, algorithm any) (any, error) {
		return pgcryptoDigest([]byte(data.(string)), algorithm.(string))
	},
}

var pgcrypto_digest_bytea = framework.Function2{
	Name:       "digest",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, data any, algorithm any) (any, error) {
		return pgcryptoDigest(data.([]byte), algorithm.(string))
	},
}

var pgcrypto_hmac_text = framework.Function3{
	Name:       "hmac",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, key any, algorithm any) (any, error) {
		return pgcryptoHMAC([]byte(data.(string)), []byte(key.(string)), algorithm.(string))
	},
}

var pgcrypto_hmac_bytea = framework.Function3{
	Name:       "hmac",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, key any, algorithm any) (any, error) {
		return pgcryptoHMAC(data.([]byte), key.([]byte), algorithm.(string))
	},
}

var pgcrypto_gen_random_bytes = framework.Function1{
	Name:       "gen_random_bytes",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, count any) (any, error) {
		byteCount := count.(int32)
		if byteCount < 1 || byteCount > 1024 {
			return nil, errors.Errorf("Length not in range")
		}
		ret := make([]byte, byteCount)
		_, err := rand.Read(ret)
		return ret, err
	},
}

func pgcryptoDigest(data []byte, algorithm string) ([]byte, error) {
	hashFactory, err := pgcryptoHash(algorithm)
	if err != nil {
		return nil, err
	}
	h := hashFactory()
	_, _ = h.Write(data)
	return h.Sum(nil), nil
}

func pgcryptoHMAC(data []byte, key []byte, algorithm string) ([]byte, error) {
	hashFactory, err := pgcryptoHash(algorithm)
	if err != nil {
		return nil, err
	}
	h := hmac.New(hashFactory, key)
	_, _ = h.Write(data)
	return h.Sum(nil), nil
}

func pgcryptoHash(algorithm string) (func() hash.Hash, error) {
	switch strings.ReplaceAll(strings.ToLower(algorithm), "-", "") {
	case "md5":
		return md5.New, nil
	case "sha1":
		return sha1.New, nil
	case "sha224":
		return sha256.New224, nil
	case "sha256":
		return sha256.New, nil
	case "sha384":
		return sha512.New384, nil
	case "sha512":
		return sha512.New, nil
	}
	return nil, errors.Errorf("unsupported pgcrypto digest algorithm: %s", algorithm)
}
