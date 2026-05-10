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
	"encoding/base64"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/crypto/blowfish"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	pgcryptoBcryptDefaultCost = 6
	pgcryptoBcryptMinCost     = 4
	pgcryptoBcryptMaxCost     = 31
	pgcryptoBcryptSaltBytes   = 16
	pgcryptoBcryptSaltLength  = 29
)

var pgcryptoBcryptEncoding = base64.NewEncoding("./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").WithPadding(base64.NoPadding)
var pgcryptoBcryptMagicCipherData = []byte("OrpheanBeholderScryDoubt")

// initPgCrypto registers the pgcrypto-compatible functions to the catalog.
func initPgCrypto() {
	framework.RegisterFunction(pgcrypto_crypt)
	framework.RegisterFunction(pgcrypto_digest_text)
	framework.RegisterFunction(pgcrypto_digest_bytea)
	framework.RegisterFunction(pgcrypto_gen_salt)
	framework.RegisterFunction(pgcrypto_gen_salt_with_count)
	framework.RegisterFunction(pgcrypto_gen_random_bytes)
	framework.RegisterFunction(pgcrypto_hmac_text)
	framework.RegisterFunction(pgcrypto_hmac_bytea)
}

var pgcrypto_crypt = framework.Function2{
	Name:       "crypt",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, password any, salt any) (any, error) {
		return pgcryptoCrypt(password.(string), salt.(string))
	},
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

var pgcrypto_gen_salt = framework.Function1{
	Name:               "gen_salt",
	Return:             pgtypes.Text,
	Parameters:         [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:             true,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, saltType any) (any, error) {
		return pgcryptoGenSalt(saltType.(string), pgcryptoBcryptDefaultCost)
	},
}

var pgcrypto_gen_salt_with_count = framework.Function2{
	Name:               "gen_salt",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Int32},
	Strict:             true,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, saltType any, iterCount any) (any, error) {
		return pgcryptoGenSalt(saltType.(string), int(iterCount.(int32)))
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

func pgcryptoCrypt(password string, salt string) (string, error) {
	cost, encodedSalt, minor, ok, err := pgcryptoBcryptSaltParts(salt)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	return pgcryptoBcryptHash(password, cost, minor, encodedSalt)
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

func pgcryptoGenSalt(saltType string, iterCount int) (string, error) {
	switch strings.ToLower(saltType) {
	case "bf":
	default:
		return "", errors.Errorf("unsupported pgcrypto gen_salt type: %s", saltType)
	}
	if iterCount < pgcryptoBcryptMinCost || iterCount > pgcryptoBcryptMaxCost {
		return "", errors.Errorf("gen_salt iteration count %d is outside allowed inclusive range %d..%d for bf", iterCount, pgcryptoBcryptMinCost, pgcryptoBcryptMaxCost)
	}
	rawSalt := make([]byte, pgcryptoBcryptSaltBytes)
	if _, err := rand.Read(rawSalt); err != nil {
		return "", err
	}
	encodedSalt := pgcryptoBcryptEncoding.EncodeToString(rawSalt)
	return fmt.Sprintf("$2a$%02d$%s", iterCount, encodedSalt), nil
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

func pgcryptoBcryptSaltParts(salt string) (int, string, byte, bool, error) {
	if len(salt) < pgcryptoBcryptSaltLength {
		return 0, "", 0, false, nil
	}
	if salt[0] != '$' || salt[1] != '2' || salt[3] != '$' || salt[6] != '$' {
		return 0, "", 0, false, nil
	}
	cost, err := strconv.Atoi(salt[4:6])
	if err != nil {
		return 0, "", 0, false, errors.Errorf("invalid bcrypt salt cost: %s", salt[4:6])
	}
	if cost < pgcryptoBcryptMinCost || cost > pgcryptoBcryptMaxCost {
		return 0, "", 0, false, errors.Errorf("bcrypt salt cost %d is outside allowed inclusive range %d..%d", cost, pgcryptoBcryptMinCost, pgcryptoBcryptMaxCost)
	}
	encodedSalt := salt[7:pgcryptoBcryptSaltLength]
	return cost, encodedSalt, salt[2], true, nil
}

func pgcryptoBcryptHash(password string, cost int, minor byte, encodedSalt string) (string, error) {
	hash, err := pgcryptoBcrypt(pgcryptoBcryptPasswordBytes(password), cost, encodedSalt)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("$2%c$%02d$%s%s", minor, cost, encodedSalt, hash), nil
}

func pgcryptoBcrypt(password []byte, cost int, encodedSalt string) (string, error) {
	decodedSalt, err := pgcryptoBcryptEncoding.DecodeString(encodedSalt)
	if err != nil {
		return "", err
	}
	cipherData := make([]byte, len(pgcryptoBcryptMagicCipherData))
	copy(cipherData, pgcryptoBcryptMagicCipherData)
	key := append(password[:len(password):len(password)], 0)
	cipher, err := blowfish.NewSaltedCipher(key, decodedSalt)
	if err != nil {
		return "", err
	}
	rounds := uint64(1) << uint(cost)
	for i := uint64(0); i < rounds; i++ {
		blowfish.ExpandKey(key, cipher)
		blowfish.ExpandKey(decodedSalt, cipher)
	}
	for i := 0; i < 24; i += 8 {
		for j := 0; j < 64; j++ {
			cipher.Encrypt(cipherData[i:i+8], cipherData[i:i+8])
		}
	}
	return pgcryptoBcryptEncoding.EncodeToString(cipherData[:23]), nil
}

func pgcryptoBcryptPasswordBytes(password string) []byte {
	passwordBytes := []byte(password)
	if len(passwordBytes) > 72 {
		passwordBytes = passwordBytes[:72]
	}
	return passwordBytes
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
