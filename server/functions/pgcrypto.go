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
	"crypto/aes"
	"crypto/cipher"
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
	framework.RegisterFunction(pgcrypto_decrypt)
	framework.RegisterFunction(pgcrypto_decrypt_iv)
	framework.RegisterFunction(pgcrypto_digest_text)
	framework.RegisterFunction(pgcrypto_digest_bytea)
	framework.RegisterFunction(pgcrypto_encrypt)
	framework.RegisterFunction(pgcrypto_encrypt_iv)
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

var pgcrypto_encrypt = framework.Function3{
	Name:       "encrypt",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, key any, cipherType any) (any, error) {
		return pgcryptoRawCipher(data.([]byte), key.([]byte), nil, cipherType.(string), true)
	},
}

var pgcrypto_decrypt = framework.Function3{
	Name:       "decrypt",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, key any, cipherType any) (any, error) {
		return pgcryptoRawCipher(data.([]byte), key.([]byte), nil, cipherType.(string), false)
	},
}

var pgcrypto_encrypt_iv = framework.Function4{
	Name:       "encrypt_iv",
	Return:     pgtypes.Bytea,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, data any, key any, iv any, cipherType any) (any, error) {
		return pgcryptoRawCipher(data.([]byte), key.([]byte), iv.([]byte), cipherType.(string), true)
	},
}

var pgcrypto_decrypt_iv = framework.Function4{
	Name:       "decrypt_iv",
	Return:     pgtypes.Bytea,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, data any, key any, iv any, cipherType any) (any, error) {
		return pgcryptoRawCipher(data.([]byte), key.([]byte), iv.([]byte), cipherType.(string), false)
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

type pgcryptoRawCipherConfig struct {
	algorithm string
	mode      string
	padding   string
}

func pgcryptoRawCipher(data []byte, key []byte, iv []byte, cipherType string, encrypt bool) ([]byte, error) {
	config, err := pgcryptoParseRawCipherType(cipherType)
	if err != nil {
		return nil, err
	}
	if config.algorithm != "aes" {
		return nil, errors.Errorf("unsupported pgcrypto cipher algorithm: %s", config.algorithm)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Errorf("invalid pgcrypto aes key length: %d", len(key))
	}
	blockSize := block.BlockSize()
	input := append([]byte(nil), data...)
	if encrypt {
		input, err = pgcryptoPad(input, blockSize, config.padding)
	} else if len(input)%blockSize != 0 {
		return nil, errors.New("data not a multiple of block size")
	}
	if err != nil {
		return nil, err
	}

	output := make([]byte, len(input))
	switch config.mode {
	case "cbc":
		normalizedIV := pgcryptoNormalizeIV(iv, blockSize)
		if encrypt {
			cipher.NewCBCEncrypter(block, normalizedIV).CryptBlocks(output, input)
		} else {
			cipher.NewCBCDecrypter(block, normalizedIV).CryptBlocks(output, input)
		}
	case "ecb":
		for i := 0; i < len(input); i += blockSize {
			if encrypt {
				block.Encrypt(output[i:i+blockSize], input[i:i+blockSize])
			} else {
				block.Decrypt(output[i:i+blockSize], input[i:i+blockSize])
			}
		}
	default:
		return nil, errors.Errorf("unsupported pgcrypto cipher mode: %s", config.mode)
	}
	if !encrypt {
		output, err = pgcryptoUnpad(output, blockSize, config.padding)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

func pgcryptoParseRawCipherType(cipherType string) (pgcryptoRawCipherConfig, error) {
	config := pgcryptoRawCipherConfig{
		mode:    "cbc",
		padding: "pkcs",
	}
	parts := strings.Split(strings.ToLower(cipherType), "/")
	cipherParts := strings.Split(parts[0], "-")
	switch len(cipherParts) {
	case 1:
		config.algorithm = cipherParts[0]
	case 2:
		config.algorithm = cipherParts[0]
		config.mode = cipherParts[1]
	default:
		return config, errors.Errorf("invalid pgcrypto cipher type: %s", cipherType)
	}
	if config.algorithm == "" {
		return config, errors.Errorf("invalid pgcrypto cipher type: %s", cipherType)
	}
	for _, option := range parts[1:] {
		if strings.HasPrefix(option, "pad:") {
			config.padding = strings.TrimPrefix(option, "pad:")
			continue
		}
		return config, errors.Errorf("unsupported pgcrypto cipher option: %s", option)
	}
	switch config.mode {
	case "cbc", "ecb":
	default:
		return config, errors.Errorf("unsupported pgcrypto cipher mode: %s", config.mode)
	}
	switch config.padding {
	case "pkcs", "none":
	default:
		return config, errors.Errorf("unsupported pgcrypto cipher padding: %s", config.padding)
	}
	return config, nil
}

func pgcryptoNormalizeIV(iv []byte, blockSize int) []byte {
	normalizedIV := make([]byte, blockSize)
	copy(normalizedIV, iv)
	return normalizedIV
}

func pgcryptoPad(data []byte, blockSize int, padding string) ([]byte, error) {
	switch padding {
	case "none":
		if len(data)%blockSize != 0 {
			return nil, errors.New("data not a multiple of block size")
		}
		return data, nil
	case "pkcs":
		padLen := blockSize - len(data)%blockSize
		for i := 0; i < padLen; i++ {
			data = append(data, byte(padLen))
		}
		return data, nil
	default:
		return nil, errors.Errorf("unsupported pgcrypto cipher padding: %s", padding)
	}
}

func pgcryptoUnpad(data []byte, blockSize int, padding string) ([]byte, error) {
	switch padding {
	case "none":
		return data, nil
	case "pkcs":
		if len(data) == 0 || len(data)%blockSize != 0 {
			return nil, errors.New("invalid padding")
		}
		padLen := int(data[len(data)-1])
		if padLen == 0 || padLen > blockSize || padLen > len(data) {
			return nil, errors.New("invalid padding")
		}
		for _, b := range data[len(data)-padLen:] {
			if int(b) != padLen {
				return nil, errors.New("invalid padding")
			}
		}
		return data[:len(data)-padLen], nil
	default:
		return nil, errors.Errorf("unsupported pgcrypto cipher padding: %s", padding)
	}
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
