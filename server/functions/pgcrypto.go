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
	"crypto/des"
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
	cryptdes "github.com/sergeymakinen/go-crypt/des"
	cryptdesext "github.com/sergeymakinen/go-crypt/desext"
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
	pgcryptoMD5SaltBytes      = 6
	pgcryptoDESSaltBytes      = 2
	pgcryptoXDESInputBytes    = 3
	pgcryptoXDESDefaultRounds = 29 * 25
	pgcryptoXDESMaxRounds     = 0xFFFFFF
	pgcryptoArmorLineLength   = 76
	pgcryptoArmorCRC24Init    = 0x00b704ce
	pgcryptoArmorCRC24Poly    = 0x01864cfb
)

var pgcryptoBcryptEncoding = base64.NewEncoding("./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").WithPadding(base64.NoPadding)
var pgcryptoBcryptMagicCipherData = []byte("OrpheanBeholderScryDoubt")
var pgcryptoMD5Encoding = []byte("./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
var pgcryptoCryptBigEndianEncoding = base64.NewEncoding(string(pgcryptoMD5Encoding)).WithPadding(base64.NoPadding)

// initPgCrypto registers the pgcrypto-compatible functions to the catalog.
func initPgCrypto() {
	framework.RegisterFunction(pgcrypto_crypt)
	framework.RegisterFunction(pgcrypto_armor)
	framework.RegisterFunction(pgcrypto_armor_with_headers)
	framework.RegisterFunction(pgcrypto_decrypt)
	framework.RegisterFunction(pgcrypto_decrypt_iv)
	framework.RegisterFunction(pgcrypto_dearmor)
	framework.RegisterFunction(pgcrypto_digest_text)
	framework.RegisterFunction(pgcrypto_digest_bytea)
	framework.RegisterFunction(pgcrypto_encrypt)
	framework.RegisterFunction(pgcrypto_encrypt_iv)
	framework.RegisterFunction(pgcrypto_gen_salt)
	framework.RegisterFunction(pgcrypto_gen_salt_with_count)
	framework.RegisterFunction(pgcrypto_gen_random_bytes)
	framework.RegisterFunction(pgcrypto_hmac_text)
	framework.RegisterFunction(pgcrypto_hmac_bytea)
	framework.RegisterFunction(pgcrypto_pgp_armor_headers)
	framework.RegisterFunction(pgcrypto_pgp_key_id)
	framework.RegisterFunction(pgcrypto_pgp_sym_encrypt)
	framework.RegisterFunction(pgcrypto_pgp_sym_encrypt_options)
	framework.RegisterFunction(pgcrypto_pgp_sym_encrypt_bytea)
	framework.RegisterFunction(pgcrypto_pgp_sym_encrypt_bytea_options)
	framework.RegisterFunction(pgcrypto_pgp_sym_decrypt)
	framework.RegisterFunction(pgcrypto_pgp_sym_decrypt_options)
	framework.RegisterFunction(pgcrypto_pgp_sym_decrypt_bytea)
	framework.RegisterFunction(pgcrypto_pgp_sym_decrypt_bytea_options)
	framework.RegisterFunction(pgcrypto_pgp_pub_encrypt)
	framework.RegisterFunction(pgcrypto_pgp_pub_encrypt_options)
	framework.RegisterFunction(pgcrypto_pgp_pub_encrypt_bytea)
	framework.RegisterFunction(pgcrypto_pgp_pub_encrypt_bytea_options)
	framework.RegisterFunction(pgcrypto_pgp_pub_decrypt)
	framework.RegisterFunction(pgcrypto_pgp_pub_decrypt_options)
	framework.RegisterFunction(pgcrypto_pgp_pub_decrypt_options_password)
	framework.RegisterFunction(pgcrypto_pgp_pub_decrypt_bytea)
	framework.RegisterFunction(pgcrypto_pgp_pub_decrypt_bytea_options)
	framework.RegisterFunction(pgcrypto_pgp_pub_decrypt_bytea_options_password)
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

var pgcrypto_armor = framework.Function1{
	Name:       "armor",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, data any) (any, error) {
		return pgcryptoArmor(data.([]byte), nil, nil), nil
	},
}

var pgcrypto_armor_with_headers = framework.Function3{
	Name:       "armor",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.TextArray, pgtypes.TextArray},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, keys any, values any) (any, error) {
		headerKeys, headerValues, err := pgcryptoArmorHeaderArrays(ctx, keys, values)
		if err != nil {
			return nil, err
		}
		return pgcryptoArmor(data.([]byte), headerKeys, headerValues), nil
	},
}

var pgcrypto_dearmor = framework.Function1{
	Name:       "dearmor",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, armored any) (any, error) {
		return pgcryptoDearmor(armored.(string))
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
		return pgcryptoGenSalt(saltType.(string), 0, false)
	},
}

var pgcrypto_gen_salt_with_count = framework.Function2{
	Name:               "gen_salt",
	Return:             pgtypes.Text,
	Parameters:         [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Int32},
	Strict:             true,
	IsNonDeterministic: true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, saltType any, iterCount any) (any, error) {
		return pgcryptoGenSalt(saltType.(string), int(iterCount.(int32)), true)
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

func pgcryptoArmor(data []byte, headerKeys []string, headerValues []string) string {
	var builder strings.Builder
	builder.WriteString("-----BEGIN PGP MESSAGE-----\n")
	for i := range headerKeys {
		builder.WriteString(headerKeys[i])
		builder.WriteString(": ")
		builder.WriteString(headerValues[i])
		builder.WriteByte('\n')
	}
	builder.WriteByte('\n')

	encoded := base64.StdEncoding.EncodeToString(data)
	for len(encoded) > 0 {
		lineLength := pgcryptoArmorLineLength
		if len(encoded) < lineLength {
			lineLength = len(encoded)
		}
		builder.WriteString(encoded[:lineLength])
		builder.WriteByte('\n')
		encoded = encoded[lineLength:]
	}

	builder.WriteByte('=')
	builder.WriteString(pgcryptoArmorCRC24Base64(data))
	builder.WriteString("\n-----END PGP MESSAGE-----\n")
	return builder.String()
}

func pgcryptoArmorHeaderArrays(ctx *sql.Context, keys any, values any) ([]string, []string, error) {
	keyValues, err := textArrayArg(ctx, keys)
	if err != nil {
		return nil, nil, err
	}
	valueValues, err := textArrayArg(ctx, values)
	if err != nil {
		return nil, nil, err
	}
	if len(keyValues) != len(valueValues) {
		return nil, nil, errors.Errorf("mismatched array dimensions")
	}
	headerKeys := make([]string, len(keyValues))
	headerValues := make([]string, len(valueValues))
	for i := range keyValues {
		if keyValues[i] == nil || valueValues[i] == nil {
			return nil, nil, errors.Errorf("armor header arrays must not contain nulls")
		}
		key, ok := keyValues[i].(string)
		if !ok {
			return nil, nil, errors.Errorf("expected text armor header key, got %T", keyValues[i])
		}
		value, ok := valueValues[i].(string)
		if !ok {
			return nil, nil, errors.Errorf("expected text armor header value, got %T", valueValues[i])
		}
		headerKeys[i] = key
		headerValues[i] = value
	}
	return headerKeys, headerValues, nil
}

func pgcryptoDearmor(armored string) ([]byte, error) {
	lines := pgcryptoArmorLines(armored)
	begin := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "-----BEGIN") {
			begin = i
			break
		}
	}
	if begin < 0 || !pgcryptoArmorHeaderLine(lines[begin], "BEGIN") {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}

	end := -1
	for i := begin + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "-----END") {
			end = i
			break
		}
	}
	if end < 0 || !pgcryptoArmorHeaderLine(lines[end], "END") {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}

	base64Start := begin + 1
	for base64Start < end && strings.TrimSpace(lines[base64Start]) != "" {
		base64Start++
	}
	if base64Start >= end {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}
	base64Start++

	crcLine := -1
	for i := end - 1; i >= base64Start; i-- {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), "=") {
			crcLine = i
			break
		}
	}
	if crcLine < 0 {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}

	expectedCRC, err := pgcryptoArmorDecodeCRC24(strings.TrimSpace(lines[crcLine]))
	if err != nil {
		return nil, err
	}
	encoded := strings.Builder{}
	for _, line := range lines[base64Start:crcLine] {
		encoded.WriteString(strings.TrimSpace(line))
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded.String())
	if err != nil {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}
	if pgcryptoArmorCRC24(decoded) != expectedCRC {
		return nil, errors.Errorf("Corrupt ascii-armor")
	}
	return decoded, nil
}

func pgcryptoArmorLines(armored string) []string {
	normalized := strings.ReplaceAll(armored, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	return strings.Split(normalized, "\n")
}

func pgcryptoArmorHeaderLine(line string, marker string) bool {
	if !strings.HasPrefix(line, "-----"+marker) {
		return false
	}
	rest := strings.TrimPrefix(line, "-----"+marker)
	rest = strings.TrimSpace(rest)
	return strings.HasSuffix(rest, "-----")
}

func pgcryptoArmorDecodeCRC24(line string) (uint32, error) {
	if len(line) != 5 || line[0] != '=' {
		return 0, errors.Errorf("Corrupt ascii-armor")
	}
	decoded, err := base64.StdEncoding.DecodeString(line[1:])
	if err != nil || len(decoded) != 3 {
		return 0, errors.Errorf("Corrupt ascii-armor")
	}
	return uint32(decoded[0])<<16 | uint32(decoded[1])<<8 | uint32(decoded[2]), nil
}

func pgcryptoArmorCRC24Base64(data []byte) string {
	crc := pgcryptoArmorCRC24(data)
	crcBytes := []byte{byte(crc >> 16), byte(crc >> 8), byte(crc)}
	return base64.StdEncoding.EncodeToString(crcBytes)
}

func pgcryptoArmorCRC24(data []byte) uint32 {
	crc := uint32(pgcryptoArmorCRC24Init)
	for _, b := range data {
		crc ^= uint32(b) << 16
		for i := 0; i < 8; i++ {
			crc <<= 1
			if crc&0x1000000 != 0 {
				crc ^= pgcryptoArmorCRC24Poly
			}
		}
	}
	return crc & 0xffffff
}

func pgcryptoCrypt(password string, salt string) (string, error) {
	if md5Salt, ok := pgcryptoMD5SaltParts(salt); ok {
		return pgcryptoMD5Crypt(password, md5Salt), nil
	}
	if strings.HasPrefix(salt, "_") {
		return pgcryptoXDESCrypt(password, salt)
	}
	cost, encodedSalt, minor, ok, err := pgcryptoBcryptSaltParts(salt)
	if err != nil {
		return "", err
	}
	if ok {
		return pgcryptoBcryptHash(password, cost, minor, encodedSalt)
	}
	if strings.HasPrefix(salt, "$") {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	return pgcryptoDESCrypt(password, salt)
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
	var block cipher.Block
	switch config.algorithm {
	case "aes":
		block, err = aes.NewCipher(key)
		if err != nil {
			return nil, errors.Errorf("invalid pgcrypto aes key length: %d", len(key))
		}
	case "bf":
		block, err = blowfish.NewCipher(key)
		if err != nil {
			return nil, errors.Errorf("invalid pgcrypto bf key length: %d", len(key))
		}
	case "des":
		block, err = des.NewCipher(key)
		if err != nil {
			return nil, errors.Errorf("invalid pgcrypto des key length: %d", len(key))
		}
	case "3des":
		block, err = des.NewTripleDESCipher(key)
		if err != nil {
			return nil, errors.Errorf("invalid pgcrypto 3des key length: %d", len(key))
		}
	default:
		return nil, errors.Errorf("unsupported pgcrypto cipher algorithm: %s", config.algorithm)
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

func pgcryptoGenSalt(saltType string, iterCount int, hasIterCount bool) (string, error) {
	switch strings.ToLower(saltType) {
	case "bf":
		if !hasIterCount {
			iterCount = pgcryptoBcryptDefaultCost
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
	case "md5":
		if hasIterCount && iterCount != 1000 {
			return "", errors.Errorf("gen_salt iteration count %d is unsupported for md5", iterCount)
		}
		rawSalt := make([]byte, pgcryptoMD5SaltBytes)
		if _, err := rand.Read(rawSalt); err != nil {
			return "", err
		}
		return pgcryptoMD5Salt(rawSalt), nil
	case "des":
		if hasIterCount && iterCount != 0 && iterCount != 25 {
			return "", errors.Errorf("gen_salt iteration count %d is unsupported for des", iterCount)
		}
		rawSalt := make([]byte, pgcryptoDESSaltBytes)
		if _, err := rand.Read(rawSalt); err != nil {
			return "", err
		}
		var builder strings.Builder
		for _, b := range rawSalt {
			builder.WriteByte(pgcryptoMD5Encoding[b&0x3f])
		}
		return builder.String(), nil
	case "xdes":
		if !hasIterCount || iterCount == 0 {
			iterCount = pgcryptoXDESDefaultRounds
		}
		if iterCount < 1 || iterCount > pgcryptoXDESMaxRounds || iterCount%2 == 0 {
			return "", errors.Errorf("gen_salt iteration count %d is outside supported odd range 1..%d for xdes", iterCount, pgcryptoXDESMaxRounds)
		}
		rawSalt := make([]byte, pgcryptoXDESInputBytes)
		if _, err := rand.Read(rawSalt); err != nil {
			return "", err
		}
		saltValue := uint32(rawSalt[0]) | uint32(rawSalt[1])<<8 | uint32(rawSalt[2])<<16
		return "_" + pgcryptoCryptEncode64(uint32(iterCount), 4) + pgcryptoCryptEncode64(saltValue, 4), nil
	default:
		return "", errors.Errorf("unsupported pgcrypto gen_salt type: %s", saltType)
	}
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

func pgcryptoDESCrypt(password string, salt string) (string, error) {
	if len(salt) < pgcryptoDESSaltBytes {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	saltBytes := []byte(salt[:pgcryptoDESSaltBytes])
	if !pgcryptoValidCryptSalt(saltBytes) {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	passwordBytes := []byte(password)
	if len(passwordBytes) > cryptdes.MaxPasswordLength {
		passwordBytes = passwordBytes[:cryptdes.MaxPasswordLength]
	}
	key, err := cryptdes.Key(passwordBytes, saltBytes)
	if err != nil {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	return string(saltBytes) + pgcryptoCryptBigEndianEncoding.EncodeToString(key), nil
}

func pgcryptoXDESCrypt(password string, salt string) (string, error) {
	const prefixLength = 1 + 4 + 4
	if len(salt) < prefixLength {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	roundsPart := salt[1:5]
	saltPart := salt[5:9]
	if !pgcryptoValidCryptSalt([]byte(roundsPart)) || !pgcryptoValidCryptSalt([]byte(saltPart)) {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	rounds, ok := pgcryptoCryptDecode64(roundsPart)
	if !ok || rounds < 1 || rounds > pgcryptoXDESMaxRounds {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	key, err := cryptdesext.Key([]byte(password), []byte(saltPart), rounds)
	if err != nil {
		return "", errors.Errorf("unsupported pgcrypto crypt salt")
	}
	return salt[:prefixLength] + pgcryptoCryptBigEndianEncoding.EncodeToString(key), nil
}

func pgcryptoMD5SaltParts(salt string) (string, bool) {
	const magic = "$1$"
	if !strings.HasPrefix(salt, magic) {
		return "", false
	}
	salt = salt[len(magic):]
	if end := strings.IndexByte(salt, '$'); end >= 0 {
		salt = salt[:end]
	}
	if len(salt) > 8 {
		salt = salt[:8]
	}
	return salt, true
}

func pgcryptoMD5Salt(input []byte) string {
	var builder strings.Builder
	builder.WriteString("$1$")
	value := uint32(input[0]) | uint32(input[1])<<8 | uint32(input[2])<<16
	pgcryptoMD5To64(&builder, value, 4)
	value = uint32(input[3]) | uint32(input[4])<<8 | uint32(input[5])<<16
	pgcryptoMD5To64(&builder, value, 4)
	return builder.String()
}

func pgcryptoMD5Crypt(password string, salt string) string {
	const magic = "$1$"
	passwordBytes := []byte(password)
	saltBytes := []byte(salt)

	alternate := md5.New()
	_, _ = alternate.Write(passwordBytes)
	_, _ = alternate.Write(saltBytes)
	_, _ = alternate.Write(passwordBytes)
	final := alternate.Sum(nil)

	ctx := md5.New()
	_, _ = ctx.Write(passwordBytes)
	_, _ = ctx.Write([]byte(magic))
	_, _ = ctx.Write(saltBytes)
	for passwordLen := len(passwordBytes); passwordLen > 0; passwordLen -= md5.Size {
		chunkLen := passwordLen
		if chunkLen > md5.Size {
			chunkLen = md5.Size
		}
		_, _ = ctx.Write(final[:chunkLen])
	}
	for i := range final {
		final[i] = 0
	}
	for i := len(passwordBytes); i > 0; i >>= 1 {
		if i&1 == 1 {
			_, _ = ctx.Write(final[:1])
		} else {
			_, _ = ctx.Write(passwordBytes[:1])
		}
	}
	final = ctx.Sum(nil)

	for i := 0; i < 1000; i++ {
		round := md5.New()
		if i&1 == 1 {
			_, _ = round.Write(passwordBytes)
		} else {
			_, _ = round.Write(final)
		}
		if i%3 != 0 {
			_, _ = round.Write(saltBytes)
		}
		if i%7 != 0 {
			_, _ = round.Write(passwordBytes)
		}
		if i&1 == 1 {
			_, _ = round.Write(final)
		} else {
			_, _ = round.Write(passwordBytes)
		}
		final = round.Sum(nil)
	}

	var builder strings.Builder
	builder.WriteString(magic)
	builder.WriteString(salt)
	builder.WriteByte('$')
	pgcryptoMD5To64(&builder, uint32(final[0])<<16|uint32(final[6])<<8|uint32(final[12]), 4)
	pgcryptoMD5To64(&builder, uint32(final[1])<<16|uint32(final[7])<<8|uint32(final[13]), 4)
	pgcryptoMD5To64(&builder, uint32(final[2])<<16|uint32(final[8])<<8|uint32(final[14]), 4)
	pgcryptoMD5To64(&builder, uint32(final[3])<<16|uint32(final[9])<<8|uint32(final[15]), 4)
	pgcryptoMD5To64(&builder, uint32(final[4])<<16|uint32(final[10])<<8|uint32(final[5]), 4)
	pgcryptoMD5To64(&builder, uint32(final[11]), 2)
	return builder.String()
}

func pgcryptoMD5To64(builder *strings.Builder, value uint32, count int) {
	for i := 0; i < count; i++ {
		builder.WriteByte(pgcryptoMD5Encoding[value&0x3f])
		value >>= 6
	}
}

func pgcryptoCryptEncode64(value uint32, count int) string {
	var builder strings.Builder
	for i := 0; i < count; i++ {
		builder.WriteByte(pgcryptoMD5Encoding[value&0x3f])
		value >>= 6
	}
	return builder.String()
}

func pgcryptoCryptDecode64(input string) (uint32, bool) {
	var value uint32
	for i := 0; i < len(input); i++ {
		index := pgcryptoCryptEncodingIndex(input[i])
		if index < 0 {
			return 0, false
		}
		value |= uint32(index) << uint(i*6)
	}
	return value, true
}

func pgcryptoCryptEncodingIndex(b byte) int {
	for i, candidate := range pgcryptoMD5Encoding {
		if candidate == b {
			return i
		}
	}
	return -1
}

func pgcryptoValidCryptSalt(salt []byte) bool {
	for _, b := range salt {
		if pgcryptoCryptEncodingIndex(b) < 0 {
			return false
		}
	}
	return true
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
