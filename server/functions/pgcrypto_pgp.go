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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func pgcryptoPGPUnsupported(operation string) error {
	return pgerror.New(pgcode.ExternalRoutineInvocationException, "pgcrypto PGP "+operation+" is not yet supported")
}

var pgcrypto_pgp_key_id = framework.Function1{
	Name:       "pgp_key_id",
	Return:     pgtypes.Text,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, msg any) (any, error) {
		return nil, pgcryptoPGPUnsupported("key inspection")
	},
}

var pgcrypto_pgp_sym_encrypt = framework.Function2{
	Name:       "pgp_sym_encrypt",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, data any, psw any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_sym_encrypt_options = framework.Function3{
	Name:       "pgp_sym_encrypt",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, psw any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_sym_encrypt_bytea = framework.Function2{
	Name:       "pgp_sym_encrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, data any, psw any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_sym_encrypt_bytea_options = framework.Function3{
	Name:       "pgp_sym_encrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, psw any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_sym_decrypt = framework.Function2{
	Name:       "pgp_sym_decrypt",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, msg any, psw any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_sym_decrypt_options = framework.Function3{
	Name:       "pgp_sym_decrypt",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, msg any, psw any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_sym_decrypt_bytea = framework.Function2{
	Name:       "pgp_sym_decrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, msg any, psw any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_sym_decrypt_bytea_options = framework.Function3{
	Name:       "pgp_sym_decrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, msg any, psw any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_pub_encrypt = framework.Function2{
	Name:       "pgp_pub_encrypt",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, data any, key any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_pub_encrypt_options = framework.Function3{
	Name:       "pgp_pub_encrypt",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, key any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_pub_encrypt_bytea = framework.Function2{
	Name:       "pgp_pub_encrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, data any, key any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_pub_encrypt_bytea_options = framework.Function3{
	Name:       "pgp_pub_encrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, data any, key any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("encryption")
	},
}

var pgcrypto_pgp_pub_decrypt = framework.Function2{
	Name:       "pgp_pub_decrypt",
	Return:     pgtypes.Text,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, msg any, key any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_pub_decrypt_options = framework.Function3{
	Name:       "pgp_pub_decrypt",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, msg any, key any, psw any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_pub_decrypt_options_password = framework.Function4{
	Name:       "pgp_pub_decrypt",
	Return:     pgtypes.Text,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, msg any, key any, psw any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_pub_decrypt_bytea = framework.Function2{
	Name:       "pgp_pub_decrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, msg any, key any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_pub_decrypt_bytea_options = framework.Function3{
	Name:       "pgp_pub_decrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, msg any, key any, psw any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}

var pgcrypto_pgp_pub_decrypt_bytea_options_password = framework.Function4{
	Name:       "pgp_pub_decrypt_bytea",
	Return:     pgtypes.Bytea,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Bytea, pgtypes.Bytea, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, msg any, key any, psw any, options any) (any, error) {
		return nil, pgcryptoPGPUnsupported("decryption")
	},
}
