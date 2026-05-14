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

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

// TestCommonExtensionsProbe pins how far PG's most-emitted extension
// DDL (`CREATE EXTENSION IF NOT EXISTS uuid-ossp`, `pgcrypto`,
// `citext`, `hstore`) lands today, plus the runtime function shapes ORMs
// reach for. Per the Schema/DDL TODO in
// docs/app-compatibility-checklist.md.
func TestCommonExtensionsProbe(t *testing.T) {
	RunScripts(t, []ScriptTest{
		{
			Name:        "CREATE EXTENSION uuid-ossp keyword acceptance",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
				},
			},
		},
		{
			Name: "uuid-ossp uuid_generate_v4 runtime call",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT length(uuid_generate_v4()::text)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0001-select-length-uuid_generate_v4-::text-::text"},
				},
				{
					Query: `SELECT uuid_nil()::text, uuid_ns_dns()::text, uuid_ns_url()::text, uuid_ns_oid()::text, uuid_ns_x500()::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0002-select-uuid_nil-::text-uuid_ns_dns-::text"},
				},
				{
					Query: `SELECT uuid_generate_v3(uuid_ns_dns(), 'www.example.com')::text, uuid_generate_v5(uuid_ns_dns(), 'www.example.com')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0003-select-uuid_generate_v3-uuid_ns_dns-www.example.com-::text"},
				},
				{
					Query: `SELECT length(uuid_generate_v1()::text)::text, substr(uuid_generate_v1()::text, 15, 1), length(uuid_generate_v1mc()::text)::text, substr(uuid_generate_v1mc()::text, 15, 1);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0004-select-length-uuid_generate_v1-::text-::text"},
				},
			},
		},
		{
			Name: "uuid-ossp WITH SCHEMA exposes qualified functions",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION "uuid-ossp" WITH SCHEMA extensions;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT length(extensions.uuid_generate_v4()::text)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0005-select-length-extensions.uuid_generate_v4-::text-::text"},
				},
				{
					Query: `SELECT extensions.uuid_generate_v3(extensions.uuid_ns_dns(), 'www.example.com')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0006-select-extensions.uuid_generate_v3-extensions.uuid_ns_dns-www.example.com-::text"},
				},
			},
		},
		{
			Name: "CREATE EXTENSION plpgsql dump compatibility shim",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'plpgsql';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0007-select-e.extname-n.nspname-e.extrelocatable-e.extversion"},
				},
			},
		},
		{
			// pgcrypto's catalog install file uses `name OUT type`
			// parameters in CREATE FUNCTION declarations. This pins
			// the dump-facing extension load shape, while the
			// `gen_random_uuid` runtime assertion below covers the
			// function most ORM schemas need from pgcrypto-era dumps.
			Name:        "CREATE EXTENSION pgcrypto keyword acceptance",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					Query: `CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
				},
				{
					Query: `CREATE EXTENSION pgcrypto;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0008-create-extension-pgcrypto", Compare: "sqlstate"},

					// gen_random_uuid is a builtin in PG 13+; pgcrypto used
					// to provide it. Real-world apps depend on this being
					// callable for default UUID PKs.

				},
			},
		},
		{

			Name:        "gen_random_uuid runtime call",
			SetUpScript: []string{},
			Assertions: []ScriptTestAssertion{
				{
					// Don't assert the value (it's random), just
					// that the call shape lands and the result
					// type-castable to text has the right length.
					Query: `SELECT length(gen_random_uuid()::text)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0009-select-length-gen_random_uuid-::text-::text"},
				},
			},
		},
		{
			Name: "pgcrypto digest hmac and random-byte runtime calls",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS pgcrypto;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT digest('abc', 'sha256')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0010-select-digest-abc-sha256-::text"},
				},
				{
					Query: `SELECT digest('\x616263'::bytea, 'sha1')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0011-select-digest-\\x616263-::bytea-sha1"},
				},
				{
					Query: `SELECT hmac('what do ya want for nothing?', 'Jefe', 'md5')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0012-select-hmac-what-do-ya"},
				},
				{
					Query: `SELECT hmac('\x7768617420646f2079612077616e7420666f72206e6f7468696e673f'::bytea, '\x4a656665'::bytea, 'md5')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0013-select-hmac-::bytea-\\x4a656665-::bytea"},
				},
				{
					Query: `SELECT digest('abc', 'unknown');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0014-select-digest-abc-unknown", Compare: "sqlstate"},
				},
				{
					Query: `SELECT length(gen_random_bytes(16)::text)::text, left(gen_random_bytes(4)::text, 2);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0015-select-length-gen_random_bytes-16-::text"},
				},
				{
					Query: `SELECT gen_random_bytes(1025);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0016-select-gen_random_bytes-1025", Compare: "sqlstate"},
				},
				{
					Query: `SELECT dearmor(armor('\x68656c6c6f20706763727970746f'::bytea))::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0017-select-dearmor-armor-\\x68656c6c6f20706763727970746f-::bytea"},
				},
				{
					Query: `SELECT (armor('\x68656c6c6f'::bytea, ARRAY['Comment']::text[], ARRAY['Doltgres']::text[]) LIKE '%Comment: Doltgres%')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0018-select-armor-\\x68656c6c6f-::bytea-array["},
				},
				{
					Query: `SELECT key, value FROM pgp_armor_headers(armor('\x68656c6c6f'::bytea, ARRAY['Comment','Version']::text[], ARRAY['Doltgres','1']::text[])) ORDER BY key;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0019-select-key-value-from-pgp_armor_headers"},
				},
				{
					Query: `SELECT pgp_armor_headers(armor('\x68656c6c6f'::bytea, ARRAY['Comment']::text[], ARRAY['Doltgres']::text[]));`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0020-select-pgp_armor_headers-armor-\\x68656c6c6f-::bytea"},
				},
				{
					Query: `SELECT armor('\x00'::bytea, ARRAY['Comment']::text[], ARRAY[]::text[]);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0021-select-armor-\\x00-::bytea-array[", Compare: "sqlstate"},
				},
				{
					Query: `SELECT dearmor('-----BEGIN PGP MESSAGE-----

AAAA
=AAAA
-----END PGP MESSAGE-----
');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0022-select-dearmor-begin-pgp-message", Compare: "sqlstate"},
				},
				{
					Query: `SELECT left(gen_salt('bf'), 7), length(gen_salt('bf'))::text, left(gen_salt('bf', 4), 7), length(gen_salt('bf', 4))::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0023-select-left-gen_salt-bf-7"},
				},
				{
					Query: `WITH hashed AS (
						SELECT crypt('correct horse battery staple', gen_salt('bf', 4)) AS password_hash
					)
					SELECT length(password_hash)::text, left(password_hash, 7),
						password_hash = crypt('correct horse battery staple', password_hash),
						password_hash = crypt('wrong password', password_hash)
					FROM hashed;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0024-with-hashed-as-select-crypt"},
				},
				{
					Query: `SELECT crypt('allmine', '$2a$10$XajjQvNhvvRt5GSeFk1xFe');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0025-select-crypt-allmine-$2a$10$xajjqvnhvvrt5gsefk1xfe"},
				},
				{
					Query: `SELECT encrypt('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, 'aes')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0026-select-encrypt-\\x68656c6c6f20706763727970746f-::bytea-\\x30313233343536373839616263646566"},
				},
				{
					Query: `SELECT decrypt('\xc105fd4a7fae9b39f59ea9a363439e11'::bytea, '\x30313233343536373839616263646566'::bytea, 'aes')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0027-select-decrypt-\\xc105fd4a7fae9b39f59ea9a363439e11-::bytea-\\x30313233343536373839616263646566"},
				},
				{
					Query: `SELECT encrypt_iv('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, '\x69766976697669766976697669766976'::bytea, 'aes-cbc/pad:pkcs')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0028-select-encrypt_iv-\\x68656c6c6f20706763727970746f-::bytea-\\x30313233343536373839616263646566"},
				},
				{
					Query: `SELECT decrypt_iv('\x07ae2f58e0963a6b89784cff3f2247ed'::bytea, '\x30313233343536373839616263646566'::bytea, '\x69766976697669766976697669766976'::bytea, 'aes-cbc/pad:pkcs')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0029-select-decrypt_iv-\\x07ae2f58e0963a6b89784cff3f2247ed-::bytea-\\x30313233343536373839616263646566"},
				},
				{
					Query: `SELECT encrypt('\x31323334353637383930616263646566'::bytea, '\x30313233343536373839616263646566'::bytea, 'aes-ecb/pad:none')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0030-select-encrypt-\\x31323334353637383930616263646566-::bytea-\\x30313233343536373839616263646566"},
				},
				{
					Query: `SELECT encrypt('\x73686f7274'::bytea, '\x30313233343536373839616263646566'::bytea, 'aes-cbc/pad:none');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0031-select-encrypt-\\x73686f7274-::bytea-\\x30313233343536373839616263646566", Compare: "sqlstate"},
				},
				{
					Query: `SELECT encrypt('\x00'::bytea, '\x00'::bytea, 'aes');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0032-select-encrypt-\\x00-::bytea-\\x00", ColumnModes: []string{"bytea"}},
				},
				{
					Query:    `SELECT encrypt('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, 'bf')::text;`,
					Expected: []sql.Row{{`\xa50945ee7031548efa0c256a14547425`}},
				},
				{
					Query:    `SELECT decrypt('\xa50945ee7031548efa0c256a14547425'::bytea, '\x30313233343536373839616263646566'::bytea, 'bf')::text;`,
					Expected: []sql.Row{{`\x68656c6c6f20706763727970746f`}},
				},
				{
					Query:    `SELECT encrypt_iv('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, '\x6976697669766976'::bytea, 'bf-cbc/pad:pkcs')::text;`,
					Expected: []sql.Row{{`\x1a69f0985e7a9c770ead78de6057f4b0`}},
				},
				{
					Query:    `SELECT decrypt_iv('\x1a69f0985e7a9c770ead78de6057f4b0'::bytea, '\x30313233343536373839616263646566'::bytea, '\x6976697669766976'::bytea, 'bf-cbc/pad:pkcs')::text;`,
					Expected: []sql.Row{{`\x68656c6c6f20706763727970746f`}},
				},
				{
					Query:       `SELECT encrypt('\x00'::bytea, '\x'::bytea, 'bf');`,
					ExpectedErr: `invalid pgcrypto bf key length: 0`,
				},
				{
					Query:    `SELECT encrypt('\x68656c6c6f20706763727970746f'::bytea, '\x3031323334353637'::bytea, 'des')::text;`,
					Expected: []sql.Row{{`\x479c97a9a5e66d627eb30c9715f6aee7`}},
				},
				{
					Query:    `SELECT decrypt('\x479c97a9a5e66d627eb30c9715f6aee7'::bytea, '\x3031323334353637'::bytea, 'des')::text;`,
					Expected: []sql.Row{{`\x68656c6c6f20706763727970746f`}},
				},
				{
					Query:    `SELECT encrypt('\x68656c6c6f20706763727970746f'::bytea, '\x303132333435363738396162636465663031323334353637'::bytea, '3des')::text;`,
					Expected: []sql.Row{{`\xc4aa8daa2d432d02d5bf4fe0dac71441`}},
				},
				{
					Query:    `SELECT encrypt_iv('\x68656c6c6f20706763727970746f'::bytea, '\x303132333435363738396162636465663031323334353637'::bytea, '\x6976697669766976'::bytea, '3des-cbc/pad:pkcs')::text;`,
					Expected: []sql.Row{{`\x66c67f410cc4d89d332c04d3ae345ed2`}},
				},
				{
					Query:    `SELECT decrypt_iv('\x66c67f410cc4d89d332c04d3ae345ed2'::bytea, '\x303132333435363738396162636465663031323334353637'::bytea, '\x6976697669766976'::bytea, '3des-cbc/pad:pkcs')::text;`,
					Expected: []sql.Row{{`\x68656c6c6f20706763727970746f`}},
				},
				{
					Query:       `SELECT encrypt('\x00'::bytea, '\x00'::bytea, '3des');`,
					ExpectedErr: `invalid pgcrypto 3des key length: 1`,
				},
				{
					Query:       `SELECT gen_salt('bf', 3);`,
					ExpectedErr: `gen_salt iteration count 3 is outside allowed inclusive range 4..31 for bf`,
				},
				{
					Query:    `SELECT left(gen_salt('md5'), 3), length(gen_salt('md5'))::text;`,
					Expected: []sql.Row{{"$1$", "11"}},
				},
				{
					Query:    `SELECT left(gen_salt('md5', 1000), 3), length(gen_salt('md5', 1000))::text;`,
					Expected: []sql.Row{{"$1$", "11"}},
				},
				{
					Query:       `SELECT gen_salt('md5', 999);`,
					ExpectedErr: `gen_salt iteration count 999 is unsupported for md5`,
				},
				{
					Query: `WITH hashed AS (
						SELECT crypt('correct horse battery staple', gen_salt('md5')) AS password_hash
					)
					SELECT length(password_hash)::text, left(password_hash, 3),
						password_hash = crypt('correct horse battery staple', password_hash),
						password_hash = crypt('wrong password', password_hash)
					FROM hashed;`,
					Expected: []sql.Row{{"34", "$1$", "t", "f"}},
				},
				{
					Query:    `SELECT crypt('password', '$1$saltstring');`,
					Expected: []sql.Row{{"$1$saltstri$qQY4WxjABChYG1ccLpfkz/"}},
				},
				{
					Query:    `SELECT length(gen_salt('des'))::text, length(gen_salt('des', 25))::text;`,
					Expected: []sql.Row{{"2", "2"}},
				},
				{
					Query:    `SELECT crypt('password', 'eN'), crypt('password123', 'eN'), crypt('password', 'eNBO0nZMf3rWM');`,
					Expected: []sql.Row{{"eNBO0nZMf3rWM", "eNBO0nZMf3rWM", "eNBO0nZMf3rWM"}},
				},
				{
					Query:    `SELECT left(gen_salt('xdes'), 1), length(gen_salt('xdes'))::text, left(gen_salt('xdes', 725), 5), length(gen_salt('xdes', 725))::text;`,
					Expected: []sql.Row{{"_", "9", "_J9..", "9"}},
				},
				{
					Query:    `SELECT crypt('password', '_6C/.yaiu'), crypt('password', '_6C/.yaiu.qYIjNR7X.s'), crypt('test', '_6C/.yaiu.qYIjNR7X.s') = '_6C/.yaiu.qYIjNR7X.s';`,
					Expected: []sql.Row{{"_6C/.yaiu.qYIjNR7X.s", "_6C/.yaiu.qYIjNR7X.s", "f"}},
				},
				{
					Query:       `SELECT gen_salt('des', 26);`,
					ExpectedErr: `gen_salt iteration count 26 is unsupported for des`,
				},
				{
					Query:       `SELECT gen_salt('xdes', 724);`,
					ExpectedErr: `gen_salt iteration count 724 is outside supported odd range 1..16777215 for xdes`,
				},
				{
					Query:       `SELECT pgp_sym_encrypt('secret', 'passphrase');`,
					ExpectedErr: `pgcrypto PGP encryption is not yet supported`,
				},
				{
					Query:       `SELECT pgp_sym_decrypt('\x00'::bytea, 'passphrase');`,
					ExpectedErr: `pgcrypto PGP decryption is not yet supported`,
				},
				{
					Query:       `SELECT pgp_key_id('\x00'::bytea);`,
					ExpectedErr: `pgcrypto PGP key inspection is not yet supported`,
				},
			},
		},
		{
			Name: "loaded extension appears in pg_extension",
			SetUpScript: []string{
				`CREATE EXTENSION "uuid-ossp";`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion, e.extconfig, e.extcondition
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'uuid-ossp';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0059-select-e.extname-n.nspname-e.extrelocatable-e.extversion", ColumnModes: []string{"structural", "schema"}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION WITH SCHEMA records target namespace",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion, e.extconfig, e.extcondition
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'pgcrypto';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0060-select-e.extname-n.nspname-e.extrelocatable-e.extversion"},
				},
			},
		},
		{
			Name: "pgcrypto functions support extension schema qualification and routine ACLs",
			SetUpScript: []string{
				`CREATE USER ext_user PASSWORD 'a';`,
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA extensions;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extensions.digest('abc', 'sha256')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0061-select-extensions.digest-abc-sha256-::text"},
				},
				{
					Query: `SELECT extensions.crypt('allmine', '$2a$10$XajjQvNhvvRt5GSeFk1xFe');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0062-select-extensions.crypt-allmine-$2a$10$xajjqvnhvvrt5gsefk1xfe"},
				},
				{
					Query: `SELECT extensions.encrypt('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, 'aes')::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0063-select-extensions.encrypt-\\x68656c6c6f20706763727970746f-::bytea-\\x30313233343536373839616263646566"},
				},
				{
					Query:    `SELECT extensions.digest('abc', 'sha256')::text;`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0064-select-extensions.digest-abc-sha256-::text", Compare: "sqlstate"},
				},
				{
					Query: `GRANT USAGE ON SCHEMA extensions TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.digest(text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.crypt(text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.encrypt(bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.decrypt(bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.armor(bytea) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.dearmor(text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_armor_headers(text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.encrypt_iv(bytea, bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.decrypt_iv(bytea, bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.gen_random_bytes(integer) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.gen_random_uuid() TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_key_id(bytea) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt(text, text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_encrypt_bytea(bytea, text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt(bytea, text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_sym_decrypt_bytea(bytea, text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt(text, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_encrypt_bytea(bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt(bytea, bytea, text, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text) TO ext_user;`,
				},
				{
					Query: `GRANT ALL ON FUNCTION extensions.pgp_pub_decrypt_bytea(bytea, bytea, text, text) TO ext_user;`,
				},
				{
					Query:    `SELECT extensions.digest('abc', 'sha256')::text;`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0065-select-extensions.digest-abc-sha256-::text"},
				},
				{
					Query:    `SELECT length(extensions.gen_random_bytes(4)::text)::text, left(extensions.gen_random_uuid()::text, 1) IS NOT NULL;`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0066-select-length-extensions.gen_random_bytes-4-::text"},
				},
				{
					Query:    `SELECT extensions.crypt('allmine', '$2a$10$XajjQvNhvvRt5GSeFk1xFe');`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0067-select-extensions.crypt-allmine-$2a$10$xajjqvnhvvrt5gsefk1xfe"},
				},
				{
					Query:    `SELECT extensions.decrypt(extensions.encrypt('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, 'aes'), '\x30313233343536373839616263646566'::bytea, 'aes')::text;`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0068-select-extensions.decrypt-extensions.encrypt-\\x68656c6c6f20706763727970746f-::bytea"},
				},
				{
					Query:    `SELECT extensions.dearmor(extensions.armor('\x68656c6c6f'::bytea))::text;`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0069-select-extensions.dearmor-extensions.armor-\\x68656c6c6f-::bytea"},
				},
				{
					Query:    `SELECT extensions.pgp_armor_headers(extensions.armor('\x68656c6c6f'::bytea, ARRAY['Comment']::text[], ARRAY['Doltgres']::text[]));`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0070-select-extensions.pgp_armor_headers-extensions.armor-\\x68656c6c6f-::bytea"},
				},
				{
					Query:    `SELECT extensions.decrypt_iv(extensions.encrypt_iv('\x68656c6c6f20706763727970746f'::bytea, '\x30313233343536373839616263646566'::bytea, '\x69766976697669766976697669766976'::bytea, 'aes-cbc/pad:pkcs'), '\x30313233343536373839616263646566'::bytea, '\x69766976697669766976697669766976'::bytea, 'aes-cbc/pad:pkcs')::text;`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0071-select-extensions.decrypt_iv-extensions.encrypt_iv-\\x68656c6c6f20706763727970746f-::bytea"},
				},
				{
					Query:    `SELECT extensions.pgp_key_id('\x00'::bytea);`,
					Username: `ext_user`,
					Password: `a`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0072-select-extensions.pgp_key_id-\\x00-::bytea", Compare: "sqlstate"},
				},
			},
		},
		{
			Name: "CREATE EXTENSION vector enables built-in pgvector type and dense distance operators",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS vector;`,
				`CREATE TABLE embeddings (id integer primary key, embedding vector(3));`,
				`INSERT INTO embeddings VALUES (1, '[1,2,3]');`,
				`INSERT INTO embeddings VALUES (2, '[4,6,3]');`,
				`INSERT INTO embeddings VALUES (3, '[1,0,0]');`,
				`CREATE TABLE vector_casts (id integer primary key, embedding vector(3));`,
				`INSERT INTO vector_casts VALUES (1, ARRAY[1,2,3]::integer[]);`,
				`CREATE TABLE vector_agg_mixed (id integer primary key, embedding vector);`,
				`INSERT INTO vector_agg_mixed VALUES (1, '[1,2]'), (2, '[1,2,3]');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'vector';`,
					Expected: []sql.Row{{"vector", "public"}},
				},
				{
					Query: `SELECT amname, amtype::text
FROM pg_catalog.pg_am
WHERE amname IN ('hnsw', 'ivfflat')
ORDER BY amname;`,
					Expected: []sql.Row{
						{"hnsw", "i"},
						{"ivfflat", "i"},
					},
				},
				{
					Query: `SELECT am.amname, opc.opcname, typ.typname, opc.opcdefault
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
WHERE (am.amname = 'btree' AND opc.opcname = 'vector_ops')
	OR (am.amname IN ('hnsw', 'ivfflat')
		AND opc.opcname IN ('vector_l2_ops', 'vector_ip_ops', 'vector_cosine_ops', 'vector_l1_ops', 'bit_hamming_ops', 'bit_jaccard_ops'))
ORDER BY am.amname, opc.opcname;`,
					Expected: []sql.Row{
						{"btree", "vector_ops", "vector", "t"},
						{"hnsw", "bit_hamming_ops", "bit", "f"},
						{"hnsw", "bit_jaccard_ops", "bit", "f"},
						{"hnsw", "vector_cosine_ops", "vector", "f"},
						{"hnsw", "vector_ip_ops", "vector", "f"},
						{"hnsw", "vector_l1_ops", "vector", "f"},
						{"hnsw", "vector_l2_ops", "vector", "f"},
						{"ivfflat", "bit_hamming_ops", "bit", "f"},
						{"ivfflat", "vector_cosine_ops", "vector", "f"},
						{"ivfflat", "vector_ip_ops", "vector", "f"},
						{"ivfflat", "vector_l2_ops", "vector", "t"},
					},
				},
				{
					Query: `SELECT oprname
FROM pg_catalog.pg_operator
WHERE oprname IN ('<->', '<~>', '<%>')
ORDER BY oprname;`,
					Expected: []sql.Row{
						{"<%>"},
						{"<->"},
						{"<~>"},
					},
				},
				{
					Query: `SELECT am.amname, opf.opfname, amop.amopstrategy, amop.amoppurpose, sort_opf.opfname
FROM pg_catalog.pg_amop amop
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amop.amopfamily
JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
JOIN pg_catalog.pg_opfamily sort_opf ON sort_opf.oid = amop.amopsortfamily
WHERE am.amname = 'hnsw'
	AND opf.opfname = 'vector_l2_ops'
	AND amop.amopstrategy = 1;`,
					Expected: []sql.Row{{"hnsw", "vector_l2_ops", int16(1), "o", "float_ops"}},
				},
				{
					Query: `SELECT am.amname, opf.opfname, array_to_string(array_agg(amproc.amprocnum::text || ':' || amproc.amproc ORDER BY amproc.amprocnum), ',')
FROM pg_catalog.pg_amproc amproc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amproc.amprocfamily
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE (am.amname = 'ivfflat' AND opf.opfname = 'vector_cosine_ops')
	OR (am.amname = 'hnsw' AND opf.opfname = 'bit_jaccard_ops')
GROUP BY am.amname, opf.opfname
ORDER BY am.amname, opf.opfname;`,
					Expected: []sql.Row{
						{"hnsw", "bit_jaccard_ops", "1:jaccard_distance,3:hnsw_bit_support"},
						{"ivfflat", "vector_cosine_ops", "1:vector_negative_inner_product,2:vector_norm,3:vector_spherical_distance,4:vector_norm"},
					},
				},
				{
					Query:       `CREATE INDEX embeddings_hnsw_idx ON embeddings USING hnsw (embedding vector_l2_ops);`,
					ExpectedErr: `index method hnsw is not yet supported`,
				},
				{
					Query:       `CREATE INDEX embeddings_ivfflat_idx ON embeddings USING ivfflat (embedding vector_l2_ops);`,
					ExpectedErr: `index method ivfflat is not yet supported`,
				},
				{
					Query:    `CREATE TABLE halfvec_schema_probe (id integer primary key, embedding halfvec(3));`,
					Expected: []sql.Row{},
				},
				{
					Query:    `CREATE TABLE sparsevec_schema_probe (id integer primary key, embedding sparsevec(3));`,
					Expected: []sql.Row{},
				},
				{
					Query:    `INSERT INTO halfvec_schema_probe VALUES (1, NULL);`,
					Expected: []sql.Row{},
				},
				{
					Query:    `INSERT INTO sparsevec_schema_probe VALUES (1, NULL);`,
					Expected: []sql.Row{},
				},
				{
					Query: `SELECT typname
FROM pg_catalog.pg_type
WHERE typname IN ('halfvec', 'sparsevec')
ORDER BY typname;`,
					Expected: []sql.Row{
						{"halfvec"},
						{"sparsevec"},
					},
				},
				{
					Query:       `INSERT INTO halfvec_schema_probe VALUES (2, '[1,2,3]');`,
					ExpectedErr: `pgvector halfvec values are not yet supported`,
				},
				{
					Query:       `INSERT INTO sparsevec_schema_probe VALUES (2, '{1:1}/3');`,
					ExpectedErr: `pgvector sparsevec values are not yet supported`,
				},
				{
					Query:    `SELECT embedding FROM embeddings WHERE id = 1;`,
					Expected: []sql.Row{{"[1,2,3]"}},
				},
				{
					Query:    `SELECT l2_distance('[1,2,3]'::vector, '[4,6,3]'::vector)::text, inner_product('[1,2,3]'::vector, '[4,6,3]'::vector)::text, vector_negative_inner_product('[1,2,3]'::vector, '[4,6,3]'::vector)::text, l1_distance('[1,2,3]'::vector, '[4,6,3]'::vector)::text;`,
					Expected: []sql.Row{{"5", "25", "-25", "7"}},
				},
				{
					Query:    `SELECT (embedding <-> '[4,6,3]'::vector)::text, (embedding <#> '[4,6,3]'::vector)::text, (embedding <+> '[4,6,3]'::vector)::text FROM embeddings WHERE id = 1;`,
					Expected: []sql.Row{{"5", "-25", "7"}},
				},
				{
					Query:    `SELECT ('[1,0,0]'::vector <=> '[0,1,0]'::vector)::text, cosine_distance('[1,0,0]'::vector, '[0,1,0]'::vector)::text;`,
					Expected: []sql.Row{{"1", "1"}},
				},
				{
					Query:    `SELECT vector_spherical_distance('[1,0]'::vector, '[1,0]'::vector)::text, vector_spherical_distance('[1,0]'::vector, '[0,1]'::vector)::text, vector_spherical_distance('[1,0]'::vector, '[-1,0]'::vector)::text;`,
					Expected: []sql.Row{{"0", "0.5", "1"}},
				},
				{
					Query:    `SELECT ('[1,2,3]'::vector < '[1,2,4]'::vector)::text, ('[1,2,3]'::vector <= '[1,2,3]'::vector)::text, ('[1,2,4]'::vector > '[1,2,3]'::vector)::text, ('[1,2,4]'::vector >= '[1,2,4]'::vector)::text;`,
					Expected: []sql.Row{{"true", "true", "true", "true"}},
				},
				{
					Query:    `SELECT vector_lt('[1,2,3]'::vector, '[1,2,4]'::vector)::text, vector_le('[1,2,3]'::vector, '[1,2,3]'::vector)::text, vector_gt('[1,2,4]'::vector, '[1,2,3]'::vector)::text, vector_ge('[1,2,4]'::vector, '[1,2,4]'::vector)::text;`,
					Expected: []sql.Row{{"true", "true", "true", "true"}},
				},
				{
					Query:    `SELECT vector_dims('[3,4,0]'::vector), vector_norm('[3,4,0]'::vector)::text, l2_normalize('[3,4,0]'::vector)::text, subvector('[1,2,3,4]'::vector, 2, 2)::text;`,
					Expected: []sql.Row{{3, "5", "[0.6,0.8,0]", "[2,3]"}},
				},
				{
					Query:    `SELECT binary_quantize('[-1,0,0.5,2]'::vector)::text, binary_quantize('[-2,0,-0.1]'::vector)::text;`,
					Expected: []sql.Row{{"0011", "000"}},
				},
				{
					Query:    `SELECT hamming_distance(B'1010', B'1110')::text, (B'1010' <~> B'1110')::text, jaccard_distance(B'1010', B'1100')::text, (B'1010' <%> B'1100')::text, jaccard_distance(B'000', B'000')::text;`,
					Expected: []sql.Row{{"1", "1", "0.6666666666666667", "0.6666666666666667", "1"}},
				},
				{
					Query:    `SELECT (B'1010' OPERATOR(public.<~>) B'1110')::text;`,
					Expected: []sql.Row{{"1"}},
				},
				{
					Query:    `SELECT embedding FROM vector_casts WHERE id = 1;`,
					Expected: []sql.Row{{"[1,2,3]"}},
				},
				{
					Query:    `SELECT array_to_vector(ARRAY[1,2,3]::integer[], 3, true)::text, array_to_vector(ARRAY[1.5,2.25]::real[], -1, false)::text, array_to_vector(ARRAY[1.25,2.5]::double precision[], -1, false)::text, array_to_vector(ARRAY[1.75,2.125]::numeric[], -1, false)::text;`,
					Expected: []sql.Row{{"[1,2,3]", "[1.5,2.25]", "[1.25,2.5]", "[1.75,2.125]"}},
				},
				{
					Query:    `SELECT array_to_string(vector_to_float4('[1.5,2.25]'::vector, -1, false), ','), array_to_string(('[3,4,5]'::vector)::real[], ','), (ARRAY[6,7,8]::integer[]::vector(3))::text;`,
					Expected: []sql.Row{{"1.5,2.25", "3,4,5", "[6,7,8]"}},
				},
				{
					Query:    `SELECT ('[1,2,3]'::vector + '[4,5,6]'::vector)::text, ('[1,2,3]'::vector - '[4,5,6]'::vector)::text, ('[1,2,3]'::vector * '[4,5,6]'::vector)::text, ('[1,2,3]'::vector || '[4,5,6]'::vector)::text;`,
					Expected: []sql.Row{{"[5,7,9]", "[-3,-3,-3]", "[4,10,18]", "[1,2,3,4,5,6]"}},
				},
				{
					Query:    `SELECT vector_add('[1,2,3]'::vector, '[4,5,6]'::vector)::text, vector_sub('[1,2,3]'::vector, '[4,5,6]'::vector)::text, vector_mul('[1,2,3]'::vector, '[4,5,6]'::vector)::text, vector_concat('[1,2,3]'::vector, '[4,5,6]'::vector)::text;`,
					Expected: []sql.Row{{"[5,7,9]", "[-3,-3,-3]", "[4,10,18]", "[1,2,3,4,5,6]"}},
				},
				{
					Query:    `SELECT array_to_string(vector_accum(ARRAY[0]::double precision[], '[1,2,3]'::vector), ','), vector_avg(ARRAY[2,4,6,8]::double precision[])::text, array_to_string(vector_combine(ARRAY[2,4,6]::double precision[], ARRAY[3,9,12]::double precision[]), ',');`,
					Expected: []sql.Row{{"1,1,2,3", "[2,3,4]", "5,13,18"}},
				},
				{
					Query:    `SELECT sum(embedding)::text, avg(embedding)::text FROM embeddings WHERE id IN (1, 2);`,
					Expected: []sql.Row{{"[5,8,6]", "[2.5,4,3]"}},
				},
				{
					Query:    `SELECT sum(embedding) IS NULL, avg(embedding) IS NULL FROM embeddings WHERE false;`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query:    `SELECT sum(NULL::vector) IS NULL, avg(NULL::vector) IS NULL;`,
					Expected: []sql.Row{{"t", "t"}},
				},
				{
					Query:    `SELECT vector_avg(ARRAY[0]::double precision[]) IS NULL, array_to_string(vector_combine(ARRAY[0]::double precision[], ARRAY[2,4,6]::double precision[]), ',');`,
					Expected: []sql.Row{{"t", "2,4,6"}},
				},
				{
					Query:       `SELECT subvector('[1,2,3]'::vector, 2, 0);`,
					ExpectedErr: `vector must have at least 1 dimension`,
				},
				{
					Query:       `SELECT vector_accum(ARRAY[1,2,3]::double precision[], '[4,5,6]'::vector);`,
					ExpectedErr: `expected 2 dimensions, not 3`,
				},
				{
					Query:       `SELECT sum(embedding)::text FROM vector_agg_mixed;`,
					ExpectedErr: `different vector dimensions 2 and 3`,
				},
				{
					Query:       `SELECT array_to_vector(ARRAY[1,NULL,3]::integer[], -1, false);`,
					ExpectedErr: `array must not contain nulls`,
				},
				{
					Query:       `SELECT ARRAY[1,2]::integer[]::vector(3);`,
					ExpectedErr: `expected 3 dimensions, not 2`,
				},
				{
					Query:    `SELECT id FROM embeddings ORDER BY embedding <-> '[4,6,3]'::vector, id;`,
					Expected: []sql.Row{{2}, {1}, {3}},
				},
				{
					Query:    `SELECT (embedding OPERATOR(public.<->) '[4,6,3]'::vector)::text FROM embeddings WHERE id = 1;`,
					Expected: []sql.Row{{"5"}},
				},
				{
					Query:       `SELECT l2_distance('[1,2]'::vector, '[1,2,3]'::vector);`,
					ExpectedErr: `different vector dimensions 2 and 3`,
				},
				{
					Query:       `SELECT hamming_distance(B'101', B'1010');`,
					ExpectedErr: `different bit lengths 3 and 4`,
				},
			},
		},
		{
			Name: "CREATE EXTENSION btree_gist dump compatibility shim",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;`,
				`CREATE TABLE btree_gist_probe (id integer primary key, v integer);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname, e.extrelocatable, e.extversion
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'btree_gist';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0118-select-e.extname-n.nspname-e.extrelocatable-e.extversion", Cleanup: []string{"DROP TABLE IF EXISTS btree_gist_probe CASCADE", "DROP EXTENSION IF EXISTS btree_gist CASCADE"}},
				},
				{
					Query: `SELECT opc.opcname, opf.opfname, typ.typname, n.nspname, opc.opcdefault
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = opc.opcfamily
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
JOIN pg_catalog.pg_namespace n ON n.oid = opc.opcnamespace
WHERE am.amname = 'gist'
	AND opc.opcname IN ('gist_bool_ops', 'gist_int4_ops', 'gist_text_ops', 'gist_uuid_ops')
ORDER BY opc.opcname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0119-select-opc.opcname-opf.opfname-typ.typname-n.nspname", Cleanup: []string{"DROP TABLE IF EXISTS btree_gist_probe CASCADE", "DROP EXTENSION IF EXISTS btree_gist CASCADE"}},
				},
				{
					Query: `SELECT opf.opfname, n.nspname, COUNT(*)
FROM pg_catalog.pg_amop amop
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amop.amopfamily
JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
JOIN pg_catalog.pg_namespace n ON n.oid = opf.opfnamespace
WHERE am.amname = 'gist'
	AND opf.opfname IN ('gist_bool_ops', 'gist_int4_ops', 'gist_text_ops', 'gist_uuid_ops')
GROUP BY opf.opfname, n.nspname
ORDER BY opf.opfname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0120-select-opf.opfname-n.nspname-count-*", Cleanup: []string{"DROP TABLE IF EXISTS btree_gist_probe CASCADE", "DROP EXTENSION IF EXISTS btree_gist CASCADE"}},
				},
				{
					Query: `SELECT opf.opfname, amop.amopstrategy, amop.amoppurpose, btree_opf.opfname
FROM pg_catalog.pg_amop amop
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amop.amopfamily
JOIN pg_catalog.pg_opfamily btree_opf ON btree_opf.oid = amop.amopsortfamily
JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
WHERE am.amname = 'gist'
	AND opf.opfname = 'gist_int4_ops'
	AND amop.amopstrategy = 15;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0121-select-opf.opfname-amop.amopstrategy-amop.amoppurpose-btree_opf.opfname", Cleanup: []string{"DROP TABLE IF EXISTS btree_gist_probe CASCADE", "DROP EXTENSION IF EXISTS btree_gist CASCADE"}},
				},
				{
					Query: `SELECT opf.opfname, amproc.amprocnum, amproc.amproc
FROM pg_catalog.pg_amproc amproc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amproc.amprocfamily
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE am.amname = 'gist'
	AND opf.opfname = 'gist_int4_ops'
	AND amproc.amprocnum IN (1, 8, 9)
ORDER BY amproc.amprocnum;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0122-select-opf.opfname-amproc.amprocnum-amproc.amproc-from", Cleanup: []string{"DROP TABLE IF EXISTS btree_gist_probe CASCADE", "DROP EXTENSION IF EXISTS btree_gist CASCADE"}},
				},
				{
					Query: `CREATE INDEX btree_gist_probe_v_idx ON btree_gist_probe USING gist (v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0123-create-index-btree_gist_probe_v_idx-on-btree_gist_probe", Cleanup: []string{"DROP TABLE IF EXISTS btree_gist_probe CASCADE"}},
				},
			},
		},
		{
			Name: "btree_gist WITH SCHEMA exposes target-schema catalog rows",
			SetUpScript: []string{
				`CREATE SCHEMA extensions;`,
				`CREATE EXTENSION btree_gist WITH SCHEMA extensions;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'btree_gist';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0124-select-e.extname-n.nspname-from-pg_catalog.pg_extension"},
				},
				{
					Query: `SELECT opc.opcname, n.nspname
						FROM pg_catalog.pg_opclass opc
						JOIN pg_catalog.pg_namespace n ON n.oid = opc.opcnamespace
						WHERE opc.opcname = 'gist_text_ops';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0125-select-opc.opcname-n.nspname-from-pg_catalog.pg_opclass"},
				},
			},
		},
		{
			Name: "CREATE EXTENSION citext installs case-insensitive text type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;`,
				`CREATE TABLE app_users (id integer primary key, email public.citext UNIQUE);`,
				`INSERT INTO app_users VALUES (1, 'Alice@Example.com');`,
				`INSERT INTO app_users VALUES (2, 'bob@example.com');`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'citext';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0126-select-e.extname-n.nspname-from-pg_catalog.pg_extension", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `SELECT email::text FROM app_users WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0127-select-email::text-from-app_users-where", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `SELECT opc.opcname, am.amname, typ.typname, opc.opcdefault::text
						FROM pg_catalog.pg_opclass opc
						JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
						JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
						WHERE opc.opcname = 'citext_ops';`,
					Expected: []sql.Row{{"citext_ops", "btree", "citext", "true"}},
				},
				{
					Query: `SELECT opf.opfname, amop.amopstrategy, opr.oprname
						FROM pg_catalog.pg_amop amop
						JOIN pg_catalog.pg_opfamily opf ON opf.oid = amop.amopfamily
						JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
						JOIN pg_catalog.pg_operator opr ON opr.oid = amop.amopopr
						WHERE am.amname = 'btree'
							AND opf.opfname = 'citext_ops'
						ORDER BY amop.amopstrategy;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0129-select-opf.opfname-amop.amopstrategy-opr.oprname-from", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `SELECT opf.opfname, amproc.amprocnum, amproc.amproc::regproc::text
						FROM pg_catalog.pg_amproc amproc
						JOIN pg_catalog.pg_opfamily opf ON opf.oid = amproc.amprocfamily
						JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
						WHERE am.amname = 'btree'
							AND opf.opfname = 'citext_ops';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0130-select-opf.opfname-amproc.amprocnum-amproc.amproc::regproc::text-from", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `SELECT ('Alice@Example.com'::public.citext = 'alice@example.com'::public.citext)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0131-select-alice@example.com-::public.citext-=-alice@example.com", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `SELECT ('Alice@Example.com'::public.citext <> 'alice@example.com'::public.citext)::text, ('bob@example.com'::public.citext > 'ALICE@example.com'::public.citext)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0132-select-alice@example.com-::public.citext-<>-alice@example.com", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `EXPLAIN SELECT id FROM app_users WHERE email = 'alice@example.com'::public.citext;`,
					Expected: []sql.Row{
						{"Project"},
						{" ├─ columns: [app_users.id]"},
						{" └─ Filter"},
						{"     ├─ app_users.email = 'alice@example.com'"},
						{"     └─ IndexedTableAccess(app_users)"},
						{"         ├─ index: [app_users.__doltgres_citext_94cb67fc_0]"},
						{"         └─ filters: [{[alice@example.com, alice@example.com]}]"},
					},
				},
				{
					Query: `SELECT id FROM app_users WHERE email = 'alice@example.com'::public.citext;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0134-select-id-from-app_users-where", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `SELECT id FROM app_users WHERE email > 'alice@example.com'::public.citext ORDER BY id;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0135-select-id-from-app_users-where", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `UPDATE app_users SET email = 'BOB@example.com' WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0136-update-app_users-set-email-=", Compare: "sqlstate", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
				{
					Query: `INSERT INTO app_users VALUES (3, 'alice@example.com');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0137-insert-into-app_users-values-3", Compare: "sqlstate", Cleanup: []string{"DROP EXTENSION IF EXISTS citext CASCADE", "DROP TABLE IF EXISTS app_users CASCADE"}},
				},
			},
		},
		{
			Name: "CREATE EXTENSION hstore installs text-compatible type",
			SetUpScript: []string{
				`CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public;`,
				`CREATE TABLE vending_machines (id integer primary key, inventory public.hstore);`,
				`INSERT INTO vending_machines VALUES (1, '"A"=>"2", "B"=>"5"');`,
				`INSERT INTO vending_machines VALUES (2, '"empty"=>NULL, "quoted key"=>"a,b=>c", "quote\"slash\\"=>"v\"\\x"');`,
				`INSERT INTO vending_machines VALUES (3, '"A"=>"2", "B"=>"5", "empty"=>NULL');`,
				`CREATE TYPE hstore_person AS (name text, age integer, active boolean, note text);`,
				`CREATE TYPE hstore_pop_base AS (a int, b text, c bool);`,
				`CREATE TYPE hstore_pop_row AS (a int, b text[], c hstore_pop_base, j jsonb);`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT e.extname, n.nspname
						FROM pg_catalog.pg_extension e
						JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
						WHERE e.extname = 'hstore';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0138-select-e.extname-n.nspname-from-pg_catalog.pg_extension", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory::text FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0139-select-inventory::text-from-vending_machines-where", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory -> 'A', fetchval(inventory, 'B') FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0140-select-inventory->-a-fetchval", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (inventory -> 'missing') IS NULL FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0141-select-inventory->-missing-is", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory -> 'empty', inventory -> 'quoted key', inventory -> E'quote"slash\\' FROM vending_machines WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0142-select-inventory->-empty-inventory", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory ? 'A', inventory ? 'missing' FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0143-select-inventory-?-a-inventory", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT exist(inventory, 'empty'), defined(inventory, 'empty'), isexists(inventory, 'quoted key'), isdefined(inventory, 'quoted key') FROM vending_machines WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0144-select-exist-inventory-empty-defined", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory ?| ARRAY['missing', 'B'], inventory ?| ARRAY['missing', 'other'], inventory ?& ARRAY['A', 'B'], inventory ?& ARRAY['A', 'missing'] FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0145-select-inventory-?|-array[-missing", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT exists_any(inventory, ARRAY['missing', 'quoted key']), exists_all(inventory, ARRAY['empty', 'quoted key']) FROM vending_machines WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0146-select-exists_any-inventory-array[-missing", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory ?| ARRAY[NULL]::text[], inventory ?& ARRAY[NULL]::text[], inventory ?| ARRAY[]::text[], inventory ?& ARRAY[]::text[] FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0147-select-inventory-?|-array[null]::text[]-inventory", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory @> '"A"=>"2"'::public.hstore, inventory @> '"A"=>"9"'::public.hstore, inventory @> '"missing"=>"1"'::public.hstore, inventory <@ '"A"=>"2", "B"=>"5", "C"=>"6"'::public.hstore FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0148-select-inventory-@>-a-=>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT inventory @> '"empty"=>NULL'::public.hstore, inventory @> '"empty"=>"x"'::public.hstore, inventory <@ '"empty"=>NULL, "quoted key"=>"a,b=>c", "quote\"slash\\"=>"v\"\\x", "extra"=>"1"'::public.hstore FROM vending_machines WHERE id = 2;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0149-select-inventory-@>-empty-=>null", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hs_contains(inventory, '"A"=>"2"'::public.hstore), hs_contained(inventory, '"A"=>"2", "B"=>"5"'::public.hstore) FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0150-select-hs_contains-inventory-a-=>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (inventory || '"B"=>"9", "C"=>NULL'::public.hstore)::text, hs_concat(inventory, '"A"=>NULL'::public.hstore)::text FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0151-select-inventory-||-b-=>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT delete(inventory, 'A')::text, (inventory - 'B'::text)::text FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0152-select-delete-inventory-a-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT delete('"A"=>"2", "B"=>"5", "C"=>"6"'::public.hstore, ARRAY['A', 'C'])::text, ('"A"=>"2", "B"=>"5"'::public.hstore - ARRAY[NULL]::text[])::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0153-select-delete-a-=>-2", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT ('"A"=>"2", "B"=>"5"'::public.hstore - '"A"=>"9", "B"=>"5"'::public.hstore)::text, delete('"empty"=>NULL, "quoted key"=>"a,b=>c"'::public.hstore, '"empty"=>NULL'::public.hstore)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0154-select-a-=>-2-b", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT array_to_string(inventory -> ARRAY['B', 'missing', 'empty', 'A'], '|', '<NULL>') FROM vending_machines WHERE id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0155-select-array_to_string-inventory->-array[", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT array_to_string(slice_array(inventory, ARRAY['A', 'empty', 'missing']), '|', '<NULL>') FROM vending_machines WHERE id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0156-select-array_to_string-slice_array-inventory-array[", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT slice(inventory, ARRAY['empty', 'missing', 'A'])::text FROM vending_machines WHERE id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0157-select-slice-inventory-array[-empty", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (inventory -> ARRAY[]::text[])::text, slice(inventory, ARRAY[]::text[])::text FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0158-select-inventory->-array[]::text[]-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT array_to_string(inventory -> ARRAY[NULL]::text[], '|', '<NULL>'), slice(inventory, ARRAY[NULL]::text[])::text FROM vending_machines WHERE id = 1;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0159-select-array_to_string-inventory->-array[null]::text[]", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT array_to_string(akeys(inventory), '|', '<NULL>'), array_to_string(avals(inventory), '|', '<NULL>'), array_to_string(hstore_to_array(inventory), '|', '<NULL>') FROM vending_machines WHERE id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0160-select-array_to_string-akeys-inventory-|", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_to_matrix(inventory)::text, array_to_string(hstore_to_matrix(inventory), '|', '<NULL>') FROM vending_machines WHERE id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0161-select-hstore_to_matrix-inventory-::text-array_to_string", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT array_length(hstore_to_matrix(inventory), 1), array_length(hstore_to_matrix(inventory), 2), array_upper(hstore_to_matrix(inventory), 1), array_upper(hstore_to_matrix(inventory), 2) FROM vending_machines WHERE id = 3;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0162-select-array_length-hstore_to_matrix-inventory-1", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_to_matrix('"quote"=>"a,b", "emptystr"=>""'::public.hstore)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0163-select-hstore_to_matrix-quote-=>-a", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (SELECT array_to_string(array_agg(k), '|', '<NULL>') FROM skeys('"A"=>"2", "B"=>"5", "empty"=>NULL'::public.hstore) AS t(k)), (SELECT array_to_string(array_agg(v), '|', '<NULL>') FROM svals('"A"=>"2", "B"=>"5", "empty"=>NULL'::public.hstore) AS t(v));`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0164-select-select-array_to_string-array_agg-k", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (SELECT count(*)::text FROM skeys(''::public.hstore)), (SELECT count(*)::text FROM svals(''::public.hstore)), (SELECT count(*)::text FROM skeys(NULL::public.hstore)), (SELECT count(*)::text FROM svals(NULL::public.hstore));`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0165-select-select-count-*-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT skeys('"B"=>"5", "A"=>"2"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0166-select-skeys-b-=>-5", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT svals('"A"=>"2", "empty"=>NULL, "B"=>"5"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0167-select-svals-a-=>-2", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT key, COALESCE(value, '<NULL>') FROM each('"B"=>"5", "A"=>"2", "empty"=>NULL, "quote"=>"a\"b"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0168-select-key-coalesce-value-<null>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT k, COALESCE(v, '<NULL>') FROM each('"A"=>"2", "empty"=>NULL'::public.hstore) AS t(k, v);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0169-select-k-coalesce-v-<null>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT k, COALESCE(value, '<NULL>') FROM each('"A"=>"2", "empty"=>NULL'::public.hstore) AS t(k);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0170-select-k-coalesce-value-<null>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (SELECT count(*)::text FROM each(''::public.hstore)), (SELECT count(*)::text FROM each(NULL::public.hstore));`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0171-select-select-count-*-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT each('"B"=>"5", "A"=>"2"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0172-select-each-b-=>-5", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore(ARRAY['n', 'float', 'bool', 'str', 'empty', 'bad'], ARRAY['12', '3.5', 'true', '012', NULL, '12x'])::text, array_to_string(hstore_to_array('"n"=>"12", "float"=>"3.5", "bool"=>"true", "str"=>"012", "empty"=>NULL, "bad"=>"12x"'::public.hstore), '|', '<NULL>');`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0173-select-hstore-array[-n-float", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT akeys(''::public.hstore)::text, avals(''::public.hstore)::text, hstore_to_array(''::public.hstore)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0174-select-akeys-::public.hstore-::text-avals", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_to_matrix(''::public.hstore)::text, hstore_to_matrix(NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0175-select-hstore_to_matrix-::public.hstore-::text-hstore_to_matrix", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT akeys(NULL::public.hstore) IS NULL, avals(NULL::public.hstore) IS NULL, hstore_to_array(NULL::public.hstore) IS NULL, hstore_to_matrix(NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0176-select-akeys-null::public.hstore-is-null", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore('A', '2')::text, hstore('empty', NULL)::text, hstore(NULL, 'x') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0177-select-hstore-a-2-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT tconvert('A', '2')::text, tconvert('empty', NULL)::text, tconvert(NULL, 'x') IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0178-select-tconvert-a-2-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore(ARRAY['B', 'A', 'empty'], ARRAY['5', '2', NULL])::text, hstore(ARRAY['A', 'A'], ARRAY['1', '2'])::text, hstore(ARRAY['A'], NULL::text[])::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0179-select-hstore-array[-b-a", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_version_diag(''::public.hstore)::text, hstore_version_diag('"A"=>"2"'::public.hstore)::text, hstore_version_diag(NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0180-select-hstore_version_diag-::public.hstore-::text-hstore_version_diag", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_out(''::public.hstore), hstore_out('"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore), hstore_out(NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0181-select-hstore_out-::public.hstore-hstore_out-b", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_in('"B"=>"5", "A"=>"2", "empty"=>NULL')::text, hstore_in('')::text, hstore_in(NULL) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0182-select-hstore_in-b-=>-5", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_send(''::public.hstore)::text, hstore_send('"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore)::text, hstore_send('"quote"=>"a\"b", "slash"=>"c\\d"'::public.hstore)::text, hstore_send(NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0183-select-hstore_send-::public.hstore-::text-hstore_send", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_hash(''::public.hstore)::text, hstore_hash('"A"=>"2"'::public.hstore)::text, hstore_hash('"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore)::text, hstore_hash('"quote"=>"a\"b", "slash"=>"c\\d"'::public.hstore)::text, hstore_hash(NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0184-select-hstore_hash-::public.hstore-::text-hstore_hash", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_hash('"A"=>"2", "B"=>"5"'::public.hstore) = hstore_hash('"B"=>"5", "A"=>"2"'::public.hstore), hstore_hash('"A"=>"first", "A"=>"second"'::public.hstore)::text, hstore_hash('"A"=>"first"'::public.hstore)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0185-select-hstore_hash-a-=>-2", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_hash_extended(''::public.hstore, 0)::text, hstore_hash_extended('"A"=>"2"'::public.hstore, 0)::text, hstore_hash_extended('"A"=>"2"'::public.hstore, 42)::text, hstore_hash_extended('"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore, 0)::text, hstore_hash_extended('"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore, -1)::text, hstore_hash_extended(NULL::public.hstore, 0) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0186-select-hstore_hash_extended-::public.hstore-0-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT am.amname, opc.opcname, opc.opcdefault, typ.typname, COALESCE(keytyp.typname, '')
FROM pg_catalog.pg_opclass opc
JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod
JOIN pg_catalog.pg_type typ ON typ.oid = opc.opcintype
LEFT JOIN pg_catalog.pg_type keytyp ON keytyp.oid = opc.opckeytype
WHERE opc.opcname IN ('btree_hstore_ops', 'hash_hstore_ops', 'gist_hstore_ops', 'gin_hstore_ops')
ORDER BY am.amname, opc.opcname;`,
					Expected: []sql.Row{
						{"btree", "btree_hstore_ops", "t", "hstore", ""},
						{"gin", "gin_hstore_ops", "t", "hstore", "text"},
						{"gist", "gist_hstore_ops", "t", "hstore", ""},
						{"hash", "hash_hstore_ops", "t", "hstore", ""},
					},
				},
				{
					Query: `SELECT am.amname, opf.opfname
FROM pg_catalog.pg_opfamily opf
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE opf.opfname IN ('btree_hstore_ops', 'hash_hstore_ops', 'gist_hstore_ops', 'gin_hstore_ops')
ORDER BY am.amname, opf.opfname;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0188-select-am.amname-opf.opfname-from-pg_catalog.pg_opfamily", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT am.amname, opf.opfname, amop.amopstrategy, opr.oprname, rt.typname
FROM pg_catalog.pg_amop amop
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amop.amopfamily
JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
JOIN pg_catalog.pg_operator opr ON opr.oid = amop.amopopr
JOIN pg_catalog.pg_type rt ON rt.oid = amop.amoprighttype
WHERE opf.opfname IN ('btree_hstore_ops', 'hash_hstore_ops', 'gist_hstore_ops', 'gin_hstore_ops')
ORDER BY am.amname, opf.opfname, amop.amopstrategy;`,
					Expected: []sql.Row{
						{"btree", "btree_hstore_ops", int16(1), "#<#", "hstore"},
						{"btree", "btree_hstore_ops", int16(2), "#<=#", "hstore"},
						{"btree", "btree_hstore_ops", int16(3), "=", "hstore"},
						{"btree", "btree_hstore_ops", int16(4), "#>=#", "hstore"},
						{"btree", "btree_hstore_ops", int16(5), "#>#", "hstore"},
						{"gin", "gin_hstore_ops", int16(7), "@>", "hstore"},
						{"gin", "gin_hstore_ops", int16(9), "?", "text"},
						{"gin", "gin_hstore_ops", int16(10), "?|", "_text"},
						{"gin", "gin_hstore_ops", int16(11), "?&", "_text"},
						{"gist", "gist_hstore_ops", int16(7), "@>", "hstore"},
						{"gist", "gist_hstore_ops", int16(9), "?", "text"},
						{"gist", "gist_hstore_ops", int16(10), "?|", "_text"},
						{"gist", "gist_hstore_ops", int16(11), "?&", "_text"},
						{"gist", "gist_hstore_ops", int16(13), "@", "hstore"},
						{"hash", "hash_hstore_ops", int16(1), "=", "hstore"},
					},
				},
				{
					Query: `SELECT am.amname, opf.opfname, amproc.amprocnum, amproc.amproc
FROM pg_catalog.pg_amproc amproc
JOIN pg_catalog.pg_opfamily opf ON opf.oid = amproc.amprocfamily
JOIN pg_catalog.pg_am am ON am.oid = opf.opfmethod
WHERE opf.opfname IN ('btree_hstore_ops', 'hash_hstore_ops', 'gist_hstore_ops', 'gin_hstore_ops')
ORDER BY am.amname, opf.opfname, amproc.amprocnum;`,
					Expected: []sql.Row{
						{"btree", "btree_hstore_ops", int16(1), "hstore_cmp"},
						{"gin", "gin_hstore_ops", int16(1), "bttextcmp"},
						{"gin", "gin_hstore_ops", int16(2), "gin_extract_hstore"},
						{"gin", "gin_hstore_ops", int16(3), "gin_extract_hstore_query"},
						{"gin", "gin_hstore_ops", int16(4), "gin_consistent_hstore"},
						{"gist", "gist_hstore_ops", int16(1), "ghstore_consistent"},
						{"gist", "gist_hstore_ops", int16(2), "ghstore_union"},
						{"gist", "gist_hstore_ops", int16(3), "ghstore_compress"},
						{"gist", "gist_hstore_ops", int16(4), "ghstore_decompress"},
						{"gist", "gist_hstore_ops", int16(5), "ghstore_penalty"},
						{"gist", "gist_hstore_ops", int16(6), "ghstore_picksplit"},
						{"gist", "gist_hstore_ops", int16(7), "ghstore_same"},
						{"hash", "hash_hstore_ops", int16(1), "hstore_hash"},
					},
				},
				{
					Query:       `CREATE INDEX vending_inventory_gin_idx ON vending_machines USING gin (inventory gin_hstore_ops);`,
					ExpectedErr: `operator class gin_hstore_ops does not exist for access method gin`,
				},
				{
					Query:       `CREATE INDEX vending_inventory_btree_idx ON vending_machines USING btree (inventory btree_hstore_ops);`,
					ExpectedErr: `operator class btree_hstore_ops does not exist for access method btree`,
				},
				{
					Query: `CREATE INDEX vending_inventory_gist_idx ON vending_machines USING gist (inventory gist_hstore_ops);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0193-create-index-vending_inventory_gist_idx-on-vending_machines", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query:       `CREATE INDEX vending_inventory_hash_idx ON vending_machines USING hash (inventory hash_hstore_ops);`,
					ExpectedErr: `index method hash is not yet supported`,
				},
				{
					Query:    `SELECT hstore_to_json('"B"=>"5", "A"=>"2", "empty"=>NULL, "quote"=>"a\"b"'::public.hstore)::text, hstore_to_jsonb('"B"=>"5", "A"=>"2", "empty"=>NULL, "quote"=>"a\"b"'::public.hstore)::text;`,
					Expected: []sql.Row{{`{"A":"2","B":"5","empty":null,"quote":"a\"b"}`, `{"A": "2", "B": "5", "empty": null, "quote": "a\"b"}`}},
				},
				{
					Query:    `SELECT hstore_to_json_loose('"n"=>"12", "float"=>"3.5", "bool"=>"true", "str"=>"012", "empty"=>NULL, "bad"=>"12x"'::public.hstore)::text, hstore_to_jsonb_loose('"n"=>"12", "float"=>"3.5", "bool"=>"true", "str"=>"012", "empty"=>NULL, "bad"=>"12x"'::public.hstore)::text;`,
					Expected: []sql.Row{{`{"n":12,"bad":"12x","str":"012","bool":"true","empty":null,"float":3.5}`, `{"n": 12, "bad": "12x", "str": "012", "bool": "true", "empty": null, "float": 3.5}`}},
				},
				{
					Query:    `SELECT ('"A"=>"2", "empty"=>NULL'::public.hstore)::json::text, ('"A"=>"2", "empty"=>NULL'::public.hstore)::jsonb::text;`,
					Expected: []sql.Row{{`{"A":"2","empty":null}`, `{"A": "2", "empty": null}`}},
				},
				{
					Query: `SELECT hstore(ARRAY['A', '2', 'B', '5', 'empty', NULL])::text, hstore(ARRAY['A', '1', 'A', '2'])::text, hstore(NULL::text[]) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0198-select-hstore-array[-a-2", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore(ARRAY[]::text[])::text, hstore(ARRAY[]::text[], ARRAY[]::text[])::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0199-select-hstore-array[]::text[]-::text-hstore", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore(ROW('Ada', 42, true, NULL)::hstore_person)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0200-select-hstore-row-ada-42", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore(ROW(1, 'x', NULL))::text, hstore(ROW('needs,quote', 'a"b', false))::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0201-select-hstore-row-1-x", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore(NULL::hstore_person)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0202-select-hstore-null::hstore_person-::text", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT '"A"=>"2", "B"=>"5"'::public.hstore = '"B"=>"5", "A"=>"2"'::public.hstore, '"A"=>NULL'::public.hstore = '"A"=>NULL'::public.hstore, '"A"=>NULL'::public.hstore = '"A"=>""'::public.hstore;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0203-select-a-=>-2-b", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT '"A"=>"2"'::public.hstore <> '"A"=>"9"'::public.hstore, '"A"=>"2"'::public.hstore = '"A"=>"2", "B"=>NULL'::public.hstore;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0204-select-a-=>-2-::public.hstore", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_cmp(''::public.hstore, '"A"=>"1"'::public.hstore)::text, hstore_cmp('"A"=>"1"'::public.hstore, '"A"=>"2"'::public.hstore)::text, hstore_cmp('"A"=>"2"'::public.hstore, '"A"=>"1"'::public.hstore)::text, hstore_cmp('"A"=>NULL'::public.hstore, '"A"=>""'::public.hstore)::text, hstore_cmp('"A"=>"1"'::public.hstore, '"A"=>"1", "B"=>"1"'::public.hstore)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0205-select-hstore_cmp-::public.hstore-a-=>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT hstore_lt('"A"=>"1"'::public.hstore, '"A"=>"2"'::public.hstore), hstore_le('"A"=>"1"'::public.hstore, '"A"=>"1"'::public.hstore), hstore_gt('"B"=>"1"'::public.hstore, '"AA"=>"1"'::public.hstore), hstore_ge('"A"=>NULL'::public.hstore, '"A"=>""'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0206-select-hstore_lt-a-=>-1", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT '"A"=>"1"'::public.hstore #<# '"A"=>"2"'::public.hstore, '"A"=>"1"'::public.hstore #<=# '"A"=>"1"'::public.hstore, '"B"=>"1"'::public.hstore #># '"AA"=>"1"'::public.hstore, '"A"=>NULL'::public.hstore #>=# '"A"=>""'::public.hstore;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0207-select-a-=>-1-::public.hstore", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT NULL::public.hstore #<# '"A"=>"1"'::public.hstore IS NULL, '"A"=>"1"'::public.hstore #># NULL::public.hstore IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0208-select-null::public.hstore-#<#-a-=>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT '"A"=>"1"'::public.hstore OPERATOR(public.#<#) '"A"=>"2"'::public.hstore;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0209-select-a-=>-1-::public.hstore", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (%% '"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore)::text, (%# '"B"=>"5", "A"=>"2", "empty"=>NULL'::public.hstore)::text;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0210-select-%%-b-=>-5", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT * FROM populate_record(NULL::hstore_pop_base, '"a"=>"5", "b"=>"from hstore", "c"=>"f", "ignored"=>"x"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0211-select-*-from-populate_record-null::hstore_pop_base", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT * FROM populate_record(ROW(10, 'base', true)::hstore_pop_base, '"a"=>"5", "b"=>NULL'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0212-select-*-from-populate_record-row", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT * FROM populate_record(ROW(10, 'base', true)::hstore_pop_base, NULL::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0213-select-*-from-populate_record-row", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT populate_record(NULL::hstore_pop_base, NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0214-select-populate_record-null::hstore_pop_base-null::public.hstore-is", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT ((ROW(10, 'base', true)::hstore_pop_base #= '"a"=>"5", "b"=>NULL'::public.hstore)).a, ((ROW(10, 'base', true)::hstore_pop_base #= '"a"=>"5", "b"=>NULL'::public.hstore)).b, ((ROW(10, 'base', true)::hstore_pop_base #= '"a"=>"5", "b"=>NULL'::public.hstore)).c;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0215-select-row-10-base-true", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT ((ROW(10, 'base', true)::hstore_pop_base #= NULL::public.hstore)).a, (NULL::hstore_pop_base #= NULL::public.hstore) IS NULL;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0216-select-row-10-base-true", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT * FROM populate_record(NULL::hstore_pop_base, '"A"=>"5", "b"=>"exact"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0217-select-*-from-populate_record-null::hstore_pop_base", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT * FROM populate_record(NULL::hstore_pop_base, '"b"=>""'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0218-select-*-from-populate_record-null::hstore_pop_base", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query:    `SELECT * FROM populate_record(NULL::hstore_pop_row, hstore(ARRAY['a', 'b', 'c', 'j'], ARRAY['1', '{2,"a b"}', '(9,nested,t)', '{"x":2}']));`,
					Expected: []sql.Row{{1, `{2,"a b"}`, `(9,nested,t)`, `{"x": 2}`}},
				},
				{
					Query: `SELECT c FROM populate_record(NULL::hstore_pop_row, hstore(ARRAY['a', 'c'], ARRAY['1', '(9,"needs,quote",)']));`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0220-select-c-from-populate_record-null::hstore_pop_row", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query: `SELECT (populate_record(NULL::hstore_pop_base, '"a"=>"7", "b"=>"scalar", "c"=>"false"'::public.hstore)).b;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0221-select-populate_record-null::hstore_pop_base-a-=>", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
				{
					Query:       `SELECT hstore(ARRAY['A', '1', 'B']::text[]);`,
					ExpectedErr: `array must have even number of elements`,
				},
				{
					Query:       `SELECT hstore(ARRAY['A', NULL], ARRAY['1', '2']);`,
					ExpectedErr: `null value not allowed for hstore key`,
				},
				{
					Query:       `SELECT hstore(ARRAY['A', 'B'], ARRAY['1']);`,
					ExpectedErr: `arrays must have same bounds`,
				},
				{
					Query:       `SELECT 'not hstore'::public.hstore -> 'missing';`,
					ExpectedErr: `invalid input syntax for type hstore`,
				},
				{
					Query: `SELECT * FROM populate_record(NULL::hstore_pop_base, '"a"=>"not-int"'::public.hstore);`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0226-select-*-from-populate_record-null::hstore_pop_base", Compare: "sqlstate", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE", "DROP TABLE IF EXISTS vending_machines CASCADE", "DROP TYPE IF EXISTS hstore_person CASCADE", "DROP TYPE IF EXISTS hstore_pop_base CASCADE", "DROP TYPE IF EXISTS hstore_pop_row CASCADE"}},
				},
			},
		},
		{
			Name: "DROP EXTENSION supports dump cleanup prelude",
			SetUpScript: []string{
				`DROP EXTENSION IF EXISTS hstore;`,
				`CREATE EXTENSION hstore WITH SCHEMA public;`,
				`DROP EXTENSION hstore;`,
			},
			Assertions: []ScriptTestAssertion{
				{
					Query: `SELECT extname FROM pg_catalog.pg_extension WHERE extname = 'hstore';`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0227-select-extname-from-pg_catalog.pg_extension-where", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE"}},
				},
				{
					Query: `DROP EXTENSION hstore;`, PostgresOracle: ScriptTestPostgresOracle{ID: "common-extensions-probe-test-testcommonextensionsprobe-0228-drop-extension-hstore", Compare: "sqlstate", Cleanup: []string{"DROP EXTENSION IF EXISTS hstore CASCADE"}},
				},
			},
		},
	})
}
