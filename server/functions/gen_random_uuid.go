// Copyright 2025 Dolthub, Inc.
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
	"crypto/rand"
	"encoding/binary"
	"net"
	"time"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/postgres/parser/uuid"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// initGenRandomUuid registers the functions to the catalog.
func initGenRandomUuid() {
	framework.RegisterFunction(gen_random_uuid)
	framework.RegisterFunction(uuid_nil)
	framework.RegisterFunction(uuid_ns_dns)
	framework.RegisterFunction(uuid_ns_url)
	framework.RegisterFunction(uuid_ns_oid)
	framework.RegisterFunction(uuid_ns_x500)
	framework.RegisterFunction(uuid_generate_v1)
	framework.RegisterFunction(uuid_generate_v1mc)
	framework.RegisterFunction(uuid_generate_v3)
	framework.RegisterFunction(uuid_generate_v4)
	framework.RegisterFunction(uuid_generate_v5)
	framework.RegisterFunction(uuidv7)
}

var gen_random_uuid = framework.Function0{
	Name:   "gen_random_uuid",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NewV4()
	},
}

var uuid_generate_v4 = framework.Function0{
	Name:   "uuid_generate_v4",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NewV4()
	},
}

var uuidv7 = framework.Function0{
	Name:               "uuidv7",
	Return:             pgtypes.Uuid,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		var out uuid.UUID
		if _, err := rand.Read(out[:]); err != nil {
			return uuid.UUID{}, err
		}
		millis := uint64(time.Now().UnixMilli())
		binary.BigEndian.PutUint32(out[0:4], uint32(millis>>16))
		binary.BigEndian.PutUint16(out[4:6], uint16(millis))
		out[6] = (out[6] & 0x0f) | 0x70
		out[8] = (out[8] & 0x3f) | 0x80
		return out, nil
	},
}

var uuid_nil = framework.Function0{
	Name:   "uuid_nil",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.Nil, nil
	},
}

var uuid_ns_dns = framework.Function0{
	Name:   "uuid_ns_dns",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NamespaceDNS, nil
	},
}

var uuid_ns_url = framework.Function0{
	Name:   "uuid_ns_url",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NamespaceURL, nil
	},
}

var uuid_ns_oid = framework.Function0{
	Name:   "uuid_ns_oid",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NamespaceOID, nil
	},
}

var uuid_ns_x500 = framework.Function0{
	Name:   "uuid_ns_x500",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NamespaceX500, nil
	},
}

var uuid_generate_v1 = framework.Function0{
	Name:   "uuid_generate_v1",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NewV1()
	},
}

var uuid_generate_v1mc = framework.Function0{
	Name:   "uuid_generate_v1mc",
	Return: pgtypes.Uuid,
	Strict: true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uuid.NewGenWithHWAF(randomMulticastHardwareAddr).NewV1()
	},
}

var uuid_generate_v3 = framework.Function2{
	Name:       "uuid_generate_v3",
	Return:     pgtypes.Uuid,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Uuid, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, namespace any, name any) (any, error) {
		return uuid.NewV3(namespace.(uuid.UUID), name.(string)), nil
	},
}

var uuid_generate_v5 = framework.Function2{
	Name:       "uuid_generate_v5",
	Return:     pgtypes.Uuid,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Uuid, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, namespace any, name any) (any, error) {
		return uuid.NewV5(namespace.(uuid.UUID), name.(string)), nil
	},
}

func randomMulticastHardwareAddr() (net.HardwareAddr, error) {
	addr := make(net.HardwareAddr, 6)
	if _, err := rand.Read(addr); err != nil {
		return nil, err
	}
	addr[0] |= 0x01
	return addr, nil
}
