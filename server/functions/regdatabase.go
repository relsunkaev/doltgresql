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
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initRegdatabase registers the regdatabase IO functions to the catalog.
func initRegdatabase() {
	framework.RegisterFunction(regdatabasein)
	framework.RegisterFunction(regdatabaseout)
	framework.RegisterFunction(regdatabaserecv)
	framework.RegisterFunction(regdatabasesend)
}

var regdatabasein = framework.Function1{
	Name:       "regdatabasein",
	Return:     pgtypes.Regdatabase,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if parsedOid, err := strconv.ParseUint(input, 10, 32); err == nil {
			if internalID := id.Cache().ToInternal(uint32(parsedOid)); internalID.IsValid() {
				return internalID, nil
			}
			return id.NewOID(uint32(parsedOid)).AsId(), nil
		}
		sections, err := ioInputSections(input)
		if err != nil {
			return id.Null, err
		}
		if len(sections) != 1 {
			return id.Null, errors.Errorf("invalid name syntax")
		}
		databaseName := sections[0]
		if !dsess.DSessFromSess(ctx.Session).Provider().HasDatabase(ctx, databaseName) {
			return id.Null, errors.Errorf(`database "%s" does not exist`, input)
		}
		return id.NewDatabase(databaseName).AsId(), nil
	},
}

var regdatabaseout = framework.Function1{
	Name:       "regdatabaseout",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regdatabase},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(id.Id)
		if input.Section() == id.Section_OID {
			if input.Segment(0) == "0" {
				return "-", nil
			}
			oid := id.Oid(input).OID()
			if internalID := id.Cache().ToInternal(oid); internalID.Section() == id.Section_Database {
				return internalID.Segment(0), nil
			}
			for _, database := range dsess.DSessFromSess(ctx.Session).Provider().AllDatabases(ctx) {
				databaseID := id.NewDatabase(database.Name()).AsId()
				if id.Cache().ToOID(databaseID) == oid {
					return database.Name(), nil
				}
			}
			return input.Segment(0), nil
		}
		if input.Section() == id.Section_Database {
			return input.Segment(0), nil
		}
		return input.Segment(0), nil
	},
}

var regdatabaserecv = framework.Function1{
	Name:       "regdatabaserecv",
	Return:     pgtypes.Regdatabase,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		reader := utils.NewWireReader(data)
		cachedID := id.Cache().ToInternal(reader.ReadUint32())
		return cachedID, nil
	},
}

var regdatabasesend = framework.Function1{
	Name:       "regdatabasesend",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Regdatabase},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint32(id.Cache().ToOID(val.(id.Id)))
		return writer.BufferData(), nil
	},
}
