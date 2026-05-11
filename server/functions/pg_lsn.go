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
	"cmp"
	"encoding/binary"
	"math/big"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"github.com/shopspring/decimal"

	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/replsource"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

var logicalDecodingMessages = struct {
	sync.Mutex
	pending map[uint32][]replsource.WALMessage
}{
	pending: make(map[uint32][]replsource.WALMessage),
}

// initPgLsn registers the functions to the catalog.
func initPgLsn() {
	framework.RegisterFunction(pg_lsn_in)
	framework.RegisterFunction(pg_lsn_out)
	framework.RegisterFunction(pg_lsn_recv)
	framework.RegisterFunction(pg_lsn_send)
	framework.RegisterFunction(pg_lsn_cmp)
	framework.RegisterFunction(pg_wal_lsn_diff)
	framework.RegisterFunction(pg_current_wal_lsn)
	framework.RegisterFunction(pg_logical_emit_message)
	framework.RegisterFunction(pg_logical_emit_message_flush)
	framework.RegisterFunction(pg_last_wal_receive_lsn)
	framework.RegisterFunction(pg_last_wal_replay_lsn)
	framework.RegisterFunction(pg_lsn_larger)
	framework.RegisterFunction(pg_lsn_smaller)
}

// pg_lsn_in represents the PostgreSQL function of pg_lsn type IO input.
var pg_lsn_in = framework.Function1{
	Name:       "pg_lsn_in",
	Return:     pgtypes.PgLsn,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ParsePgLsn(val.(string))
	},
}

// pg_lsn_out represents the PostgreSQL function of pg_lsn type IO output.
var pg_lsn_out = framework.Function1{
	Name:       "pg_lsn_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.FormatPgLsn(val.(uint64)), nil
	},
}

// pg_lsn_recv represents the PostgreSQL function of pg_lsn type IO receive.
var pg_lsn_recv = framework.Function1{
	Name:       "pg_lsn_recv",
	Return:     pgtypes.PgLsn,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		reader := utils.NewWireReader(data)
		return reader.ReadUint64(), nil
	},
}

// pg_lsn_send represents the PostgreSQL function of pg_lsn type IO send.
var pg_lsn_send = framework.Function1{
	Name:       "pg_lsn_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		writer := utils.NewWireWriter()
		writer.WriteUint64(val.(uint64))
		return writer.BufferData(), nil
	},
}

// pg_lsn_cmp represents the PostgreSQL btree comparator for pg_lsn.
var pg_lsn_cmp = framework.Function2{
	Name:       "pg_lsn_cmp",
	Return:     pgtypes.Int32,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		return int32(cmp.Compare(val1.(uint64), val2.(uint64))), nil
	},
}

// pg_wal_lsn_diff represents the PostgreSQL function of the same name.
var pg_wal_lsn_diff = framework.Function2{
	Name:       "pg_wal_lsn_diff",
	Return:     pgtypes.Numeric,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		left := new(big.Int).SetUint64(val1.(uint64))
		right := new(big.Int).SetUint64(val2.(uint64))
		return decimal.NewFromBigInt(left.Sub(left, right), 0), nil
	},
}

// pg_current_wal_lsn reports the highest local logical replication source LSN.
var pg_current_wal_lsn = framework.Function0{
	Name:               "pg_current_wal_lsn",
	Return:             pgtypes.PgLsn,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return uint64(replsource.CurrentLSN()), nil
	},
}

// pg_logical_emit_message represents the PostgreSQL function of the same name.
var pg_logical_emit_message = framework.Function3{
	Name:               "pg_logical_emit_message",
	Return:             pgtypes.PgLsn,
	Parameters:         [3]*pgtypes.DoltgresType{pgtypes.Bool, pgtypes.Text, pgtypes.Text},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, transactional any, prefix any, content any) (any, error) {
		return emitLogicalDecodingMessage(ctx, transactional, prefix, content)
	},
}

// pg_logical_emit_message_flush represents the PostgreSQL 17 overload with the flush parameter.
var pg_logical_emit_message_flush = framework.Function4{
	Name:               "pg_logical_emit_message",
	Return:             pgtypes.PgLsn,
	Parameters:         [4]*pgtypes.DoltgresType{pgtypes.Bool, pgtypes.Text, pgtypes.Text, pgtypes.Bool},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, transactional any, prefix any, content any, _ any) (any, error) {
		return emitLogicalDecodingMessage(ctx, transactional, prefix, content)
	},
}

// pg_last_wal_receive_lsn reports NULL because Doltgres is not in standby recovery mode.
var pg_last_wal_receive_lsn = framework.Function0{
	Name:               "pg_last_wal_receive_lsn",
	Return:             pgtypes.PgLsn,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return nil, nil
	},
}

func emitLogicalDecodingMessage(ctx *sql.Context, transactional any, prefix any, content any) (any, error) {
	transactional, err := sql.UnwrapAny(ctx, transactional)
	if err != nil {
		return nil, err
	}
	prefix, err = sql.UnwrapAny(ctx, prefix)
	if err != nil {
		return nil, err
	}
	content, err = sql.UnwrapAny(ctx, content)
	if err != nil {
		return nil, err
	}

	lsn := replsource.AdvanceLSN()
	walData := encodeLogicalDecodingMessage(lsn, transactional.(bool), prefix.(string), []byte(content.(string)))
	message := replsource.WALMessage{
		WALStart:     lsn,
		ServerWALEnd: lsn,
		WALData:      walData,
	}
	if transactional.(bool) {
		queueTransactionalLogicalDecodingMessage(ctx, message)
		return uint64(lsn), nil
	}
	err = replsource.Broadcast(nil, []replsource.WALMessage{message})
	if err != nil {
		return nil, err
	}
	return uint64(lsn), nil
}

func queueTransactionalLogicalDecodingMessage(ctx *sql.Context, message replsource.WALMessage) {
	if ctx == nil {
		_ = replsource.Broadcast(nil, []replsource.WALMessage{message})
		return
	}
	connectionID := uint32(ctx.Session.ID())
	logicalDecodingMessages.Lock()
	defer logicalDecodingMessages.Unlock()
	logicalDecodingMessages.pending[connectionID] = append(logicalDecodingMessages.pending[connectionID], cloneLogicalDecodingMessage(message))
}

// CommitSessionLogicalDecodingMessages publishes transactional logical decoding
// messages queued by pg_logical_emit_message(..., true, ...).
func CommitSessionLogicalDecodingMessages(connectionID uint32) error {
	messages := takeSessionLogicalDecodingMessages(connectionID)
	if len(messages) == 0 {
		return nil
	}
	return replsource.Broadcast(nil, messages)
}

// RollbackSessionLogicalDecodingMessages discards transactional logical
// decoding messages queued by pg_logical_emit_message(..., true, ...).
func RollbackSessionLogicalDecodingMessages(connectionID uint32) {
	_ = takeSessionLogicalDecodingMessages(connectionID)
}

func takeSessionLogicalDecodingMessages(connectionID uint32) []replsource.WALMessage {
	logicalDecodingMessages.Lock()
	defer logicalDecodingMessages.Unlock()
	messages := logicalDecodingMessages.pending[connectionID]
	delete(logicalDecodingMessages.pending, connectionID)
	return messages
}

func cloneLogicalDecodingMessage(message replsource.WALMessage) replsource.WALMessage {
	return replsource.WALMessage{
		WALStart:     message.WALStart,
		ServerWALEnd: message.ServerWALEnd,
		WALData:      append([]byte(nil), message.WALData...),
	}
}

func encodeLogicalDecodingMessage(lsn pglogrepl.LSN, transactional bool, prefix string, content []byte) []byte {
	data := make([]byte, 0, 1+1+8+len(prefix)+1+4+len(content))
	data = append(data, byte(pglogrepl.MessageTypeMessage))
	if transactional {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	data = binary.BigEndian.AppendUint64(data, uint64(lsn))
	data = append(data, prefix...)
	data = append(data, 0)
	data = binary.BigEndian.AppendUint32(data, uint32(len(content)))
	data = append(data, content...)
	return data
}

// pg_last_wal_replay_lsn reports NULL because Doltgres is not in standby recovery mode.
var pg_last_wal_replay_lsn = framework.Function0{
	Name:               "pg_last_wal_replay_lsn",
	Return:             pgtypes.PgLsn,
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context) (any, error) {
		return nil, nil
	},
}

// pg_lsn_larger returns the larger pg_lsn.
var pg_lsn_larger = framework.Function2{
	Name:       "pg_lsn_larger",
	Return:     pgtypes.PgLsn,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		if val1.(uint64) >= val2.(uint64) {
			return val1, nil
		}
		return val2, nil
	},
}

// pg_lsn_smaller returns the smaller pg_lsn.
var pg_lsn_smaller = framework.Function2{
	Name:       "pg_lsn_smaller",
	Return:     pgtypes.PgLsn,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.PgLsn, pgtypes.PgLsn},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, val1, val2 any) (any, error) {
		if val1.(uint64) <= val2.(uint64) {
			return val1, nil
		}
		return val2, nil
	},
}
