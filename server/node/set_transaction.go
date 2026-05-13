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

package node

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
)

type TransactionReadWriteMode uint8

const (
	TransactionReadWriteUnspecified TransactionReadWriteMode = iota
	TransactionReadOnly
	TransactionReadWrite
)

type TransactionDeferrableMode uint8

const (
	TransactionDeferrableUnspecified TransactionDeferrableMode = iota
	TransactionDeferrable
	TransactionNotDeferrable
)

// SetTransaction carries PostgreSQL SET TRANSACTION modes to the connection
// handler, which owns transaction boundaries.
type SetTransaction struct {
	ReadWriteMode  TransactionReadWriteMode
	DeferrableMode TransactionDeferrableMode
	Isolation      bool
	Snapshot       string
}

var _ vitess.Injectable = (*SetTransaction)(nil)
var _ sql.ExecSourceRel = (*SetTransaction)(nil)

func (s SetTransaction) Resolved() bool {
	return true
}

func (s SetTransaction) String() string {
	return "SET TRANSACTION"
}

func (s SetTransaction) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (s SetTransaction) Children() []sql.Node {
	return nil
}

func (s SetTransaction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return s, nil
}

func (s SetTransaction) IsReadOnly() bool {
	return true
}

func (s SetTransaction) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return s, nil
}

func (s SetTransaction) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// SetSessionCharacteristics carries SET SESSION CHARACTERISTICS AS TRANSACTION
// modes to the connection handler.
type SetSessionCharacteristics struct {
	ReadWriteMode TransactionReadWriteMode
}

var _ vitess.Injectable = (*SetSessionCharacteristics)(nil)
var _ sql.ExecSourceRel = (*SetSessionCharacteristics)(nil)

func (s SetSessionCharacteristics) Resolved() bool {
	return true
}

func (s SetSessionCharacteristics) String() string {
	return "SET SESSION CHARACTERISTICS"
}

func (s SetSessionCharacteristics) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

func (s SetSessionCharacteristics) Children() []sql.Node {
	return nil
}

func (s SetSessionCharacteristics) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return s, nil
}

func (s SetSessionCharacteristics) IsReadOnly() bool {
	return true
}

func (s SetSessionCharacteristics) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	return s, nil
}

func (s SetSessionCharacteristics) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}
