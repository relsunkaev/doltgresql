// Copyright 2024 Dolthub, Inc.
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

package ast

import (
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/server/auth"
)

// Context contains any relevant context for the AST conversion. For example, the auth system uses the context to
// determine which larger statement an expression exists in, which may influence how the expression should handle
// authorization.
type Context struct {
	authContext   *auth.AuthContext
	originalQuery string

	// resolveExcludedRefs, when true, rewrites references to the
	// EXCLUDED pseudo-table (e.g. `EXCLUDED.col`) into MySQL's
	// `values(col)` function. PostgreSQL exposes EXCLUDED only
	// inside ON CONFLICT ... DO UPDATE SET / WHERE clauses; outside
	// that scope EXCLUDED is just an ordinary identifier and must
	// not be rewritten.
	resolveExcludedRefs bool

	// setOpOperandDepth marks SELECT conversion while it is an operand
	// of UNION / INTERSECT / EXCEPT. GMS otherwise preserves source
	// table column indexes for bare column projections, which can
	// scramble set-op schemas when a catalog query selects a sparse
	// subset of columns.
	setOpOperandDepth int

	tableOIDSchema string
	tableOIDTable  string

	ignoreSelectAuthDepth int

	wholeRowDuplicateAliases map[string]wholeRowDuplicateAlias
}

type wholeRowDuplicateAlias struct {
	tableName  string
	fieldNames []string
}

// NewContext returns a new *Context.
func NewContext(postgresStmt parser.Statement) *Context {
	return &Context{
		authContext:   auth.NewAuthContext(),
		originalQuery: postgresStmt.SQL,
	}
}

// Auth returns the portion that handles authentication.
func (ctx *Context) Auth() *auth.AuthContext {
	return ctx.authContext
}

// WithExcludedRefs runs fn with EXCLUDED rewriting enabled and
// restores the previous value when fn returns. Used to bracket
// AST conversion of ON CONFLICT ... DO UPDATE expressions so
// references to the EXCLUDED pseudo-table become `values(col)`.
func (ctx *Context) WithExcludedRefs(fn func() error) error {
	prev := ctx.resolveExcludedRefs
	ctx.resolveExcludedRefs = true
	defer func() { ctx.resolveExcludedRefs = prev }()
	return fn()
}

// ResolveExcludedRefs reports whether EXCLUDED.col should be
// rewritten to values(col) at this point in AST conversion.
func (ctx *Context) ResolveExcludedRefs() bool {
	return ctx.resolveExcludedRefs
}

func (ctx *Context) WithSetOpOperand(fn func() error) error {
	ctx.setOpOperandDepth++
	defer func() { ctx.setOpOperandDepth-- }()
	return fn()
}

func (ctx *Context) WithSelectAuthIgnored(fn func() error) error {
	ctx.ignoreSelectAuthDepth++
	defer func() { ctx.ignoreSelectAuthDepth-- }()
	return fn()
}

func (ctx *Context) SelectAuthType() string {
	if ctx.ignoreSelectAuthDepth > 0 {
		return auth.AuthType_IGNORE
	}
	return auth.AuthType_SELECT
}

func (ctx *Context) InSetOpOperand() bool {
	return ctx.setOpOperandDepth > 0
}

func (ctx *Context) WithTableOIDRelation(schemaName, tableName string, fn func() error) error {
	prevSchema := ctx.tableOIDSchema
	prevTable := ctx.tableOIDTable
	ctx.tableOIDSchema = schemaName
	ctx.tableOIDTable = tableName
	defer func() {
		ctx.tableOIDSchema = prevSchema
		ctx.tableOIDTable = prevTable
	}()
	return fn()
}

func (ctx *Context) TableOIDRelation() (schemaName, tableName string, ok bool) {
	return ctx.tableOIDSchema, ctx.tableOIDTable, ctx.tableOIDTable != ""
}
