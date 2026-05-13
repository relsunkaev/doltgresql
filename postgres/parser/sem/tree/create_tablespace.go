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

package tree

import "github.com/dolthub/doltgresql/postgres/parser/lex"

var _ Statement = &CreateTablespace{}

// CreateTablespace represents a CREATE TABLESPACE statement.
type CreateTablespace struct {
	Name     Name
	Owner    string
	Location string
	Options  StorageParams
}

// Format implements the NodeFormatter interface.
func (node *CreateTablespace) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE TABLESPACE ")
	ctx.FormatNode(&node.Name)
	if node.Owner != "" {
		ctx.WriteString(" OWNER ")
		ctx.FormatNameP(&node.Owner)
	}
	ctx.WriteString(" LOCATION ")
	lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.Location, ctx.flags.EncodeFlags())
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(")")
	}
}
