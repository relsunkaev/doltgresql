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

// Notify represents a NOTIFY statement.
type Notify struct {
	Channel Name
	Payload *string
}

func (node *Notify) String() string { return AsString(node) }

// Format implements the NodeFormatter interface.
func (node *Notify) Format(ctx *FmtCtx) {
	ctx.WriteString("NOTIFY ")
	ctx.FormatNode(&node.Channel)
	if node.Payload != nil {
		ctx.WriteString(", ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, *node.Payload, ctx.flags.EncodeFlags())
	}
}
