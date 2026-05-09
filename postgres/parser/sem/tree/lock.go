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

// LockTable represents a PostgreSQL LOCK TABLE statement.
type LockTable struct {
	Tables TableNames
	Mode   string
	Nowait bool
}

var _ Statement = &LockTable{}

func (node *LockTable) String() string {
	return AsString(node)
}

func (node *LockTable) StatementType() StatementType {
	return Ack
}

func (node *LockTable) StatementTag() string {
	return "LOCK TABLE"
}

// Format implements the NodeFormatter interface.
func (node *LockTable) Format(ctx *FmtCtx) {
	ctx.WriteString("LOCK TABLE ")
	ctx.FormatNode(&node.Tables)
	if len(node.Mode) > 0 {
		ctx.WriteString(" IN ")
		ctx.WriteString(node.Mode)
		ctx.WriteString(" MODE")
	}
	if node.Nowait {
		ctx.WriteString(" NOWAIT")
	}
}
