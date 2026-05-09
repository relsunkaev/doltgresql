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

// Listen represents a PostgreSQL LISTEN statement.
type Listen struct {
	Channel Name
}

var _ Statement = &Listen{}

func (node *Listen) String() string {
	return AsString(node)
}

func (node *Listen) StatementType() StatementType {
	return Ack
}

func (node *Listen) StatementTag() string {
	return "LISTEN"
}

// Format implements the NodeFormatter interface.
func (node *Listen) Format(ctx *FmtCtx) {
	ctx.WriteString("LISTEN ")
	ctx.FormatNode(&node.Channel)
}

// Unlisten represents a PostgreSQL UNLISTEN statement.
type Unlisten struct {
	Channel Name
	All     bool
}

var _ Statement = &Unlisten{}

func (node *Unlisten) String() string {
	return AsString(node)
}

func (node *Unlisten) StatementType() StatementType {
	return Ack
}

func (node *Unlisten) StatementTag() string {
	return "UNLISTEN"
}

// Format implements the NodeFormatter interface.
func (node *Unlisten) Format(ctx *FmtCtx) {
	ctx.WriteString("UNLISTEN ")
	if node.All {
		ctx.WriteString("*")
		return
	}
	ctx.FormatNode(&node.Channel)
}
