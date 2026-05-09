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

// DoBlock represents a PostgreSQL anonymous DO code block.
type DoBlock struct {
	Code     string
	Language string
}

func (node *DoBlock) String() string { return AsString(node) }

// Format implements the NodeFormatter interface.
func (node *DoBlock) Format(ctx *FmtCtx) {
	ctx.WriteString("DO ")
	ctx.WriteString(node.Code)
	if node.Language != "" {
		ctx.WriteString(" LANGUAGE ")
		ctx.WriteString(node.Language)
	}
}
