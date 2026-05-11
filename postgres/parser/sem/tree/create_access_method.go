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

var _ Statement = &CreateAccessMethod{}

// CreateAccessMethod represents a CREATE ACCESS METHOD statement.
type CreateAccessMethod struct {
	Name    Name
	Type    string
	Handler Name
}

// Format implements the NodeFormatter interface.
func (node *CreateAccessMethod) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE ACCESS METHOD ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" TYPE ")
	switch node.Type {
	case "t":
		ctx.WriteString("TABLE")
	case "i":
		ctx.WriteString("INDEX")
	default:
		ctx.WriteString(node.Type)
	}
	ctx.WriteString(" HANDLER ")
	ctx.FormatNode(&node.Handler)
}
