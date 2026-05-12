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

var _ Statement = &CreateForeignDataWrapper{}
var _ Statement = &AlterForeignTable{}
var _ Statement = &AlterForeignDataWrapper{}
var _ Statement = &DropForeignTable{}
var _ Statement = &DropForeignDataWrapper{}
var _ Statement = &CreateForeignServer{}
var _ Statement = &AlterForeignServer{}
var _ Statement = &DropForeignServer{}
var _ Statement = &CreateUserMapping{}
var _ Statement = &AlterUserMapping{}
var _ Statement = &DropUserMapping{}
var _ Statement = &ImportForeignSchema{}

// CreateForeignDataWrapper represents a CREATE FOREIGN DATA WRAPPER statement.
type CreateForeignDataWrapper struct {
	Name    Name
	Options []KVOption
}

// AlterForeignDataWrapper represents an ALTER FOREIGN DATA WRAPPER statement.
type AlterForeignDataWrapper struct {
	Name    Name
	Options []KVOption
}

// AlterForeignTable represents ALTER FOREIGN TABLE options.
type AlterForeignTable struct {
	Name    *UnresolvedObjectName
	Options []KVOption
}

// DropForeignTable represents a DROP FOREIGN TABLE statement.
type DropForeignTable struct {
	Names        TableNames
	IfExists     bool
	DropBehavior DropBehavior
}

// DropForeignDataWrapper represents a DROP FOREIGN DATA WRAPPER statement.
type DropForeignDataWrapper struct {
	Names        NameList
	IfExists     bool
	DropBehavior DropBehavior
}

// CreateForeignServer represents a CREATE SERVER statement.
type CreateForeignServer struct {
	Name    Name
	Wrapper Name
	Type    string
	Version string
	Options []KVOption
}

// AlterForeignServer represents an ALTER SERVER statement.
type AlterForeignServer struct {
	Name    Name
	Version string
	Options []KVOption
}

// DropForeignServer represents a DROP SERVER statement.
type DropForeignServer struct {
	Names        NameList
	IfExists     bool
	DropBehavior DropBehavior
}

// CreateUserMapping represents a CREATE USER MAPPING statement.
type CreateUserMapping struct {
	User    string
	Server  Name
	Options []KVOption
}

// AlterUserMapping represents an ALTER USER MAPPING statement.
type AlterUserMapping struct {
	User    string
	Server  Name
	Options []KVOption
}

// DropUserMapping represents a DROP USER MAPPING statement.
type DropUserMapping struct {
	User     string
	Server   Name
	IfExists bool
}

// ImportForeignSchema represents an IMPORT FOREIGN SCHEMA statement.
type ImportForeignSchema struct {
	Schema Name
	Server Name
	Into   Name
}

// Format implements the NodeFormatter interface.
func (node *CreateForeignDataWrapper) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE FOREIGN DATA WRAPPER ")
	ctx.FormatNode(&node.Name)
}

// Format implements the NodeFormatter interface.
func (node *AlterForeignDataWrapper) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FOREIGN DATA WRAPPER ")
	ctx.FormatNode(&node.Name)
}

// Format implements the NodeFormatter interface.
func (node *AlterForeignTable) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER FOREIGN TABLE ")
	ctx.FormatNode(node.Name)
}

// Format implements the NodeFormatter interface.
func (node *DropForeignTable) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP FOREIGN TABLE ")
	ctx.FormatNode(&node.Names)
}

// Format implements the NodeFormatter interface.
func (node *DropForeignDataWrapper) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP FOREIGN DATA WRAPPER ")
	ctx.FormatNode(&node.Names)
}

// Format implements the NodeFormatter interface.
func (node *CreateForeignServer) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SERVER ")
	ctx.FormatNode(&node.Name)
}

// Format implements the NodeFormatter interface.
func (node *AlterForeignServer) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER SERVER ")
	ctx.FormatNode(&node.Name)
}

// Format implements the NodeFormatter interface.
func (node *DropForeignServer) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SERVER ")
	ctx.FormatNode(&node.Names)
}

// Format implements the NodeFormatter interface.
func (node *CreateUserMapping) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE USER MAPPING")
}

// Format implements the NodeFormatter interface.
func (node *AlterUserMapping) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER USER MAPPING")
}

// Format implements the NodeFormatter interface.
func (node *DropUserMapping) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP USER MAPPING")
}

// Format implements the NodeFormatter interface.
func (node *ImportForeignSchema) Format(ctx *FmtCtx) {
	ctx.WriteString("IMPORT FOREIGN SCHEMA ")
	ctx.FormatNode(&node.Schema)
}
