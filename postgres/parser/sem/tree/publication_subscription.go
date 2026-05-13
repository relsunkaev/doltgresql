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

// PublicationTable is a table entry in CREATE/ALTER PUBLICATION.
type PublicationTable struct {
	Name      TableName
	Columns   NameList
	RowFilter Expr
}

// Format implements the NodeFormatter interface.
func (node *PublicationTable) Format(ctx *FmtCtx) {
	ctx.WriteString("TABLE ")
	ctx.FormatNode(&node.Name)
	if len(node.Columns) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Columns)
		ctx.WriteByte(')')
	}
	if node.RowFilter != nil {
		ctx.WriteString(" WHERE (")
		ctx.FormatNode(node.RowFilter)
		ctx.WriteByte(')')
	}
}

// PublicationTables is a list of publication table entries.
type PublicationTables []PublicationTable

// Format implements the NodeFormatter interface.
func (nodes *PublicationTables) Format(ctx *FmtCtx) {
	for i := range *nodes {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*nodes)[i])
	}
}

// PublicationTargets are the object memberships attached to a publication.
type PublicationTargets struct {
	AllTables bool
	Tables    PublicationTables
	Schemas   []string
}

// Format implements the NodeFormatter interface.
func (node *PublicationTargets) Format(ctx *FmtCtx) {
	switch {
	case node.AllTables:
		ctx.WriteString("FOR ALL TABLES")
	case len(node.Tables) > 0:
		ctx.WriteString("FOR ")
		ctx.FormatNode(&node.Tables)
	case len(node.Schemas) > 0:
		ctx.WriteString("FOR TABLES IN SCHEMA ")
		for i, schema := range node.Schemas {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatName(schema)
		}
	}
}

// CreatePublication represents a CREATE PUBLICATION statement.
type CreatePublication struct {
	Name    Name
	Targets PublicationTargets
	Options KVOptions
}

// Format implements the NodeFormatter interface.
func (node *CreatePublication) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE PUBLICATION ")
	ctx.FormatNode(&node.Name)
	if node.Targets.AllTables || len(node.Targets.Tables) > 0 || len(node.Targets.Schemas) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.Targets)
	}
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH (")
		ctx.FormatNode(&node.Options)
		ctx.WriteByte(')')
	}
}

// PublicationAlterAction is the action in an ALTER PUBLICATION statement.
type PublicationAlterAction string

const (
	PublicationAlterAddTables   PublicationAlterAction = "ADD TABLE"
	PublicationAlterSetTables   PublicationAlterAction = "SET TABLE"
	PublicationAlterDropTables  PublicationAlterAction = "DROP TABLE"
	PublicationAlterAddSchemas  PublicationAlterAction = "ADD TABLES IN SCHEMA"
	PublicationAlterSetSchemas  PublicationAlterAction = "SET TABLES IN SCHEMA"
	PublicationAlterDropSchemas PublicationAlterAction = "DROP TABLES IN SCHEMA"
	PublicationAlterSetOptions  PublicationAlterAction = "SET"
	PublicationAlterRename      PublicationAlterAction = "RENAME"
	PublicationAlterOwner       PublicationAlterAction = "OWNER"
)

// AlterPublication represents an ALTER PUBLICATION statement.
type AlterPublication struct {
	Name    Name
	Action  PublicationAlterAction
	Targets PublicationTargets
	Options KVOptions
	NewName Name
	Owner   string
}

// Format implements the NodeFormatter interface.
func (node *AlterPublication) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER PUBLICATION ")
	ctx.FormatNode(&node.Name)
	ctx.WriteByte(' ')
	if node.Action == PublicationAlterRename {
		ctx.WriteString("RENAME TO ")
		ctx.FormatNode(&node.NewName)
		return
	}
	if node.Action == PublicationAlterOwner {
		ctx.WriteString("OWNER TO ")
		ctx.WriteString(node.Owner)
		return
	}
	ctx.WriteString(string(node.Action))
	if node.Targets.AllTables || len(node.Targets.Tables) > 0 || len(node.Targets.Schemas) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.Targets)
	}
	if len(node.Options) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Options)
		ctx.WriteByte(')')
	}
}

// DropPublication represents a DROP PUBLICATION statement.
type DropPublication struct {
	Names        NameList
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropPublication) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP PUBLICATION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Names)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(dropBehaviorName[node.DropBehavior])
	}
}

// CreateSubscription represents a CREATE SUBSCRIPTION statement.
type CreateSubscription struct {
	Name         Name
	ConnInfo     string
	Publications NameList
	Options      KVOptions
}

// Format implements the NodeFormatter interface.
func (node *CreateSubscription) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SUBSCRIPTION ")
	ctx.FormatNode(&node.Name)
	ctx.WriteString(" CONNECTION ")
	lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.ConnInfo, ctx.flags.EncodeFlags())
	ctx.WriteString(" PUBLICATION ")
	ctx.FormatNode(&node.Publications)
	if len(node.Options) > 0 {
		ctx.WriteString(" WITH (")
		ctx.FormatNode(&node.Options)
		ctx.WriteByte(')')
	}
}

// SubscriptionAlterAction is the action in an ALTER SUBSCRIPTION statement.
type SubscriptionAlterAction string

const (
	SubscriptionAlterConnection      SubscriptionAlterAction = "CONNECTION"
	SubscriptionAlterSetPublication  SubscriptionAlterAction = "SET PUBLICATION"
	SubscriptionAlterAddPublication  SubscriptionAlterAction = "ADD PUBLICATION"
	SubscriptionAlterDropPublication SubscriptionAlterAction = "DROP PUBLICATION"
	SubscriptionAlterRefresh         SubscriptionAlterAction = "REFRESH PUBLICATION"
	SubscriptionAlterEnable          SubscriptionAlterAction = "ENABLE"
	SubscriptionAlterDisable         SubscriptionAlterAction = "DISABLE"
	SubscriptionAlterSetOptions      SubscriptionAlterAction = "SET"
	SubscriptionAlterSkip            SubscriptionAlterAction = "SKIP"
	SubscriptionAlterRename          SubscriptionAlterAction = "RENAME"
	SubscriptionAlterOwner           SubscriptionAlterAction = "OWNER"
)

// AlterSubscription represents an ALTER SUBSCRIPTION statement.
type AlterSubscription struct {
	Name         Name
	Action       SubscriptionAlterAction
	ConnInfo     string
	Publications NameList
	Options      KVOptions
	SkipLSN      string
	NewName      Name
	Owner        string
}

// Format implements the NodeFormatter interface.
func (node *AlterSubscription) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER SUBSCRIPTION ")
	ctx.FormatNode(&node.Name)
	ctx.WriteByte(' ')
	switch node.Action {
	case SubscriptionAlterConnection:
		ctx.WriteString("CONNECTION ")
		lex.EncodeSQLStringWithFlags(&ctx.Buffer, node.ConnInfo, ctx.flags.EncodeFlags())
	case SubscriptionAlterRename:
		ctx.WriteString("RENAME TO ")
		ctx.FormatNode(&node.NewName)
	case SubscriptionAlterOwner:
		ctx.WriteString("OWNER TO ")
		ctx.FormatNameP(&node.Owner)
	case SubscriptionAlterSetPublication, SubscriptionAlterAddPublication, SubscriptionAlterDropPublication:
		ctx.WriteString(string(node.Action))
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.Publications)
	case SubscriptionAlterSetOptions, SubscriptionAlterRefresh, SubscriptionAlterSkip:
		ctx.WriteString(string(node.Action))
		if len(node.Options) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.Options)
			ctx.WriteByte(')')
		}
	default:
		ctx.WriteString(string(node.Action))
	}
}

// DropSubscription represents a DROP SUBSCRIPTION statement.
type DropSubscription struct {
	Name         Name
	IfExists     bool
	DropBehavior DropBehavior
}

// Format implements the NodeFormatter interface.
func (node *DropSubscription) Format(ctx *FmtCtx) {
	ctx.WriteString("DROP SUBSCRIPTION ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(&node.Name)
	if node.DropBehavior != DropDefault {
		ctx.WriteByte(' ')
		ctx.WriteString(dropBehaviorName[node.DropBehavior])
	}
}
