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

var _ Statement = &Reindex{}

// ReindexTarget identifies the relation kind targeted by a REINDEX statement.
type ReindexTarget int

const (
	ReindexIndex ReindexTarget = iota
	ReindexTable
)

// Reindex represents a REINDEX INDEX/TABLE statement.
type Reindex struct {
	Target       ReindexTarget
	Index        *TableIndexName
	Table        TableName
	Concurrently bool
}

// Format implements the NodeFormatter interface.
func (node *Reindex) Format(ctx *FmtCtx) {
	ctx.WriteString("REINDEX ")
	switch node.Target {
	case ReindexIndex:
		ctx.WriteString("INDEX ")
		if node.Concurrently {
			ctx.WriteString("CONCURRENTLY ")
		}
		ctx.FormatNode(node.Index)
	case ReindexTable:
		ctx.WriteString("TABLE ")
		if node.Concurrently {
			ctx.WriteString("CONCURRENTLY ")
		}
		ctx.FormatNode(&node.Table)
	}
}
