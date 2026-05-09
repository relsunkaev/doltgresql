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

package node

import (
	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	doltsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
)

// flipIndexComment rewrites only the comment payload of an existing
// btree index, leaving the row data — the prolly tree that backs the
// index — completely untouched. The path is:
//
//  1. Reach through the SQL table down to the underlying *doltdb.Table
//     and its schema. (Doltgres needs no Dolt-side patch; the methods
//     used here are public.)
//  2. Copy the schema, replace the named index in the IndexCollection
//     with one carrying the new IndexProperties.Comment, leaving every
//     other property — tags, prefix lengths, uniqueness, fulltext /
//     vector flags — identical.
//  3. Call doltdb.Table.UpdateSchema, which the upstream method
//     contract documents as a metadata-only mutation: "This method
//     only updates the schema of a table; the row data is unchanged."
//  4. Stitch the new table version into the working root via PutTable
//     and publish through the doltgres session.
//
// This replaces the wasteful drop+recreate that CREATE INDEX
// CONCURRENTLY's Phase 2 used to perform: the second prolly-tree
// build is gone, so CONCURRENTLY now costs roughly one build plus a
// metadata flip rather than two builds.
//
// Limitations: this only rewrites Dolt schema index metadata. Callers
// should reserve it for state-machine flips where the physical index
// tree and any sidecar storage are already correct.
func flipIndexComment(ctx *sql.Context, schemaName, tableName, indexName, newComment string) error {
	located, ok, err := locateIndex(ctx, schemaName, tableName, indexName, false)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf(`index "%s" not found while flipping comment`, indexName)
	}
	alterable, ok := located.alterable.(*doltsqle.AlterableDoltTable)
	if !ok {
		return errors.Errorf(`comment-only flip requires an AlterableDoltTable; got %T`, located.alterable)
	}
	doltTable, err := alterable.DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}
	currentSch, err := doltTable.GetSchema(ctx)
	if err != nil {
		return err
	}
	newSch := currentSch.Copy()

	existing := newSch.Indexes().GetByName(indexName)
	if existing == nil {
		return errors.Errorf(`index "%s" missing from schema while flipping comment`, indexName)
	}
	props := schema.IndexProperties{
		IsUnique:           existing.IsUnique(),
		IsSpatial:          existing.IsSpatial(),
		IsFullText:         existing.IsFullText(),
		IsVector:           existing.IsVector(),
		IsUserDefined:      existing.IsUserDefined(),
		Comment:            newComment,
		FullTextProperties: existing.FullTextProperties(),
		VectorProperties:   existing.VectorProperties(),
	}
	// AddIndexByColTags errors on duplicate names, so explicitly drop
	// the old entry first; the index data file is keyed by name and is
	// not touched by this metadata-collection edit, so the prolly tree
	// from Phase 1 is reused as-is.
	if _, err = newSch.Indexes().RemoveIndex(indexName); err != nil {
		return err
	}
	if _, err = newSch.Indexes().AddIndexByColTags(
		indexName,
		existing.IndexedColumnTags(),
		existing.PrefixLengths(),
		props,
	); err != nil {
		return err
	}

	updatedTable, err := doltTable.UpdateSchema(ctx, newSch)
	if err != nil {
		return err
	}

	session, root, err := core.GetRootFromContext(ctx)
	if err != nil {
		return err
	}
	resolvedSchemaName := schemaName
	if dst, ok := located.table.(sql.DatabaseSchemaTable); ok {
		if ds := dst.DatabaseSchema(); ds != nil {
			resolvedSchemaName = ds.SchemaName()
		}
	}
	newRoot, err := root.PutTable(ctx, doltdb.TableName{
		Name:   located.table.Name(),
		Schema: resolvedSchemaName,
	}, updatedTable)
	if err != nil {
		return err
	}
	return session.SetWorkingRoot(ctx, ctx.GetCurrentDatabase(), newRoot)
}
