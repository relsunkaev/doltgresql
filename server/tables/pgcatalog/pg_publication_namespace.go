// Copyright 2024 Dolthub, Inc.
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

package pgcatalog

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/publications"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgPublicationNamespaceName is a constant to the pg_publication_namespace name.
const PgPublicationNamespaceName = "pg_publication_namespace"

// InitPgPublicationNamespace handles registration of the pg_publication_namespace handler.
func InitPgPublicationNamespace() {
	tables.AddHandler(PgCatalogName, PgPublicationNamespaceName, PgPublicationNamespaceHandler{})
}

// PgPublicationNamespaceHandler is the handler for the pg_publication_namespace table.
type PgPublicationNamespaceHandler struct{}

var _ tables.Handler = PgPublicationNamespaceHandler{}

// Name implements the interface tables.Handler.
func (p PgPublicationNamespaceHandler) Name() string {
	return PgPublicationNamespaceName
}

// RowIter implements the interface tables.Handler.
func (p PgPublicationNamespaceHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	pubs, err := allPublications(ctx)
	if err != nil {
		return nil, err
	}
	return &pgPublicationNamespaceRowIter{publications: pubs}, nil
}

// Schema implements the interface tables.Handler.
func (p PgPublicationNamespaceHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgPublicationNamespaceSchema,
		PkOrdinals: nil,
	}
}

// pgPublicationNamespaceSchema is the schema for pg_publication_namespace.
var pgPublicationNamespaceSchema = sql.Schema{
	{Name: "oid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPublicationNamespaceName},
	{Name: "pnpubid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPublicationNamespaceName},
	{Name: "pnnspid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPublicationNamespaceName},
	{Name: "tableoid", Type: pgtypes.Oid, Default: nil, Nullable: false, Source: PgPublicationNamespaceName, Hidden: true},
}

// pgPublicationNamespaceRowIter is the sql.RowIter for the pg_publication_namespace table.
type pgPublicationNamespaceRowIter struct {
	publications []publications.Publication
	pubIdx       int
	schemaIdx    int
}

var _ sql.RowIter = (*pgPublicationNamespaceRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgPublicationNamespaceRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for iter.pubIdx < len(iter.publications) {
		pub := iter.publications[iter.pubIdx]
		if iter.schemaIdx >= len(pub.Schemas) {
			iter.pubIdx++
			iter.schemaIdx = 0
			continue
		}
		schema := pub.Schemas[iter.schemaIdx]
		iter.schemaIdx++
		return sql.Row{
			id.NewId(id.Section_Publication, pub.ID.PublicationName(), "schema", schema),
			pub.ID.AsId(),
			id.NewNamespace(schema).AsId(),
			id.NewTable(PgCatalogName, PgPublicationNamespaceName).AsId(),
		}, nil
	}
	return nil, io.EOF
}

// Close implements the interface sql.RowIter.
func (iter *pgPublicationNamespaceRowIter) Close(ctx *sql.Context) error {
	return nil
}
