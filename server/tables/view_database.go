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

package tables

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
)

var _ sql.ViewDatabase = Database{}

// CreateView persists views under a physical name when PostgreSQL's logical
// view identifier would collide in Dolt's case-insensitive schema fragment key.
func (d Database) CreateView(ctx *sql.Context, name string, selectStatement, createViewStmt string) error {
	return d.Database.CreateView(ctx, core.EncodePhysicalViewName(name), selectStatement, createViewStmt)
}

// DropView removes a view by its physical storage name while accepting the
// PostgreSQL-facing logical identifier used by parsed SQL.
func (d Database) DropView(ctx *sql.Context, name string) error {
	return d.Database.DropView(ctx, core.EncodePhysicalViewName(name))
}

// GetViewDefinition resolves a PostgreSQL logical view name to its physical
// schema-fragment name, then exposes the logical name back to callers.
func (d Database) GetViewDefinition(ctx *sql.Context, name string) (sql.ViewDefinition, bool, error) {
	view, ok, err := getLogicalViewDefinition(ctx, d.Database, name)
	if err != nil || ok {
		return view, ok, err
	}
	if d.Database.Schema() != "" {
		return sql.ViewDefinition{}, false, nil
	}
	searchPath, err := core.SearchPath(ctx)
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}
	for _, schemaName := range searchPath {
		schema, ok, err := d.Database.GetSchema(ctx, schemaName)
		if err != nil {
			return sql.ViewDefinition{}, false, err
		}
		if !ok {
			continue
		}
		viewDb, ok := schema.(sql.ViewDatabase)
		if !ok {
			continue
		}
		view, ok, err = getLogicalViewDefinition(ctx, viewDb, name)
		if err != nil || ok {
			return view, ok, err
		}
	}
	return sql.ViewDefinition{}, false, nil
}

func getLogicalViewDefinition(ctx *sql.Context, viewDb sql.ViewDatabase, name string) (sql.ViewDefinition, bool, error) {
	view, ok, err := viewDb.GetViewDefinition(ctx, core.EncodePhysicalViewName(name))
	if err != nil || ok {
		view.Name = core.DecodePhysicalViewName(view.Name)
		return view, ok, err
	}
	view, ok, err = viewDb.GetViewDefinition(ctx, name)
	view.Name = core.DecodePhysicalViewName(view.Name)
	return view, ok, err
}

// AllViews returns PostgreSQL-facing logical names for views whose persisted
// schema-fragment names had to be encoded for Dolt storage.
func (d Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	views, err := d.Database.AllViews(ctx)
	if err != nil {
		return nil, err
	}
	for i := range views {
		views[i].Name = core.DecodePhysicalViewName(views[i].Name)
	}
	return views, nil
}
