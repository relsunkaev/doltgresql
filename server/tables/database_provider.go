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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/go-mysql-server/sql"
)

type sequenceAwareDatabaseProvider struct {
	provider sql.DatabaseProvider
}

var _ sql.CollatedDatabaseProvider = sequenceAwareDatabaseProvider{}
var _ sql.ExternalStoredProcedureProvider = sequenceAwareDatabaseProvider{}
var _ sql.FunctionProvider = sequenceAwareDatabaseProvider{}
var _ sql.TableFunctionProvider = sequenceAwareDatabaseProvider{}

// WrapDatabaseProvider makes catalog database lookups return the Doltgres table
// wrapper, including for unqualified planner lookups of the current database.
func WrapDatabaseProvider(provider sql.DatabaseProvider) sql.DatabaseProvider {
	if _, ok := provider.(sequenceAwareDatabaseProvider); ok {
		return provider
	}
	return sequenceAwareDatabaseProvider{provider: provider}
}

func (p sequenceAwareDatabaseProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	database, err := p.provider.Database(ctx, name)
	if err != nil {
		return nil, err
	}
	return wrapDatabase(database), nil
}

func (p sequenceAwareDatabaseProvider) HasDatabase(ctx *sql.Context, name string) bool {
	return p.provider.HasDatabase(ctx, name)
}

func (p sequenceAwareDatabaseProvider) AllDatabases(ctx *sql.Context) []sql.Database {
	databases := p.provider.AllDatabases(ctx)
	for i, database := range databases {
		databases[i] = wrapDatabase(database)
	}
	return databases
}

func (p sequenceAwareDatabaseProvider) CreateDatabase(ctx *sql.Context, name string) error {
	provider, ok := p.provider.(sql.MutableDatabaseProvider)
	if !ok {
		return sql.ErrImmutableDatabaseProvider.New()
	}
	return provider.CreateDatabase(ctx, name)
}

func (p sequenceAwareDatabaseProvider) DropDatabase(ctx *sql.Context, name string) error {
	provider, ok := p.provider.(sql.MutableDatabaseProvider)
	if !ok {
		return sql.ErrImmutableDatabaseProvider.New()
	}
	return provider.DropDatabase(ctx, name)
}

func (p sequenceAwareDatabaseProvider) CreateCollatedDatabase(ctx *sql.Context, name string, collation sql.CollationID) error {
	if provider, ok := p.provider.(sql.CollatedDatabaseProvider); ok {
		return provider.CreateCollatedDatabase(ctx, name, collation)
	}
	if provider, ok := p.provider.(sql.MutableDatabaseProvider); ok {
		if err := provider.CreateDatabase(ctx, name); err != nil {
			return err
		}
		if database, err := p.Database(ctx, name); err == nil {
			if collatedDatabase, ok := database.(sql.CollatedDatabase); ok {
				return collatedDatabase.SetCollation(ctx, collation)
			}
		}
		return nil
	}
	return sql.ErrImmutableDatabaseProvider.New()
}

func (p sequenceAwareDatabaseProvider) Function(ctx *sql.Context, name string) (sql.Function, bool) {
	provider, ok := p.provider.(sql.FunctionProvider)
	if !ok {
		return nil, false
	}
	return provider.Function(ctx, name)
}

func (p sequenceAwareDatabaseProvider) ExternalStoredProcedure(ctx *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	provider, ok := p.provider.(sql.ExternalStoredProcedureProvider)
	if !ok {
		return nil, nil
	}
	return provider.ExternalStoredProcedure(ctx, name, numOfParams)
}

func (p sequenceAwareDatabaseProvider) ExternalStoredProcedures(ctx *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	provider, ok := p.provider.(sql.ExternalStoredProcedureProvider)
	if !ok {
		return nil, nil
	}
	return provider.ExternalStoredProcedures(ctx, name)
}

func (p sequenceAwareDatabaseProvider) TableFunction(ctx *sql.Context, name string) (sql.TableFunction, bool) {
	provider, ok := p.provider.(sql.TableFunctionProvider)
	if !ok {
		return nil, false
	}
	return provider.TableFunction(ctx, name)
}

func (p sequenceAwareDatabaseProvider) WithTableFunctions(fns ...sql.TableFunction) (sql.TableFunctionProvider, error) {
	provider, ok := p.provider.(sql.TableFunctionProvider)
	if !ok {
		return nil, fmt.Errorf("database provider does not implement sql.TableFunctionProvider")
	}
	nextProvider, err := provider.WithTableFunctions(fns...)
	if err != nil {
		return nil, err
	}
	if databaseProvider, ok := nextProvider.(sql.DatabaseProvider); ok {
		return WrapDatabaseProvider(databaseProvider).(sql.TableFunctionProvider), nil
	}
	return nextProvider, nil
}

func wrapDatabase(database sql.Database) sql.Database {
	if _, ok := database.(Database); ok {
		return database
	}
	if doltDatabase, ok := database.(sqle.Database); ok {
		return Database{Database: doltDatabase}
	}
	return database
}
