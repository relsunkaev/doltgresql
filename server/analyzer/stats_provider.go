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

package analyzer

import (
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	gmsanalyzer "github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

type branchStatsProvider interface {
	GetTableDoltStats(ctx *sql.Context, branch, db, schema, table string) ([]*stats.Statistic, error)
}

type schemaAwareStatsProvider struct {
	sql.StatsProvider
	branchStats branchStatsProvider
}

var _ sql.StatsProvider = (*schemaAwareStatsProvider)(nil)

func UseSchemaAwareStatsProvider(ctx *sql.Context, a *gmsanalyzer.Analyzer, node sql.Node, scope *plan.Scope, selector gmsanalyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	a.Catalog.StatsProvider = newSchemaAwareStatsProvider(a.Catalog.StatsProvider)
	return node, transform.SameTree, nil
}

func newSchemaAwareStatsProvider(provider sql.StatsProvider) sql.StatsProvider {
	if provider == nil {
		return nil
	}
	if _, ok := provider.(*schemaAwareStatsProvider); ok {
		return provider
	}
	branchStats, ok := provider.(branchStatsProvider)
	if !ok {
		return provider
	}
	return &schemaAwareStatsProvider{
		StatsProvider: provider,
		branchStats:   branchStats,
	}
}

func (p *schemaAwareStatsProvider) GetTableStats(ctx *sql.Context, db string, table sql.Table) ([]sql.Statistic, error) {
	tableStats, err := p.StatsProvider.GetTableStats(ctx, db, table)
	if err != nil || len(tableStats) > 0 {
		return schemaQualifyTableStats(tableStats, db, table), err
	}
	return p.getSchemaTableStats(ctx, db, table)
}

func (p *schemaAwareStatsProvider) GetStats(ctx *sql.Context, qual sql.StatQualifier, cols []string) (sql.Statistic, bool) {
	stat, ok := p.StatsProvider.GetStats(ctx, qual, cols)
	if ok {
		return schemaQualifyStatistic(stat, qual.Database, qual.Schema(), qual.Table()), true
	}
	tableStats, err := p.getSchemaTableStatsForName(ctx, qual.Database, qual.Schema(), qual.Table())
	if err != nil {
		return nil, false
	}
	for _, stat := range tableStats {
		if strings.EqualFold(stat.Qualifier().Index(), qual.Index()) {
			return schemaQualifyStatistic(stat, qual.Database, qual.Schema(), qual.Table()), true
		}
	}
	return nil, false
}

func (p *schemaAwareStatsProvider) RowCount(ctx *sql.Context, db string, table sql.Table) (uint64, error) {
	rowCount, err := p.StatsProvider.RowCount(ctx, db, table)
	if err != nil || rowCount > 0 {
		return rowCount, err
	}
	tableStats, err := p.getSchemaTableStats(ctx, db, table)
	if err != nil {
		return 0, err
	}
	for _, stat := range tableStats {
		if strings.EqualFold(stat.Qualifier().Index(), "primary") {
			return stat.RowCount(), nil
		}
	}
	return 0, nil
}

func (p *schemaAwareStatsProvider) DataLength(ctx *sql.Context, db string, table sql.Table) (uint64, error) {
	dataLength, err := p.StatsProvider.DataLength(ctx, db, table)
	if err != nil || dataLength > 0 {
		return dataLength, err
	}
	tableStats, err := p.getSchemaTableStats(ctx, db, table)
	if err != nil {
		return 0, err
	}
	for _, stat := range tableStats {
		if strings.EqualFold(stat.Qualifier().Index(), "primary") {
			return stat.RowCount(), nil
		}
	}
	return 0, nil
}

func (p *schemaAwareStatsProvider) GetTableDoltStats(ctx *sql.Context, branch, db, schema, table string) ([]*stats.Statistic, error) {
	return p.branchStats.GetTableDoltStats(ctx, branch, db, schema, table)
}

func (p *schemaAwareStatsProvider) getSchemaTableStats(ctx *sql.Context, db string, table sql.Table) ([]sql.Statistic, error) {
	schema := tableSchemaName(table)
	if schema == "" {
		return nil, nil
	}
	return p.getSchemaTableStatsForName(ctx, db, schema, table.Name())
}

func (p *schemaAwareStatsProvider) getSchemaTableStatsForName(ctx *sql.Context, db, schema, table string) ([]sql.Statistic, error) {
	if schema == "" || table == "" {
		return nil, nil
	}
	branch := currentStatsBranch(ctx)
	statsForTable, err := p.branchStats.GetTableDoltStats(ctx, branch, strings.ToLower(db), strings.ToLower(schema), strings.ToLower(table))
	if err != nil {
		return nil, err
	}
	ret := make([]sql.Statistic, 0, len(statsForTable))
	for _, stat := range statsForTable {
		ret = append(ret, schemaQualifyStatistic(stat, db, schema, table))
	}
	return ret, nil
}

func schemaQualifyStatistic(stat sql.Statistic, db, schema, table string) sql.Statistic {
	if stat == nil {
		return nil
	}
	if concrete, ok := stat.(*stats.Statistic); ok {
		ret := *concrete
		ret.Qual = sql.NewStatQualifier(db, schema, table, stat.Qualifier().Index())
		return &ret
	}
	return stat
}

func schemaQualifyTableStats(tableStats []sql.Statistic, db string, table sql.Table) []sql.Statistic {
	schema := tableSchemaName(table)
	if schema == "" {
		return tableStats
	}
	ret := make([]sql.Statistic, 0, len(tableStats))
	for _, stat := range tableStats {
		ret = append(ret, schemaQualifyStatistic(stat, db, schema, table.Name()))
	}
	return ret
}

func tableSchemaName(table sql.Table) string {
	schemaTable, ok := table.(sql.DatabaseSchemaTable)
	if !ok {
		return ""
	}
	databaseSchema := schemaTable.DatabaseSchema()
	if databaseSchema == nil {
		return ""
	}
	return strings.ToLower(databaseSchema.SchemaName())
}

func currentStatsBranch(ctx *sql.Context) string {
	if ctx == nil || ctx.Session == nil {
		return env.DefaultInitBranch
	}
	doltSession, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return env.DefaultInitBranch
	}
	branch, err := doltSession.GetBranch(ctx)
	if err != nil || branch == "" {
		return env.DefaultInitBranch
	}
	return strings.ToLower(branch)
}
