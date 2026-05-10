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
	"time"

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
	qualifierDb := tableStatisticQualifierDatabase(table)
	tableStats, err := p.StatsProvider.GetTableStats(ctx, db, table)
	if err != nil {
		tableStats = schemaQualifyTableStats(tableStats, qualifierDb, table)
		if !isProjectedStatsBuildError(err) {
			return tableStats, err
		}
		schemaStats, schemaErr := p.getSchemaTableStats(ctx, db, table)
		if schemaErr != nil {
			return tableStats, schemaErr
		}
		if len(schemaStats) > 0 {
			tableStats = schemaQualifyTableStats(schemaStats, qualifierDb, table)
		}
		return p.withSyntheticMissingIndexStats(ctx, qualifierDb, table, tableStats)
	}
	if len(tableStats) > 0 {
		return p.withSyntheticMissingIndexStats(ctx, qualifierDb, table, schemaQualifyTableStats(tableStats, qualifierDb, table))
	}
	tableStats, err = p.getSchemaTableStats(ctx, db, table)
	if err != nil || len(tableStats) > 0 {
		return p.withSyntheticMissingIndexStats(ctx, qualifierDb, table, schemaQualifyTableStats(tableStats, qualifierDb, table))
	}
	return p.withSyntheticMissingIndexStats(ctx, qualifierDb, table, nil)
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

func (p *schemaAwareStatsProvider) withSyntheticMissingIndexStats(ctx *sql.Context, db string, table sql.Table, tableStats []sql.Statistic) ([]sql.Statistic, error) {
	indexAddressable, ok := table.(sql.IndexAddressable)
	if !ok {
		return tableStats, nil
	}
	indexes, err := indexAddressable.GetIndexes(ctx)
	if err != nil || len(indexes) == 0 {
		return tableStats, err
	}
	db = strings.ToLower(db)
	schemaName := tableSchemaName(table)
	tableName := strings.ToLower(table.Name())
	existing := make(map[sql.StatQualifier]struct{}, len(tableStats))
	for _, stat := range tableStats {
		existing[stat.Qualifier()] = struct{}{}
	}
	missing := make([]sql.Index, 0)
	for _, index := range indexes {
		qualifier := sql.NewStatQualifier(db, schemaName, tableName, strings.ToLower(index.ID()))
		if _, ok := existing[qualifier]; !ok {
			missing = append(missing, index)
		}
	}
	if len(missing) == 0 {
		return tableStats, nil
	}
	rowCount, _ := p.StatsProvider.RowCount(ctx, db, table)
	if rowCount == 0 {
		if statsTable, ok := table.(sql.StatisticsTable); ok {
			if count, _, err := statsTable.RowCount(ctx); err != nil {
				return nil, err
			} else {
				rowCount = count
			}
		}
	}
	ret := append([]sql.Statistic(nil), tableStats...)
	for _, index := range missing {
		columns, types := indexStatisticColumns(ctx, index, tableName)
		var class sql.IndexClass = sql.IndexClassDefault
		if index.IsSpatial() {
			class = sql.IndexClassSpatial
		} else if index.IsFullText() {
			class = sql.IndexClassFulltext
		}
		qualifier := sql.NewStatQualifier(strings.ToLower(db), schemaName, tableName, strings.ToLower(index.ID()))
		stat := stats.NewStatistic(rowCount, rowCount, 0, 0, time.Now(), qualifier, columns, types, nil, class, nil)
		fds, idxCols := syntheticIndexFuncDeps(ctx, table, tableName, index)
		ret = append(ret, stat.WithFuncDeps(fds).WithColSet(idxCols))
	}
	return ret, nil
}

func syntheticIndexFuncDeps(ctx *sql.Context, table sql.Table, tableName string, index sql.Index) (*sql.FuncDepSet, sql.ColSet) {
	schema := tableSchemaForStats(ctx, table)
	schema = append(sql.Schema(nil), schema...)

	var idxCols sql.ColSet
	for _, columnType := range index.ColumnExpressionTypes(ctx) {
		columnName := indexStatisticColumnName(columnType.Expression, tableName)
		ordinal := schema.IndexOfColName(columnName)
		if ordinal < 0 {
			schema = append(schema, &sql.Column{
				Name:     columnName,
				Source:   tableName,
				Type:     columnType.Type,
				Nullable: true,
			})
			ordinal = len(schema) - 1
		}
		idxCols.Add(sql.ColumnId(ordinal + 1))
	}

	var all sql.ColSet
	var notNull sql.ColSet
	for i, column := range schema {
		columnID := sql.ColumnId(i + 1)
		all.Add(columnID)
		if !column.Nullable {
			notNull.Add(columnID)
		}
	}

	var strictKeys []sql.ColSet
	var laxKeys []sql.ColSet
	if index.IsUnique() && !idxCols.Empty() {
		strict := true
		for columnID, ok := idxCols.Next(1); ok; columnID, ok = idxCols.Next(columnID + 1) {
			if !notNull.Contains(columnID) {
				strict = false
				break
			}
		}
		if strict {
			strictKeys = append(strictKeys, idxCols)
		} else {
			laxKeys = append(laxKeys, idxCols)
		}
	}

	return sql.NewTablescanFDs(all, strictKeys, laxKeys, notNull), idxCols
}

func isProjectedStatsBuildError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "column not found on table during stats building")
}

func tableSchemaForStats(ctx *sql.Context, table sql.Table) sql.Schema {
	if primaryKeyTable, ok := table.(sql.PrimaryKeyTable); ok {
		return primaryKeyTable.PrimaryKeySchema(ctx).Schema
	}
	return table.Schema(ctx)
}

func indexStatisticColumns(ctx *sql.Context, index sql.Index, tableName string) ([]string, []sql.Type) {
	columnTypes := index.ColumnExpressionTypes(ctx)
	columns := make([]string, 0, len(columnTypes))
	types := make([]sql.Type, 0, len(columnTypes))
	for _, columnType := range columnTypes {
		columns = append(columns, indexStatisticColumnName(columnType.Expression, tableName))
		types = append(types, columnType.Type)
	}
	return columns, types
}

func indexStatisticColumnName(expression string, tableName string) string {
	return strings.TrimPrefix(strings.ToLower(expression), tableName+".")
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

func tableStatisticQualifierDatabase(table sql.Table) string {
	if databaseTable, ok := table.(sql.Databaseable); ok {
		return strings.ToLower(databaseTable.Database())
	}
	if databaser, ok := table.(sql.Databaser); ok {
		if database := databaser.Database(); database != nil {
			return strings.ToLower(database.Name())
		}
	}
	return ""
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
