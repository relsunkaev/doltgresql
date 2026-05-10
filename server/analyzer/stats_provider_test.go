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
	"errors"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/require"
)

func TestSchemaAwareStatsProviderUsesSchemaQualifiedDoltStats(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := statsProviderTestTable{}
	secondary := stats.NewStatistic(
		6,
		3,
		0,
		0,
		time.Now(),
		sql.NewStatQualifier("postgres", "", "stats_provider_costing", "stats_provider_costing_tenant_name_idx"),
		[]string{"tenant", "name"},
		[]sql.Type{types.Int64, types.Text},
		nil,
		sql.IndexClassDefault,
		nil,
	)
	primary := stats.NewStatistic(
		6,
		6,
		0,
		0,
		time.Now(),
		sql.NewStatQualifier("postgres", "", "stats_provider_costing", "primary"),
		[]string{"id"},
		[]sql.Type{types.Int64},
		nil,
		sql.IndexClassDefault,
		nil,
	)
	base := &fakeBranchStatsProvider{stats: []*stats.Statistic{primary, secondary}}

	provider := newSchemaAwareStatsProvider(base)

	tableStats, err := provider.GetTableStats(ctx, "postgres", table)
	require.NoError(t, err)
	require.Len(t, tableStats, 2)
	require.Equal(t, "public", tableStats[0].Qualifier().Schema())
	require.Equal(t, "public", base.lastSchema)

	qual := sql.NewStatQualifier("postgres", "public", "stats_provider_costing", "stats_provider_costing_tenant_name_idx")
	indexStats, ok := provider.GetStats(ctx, qual, []string{"tenant", "name"})
	require.True(t, ok)
	require.Equal(t, uint64(3), indexStats.DistinctCount())
	require.Equal(t, qual, indexStats.Qualifier())

	rowCount, err := provider.RowCount(ctx, "postgres", table)
	require.NoError(t, err)
	require.Equal(t, uint64(6), rowCount)

	dataLength, err := provider.DataLength(ctx, "postgres", table)
	require.NoError(t, err)
	require.Equal(t, uint64(6), dataLength)
}

func TestSchemaAwareStatsProviderFallsBackForProjectedIndexStats(t *testing.T) {
	ctx := sql.NewEmptyContext()
	table := projectedStatsProviderTestTable{
		schema: sql.Schema{
			{Name: "name", Source: "django_account", Type: types.Text, Nullable: false},
		},
		indexes: []sql.Index{
			statsProviderTestIndex{id: "PRIMARY", expr: "id", typ: types.Int64, unique: true},
			statsProviderTestIndex{id: "app_account_name_key", expr: "name", typ: types.Text, unique: true},
		},
	}
	base := &fakeBranchStatsProvider{
		tableStatsErr: errors.New("column not found on table during stats building: id"),
		rowCount:      3,
	}
	provider := newSchemaAwareStatsProvider(base)

	tableStats, err := provider.GetTableStats(ctx, "postgres", table)
	require.NoError(t, err)
	require.Len(t, tableStats, 2)

	byIndex := make(map[string]sql.Statistic)
	for _, stat := range tableStats {
		byIndex[stat.Qualifier().Index()] = stat
		require.Empty(t, stat.Qualifier().Database)
		require.Equal(t, "public", stat.Qualifier().Schema())
		require.Equal(t, "django_account", stat.Qualifier().Table())
		require.NotNil(t, stat.FuncDeps())
		require.False(t, stat.ColSet().Empty())
		require.Equal(t, uint64(3), stat.RowCount())
	}
	require.Equal(t, []string{"id"}, byIndex["primary"].Columns())
	require.Equal(t, []string{"name"}, byIndex["app_account_name_key"].Columns())
}

type fakeBranchStatsProvider struct {
	stats         []*stats.Statistic
	tableStatsErr error
	rowCount      uint64
	lastSchema    string
}

var _ sql.StatsProvider = (*fakeBranchStatsProvider)(nil)

func (p *fakeBranchStatsProvider) GetTableDoltStats(_ *sql.Context, _, _, schema, _ string) ([]*stats.Statistic, error) {
	p.lastSchema = schema
	return p.stats, nil
}

func (p *fakeBranchStatsProvider) GetTableStats(*sql.Context, string, sql.Table) ([]sql.Statistic, error) {
	return nil, p.tableStatsErr
}

func (p *fakeBranchStatsProvider) AnalyzeTable(*sql.Context, sql.Table, string) error {
	return nil
}

func (p *fakeBranchStatsProvider) SetStats(*sql.Context, sql.Statistic) error {
	return nil
}

func (p *fakeBranchStatsProvider) GetStats(*sql.Context, sql.StatQualifier, []string) (sql.Statistic, bool) {
	return nil, false
}

func (p *fakeBranchStatsProvider) DropStats(*sql.Context, sql.StatQualifier, []string) error {
	return nil
}

func (p *fakeBranchStatsProvider) DropDbStats(*sql.Context, string, bool) error {
	return nil
}

func (p *fakeBranchStatsProvider) RowCount(*sql.Context, string, sql.Table) (uint64, error) {
	return p.rowCount, nil
}

func (p *fakeBranchStatsProvider) DataLength(*sql.Context, string, sql.Table) (uint64, error) {
	return 0, nil
}

type statsProviderTestTable struct{}

var _ sql.Table = statsProviderTestTable{}
var _ sql.DatabaseSchemaTable = statsProviderTestTable{}

func (statsProviderTestTable) Name() string {
	return "stats_provider_costing"
}

func (statsProviderTestTable) String() string {
	return "stats_provider_costing"
}

func (statsProviderTestTable) Database() string {
	return "postgres"
}

func (statsProviderTestTable) Schema(*sql.Context) sql.Schema {
	return nil
}

func (statsProviderTestTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (statsProviderTestTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return nil, nil
}

func (statsProviderTestTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, nil
}

func (statsProviderTestTable) DatabaseSchema() sql.DatabaseSchema {
	return statsProviderTestSchema{}
}

type statsProviderTestSchema struct{}

var _ sql.DatabaseSchema = statsProviderTestSchema{}

func (statsProviderTestSchema) Name() string {
	return "postgres"
}

func (statsProviderTestSchema) SchemaName() string {
	return "public"
}

func (statsProviderTestSchema) GetTableInsensitive(*sql.Context, string) (sql.Table, bool, error) {
	return nil, false, nil
}

func (statsProviderTestSchema) GetTableNames(*sql.Context) ([]string, error) {
	return nil, nil
}

type projectedStatsProviderTestTable struct {
	schema  sql.Schema
	indexes []sql.Index
}

var _ sql.Table = projectedStatsProviderTestTable{}
var _ sql.DatabaseSchemaTable = projectedStatsProviderTestTable{}
var _ sql.IndexAddressable = projectedStatsProviderTestTable{}

func (projectedStatsProviderTestTable) Name() string {
	return "django_account"
}

func (projectedStatsProviderTestTable) String() string {
	return "django_account"
}

func (t projectedStatsProviderTestTable) Schema(*sql.Context) sql.Schema {
	return t.schema
}

func (projectedStatsProviderTestTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (projectedStatsProviderTestTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return nil, nil
}

func (projectedStatsProviderTestTable) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	return nil, nil
}

func (projectedStatsProviderTestTable) DatabaseSchema() sql.DatabaseSchema {
	return statsProviderTestSchema{}
}

func (t projectedStatsProviderTestTable) GetIndexes(*sql.Context) ([]sql.Index, error) {
	return t.indexes, nil
}

func (projectedStatsProviderTestTable) IndexedAccess(*sql.Context, sql.IndexLookup) sql.IndexedTable {
	return nil
}

func (projectedStatsProviderTestTable) PreciseMatch() bool {
	return false
}

type statsProviderTestIndex struct {
	id     string
	expr   string
	typ    sql.Type
	unique bool
}

var _ sql.Index = statsProviderTestIndex{}

func (i statsProviderTestIndex) ID() string {
	return i.id
}

func (statsProviderTestIndex) Database() string {
	return "postgres"
}

func (statsProviderTestIndex) Table() string {
	return "django_account"
}

func (i statsProviderTestIndex) Expressions() []string {
	return []string{i.expr}
}

func (i statsProviderTestIndex) IsUnique() bool {
	return i.unique
}

func (statsProviderTestIndex) IsSpatial() bool {
	return false
}

func (statsProviderTestIndex) IsFullText() bool {
	return false
}

func (statsProviderTestIndex) IsVector() bool {
	return false
}

func (statsProviderTestIndex) Comment() string {
	return ""
}

func (statsProviderTestIndex) IndexType() string {
	return "BTREE"
}

func (statsProviderTestIndex) IsGenerated() bool {
	return false
}

func (i statsProviderTestIndex) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
	return []sql.ColumnExpressionType{{Expression: i.expr, Type: i.typ}}
}

func (statsProviderTestIndex) CanSupport(*sql.Context, ...sql.Range) bool {
	return true
}

func (statsProviderTestIndex) CanSupportOrderBy(sql.Expression) bool {
	return false
}

func (statsProviderTestIndex) PrefixLengths() []uint16 {
	return nil
}
