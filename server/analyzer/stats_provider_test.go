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

type fakeBranchStatsProvider struct {
	stats      []*stats.Statistic
	lastSchema string
}

var _ sql.StatsProvider = (*fakeBranchStatsProvider)(nil)

func (p *fakeBranchStatsProvider) GetTableDoltStats(_ *sql.Context, _, _, schema, _ string) ([]*stats.Statistic, error) {
	p.lastSchema = schema
	return p.stats, nil
}

func (p *fakeBranchStatsProvider) GetTableStats(*sql.Context, string, sql.Table) ([]sql.Statistic, error) {
	return nil, nil
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
	return 0, nil
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
