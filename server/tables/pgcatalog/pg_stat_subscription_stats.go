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

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/subscriptions"
	"github.com/dolthub/doltgresql/server/tables"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PgStatSubscriptionStatsName is a constant to the pg_stat_subscription_stats name.
const PgStatSubscriptionStatsName = "pg_stat_subscription_stats"

// InitPgStatSubscriptionStats handles registration of the pg_stat_subscription_stats handler.
func InitPgStatSubscriptionStats() {
	tables.AddHandler(PgCatalogName, PgStatSubscriptionStatsName, PgStatSubscriptionStatsHandler{})
}

// PgStatSubscriptionStatsHandler is the handler for the pg_stat_subscription_stats table.
type PgStatSubscriptionStatsHandler struct{}

var _ tables.Handler = PgStatSubscriptionStatsHandler{}

// Name implements the interface tables.Handler.
func (p PgStatSubscriptionStatsHandler) Name() string {
	return PgStatSubscriptionStatsName
}

// RowIter implements the interface tables.Handler.
func (p PgStatSubscriptionStatsHandler) RowIter(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	collection, err := core.GetSubscriptionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var subs []subscriptions.Subscription
	err = collection.IterateSubscriptions(ctx, func(sub subscriptions.Subscription) (stop bool, err error) {
		subs = append(subs, sub)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return &pgStatSubscriptionStatsRowIter{subscriptions: subs}, nil
}

// Schema implements the interface tables.Handler.
func (p PgStatSubscriptionStatsHandler) PkSchema() sql.PrimaryKeySchema {
	return sql.PrimaryKeySchema{
		Schema:     pgStatSubscriptionStatsSchema,
		PkOrdinals: nil,
	}
}

// pgStatSubscriptionStatsSchema is the schema for pg_stat_subscription_stats.
var pgStatSubscriptionStatsSchema = sql.Schema{
	{Name: "subid", Type: pgtypes.Oid, Default: nil, Nullable: true, Source: PgStatSubscriptionStatsName},
	{Name: "subname", Type: pgtypes.Name, Default: nil, Nullable: true, Source: PgStatSubscriptionStatsName},
	{Name: "apply_error_count", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgStatSubscriptionStatsName},
	{Name: "sync_error_count", Type: pgtypes.Int32, Default: nil, Nullable: true, Source: PgStatSubscriptionStatsName},
	{Name: "stats_reset", Type: pgtypes.TimestampTZ, Default: nil, Nullable: true, Source: PgStatSubscriptionStatsName},
}

// pgStatSubscriptionStatsRowIter is the sql.RowIter for the pg_stat_subscription_stats table.
type pgStatSubscriptionStatsRowIter struct {
	subscriptions []subscriptions.Subscription
	idx           int
}

var _ sql.RowIter = (*pgStatSubscriptionStatsRowIter)(nil)

// Next implements the interface sql.RowIter.
func (iter *pgStatSubscriptionStatsRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if iter.idx >= len(iter.subscriptions) {
		return nil, io.EOF
	}
	sub := iter.subscriptions[iter.idx]
	iter.idx++
	return sql.Row{
		sub.ID.AsId(),
		sub.ID.SubscriptionName(),
		int32(0),
		int32(0),
		nil,
	}, nil
}

// Close implements the interface sql.RowIter.
func (iter *pgStatSubscriptionStatsRowIter) Close(ctx *sql.Context) error {
	return nil
}
