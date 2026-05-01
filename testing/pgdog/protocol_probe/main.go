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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	databaseURL := flag.String("database-url", "", "PgDog PostgreSQL URL")
	customerID := flag.Int64("customer-id", 42, "customer shard key")
	baseOrderID := flag.Int64("base-order-id", 30, "first order ID written by the probe")
	timeout := flag.Duration("timeout", 20*time.Second, "overall probe timeout")
	flag.Parse()

	if *databaseURL == "" {
		exitf("database-url is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, *databaseURL)
	if err != nil {
		exitf("connect PgDog: %v", err)
	}
	defer conn.Close(context.Background())

	if _, err = conn.Prepare(ctx, "dg_extended_insert", `
		INSERT INTO customer_orders (customer_id, order_id, status, amount, note)
		VALUES ($1, $2, $3, $4, $5)`); err != nil {
		exitf("prepare insert: %v", err)
	}
	if _, err = conn.Prepare(ctx, "dg_extended_select", `
		SELECT status, amount, note
		FROM customer_orders
		WHERE customer_id = $1 AND order_id = $2`); err != nil {
		exitf("prepare select: %v", err)
	}

	if _, err = conn.Exec(ctx, "dg_extended_insert", *customerID, *baseOrderID, "extended-insert", int64(310), "extended protocol insert"); err != nil {
		exitf("execute prepared insert: %v", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		exitf("begin transaction: %v", err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE customer_orders
		SET status = $3, amount = $4, note = $5
		WHERE customer_id = $1 AND order_id = $2`,
		*customerID, *baseOrderID, "extended-updated", int64(311), "extended protocol update"); err != nil {
		_ = tx.Rollback(ctx)
		exitf("transaction update: %v", err)
	}
	if _, err = tx.Exec(ctx, "dg_extended_insert", *customerID, *baseOrderID+1, "extended-transaction", int64(320), "extended protocol transaction"); err != nil {
		_ = tx.Rollback(ctx)
		exitf("transaction prepared insert: %v", err)
	}
	if err = tx.Commit(ctx); err != nil {
		exitf("commit transaction: %v", err)
	}

	assertRow(ctx, conn, *customerID, *baseOrderID, "extended-updated", 311, "extended protocol update")
	assertRow(ctx, conn, *customerID, *baseOrderID+1, "extended-transaction", 320, "extended protocol transaction")
}

func assertRow(ctx context.Context, conn *pgx.Conn, customerID int64, orderID int64, expectedStatus string, expectedAmount int64, expectedNote string) {
	var status string
	var amount int64
	var note string
	if err := conn.QueryRow(ctx, "dg_extended_select", customerID, orderID).Scan(&status, &amount, &note); err != nil {
		exitf("select order %d: %v", orderID, err)
	}
	if status != expectedStatus || amount != expectedAmount || note != expectedNote {
		exitf("unexpected order %d: status=%q amount=%d note=%q", orderID, status, amount, note)
	}
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
