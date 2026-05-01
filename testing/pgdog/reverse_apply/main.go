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
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type relation struct {
	schema  string
	table   string
	columns []string
}

type rowValues map[string]*string

func main() {
	sourceURL := flag.String("source-url", "", "Doltgres logical replication source URL without replication=database")
	targetURL := flag.String("target-url", "", "Postgres rollback target URL")
	slot := flag.String("slot", "dg_reverse_slot", "logical replication slot")
	publication := flag.String("publication", "dg_reverse_pub", "publication name")
	commits := flag.Int("commits", 1, "number of commits to apply before exiting")
	timeout := flag.Duration("timeout", 20*time.Second, "overall apply timeout")
	createSlotOnly := flag.Bool("create-slot-only", false, "create the slot and exit")
	flag.Parse()

	if *sourceURL == "" || *targetURL == "" {
		exitf("source-url and target-url are required")
	}
	if *commits < 1 && !*createSlotOnly {
		exitf("commits must be positive")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	replConn, err := pgconn.Connect(ctx, replicationURL(*sourceURL))
	if err != nil {
		exitf("connect replication source: %v", err)
	}
	defer replConn.Close(context.Background())

	if err := createSlotIfMissing(ctx, *sourceURL, replConn, *slot); err != nil {
		exitf("create slot: %v", err)
	}
	if *createSlotOnly {
		return
	}

	targetConn, err := pgx.Connect(ctx, *targetURL)
	if err != nil {
		exitf("connect target: %v", err)
	}
	defer targetConn.Close(context.Background())

	if err := pglogrepl.StartReplication(ctx, replConn, *slot, 0, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, *publication),
		},
	}); err != nil {
		exitf("start replication: %v", err)
	}
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: 0,
		WALFlushPosition: 0,
		WALApplyPosition: 0,
		ClientTime:       time.Now(),
		ReplyRequested:   true,
	}); err != nil {
		exitf("send initial standby status update: %v", err)
	}

	relations := map[uint32]relation{}
	appliedCommits := 0
	for appliedCommits < *commits {
		msg, err := replConn.ReceiveMessage(ctx)
		if err != nil {
			exitf("receive replication message: %v", err)
		}
		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 || copyData.Data[0] != pglogrepl.XLogDataByteID {
			continue
		}
		xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
		if err != nil {
			exitf("parse xlog data: %v", err)
		}
		logicalMsg, err := pglogrepl.ParseV2(xld.WALData, false)
		if err != nil {
			exitf("parse pgoutput message: %v", err)
		}

		switch typed := logicalMsg.(type) {
		case *pglogrepl.RelationMessageV2:
			relations[typed.RelationID] = relationFromMessage(typed)
		case *pglogrepl.InsertMessageV2:
			if err := applyInsert(ctx, targetConn, relations, typed.RelationID, typed.Tuple); err != nil {
				exitf("apply insert: %v", err)
			}
		case *pglogrepl.UpdateMessageV2:
			if err := applyUpdate(ctx, targetConn, relations, typed.RelationID, typed.NewTuple); err != nil {
				exitf("apply update: %v", err)
			}
		case *pglogrepl.DeleteMessageV2:
			if err := applyDelete(ctx, targetConn, relations, typed.RelationID, typed.OldTuple); err != nil {
				exitf("apply delete: %v", err)
			}
		case *pglogrepl.CommitMessage:
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: typed.CommitLSN,
				WALFlushPosition: typed.CommitLSN,
				WALApplyPosition: typed.CommitLSN,
				ClientTime:       time.Now(),
			}); err != nil {
				exitf("send standby status update: %v", err)
			}
			appliedCommits++
		}
	}
}

func replicationURL(url string) string {
	separator := "?"
	if strings.Contains(url, "?") {
		separator = "&"
	}
	return url + separator + "replication=database&application_name=dg-reverse-apply"
}

func createSlotIfMissing(ctx context.Context, sourceURL string, conn *pgconn.PgConn, slot string) error {
	exists, err := slotExists(ctx, sourceURL, slot)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return createSlot(ctx, conn, slot)
}

func slotExists(ctx context.Context, sourceURL string, slot string) (bool, error) {
	conn, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())

	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1)`, slot).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func createSlot(ctx context.Context, conn *pgconn.PgConn, slot string) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err == nil || strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func relationFromMessage(msg *pglogrepl.RelationMessageV2) relation {
	columns := make([]string, len(msg.Columns))
	for i, column := range msg.Columns {
		columns[i] = column.Name
	}
	return relation{
		schema:  msg.Namespace,
		table:   msg.RelationName,
		columns: columns,
	}
}

func valuesFor(rel relation, tuple *pglogrepl.TupleData) (rowValues, error) {
	if tuple == nil {
		return nil, errors.New("missing tuple")
	}
	if len(tuple.Columns) > len(rel.columns) {
		return nil, fmt.Errorf("column count mismatch for %s.%s: relation=%d tuple=%d", rel.schema, rel.table, len(rel.columns), len(tuple.Columns))
	}

	values := rowValues{}
	for i, column := range tuple.Columns {
		name := rel.columns[i]
		switch column.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[name] = nil
		case pglogrepl.TupleDataTypeText:
			value := string(column.Data)
			values[name] = &value
		case pglogrepl.TupleDataTypeToast:
			return nil, fmt.Errorf("unchanged TOAST value is not supported by reverse apply helper for column %s", name)
		default:
			return nil, fmt.Errorf("unsupported tuple data type %q for column %s", column.DataType, name)
		}
	}
	return values, nil
}

func applyInsert(ctx context.Context, conn *pgx.Conn, relations map[uint32]relation, relationID uint32, tuple *pglogrepl.TupleData) error {
	rel, values, err := loadCustomerOrderValues(relations, relationID, tuple)
	if err != nil {
		return err
	}
	if rel.table != "customer_orders" || rel.schema != "public" {
		return fmt.Errorf("unexpected relation %s.%s", rel.schema, rel.table)
	}
	_, err = conn.Exec(ctx, `INSERT INTO customer_orders (customer_id, order_id, status, amount, note)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (customer_id, order_id) DO UPDATE
		SET status = EXCLUDED.status, amount = EXCLUDED.amount, note = EXCLUDED.note`,
		values.customerID, values.orderID, values.status, values.amount, values.note)
	return err
}

func applyUpdate(ctx context.Context, conn *pgx.Conn, relations map[uint32]relation, relationID uint32, tuple *pglogrepl.TupleData) error {
	rel, values, err := loadCustomerOrderValues(relations, relationID, tuple)
	if err != nil {
		return err
	}
	if rel.table != "customer_orders" || rel.schema != "public" {
		return fmt.Errorf("unexpected relation %s.%s", rel.schema, rel.table)
	}
	_, err = conn.Exec(ctx, `UPDATE customer_orders
		SET status = $3, amount = $4, note = $5
		WHERE customer_id = $1 AND order_id = $2`,
		values.customerID, values.orderID, values.status, values.amount, values.note)
	return err
}

func applyDelete(ctx context.Context, conn *pgx.Conn, relations map[uint32]relation, relationID uint32, tuple *pglogrepl.TupleData) error {
	rel, values, err := loadCustomerOrderKeyValues(relations, relationID, tuple)
	if err != nil {
		return err
	}
	if rel.table != "customer_orders" || rel.schema != "public" {
		return fmt.Errorf("unexpected relation %s.%s", rel.schema, rel.table)
	}
	_, err = conn.Exec(ctx, `DELETE FROM customer_orders WHERE customer_id = $1 AND order_id = $2`, values.customerID, values.orderID)
	return err
}

type customerOrderValues struct {
	customerID int64
	orderID    int64
	status     string
	amount     int64
	note       *string
}

type customerOrderKeyValues struct {
	customerID int64
	orderID    int64
}

func loadCustomerOrderValues(relations map[uint32]relation, relationID uint32, tuple *pglogrepl.TupleData) (relation, customerOrderValues, error) {
	rel, ok := relations[relationID]
	if !ok {
		return relation{}, customerOrderValues{}, fmt.Errorf("unknown relation ID %d", relationID)
	}
	values, err := valuesFor(rel, tuple)
	if err != nil {
		return relation{}, customerOrderValues{}, err
	}

	customerID, err := intValue(values, "customer_id")
	if err != nil {
		return relation{}, customerOrderValues{}, err
	}
	orderID, err := intValue(values, "order_id")
	if err != nil {
		return relation{}, customerOrderValues{}, err
	}
	amount, err := intValue(values, "amount")
	if err != nil {
		return relation{}, customerOrderValues{}, err
	}
	status, err := stringValue(values, "status")
	if err != nil {
		return relation{}, customerOrderValues{}, err
	}

	return rel, customerOrderValues{
		customerID: customerID,
		orderID:    orderID,
		status:     status,
		amount:     amount,
		note:       values["note"],
	}, nil
}

func loadCustomerOrderKeyValues(relations map[uint32]relation, relationID uint32, tuple *pglogrepl.TupleData) (relation, customerOrderKeyValues, error) {
	rel, ok := relations[relationID]
	if !ok {
		return relation{}, customerOrderKeyValues{}, fmt.Errorf("unknown relation ID %d", relationID)
	}
	values, err := valuesFor(rel, tuple)
	if err != nil {
		return relation{}, customerOrderKeyValues{}, err
	}

	customerID, err := intValue(values, "customer_id")
	if err != nil {
		return relation{}, customerOrderKeyValues{}, err
	}
	orderID, err := intValue(values, "order_id")
	if err != nil {
		return relation{}, customerOrderKeyValues{}, err
	}

	return rel, customerOrderKeyValues{
		customerID: customerID,
		orderID:    orderID,
	}, nil
}

func intValue(values rowValues, name string) (int64, error) {
	value, ok := values[name]
	if !ok || value == nil {
		return 0, fmt.Errorf("missing non-null integer column %s", name)
	}
	parsed, err := strconv.ParseInt(*value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return parsed, nil
}

func stringValue(values rowValues, name string) (string, error) {
	value, ok := values[name]
	if !ok || value == nil {
		return "", fmt.Errorf("missing non-null text column %s", name)
	}
	return *value, nil
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
