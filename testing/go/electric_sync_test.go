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

package _go

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	electricSmokeEnv            = "DOLTGRES_ELECTRIC_SMOKE"
	defaultSupportedElectric    = "electricsql/electric:1.6.2"
	electricDefaultSlotName     = "electric_slot_default"
	electricShapeStorageInMem   = ":memory:"
	electricShapeStorageMounted = "/electric-shapes"
)

func TestElectricSyncSmoke(t *testing.T) {
	runForEachElectricImage(t, func(t *testing.T, image string) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbDir := t.TempDir()
		port, err := sql.GetEmptyPort()
		require.NoError(t, err)
		electricPort, err := sql.GetEmptyPort()
		require.NoError(t, err)

		serverCtx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		defer func() {
			conn.Close(serverCtx)
			controller.Stop()
			require.NoError(t, controller.WaitForStop())
		}()

		const tableName = "electric_smoke_items"
		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, label TEXT NOT NULL);", tableName))
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, "CREATE PUBLICATION electric_publication_default;")
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("ALTER PUBLICATION electric_publication_default ADD TABLE %s;", tableName))
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", tableName))
		require.NoError(t, err)

		containerName := fmt.Sprintf("dg-electric-smoke-%d", time.Now().UnixNano())
		startElectricContainer(t, ctx, electricContainerConfig{
			name:         containerName,
			image:        image,
			doltgresPort: port,
			electricPort: electricPort,
		})

		baseURL := fmt.Sprintf("http://127.0.0.1:%d", electricPort)
		shape := waitForElectricShapeUpToDate(t, ctx, baseURL, tableName)
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)

		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'one');", tableName))
		require.NoError(t, err)
		shape = waitForElectricOperations(t, ctx, baseURL, tableName, shape, electricExpectedOperation{operation: "insert", id: "1"})

		conn.Close(serverCtx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())

		serverCtx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)
		shape = waitForElectricShapeUpToDate(t, ctx, baseURL, tableName)

		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("UPDATE %s SET label = 'one-updated' WHERE id = 1;", tableName))
		require.NoError(t, err)
		shape = waitForElectricOperations(t, ctx, baseURL, tableName, shape, electricExpectedOperation{operation: "update", id: "1"})

		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("DELETE FROM %s WHERE id = 1;", tableName))
		require.NoError(t, err)
		_ = waitForElectricOperations(t, ctx, baseURL, tableName, shape, electricExpectedOperation{operation: "delete", id: "1"})
	})
}

func TestElectricMultiShapeCatchupAndSchemaChange(t *testing.T) {
	runForEachElectricImage(t, func(t *testing.T, image string) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		dbDir := t.TempDir()
		shapeDir := t.TempDir()
		port, err := sql.GetEmptyPort()
		require.NoError(t, err)
		electricPort, err := sql.GetEmptyPort()
		require.NoError(t, err)
		baseURL := fmt.Sprintf("http://127.0.0.1:%d", electricPort)

		serverCtx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		defer func() {
			conn.Close(serverCtx)
			controller.Stop()
			require.NoError(t, controller.WaitForStop())
		}()

		_, err = conn.Current.Exec(serverCtx, `
			CREATE TABLE electric_accounts (id INT PRIMARY KEY, name TEXT NOT NULL);
			CREATE TABLE electric_tasks (id INT PRIMARY KEY, account_id INT NOT NULL, status TEXT NOT NULL);
			CREATE PUBLICATION electric_publication_default;
			ALTER PUBLICATION electric_publication_default ADD TABLE electric_accounts;
			ALTER PUBLICATION electric_publication_default ADD TABLE electric_tasks;
			ALTER TABLE electric_accounts REPLICA IDENTITY FULL;
			ALTER TABLE electric_tasks REPLICA IDENTITY FULL;`)
		require.NoError(t, err)

		containerName := fmt.Sprintf("dg-electric-multi-%d", time.Now().UnixNano())
		startElectricContainer(t, ctx, electricContainerConfig{
			name:            containerName,
			image:           image,
			doltgresPort:    port,
			electricPort:    electricPort,
			shapeStorageDir: shapeDir,
		})

		accounts := waitForElectricShapeUpToDate(t, ctx, baseURL, "electric_accounts")
		tasks := waitForElectricShapeUpToDate(t, ctx, baseURL, "electric_tasks")
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)

		_, err = conn.Current.Exec(serverCtx, "INSERT INTO electric_accounts VALUES (1, 'primary-account');")
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, "INSERT INTO electric_tasks VALUES (101, 1, 'open');")
		require.NoError(t, err)
		accounts = waitForElectricOperations(t, ctx, baseURL, "electric_accounts", accounts, electricExpectedOperation{operation: "insert", id: "1"})
		tasks = waitForElectricOperations(t, ctx, baseURL, "electric_tasks", tasks, electricExpectedOperation{operation: "insert", id: "101"})
		preRestartFlush := waitForReplicationSlotConfirmedFlushGreaterThan(t, serverCtx, conn, 0)

		removeElectricContainer(t, containerName)
		waitForReplicationSlotActive(t, serverCtx, conn, false)

		writeElectricBacklogConcurrently(t, serverCtx, port)

		conn.Close(serverCtx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())

		serverCtx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, false)

		startElectricContainer(t, ctx, electricContainerConfig{
			name:            containerName,
			image:           image,
			doltgresPort:    port,
			electricPort:    electricPort,
			shapeStorageDir: shapeDir,
		})
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)
		postRestartFlush := waitForReplicationSlotConfirmedFlushGreaterThan(t, serverCtx, conn, preRestartFlush)
		t.Logf("Electric advanced confirmed_flush_lsn from %s to %s after reconnect", preRestartFlush, postRestartFlush)

		accountRows := waitForElectricRows(t, ctx, baseURL, "electric_accounts", func(rows map[string]map[string]string) bool {
			return rows["1"]["name"] == "primary-account-offline-update" &&
				rows["2"]["name"] == "worker-0-row-0" &&
				rows["21"]["name"] == "worker-3-row-4"
		})
		require.Len(t, accountRows, 21)
		taskRows := waitForElectricRows(t, ctx, baseURL, "electric_tasks", func(rows map[string]map[string]string) bool {
			_, hasDeletedTask := rows["101"]
			return !hasDeletedTask &&
				rows["200"]["status"] == "queued-200" &&
				rows["203"]["status"] == "queued-203"
		})
		require.Len(t, taskRows, 4)

		_, err = conn.Current.Exec(serverCtx, "ALTER TABLE electric_accounts ADD COLUMN tier TEXT NOT NULL DEFAULT 'standard';")
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, "UPDATE electric_accounts SET tier = 'vip' WHERE id = 1;")
		require.NoError(t, err)

		accountRows = waitForElectricRows(t, ctx, baseURL, "electric_accounts", func(rows map[string]map[string]string) bool {
			return rows["1"]["tier"] == "vip" && rows["21"]["name"] == "worker-3-row-4"
		})
		require.Equal(t, "vip", accountRows["1"]["tier"])
		require.Equal(t, "worker-3-row-4", accountRows["21"]["name"])
	})
}

func TestElectricQualifiedSchemaTablePublication(t *testing.T) {
	runForEachElectricImage(t, func(t *testing.T, image string) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbDir := t.TempDir()
		port, err := sql.GetEmptyPort()
		require.NoError(t, err)
		electricPort, err := sql.GetEmptyPort()
		require.NoError(t, err)
		baseURL := fmt.Sprintf("http://127.0.0.1:%d", electricPort)

		serverCtx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		defer func() {
			conn.Close(serverCtx)
			controller.Stop()
			require.NoError(t, controller.WaitForStop())
		}()

		const qualifiedTable = "electric_schema_pub.electric_schema_items"
		_, err = conn.Current.Exec(serverCtx, `
			CREATE SCHEMA electric_schema_pub;
			CREATE TABLE electric_schema_pub.electric_schema_items (id INT PRIMARY KEY, label TEXT NOT NULL);
			CREATE TABLE electric_schema_outside_items (id INT PRIMARY KEY, label TEXT NOT NULL);
			CREATE PUBLICATION electric_publication_default;
			ALTER PUBLICATION electric_publication_default ADD TABLE electric_schema_pub.electric_schema_items;
			ALTER TABLE electric_schema_pub.electric_schema_items REPLICA IDENTITY FULL;`)
		require.NoError(t, err)

		containerName := fmt.Sprintf("dg-electric-schema-pub-%d", time.Now().UnixNano())
		startElectricContainer(t, ctx, electricContainerConfig{
			name:         containerName,
			image:        image,
			doltgresPort: port,
			electricPort: electricPort,
		})

		shape := waitForElectricShapeUpToDate(t, ctx, baseURL, qualifiedTable)
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)

		_, err = conn.Current.Exec(serverCtx, "INSERT INTO electric_schema_outside_items VALUES (1, 'outside');")
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, "INSERT INTO electric_schema_pub.electric_schema_items VALUES (2, 'schema');")
		require.NoError(t, err)
		shape = waitForElectricOperations(t, ctx, baseURL, qualifiedTable, shape, electricExpectedOperation{operation: "insert", id: "2"})
		_, err = conn.Current.Exec(serverCtx, "UPDATE electric_schema_pub.electric_schema_items SET label = 'schema-updated' WHERE id = 2;")
		require.NoError(t, err)
		_ = waitForElectricOperations(t, ctx, baseURL, qualifiedTable, shape, electricExpectedOperation{operation: "update", id: "2"})
	})
}

func TestElectricDropColumnShapeRefetch(t *testing.T) {
	runForEachElectricImage(t, func(t *testing.T, image string) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbDir := t.TempDir()
		port, err := sql.GetEmptyPort()
		require.NoError(t, err)
		electricPort, err := sql.GetEmptyPort()
		require.NoError(t, err)
		baseURL := fmt.Sprintf("http://127.0.0.1:%d", electricPort)

		serverCtx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		defer func() {
			conn.Close(serverCtx)
			controller.Stop()
			require.NoError(t, controller.WaitForStop())
		}()

		const tableName = "electric_drop_column_items"
		_, err = conn.Current.Exec(serverCtx, `
			CREATE TABLE electric_drop_column_items (id INT PRIMARY KEY, label TEXT NOT NULL, obsolete TEXT NOT NULL);
			CREATE PUBLICATION electric_publication_default;
			ALTER PUBLICATION electric_publication_default ADD TABLE electric_drop_column_items;
			ALTER TABLE electric_drop_column_items REPLICA IDENTITY FULL;`)
		require.NoError(t, err)

		containerName := fmt.Sprintf("dg-electric-drop-column-%d", time.Now().UnixNano())
		startElectricContainer(t, ctx, electricContainerConfig{
			name:         containerName,
			image:        image,
			doltgresPort: port,
			electricPort: electricPort,
		})

		shape := waitForElectricShapeUpToDate(t, ctx, baseURL, tableName)
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)

		_, err = conn.Current.Exec(serverCtx, "INSERT INTO electric_drop_column_items VALUES (1, 'one', 'legacy');")
		require.NoError(t, err)
		_ = waitForElectricOperations(t, ctx, baseURL, tableName, shape, electricExpectedOperation{operation: "insert", id: "1"})

		_, err = conn.Current.Exec(serverCtx, "ALTER TABLE electric_drop_column_items DROP COLUMN obsolete;")
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, "UPDATE electric_drop_column_items SET label = 'after-drop' WHERE id = 1;")
		require.NoError(t, err)

		rows := waitForElectricRows(t, ctx, baseURL, tableName, func(rows map[string]map[string]string) bool {
			row := rows["1"]
			if row["label"] != "after-drop" {
				return false
			}
			_, hasObsolete := row["obsolete"]
			return !hasObsolete
		})
		require.Equal(t, "after-drop", rows["1"]["label"])
		require.NotContains(t, rows["1"], "obsolete")
	})
}

func TestElectricCompatibilitySoak(t *testing.T) {
	runForEachElectricImage(t, func(t *testing.T, image string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		dbDir := t.TempDir()
		port, err := sql.GetEmptyPort()
		require.NoError(t, err)
		electricPort, err := sql.GetEmptyPort()
		require.NoError(t, err)
		baseURL := fmt.Sprintf("http://127.0.0.1:%d", electricPort)

		serverCtx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		defer func() {
			conn.Close(serverCtx)
			controller.Stop()
			require.NoError(t, controller.WaitForStop())
		}()

		_, err = conn.Current.Exec(serverCtx, `
			CREATE TABLE electric_soak_items (id INT PRIMARY KEY, label TEXT NOT NULL);
			CREATE PUBLICATION electric_publication_default;
			ALTER PUBLICATION electric_publication_default ADD TABLE electric_soak_items;
			ALTER TABLE electric_soak_items REPLICA IDENTITY FULL;`)
		require.NoError(t, err)

		containerName := fmt.Sprintf("dg-electric-soak-%d", time.Now().UnixNano())
		startElectricContainer(t, ctx, electricContainerConfig{
			name:         containerName,
			image:        image,
			doltgresPort: port,
			electricPort: electricPort,
		})

		shape := waitForElectricShapeUpToDate(t, ctx, baseURL, "electric_soak_items")
		waitForReplicationSlot(t, serverCtx, conn)
		waitForReplicationSlotActive(t, serverCtx, conn, true)

		readerCtx, stopReader := context.WithCancel(ctx)
		var shapeReads atomic.Int64
		go func() {
			readerState := shape
			for {
				select {
				case <-readerCtx.Done():
					return
				default:
				}
				next, messages, status, body, err := requestElectricShape(readerCtx, baseURL, "electric_soak_items", readerState, true)
				if err == nil && (status == http.StatusOK || status == http.StatusNoContent) {
					readerState = next
					shapeReads.Add(1)
				}
				if err == nil && status == http.StatusConflict && electricResponseMustRefetch(messages, body) {
					readerState = electricShapeState{Offset: "-1"}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()

		start := time.Now()
		for i := 1; i <= 200; i++ {
			_, err = conn.Current.Exec(serverCtx, "INSERT INTO electric_soak_items VALUES ($1, $2);", int32(i), fmt.Sprintf("item-%03d", i))
			require.NoError(t, err)
		}
		for i := 1; i <= 200; i += 2 {
			_, err = conn.Current.Exec(serverCtx, "UPDATE electric_soak_items SET label = $2 WHERE id = $1;", int32(i), fmt.Sprintf("updated-%03d", i))
			require.NoError(t, err)
		}
		for i := 2; i <= 200; i += 5 {
			_, err = conn.Current.Exec(serverCtx, "DELETE FROM electric_soak_items WHERE id = $1;", int32(i))
			require.NoError(t, err)
		}

		shape = waitForElectricOperationsWithin(t, ctx, baseURL, "electric_soak_items", shape, 3*time.Minute,
			electricExpectedOperation{operation: "insert", id: "200"},
			electricExpectedOperation{operation: "update", id: "199"},
			electricExpectedOperation{operation: "delete", id: "197"})
		elapsed := time.Since(start)
		stopReader()
		require.Less(t, elapsed, 180*time.Second)
		require.Greater(t, shapeReads.Load(), int64(0))

		rows := readElectricShapeRows(t, ctx, baseURL, "electric_soak_items")
		require.Len(t, rows, 160)
		require.Equal(t, "updated-001", rows["1"]["label"])
		require.Equal(t, "item-200", rows["200"]["label"])
		require.NotContains(t, rows, "197")
		mutationsPerSecond := float64(340) / elapsed.Seconds()
		t.Logf("Electric soak applied 340 mutations through shape %s in %s (%.2f mutations/s, %d concurrent shape reads)",
			shape.Handle, elapsed, mutationsPerSecond, shapeReads.Load())
	})
}

type electricShapeState struct {
	Handle string
	Offset string
	Cursor string
}

type electricShapeMessage struct {
	Headers map[string]any `json:"headers"`
	Value   map[string]any `json:"value"`
}

type electricExpectedOperation struct {
	operation string
	id        string
}

type electricContainerConfig struct {
	name            string
	image           string
	doltgresPort    int
	electricPort    int
	shapeStorageDir string
}

func runForEachElectricImage(t *testing.T, fn func(t *testing.T, image string)) {
	t.Helper()
	requireElectricSmokeEnabled(t)
	originalServerHost := serverHost
	serverHost = "0.0.0.0"
	t.Cleanup(func() {
		serverHost = originalServerHost
	})
	for _, image := range electricImagesFromEnv() {
		t.Run(sanitizeElectricImageForTestName(image), func(t *testing.T) {
			fn(t, image)
		})
	}
}

func requireElectricSmokeEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv(electricSmokeEnv) != "1" {
		t.Skipf("set %s=1 to run Docker-backed Electric compatibility tests", electricSmokeEnv)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for Electric compatibility tests: %v", err)
	}
}

func electricImagesFromEnv() []string {
	raw := os.Getenv("DOLTGRES_ELECTRIC_IMAGES")
	if raw == "" {
		raw = os.Getenv("DOLTGRES_ELECTRIC_IMAGE")
	}
	if raw == "" {
		raw = defaultSupportedElectric
	}
	parts := strings.Split(raw, ",")
	images := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			images = append(images, part)
		}
	}
	return images
}

func TestElectricImagesFromEnv(t *testing.T) {
	t.Setenv("DOLTGRES_ELECTRIC_IMAGES", "")
	t.Setenv("DOLTGRES_ELECTRIC_IMAGE", "")
	require.Equal(t, []string{defaultSupportedElectric}, electricImagesFromEnv())

	t.Setenv("DOLTGRES_ELECTRIC_IMAGE", "electricsql/electric:1.6.1")
	require.Equal(t, []string{"electricsql/electric:1.6.1"}, electricImagesFromEnv())

	t.Setenv("DOLTGRES_ELECTRIC_IMAGES", " electricsql/electric:1.6.2 , , electricsql/electric:latest ")
	require.Equal(t, []string{"electricsql/electric:1.6.2", "electricsql/electric:latest"}, electricImagesFromEnv())
}

func sanitizeElectricImageForTestName(image string) string {
	replacer := strings.NewReplacer("/", "_", ":", "_", "@", "_", ".", "_", "-", "_")
	return replacer.Replace(image)
}

func startElectricContainer(t *testing.T, ctx context.Context, cfg electricContainerConfig) {
	t.Helper()
	if cfg.image == "" {
		cfg.image = defaultSupportedElectric
	}
	shapeStorageDir := electricShapeStorageInMem
	var volumeArgs []string
	if cfg.shapeStorageDir != "" {
		require.NoError(t, os.MkdirAll(cfg.shapeStorageDir, 0o755))
		shapeStorageDir = electricShapeStorageMounted
		volumeArgs = []string{"-v", filepath.Clean(cfg.shapeStorageDir) + ":" + electricShapeStorageMounted}
	}
	databaseURL := fmt.Sprintf("postgresql://postgres:password@host.docker.internal:%d/postgres?sslmode=disable", cfg.doltgresPort)
	args := []string{
		"run", "-d",
		"--name", cfg.name,
		"--add-host=host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:3000", cfg.electricPort),
	}
	args = append(args, volumeArgs...)
	args = append(args,
		"-e", "DATABASE_URL="+databaseURL,
		"-e", "ELECTRIC_INSECURE=true",
		"-e", "ELECTRIC_MANUAL_TABLE_PUBLISHING=true",
		"-e", "ELECTRIC_SHAPE_DB_STORAGE_DIR="+shapeStorageDir,
		"-e", "ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE=true",
		"-e", "ELECTRIC_USAGE_REPORTING=false",
		"-e", "ELECTRIC_LOG_LEVEL=debug",
		cfg.image,
	)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoErrorf(t, err, "docker run failed:\n%s", string(out))
	t.Cleanup(func() {
		removeElectricContainer(t, cfg.name)
	})
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", cfg.name).CombinedOutput()
			t.Logf("Electric container logs:\n%s", string(logs))
		}
	})
}

func removeElectricContainer(t *testing.T, containerName string) {
	t.Helper()
	out, err := exec.Command("docker", "rm", "-f", containerName).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		require.NoErrorf(t, err, "docker rm failed:\n%s", string(out))
	}
}

func waitForElectricShapeUpToDate(t *testing.T, ctx context.Context, baseURL string, table string) electricShapeState {
	t.Helper()
	state := electricShapeState{Offset: "-1"}
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	var lastBody string
	for time.Now().Before(deadline) {
		next, messages, status, body, err := requestElectricShape(ctx, baseURL, table, state, false)
		lastBody = body
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if status == http.StatusOK {
			state = next
			if electricResponseUpToDate(messages, body) {
				return state
			}
			continue
		}
		if status == http.StatusConflict && electricResponseMustRefetch(messages, body) {
			state = electricShapeState{Offset: "-1"}
			continue
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.FailNowf(t, "Electric shape did not become up-to-date", "last error: %v\nlast body: %s", lastErr, lastBody)
	return electricShapeState{}
}

func waitForElectricOperations(t *testing.T, ctx context.Context, baseURL string, table string, state electricShapeState, expected ...electricExpectedOperation) electricShapeState {
	t.Helper()
	return waitForElectricOperationsWithin(t, ctx, baseURL, table, state, 60*time.Second, expected...)
}

func waitForElectricOperationsWithin(t *testing.T, ctx context.Context, baseURL string, table string, state electricShapeState, wait time.Duration, expected ...electricExpectedOperation) electricShapeState {
	t.Helper()
	pending := make(map[electricExpectedOperation]struct{}, len(expected))
	for _, op := range expected {
		pending[op] = struct{}{}
	}
	deadline := time.Now().Add(wait)
	var lastBody string
	for time.Now().Before(deadline) {
		next, messages, status, body, err := requestElectricShape(ctx, baseURL, table, state, true)
		lastBody = body
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if status == http.StatusNoContent {
			continue
		}
		if status == http.StatusConflict && electricResponseMustRefetch(messages, body) {
			state = electricShapeState{Offset: "-1"}
			continue
		}
		require.Equalf(t, http.StatusOK, status, "unexpected Electric status body: %s", body)
		state = next
		for _, message := range messages {
			op := electricExpectedOperation{
				operation: electricHeaderString(message.Headers, "operation"),
				id:        electricValueString(message.Value, "id"),
			}
			delete(pending, op)
			if len(pending) == 0 {
				return state
			}
		}
		if electricResponseUpToDate(messages, body) {
			continue
		}
	}
	require.FailNowf(t, "Electric operations were not observed", "pending=%v last body=%s", pending, lastBody)
	return electricShapeState{}
}

func readElectricShapeRows(t *testing.T, ctx context.Context, baseURL string, table string) map[string]map[string]string {
	t.Helper()
	state := electricShapeState{Offset: "-1"}
	rows := make(map[string]map[string]string)
	deadline := time.Now().Add(60 * time.Second)
	var lastBody string
	for time.Now().Before(deadline) {
		next, messages, status, body, err := requestElectricShape(ctx, baseURL, table, state, false)
		lastBody = body
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if status == http.StatusConflict && electricResponseMustRefetch(messages, body) {
			state = electricShapeState{Offset: "-1"}
			rows = make(map[string]map[string]string)
			continue
		}
		require.Equalf(t, http.StatusOK, status, "unexpected Electric status body: %s", body)
		state = next
		for _, message := range messages {
			if electricHeaderString(message.Headers, "control") == "up-to-date" {
				return rows
			}
			id := electricValueString(message.Value, "id")
			if id == "" {
				continue
			}
			if electricHeaderString(message.Headers, "operation") == "delete" {
				delete(rows, id)
				continue
			}
			rows[id] = electricStringMap(message.Value)
		}
		if electricResponseUpToDate(messages, body) {
			return rows
		}
	}
	require.FailNowf(t, "Electric shape snapshot did not become up-to-date", "last body=%s", lastBody)
	return nil
}

func waitForElectricRows(t *testing.T, ctx context.Context, baseURL string, table string, matches func(map[string]map[string]string) bool) map[string]map[string]string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastRows map[string]map[string]string
	for time.Now().Before(deadline) {
		lastRows = readElectricShapeRows(t, ctx, baseURL, table)
		if matches(lastRows) {
			return lastRows
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.FailNowf(t, "Electric shape rows did not match", "table=%s rows=%v", table, lastRows)
	return nil
}

func requestElectricShape(ctx context.Context, baseURL string, table string, state electricShapeState, live bool) (electricShapeState, []electricShapeMessage, int, string, error) {
	requestURL, err := url.Parse(baseURL + "/v1/shape")
	if err != nil {
		return state, nil, 0, "", err
	}
	query := requestURL.Query()
	query.Set("table", table)
	query.Set("offset", state.Offset)
	if state.Handle != "" {
		query.Set("handle", state.Handle)
	}
	if state.Cursor != "" {
		query.Set("cursor", state.Cursor)
	}
	if live {
		query.Set("live", "true")
	}
	requestURL.RawQuery = query.Encode()

	requestCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return state, nil, 0, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return state, nil, 0, "", err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return state, nil, resp.StatusCode, "", err
	}
	body := string(bodyBytes)
	next := electricShapeState{
		Handle: headerOrDefault(resp, "electric-handle", state.Handle),
		Offset: headerOrDefault(resp, "electric-offset", state.Offset),
		Cursor: headerOrDefault(resp, "electric-cursor", state.Cursor),
	}
	if resp.StatusCode == http.StatusNoContent {
		return next, nil, resp.StatusCode, body, nil
	}
	var messages []electricShapeMessage
	if len(bodyBytes) > 0 {
		if err = json.Unmarshal(bodyBytes, &messages); err != nil {
			return next, nil, resp.StatusCode, body, fmt.Errorf("Electric shape response was not an array: %w", err)
		}
	}
	return next, messages, resp.StatusCode, body, nil
}

func waitForReplicationSlot(t *testing.T, ctx context.Context, conn *Connection) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := conn.Current.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, electricDefaultSlotName).Scan(&count)
		if err == nil && count == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.FailNow(t, "Electric replication slot was not present")
}

func waitForReplicationSlotActive(t *testing.T, ctx context.Context, conn *Connection, expected bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var active bool
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT active
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, electricDefaultSlotName).Scan(&active)
		if lastErr == nil && active == expected {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	require.FailNowf(t, "Electric replication slot active state did not match", "expected active=%t", expected)
}

func waitForReplicationSlotConfirmedFlushGreaterThan(t *testing.T, ctx context.Context, conn *Connection, min pglogrepl.LSN) pglogrepl.LSN {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastLSN pglogrepl.LSN
	var lastErr error
	for time.Now().Before(deadline) {
		var confirmedFlush string
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT confirmed_flush_lsn::text
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, electricDefaultSlotName).Scan(&confirmedFlush)
		if lastErr == nil {
			var err error
			lastLSN, err = pglogrepl.ParseLSN(confirmedFlush)
			if err != nil {
				lastErr = err
			} else if lastLSN > min {
				return lastLSN
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	require.FailNowf(t, "Electric replication slot did not advance confirmed_flush_lsn", "min=%s last=%s", min, lastLSN)
	return 0
}

func electricResponseUpToDate(messages []electricShapeMessage, body string) bool {
	for _, message := range messages {
		if electricHeaderString(message.Headers, "control") == "up-to-date" {
			return true
		}
	}
	return body == "[]" || body == ""
}

func electricResponseMustRefetch(messages []electricShapeMessage, body string) bool {
	for _, message := range messages {
		if electricHeaderString(message.Headers, "control") == "must-refetch" {
			return true
		}
	}
	return strings.Contains(body, `"must-refetch"`)
}

func electricHeaderString(headers map[string]any, key string) string {
	if headers == nil {
		return ""
	}
	value, ok := headers[key]
	if !ok {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	return fmt.Sprint(value)
}

func electricValueString(value map[string]any, key string) string {
	if value == nil {
		return ""
	}
	raw, ok := value[key]
	if !ok {
		return ""
	}
	switch typed := raw.(type) {
	case string:
		return typed
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return fmt.Sprint(typed)
	}
}

func electricStringMap(value map[string]any) map[string]string {
	ret := make(map[string]string, len(value))
	for key := range value {
		ret[key] = electricValueString(value, key)
	}
	return ret
}

func headerOrDefault(resp *http.Response, header string, fallback string) string {
	value := resp.Header.Get(header)
	if value == "" {
		return fallback
	}
	return value
}

func writeElectricBacklogConcurrently(t *testing.T, ctx context.Context, port int) {
	t.Helper()
	dsn := fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", port)
	var wg sync.WaitGroup
	errCh := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close(ctx)
			for i := 0; i < 5; i++ {
				id := int32(2 + worker*5 + i)
				_, err = conn.Exec(ctx, "INSERT INTO electric_accounts VALUES ($1, $2);", id, fmt.Sprintf("worker-%d-row-%d", worker, i))
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer conn.Close(ctx)
	for i := 200; i < 204; i++ {
		_, err = conn.Exec(ctx, "INSERT INTO electric_tasks VALUES ($1, 1, $2);", int32(i), fmt.Sprintf("queued-%d", i))
		require.NoError(t, err)
	}
	_, err = conn.Exec(ctx, "UPDATE electric_accounts SET name = 'primary-account-offline-update' WHERE id = 1;")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "UPDATE electric_tasks SET status = 'closed' WHERE id = 101;")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "DELETE FROM electric_tasks WHERE id = 101;")
	require.NoError(t, err)
}
