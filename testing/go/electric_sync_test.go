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
	"strconv"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestElectricSyncSmoke(t *testing.T) {
	if os.Getenv("DOLTGRES_ELECTRIC_SMOKE") != "1" {
		t.Skip("set DOLTGRES_ELECTRIC_SMOKE=1 to run the Docker-backed Electric smoke test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for Electric smoke test: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dbDir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
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
	startElectricContainer(t, ctx, containerName, port, electricPort)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", electricPort)
	shape := waitForElectricShapeUpToDate(t, ctx, baseURL, tableName)
	waitForReplicationSlot(t, serverCtx, conn)

	_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'one');", tableName))
	require.NoError(t, err)
	shape = waitForElectricOperation(t, ctx, baseURL, tableName, shape, "insert", "1")

	conn.Close(serverCtx)
	controller.Stop()
	require.NoError(t, controller.WaitForStop())

	serverCtx, conn, controller = CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
	waitForReplicationSlot(t, serverCtx, conn)
	shape = waitForElectricShapeUpToDate(t, ctx, baseURL, tableName)

	_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("UPDATE %s SET label = 'one-updated' WHERE id = 1;", tableName))
	require.NoError(t, err)
	shape = waitForElectricOperation(t, ctx, baseURL, tableName, shape, "update", "1")

	_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("DELETE FROM %s WHERE id = 1;", tableName))
	require.NoError(t, err)
	_ = waitForElectricOperation(t, ctx, baseURL, tableName, shape, "delete", "1")
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

func startElectricContainer(t *testing.T, ctx context.Context, containerName string, doltgresPort int, electricPort int) {
	t.Helper()
	image := os.Getenv("DOLTGRES_ELECTRIC_IMAGE")
	if image == "" {
		image = "electricsql/electric:latest"
	}
	databaseURL := fmt.Sprintf("postgresql://postgres:password@host.docker.internal:%d/postgres?sslmode=disable", doltgresPort)
	args := []string{
		"run", "-d",
		"--name", containerName,
		"--add-host=host.docker.internal:host-gateway",
		"-p", fmt.Sprintf("%d:3000", electricPort),
		"-e", "DATABASE_URL=" + databaseURL,
		"-e", "ELECTRIC_INSECURE=true",
		"-e", "ELECTRIC_MANUAL_TABLE_PUBLISHING=true",
		"-e", "ELECTRIC_SHAPE_DB_STORAGE_DIR=:memory:",
		"-e", "ELECTRIC_SHAPE_DB_EXCLUSIVE_MODE=true",
		"-e", "ELECTRIC_USAGE_REPORTING=false",
		"-e", "ELECTRIC_LOG_LEVEL=debug",
		image,
	}
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoErrorf(t, err, "docker run failed:\n%s", string(out))
	t.Cleanup(func() {
		removeElectricContainer(t, containerName)
	})
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
			t.Logf("Electric container logs:\n%s", string(logs))
		}
	})
}

func removeElectricContainer(t *testing.T, containerName string) {
	t.Helper()
	out, err := exec.Command("docker", "rm", "-f", containerName).CombinedOutput()
	require.NoErrorf(t, err, "docker rm failed:\n%s", string(out))
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
		time.Sleep(500 * time.Millisecond)
	}
	require.FailNowf(t, "Electric shape did not become up-to-date", "last error: %v\nlast body: %s", lastErr, lastBody)
	return electricShapeState{}
}

func waitForElectricOperation(t *testing.T, ctx context.Context, baseURL string, table string, state electricShapeState, operation string, id string) electricShapeState {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
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
		require.Equalf(t, http.StatusOK, status, "unexpected Electric status body: %s", body)
		state = next
		for _, message := range messages {
			if electricHeaderString(message.Headers, "operation") == operation && electricValueString(message.Value, "id") == id {
				return state
			}
		}
		if electricResponseUpToDate(messages, body) {
			continue
		}
	}
	require.FailNowf(t, "Electric operation was not observed", "operation=%s id=%s last body=%s", operation, id, lastBody)
	return electricShapeState{}
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
			WHERE slot_name = 'electric_slot_default'`).Scan(&count)
		if err == nil && count == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.FailNow(t, "Electric replication slot was not present")
}

func electricResponseUpToDate(messages []electricShapeMessage, body string) bool {
	for _, message := range messages {
		if electricHeaderString(message.Headers, "control") == "up-to-date" {
			return true
		}
	}
	return body == "[]" || body == ""
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

func headerOrDefault(resp *http.Response, header string, fallback string) string {
	value := resp.Header.Get(header)
	if value == "" {
		return fallback
	}
	return value
}
