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
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

const (
	zeroSmokeEnv         = "DOLTGRES_ZERO_SMOKE"
	defaultSupportedZero = "rocicorp/zero:1.4.0"
	zeroAppID            = "dgzero"
)

func TestZeroDiscoverModeSmoke(t *testing.T) {
	runForEachZeroImage(t, func(t *testing.T, image string) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		dbDir := t.TempDir()
		port, err := sql.GetEmptyPort()
		require.NoError(t, err)
		zeroPort, err := sql.GetEmptyPort()
		require.NoError(t, err)

		serverCtx, conn, controller := CreateServerLocalInDirWithPort(t, "postgres", dbDir, port)
		defer func() {
			conn.Close(serverCtx)
			controller.Stop()
			require.NoError(t, controller.WaitForStop())
		}()

		_, err = conn.Current.Exec(serverCtx, "CREATE DATABASE zero_cvr;")
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, "CREATE DATABASE zero_change;")
		require.NoError(t, err)

		const tableName = "zero_smoke_items"
		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, label TEXT NOT NULL);", tableName))
		require.NoError(t, err)
		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL;", tableName))
		require.NoError(t, err)

		networkName := fmt.Sprintf("dg-zero-smoke-%d", time.Now().UnixNano())
		createZeroNetwork(t, ctx, networkName)

		replicaDir := t.TempDir()
		rmName := fmt.Sprintf("%s-rm", networkName)
		viewName := fmt.Sprintf("%s-view", networkName)
		cfg := zeroContainerConfig{
			image:         image,
			doltgresPort:  port,
			networkName:   networkName,
			replicaDir:    replicaDir,
			zeroPort:      zeroPort,
			upstreamDB:    "postgres",
			cvrDB:         "zero_cvr",
			changeDB:      "zero_change",
			adminPassword: "zero-admin-password",
		}
		startZeroReplicationManagerContainer(t, ctx, rmName, cfg)
		startZeroViewSyncerContainer(t, ctx, viewName, cfg)

		waitForZeroKeepalive(t, ctx, zeroPort)
		waitForZeroPublication(t, serverCtx, conn)
		slot := waitForAnyZeroReplicationSlot(t, serverCtx, conn)
		initialLSN := waitForZeroReplicationSlotActive(t, serverCtx, conn, slot)

		_, err = conn.Current.Exec(serverCtx, fmt.Sprintf("INSERT INTO %s VALUES (1, 'one');", tableName))
		require.NoError(t, err)
		_ = waitForZeroReplicationSlotConfirmedFlushGreaterThan(t, serverCtx, conn, slot, initialLSN)
	})
}

type zeroContainerConfig struct {
	image         string
	doltgresPort  int
	networkName   string
	replicaDir    string
	zeroPort      int
	upstreamDB    string
	cvrDB         string
	changeDB      string
	adminPassword string
}

func runForEachZeroImage(t *testing.T, fn func(t *testing.T, image string)) {
	t.Helper()
	requireZeroSmokeEnabled(t)
	originalServerHost := serverHost
	serverHost = "0.0.0.0"
	t.Cleanup(func() {
		serverHost = originalServerHost
	})
	for _, image := range zeroImagesFromEnv() {
		t.Run(sanitizeZeroImageForTestName(image), func(t *testing.T) {
			fn(t, image)
		})
	}
}

func requireZeroSmokeEnabled(t *testing.T) {
	t.Helper()
	if os.Getenv(zeroSmokeEnv) != "1" {
		t.Skipf("set %s=1 to run Docker-backed Zero compatibility tests", zeroSmokeEnv)
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker is required for Zero compatibility tests: %v", err)
	}
}

func zeroImagesFromEnv() []string {
	raw := os.Getenv("DOLTGRES_ZERO_IMAGES")
	if raw == "" {
		raw = os.Getenv("DOLTGRES_ZERO_IMAGE")
	}
	if raw == "" {
		raw = defaultSupportedZero
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

func TestZeroImagesFromEnv(t *testing.T) {
	t.Setenv("DOLTGRES_ZERO_IMAGES", "")
	t.Setenv("DOLTGRES_ZERO_IMAGE", "")
	require.Equal(t, []string{defaultSupportedZero}, zeroImagesFromEnv())

	t.Setenv("DOLTGRES_ZERO_IMAGE", "rocicorp/zero:1.4.0")
	require.Equal(t, []string{"rocicorp/zero:1.4.0"}, zeroImagesFromEnv())

	t.Setenv("DOLTGRES_ZERO_IMAGES", " rocicorp/zero:1.4.0 , , rocicorp/zero:1.6.0-canary.1 ")
	require.Equal(t, []string{"rocicorp/zero:1.4.0", "rocicorp/zero:1.6.0-canary.1"}, zeroImagesFromEnv())
}

func sanitizeZeroImageForTestName(image string) string {
	replacer := strings.NewReplacer("/", "_", ":", "_", "@", "_", ".", "_", "-", "_")
	return replacer.Replace(image)
}

func createZeroNetwork(t *testing.T, ctx context.Context, networkName string) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "network", "create", networkName).CombinedOutput()
	require.NoErrorf(t, err, "docker network create failed:\n%s", string(out))
	t.Cleanup(func() {
		out, err := exec.Command("docker", "network", "rm", networkName).CombinedOutput()
		if err != nil && !strings.Contains(string(out), "No such network") {
			require.NoErrorf(t, err, "docker network rm failed:\n%s", string(out))
		}
	})
}

func startZeroReplicationManagerContainer(t *testing.T, ctx context.Context, name string, cfg zeroContainerConfig) {
	t.Helper()
	args := zeroContainerArgs(t, cfg, name, filepath.Join(cfg.replicaDir, "replication-manager.db"))
	args = append(args,
		"-e", "ZERO_NUM_SYNC_WORKERS=0",
		"-e", "ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS=0",
		cfg.image,
	)
	startZeroContainer(t, ctx, name, args)
}

func startZeroViewSyncerContainer(t *testing.T, ctx context.Context, name string, cfg zeroContainerConfig) {
	t.Helper()
	args := zeroContainerArgs(t, cfg, name, filepath.Join(cfg.replicaDir, "view-syncer.db"))
	args = append(args,
		"-p", fmt.Sprintf("%d:4848", cfg.zeroPort),
		"-e", "ZERO_CHANGE_STREAMER_MODE=discover",
		cfg.image,
	)
	startZeroContainer(t, ctx, name, args)
}

func zeroContainerArgs(t *testing.T, cfg zeroContainerConfig, name string, replicaFile string) []string {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(replicaFile), 0o755))
	return []string{
		"run", "-d",
		"--name", name,
		"--network", cfg.networkName,
		"--add-host=host.docker.internal:host-gateway",
		"-v", filepath.Clean(cfg.replicaDir) + ":/zero-data",
		"-e", "ZERO_APP_ID=" + zeroAppID,
		"-e", "ZERO_UPSTREAM_DB=" + zeroDatabaseURL(cfg.doltgresPort, cfg.upstreamDB),
		"-e", "ZERO_CVR_DB=" + zeroDatabaseURL(cfg.doltgresPort, cfg.cvrDB),
		"-e", "ZERO_CHANGE_DB=" + zeroDatabaseURL(cfg.doltgresPort, cfg.changeDB),
		"-e", "ZERO_REPLICA_FILE=" + filepath.ToSlash(strings.Replace(replicaFile, cfg.replicaDir, "/zero-data", 1)),
		"-e", "ZERO_ADMIN_PASSWORD=" + cfg.adminPassword,
		"-e", "ZERO_LOG_LEVEL=debug",
		"-e", "ZERO_LITESTREAM_BACKUP_URL=file:///zero-data/litestream",
		"-e", "ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream",
		"-e", "ZERO_LITESTREAM_CONFIG_PATH=/usr/local/lib/node_modules/@rocicorp/zero/out/zero-cache/src/services/litestream/config.yml",
		"-e", "ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES=1",
		"-e", "ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT=1",
		"-e", "ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT=1",
		"-e", "ZERO_LITESTREAM_MULTIPART_CONCURRENCY=1",
		"-e", "ZERO_LITESTREAM_RESTORE_PARALLELISM=1",
	}
}

func zeroDatabaseURL(port int, database string) string {
	return fmt.Sprintf("postgres://postgres:password@host.docker.internal:%d/%s?sslmode=disable", port, database)
}

func startZeroContainer(t *testing.T, ctx context.Context, name string, args []string) {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoErrorf(t, err, "docker run failed:\n%s", string(out))
	t.Cleanup(func() {
		removeZeroContainer(t, name)
	})
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Logf("Zero container %s logs:\n%s", name, string(logs))
		}
	})
}

func removeZeroContainer(t *testing.T, containerName string) {
	t.Helper()
	out, err := exec.Command("docker", "rm", "-f", containerName).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") {
		require.NoErrorf(t, err, "docker rm failed:\n%s", string(out))
	}
}

func waitForZeroKeepalive(t *testing.T, ctx context.Context, zeroPort int) {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/keepalive", zeroPort)
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	var lastBody string
	for time.Now().Before(deadline) {
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, url, nil)
		if err == nil {
			var resp *http.Response
			resp, err = http.DefaultClient.Do(req)
			if err == nil {
				body, readErr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				lastBody = string(body)
				if readErr != nil {
					lastErr = readErr
				} else if resp.StatusCode == http.StatusOK {
					cancel()
					return
				} else {
					lastErr = fmt.Errorf("unexpected status %d", resp.StatusCode)
				}
			} else {
				lastErr = err
			}
		} else {
			lastErr = err
		}
		cancel()
		time.Sleep(500 * time.Millisecond)
	}
	require.FailNowf(t, "Zero keepalive did not become healthy", "last error: %v\nlast body: %s", lastErr, lastBody)
}

func waitForZeroPublication(t *testing.T, ctx context.Context, conn *Connection) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := conn.Current.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_catalog.pg_publication
			WHERE pubname = $1`, "_"+zeroAppID+"_public_0").Scan(&count)
		if err == nil && count == 1 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.FailNow(t, "Zero publication was not present")
}

func waitForAnyZeroReplicationSlot(t *testing.T, ctx context.Context, conn *Connection) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var slotName string
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT slot_name
			FROM pg_catalog.pg_replication_slots
			WHERE plugin = 'pgoutput'
			ORDER BY slot_name
			LIMIT 1`).Scan(&slotName)
		if lastErr == nil && slotName != "" {
			return slotName
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	require.FailNow(t, "Zero replication slot was not present")
	return ""
}

func waitForZeroReplicationSlotActive(t *testing.T, ctx context.Context, conn *Connection, slotName string) pglogrepl.LSN {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		var active bool
		var confirmedFlush string
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT active, confirmed_flush_lsn::text
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, slotName).Scan(&active, &confirmedFlush)
		if lastErr == nil && active {
			lsn, err := pglogrepl.ParseLSN(confirmedFlush)
			require.NoError(t, err)
			return lsn
		}
		time.Sleep(250 * time.Millisecond)
	}
	require.NoError(t, lastErr)
	require.FailNowf(t, "Zero replication slot did not become active", "slot=%s", slotName)
	return 0
}

func waitForZeroReplicationSlotConfirmedFlushGreaterThan(t *testing.T, ctx context.Context, conn *Connection, slotName string, min pglogrepl.LSN) pglogrepl.LSN {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastLSN pglogrepl.LSN
	var lastErr error
	for time.Now().Before(deadline) {
		var confirmedFlush string
		lastErr = conn.Current.QueryRow(ctx, `
			SELECT confirmed_flush_lsn::text
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, slotName).Scan(&confirmedFlush)
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
	require.FailNowf(t, "Zero replication slot confirmed_flush_lsn did not advance", "slot=%s last=%s min=%s", slotName, lastLSN, min)
	return lastLSN
}
