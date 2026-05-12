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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// TestJSClientSingleConnectionPipelineGuards runs real JS clients through
// single-socket concurrency patterns. The postgres.js leg is routed through a
// protocol-observing proxy that withholds backend responses long enough to prove
// the client sent multiple Execute/Query messages before the first backend
// ReadyForQuery. Drizzle and node-postgres stay in this test as compatibility
// guards, but are not labeled as pipeline proofs because their higher-level APIs
// may queue work before it reaches the socket.
func TestJSClientSingleConnectionPipelineGuards(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not on PATH; install Node.js to enable this harness")
	}
	if _, err := exec.LookPath("npm"); err != nil {
		t.Skip("npm not on PATH; install Node.js to enable this harness")
	}
	if testing.Short() {
		t.Skip("JS client pipeline harness installs node_modules; skipped under -short")
	}

	port, err := gms.GetEmptyPort()
	require.NoError(t, err)
	ctx, defaultConn, controller := CreateServerWithPort(t, "postgres", port)
	t.Cleanup(func() {
		defaultConn.Close(ctx)
		controller.Stop()
		require.NoError(t, controller.WaitForStop())
	})

	postgresJSProxy := startPostgresPipelineProbe(t, "postgres-js", port, true)
	drizzleProxy := startPostgresPipelineProbe(t, "drizzle-postgres-js", port, false)
	nodePostgresProxy := startPostgresPipelineProbe(t, "node-postgres", port, false)

	work := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(work, "package.json"), []byte(`{
  "name": "doltgres-js-client-pipeline-harness",
  "private": true,
  "type": "module"
}
`), 0o644))

	cmdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	install := exec.CommandContext(cmdCtx, "npm", "install", "--silent",
		"--no-audit", "--no-fund",
		"postgres@3.4.5", "drizzle-orm@0.45.2", "pg@8.11.3",
	)
	install.Dir = work
	install.Env = append(os.Environ(), "NO_COLOR=1")
	if out, err := install.CombinedOutput(); err != nil {
		t.Fatalf("npm install JS clients failed: %v\n%s", err, string(out))
	}

	probe := strings.ReplaceAll(`
import assert from 'node:assert/strict';
import postgres from 'postgres';
import pg from 'pg';
import { drizzle } from 'drizzle-orm/postgres-js';
import { sql as dsql } from 'drizzle-orm';

const postgresJsUrl = process.argv[2];
const drizzleUrl = process.argv[3];
const nodePostgresUrl = process.argv[4];

async function runPostgresJSPipeline() {
  const sql = postgres(postgresJsUrl, {
    max: 1,
    max_pipeline: 50,
    prepare: true,
    idle_timeout: 1,
    connect_timeout: 5,
  });

  try {
    await sql§CREATE TABLE pjs_pipeline_items (
      id integer PRIMARY KEY,
      label text NOT NULL,
      payload jsonb NOT NULL
    )§;

    const inserts = Array.from({ length: 25 }, (_, i) =>
      sql§
        INSERT INTO pjs_pipeline_items VALUES (
          ${i},
          ${'label-' + i},
          ${sql.json({ index: i, parity: i % 2 === 0 ? 'even' : 'odd' })}
        )
        RETURNING id::text AS id, payload #>> '{parity}' AS parity
      §
    );
    const inserted = await Promise.all(inserts);
    assert.equal(inserted.length, 25);
    assert.equal(inserted[0][0].id, '0');
    assert.equal(inserted[24][0].parity, 'even');

    const reads = await Promise.all(Array.from({ length: 25 }, (_, i) =>
      sql§
        SELECT label, payload #>> '{index}' AS idx
        FROM pjs_pipeline_items
        WHERE id = ${i}
      §
    ));
    assert.deepEqual(reads.map(rows => rows[0].label), Array.from({ length: 25 }, (_, i) => 'label-' + i));

    const summary = await sql§
      SELECT count(*)::int AS count,
             array_to_string(array_agg(label ORDER BY id), ',') AS labels
      FROM pjs_pipeline_items
    §;
    assert.equal(summary[0].count, 25);
    assert.equal(summary[0].labels.split(',').at(-1), 'label-24');
    return { count: summary[0].count };
  } finally {
    await sql.end({ timeout: 5 });
  }
}

async function runDrizzlePostgresJSPipeline() {
  const client = postgres(drizzleUrl, {
    max: 1,
    max_pipeline: 50,
    prepare: true,
    idle_timeout: 1,
    connect_timeout: 5,
  });
  const db = drizzle(client);

  try {
    await db.execute(dsql§CREATE TABLE drizzle_pipeline_items (
      id integer PRIMARY KEY,
      label text NOT NULL
    )§);

    await Promise.all(Array.from({ length: 20 }, (_, i) =>
      db.execute(dsql§INSERT INTO drizzle_pipeline_items VALUES (${i}, ${'drizzle-' + i})§)
    ));

    const reads = await Promise.all(Array.from({ length: 20 }, (_, i) =>
      db.execute(dsql§
        SELECT label
        FROM drizzle_pipeline_items
        WHERE id = ${i}
      §)
    ));
    assert.deepEqual(reads.map(result => result[0].label), Array.from({ length: 20 }, (_, i) => 'drizzle-' + i));

    const summary = await db.execute(dsql§SELECT count(*)::int AS count FROM drizzle_pipeline_items§);
    assert.equal(summary[0].count, 20);
    return { count: summary[0].count };
  } finally {
    await client.end({ timeout: 5 });
  }
}

async function runNodePostgresSingleClientQueue() {
  const { Client } = pg;
  const client = new Client({
    connectionString: nodePostgresUrl,
    application_name: 'node-postgres-single-client-pipeline-guard',
    connectionTimeoutMillis: 5000,
  });
  await client.connect();

  try {
    await client.query(§CREATE TABLE npg_pipeline_items (
      id integer PRIMARY KEY,
      label text NOT NULL
    )§);

    await Promise.all(Array.from({ length: 20 }, (_, i) =>
      client.query(§INSERT INTO npg_pipeline_items VALUES ($1::int4, $2::text)§, [i, 'pg-' + i])
    ));

    const reads = await Promise.all(Array.from({ length: 20 }, (_, i) =>
      client.query(§SELECT label FROM npg_pipeline_items WHERE id = $1::int4§, [i])
    ));
    assert.deepEqual(reads.map(result => result.rows[0].label), Array.from({ length: 20 }, (_, i) => 'pg-' + i));

    const summary = await client.query(§SELECT count(*)::int AS count FROM npg_pipeline_items§);
    assert.equal(summary.rows[0].count, 20);
    return { count: summary.rows[0].count };
  } finally {
    await client.end();
  }
}

const result = {
  postgresJs: await runPostgresJSPipeline(),
  drizzle: await runDrizzlePostgresJSPipeline(),
  nodePostgres: await runNodePostgresSingleClientQueue(),
};
console.log(JSON.stringify({ ok: true, result }));
`, "§", "`")
	require.NoError(t, os.WriteFile(filepath.Join(work, "probe.mjs"), []byte(probe), 0o644))

	proxyURL := func(proxyPort int) string {
		return fmt.Sprintf("postgres://postgres:password@127.0.0.1:%d/postgres?sslmode=disable", proxyPort)
	}
	cmd := exec.CommandContext(cmdCtx, "node", "probe.mjs",
		proxyURL(postgresJSProxy.port),
		proxyURL(drizzleProxy.port),
		proxyURL(nodePostgresProxy.port),
	)
	cmd.Dir = work
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "JS pipeline probe failed: %s", string(out))
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"postgresJs":{"count":25}`)
	require.Contains(t, string(out), `"drizzle":{"count":20}`)
	require.Contains(t, string(out), `"nodePostgres":{"count":20}`)

	postgresJSStats := postgresJSProxy.stats()
	drizzleStats := drizzleProxy.stats()
	nodePostgresStats := nodePostgresProxy.stats()
	require.GreaterOrEqual(t, postgresJSStats.MaxExecutionsBeforeReady, 2,
		"postgres.js should send multiple Execute/Query messages before the first backend ReadyForQuery; stats=%+v", postgresJSStats)
	require.Positive(t, drizzleStats.TotalExecutions,
		"Drizzle postgres-js compatibility guard should still execute through the protocol-observing proxy; stats=%+v", drizzleStats)
	require.Positive(t, nodePostgresStats.TotalExecutions,
		"node-postgres queue guard should still execute through the protocol-observing proxy; stats=%+v", nodePostgresStats)
}

type postgresPipelineProbe struct {
	label    string
	port     int
	listener net.Listener
	mu       sync.Mutex
	s        postgresPipelineStats

	delayUntilPipeline bool
}

type postgresPipelineStats struct {
	TotalExecutions          int
	MaxExecutionsBeforeReady int
}

func startPostgresPipelineProbe(t *testing.T, label string, targetPort int, delayUntilPipeline bool) *postgresPipelineProbe {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, rawPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)

	var port int
	_, err = fmt.Sscanf(rawPort, "%d", &port)
	require.NoError(t, err)

	p := &postgresPipelineProbe{
		label:              label,
		port:               port,
		listener:           ln,
		delayUntilPipeline: delayUntilPipeline,
	}
	target := fmt.Sprintf("127.0.0.1:%d", targetPort)
	go p.accept(target)
	t.Cleanup(func() {
		_ = ln.Close()
	})
	return p
}

func (p *postgresPipelineProbe) stats() postgresPipelineStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.s
}

func (p *postgresPipelineProbe) accept(target string) {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", target)
		if err != nil {
			_ = client.Close()
			continue
		}
		state := &postgresPipelineConnState{}
		go p.forwardFrontend(client, server, state)
		go p.forwardBackend(server, client, state)
	}
}

type postgresPipelineConnState struct {
	mu                 sync.Mutex
	startupMessageSent bool
	startupDone        bool
	currentExecutions  int
}

func (s *postgresPipelineConnState) hasStartupMessageSent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startupMessageSent
}

func (s *postgresPipelineConnState) markStartupMessageSent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startupMessageSent = true
}

func (s *postgresPipelineConnState) markExecution() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.startupDone {
		return 0
	}
	s.currentExecutions++
	return s.currentExecutions
}

func (s *postgresPipelineConnState) shouldDelayBackend() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startupDone && s.currentExecutions == 1
}

func (s *postgresPipelineConnState) waitForPipelineOrTimeout(wait time.Duration) {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		current := s.currentExecutions
		s.mu.Unlock()
		if current != 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *postgresPipelineConnState) markReadyForQuery() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.startupDone {
		s.currentExecutions = 0
	} else {
		s.startupDone = true
	}
}

func (p *postgresPipelineProbe) forwardFrontend(client net.Conn, server net.Conn, state *postgresPipelineConnState) {
	defer client.Close()
	defer server.Close()

	for {
		if !state.hasStartupMessageSent() {
			msg, isStartup, err := readUntypedFrontendMessage(client)
			if err != nil {
				return
			}
			if _, err = server.Write(msg); err != nil {
				return
			}
			if isStartup {
				state.markStartupMessageSent()
			}
			continue
		}

		messageType, msg, err := readTypedMessage(client)
		if err != nil {
			return
		}
		if messageType == 'E' || messageType == 'Q' {
			if currentExecutions := state.markExecution(); currentExecutions > 0 {
				p.recordExecutionBurst(currentExecutions)
			}
		}
		if _, err = server.Write(msg); err != nil {
			return
		}
	}
}

func (p *postgresPipelineProbe) forwardBackend(server net.Conn, client net.Conn, state *postgresPipelineConnState) {
	defer server.Close()
	defer client.Close()

	for {
		messageType, msg, err := readTypedMessage(server)
		if err != nil {
			return
		}
		if p.delayUntilPipeline && state.shouldDelayBackend() {
			state.waitForPipelineOrTimeout(250 * time.Millisecond)
		}
		if _, err = client.Write(msg); err != nil {
			return
		}
		if messageType == 'Z' {
			state.markReadyForQuery()
		}
	}
}

func (p *postgresPipelineProbe) recordExecutionBurst(currentExecutions int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.s.TotalExecutions++
	if currentExecutions > p.s.MaxExecutionsBeforeReady {
		p.s.MaxExecutionsBeforeReady = currentExecutions
	}
}

func readTypedMessage(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(header[1:5]))
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], append(header, payload...), nil
}

func readUntypedFrontendMessage(r io.Reader) ([]byte, bool, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, false, err
	}
	length := int(binary.BigEndian.Uint32(header))
	payload := make([]byte, length-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, false, err
	}
	if len(payload) < 4 {
		return append(header, payload...), false, nil
	}
	code := binary.BigEndian.Uint32(payload[:4])
	const (
		sslRequestCode = 80877103
		gssRequestCode = 80877104
	)
	return append(header, payload...), code != sslRequestCode && code != gssRequestCode, nil
}
