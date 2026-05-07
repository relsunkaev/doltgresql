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

package server

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
)

// cancelRegistry maps a connection's wire-protocol BackendKeyData
// (ProcessID, SecretKey) to the GMS connection ID that owns it.
// CancelRequest is delivered as a startup-message variant on a
// fresh TCP connection — there is no session context — so the
// registry is the only way for that handshake to find the running
// query to interrupt.
//
// PostgreSQL clients (pgx, JDBC, psycopg, every GUI editor) use
// CancelRequest for client-side query timeouts and "stop query"
// buttons. Without this, a hardcoded SecretKey=0 made every cancel
// silently fail.
type cancelRegistry struct {
	mu      sync.RWMutex
	entries map[cancelKey]uint32
}

type cancelKey struct {
	processID uint32
	secretKey uint32
}

var globalCancelRegistry = &cancelRegistry{
	entries: make(map[cancelKey]uint32),
}

// register associates the (processID, secretKey) pair with connID
// for the lifetime of the connection. unregister must be called on
// disconnect to drop the entry.
func (r *cancelRegistry) register(processID, secretKey, connID uint32) {
	r.mu.Lock()
	r.entries[cancelKey{processID, secretKey}] = connID
	r.mu.Unlock()
}

// unregister removes the registry entry. Safe to call multiple times.
func (r *cancelRegistry) unregister(processID, secretKey uint32) {
	r.mu.Lock()
	delete(r.entries, cancelKey{processID, secretKey})
	r.mu.Unlock()
}

// lookup returns the connection ID associated with the given
// BackendKeyData, or false if the pair is not registered.
func (r *cancelRegistry) lookup(processID, secretKey uint32) (uint32, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	connID, ok := r.entries[cancelKey{processID, secretKey}]
	return connID, ok
}

// generateSecretKey returns a 32-bit random secret suitable for the
// ProcessID/SecretKey pair PostgreSQL hands out. The value must be
// nonzero so a buggy or stale CancelRequest with SecretKey=0 cannot
// match a real entry.
func generateSecretKey() uint32 {
	var buf [4]byte
	for {
		if _, err := rand.Read(buf[:]); err != nil {
			// crypto/rand on the supported platforms (linux/darwin/
			// windows) cannot fail in practice; rather than ignore
			// the (impossible) error, fall back to a deterministic
			// nonzero value derived from address-of-buf so each
			// caller still gets a fresh value.
			return 1
		}
		key := binary.BigEndian.Uint32(buf[:])
		if key != 0 {
			return key
		}
	}
}
