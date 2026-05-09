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

package pgstats

import (
	"sync"
	"time"
)

// IndexStats stores the pg_stat_*_indexes counters Doltgres can derive from
// executor-level index scans.
type IndexStats struct {
	IdxScan     int64
	LastIdxScan time.Time
	IdxTupRead  int64
	IdxTupFetch int64
}

var indexStats = struct {
	sync.RWMutex
	byOID map[uint32]IndexStats
}{
	byOID: make(map[uint32]IndexStats),
}

// RecordIndexScan records that an executor began a scan for the given index.
func RecordIndexScan(indexOID uint32) {
	if indexOID == 0 {
		return
	}
	indexStats.Lock()
	stats := indexStats.byOID[indexOID]
	stats.IdxScan++
	stats.LastIdxScan = time.Now()
	indexStats.byOID[indexOID] = stats
	indexStats.Unlock()
}

// RecordIndexRows records rows returned from a simple index scan.
func RecordIndexRows(indexOID uint32, rows int64) {
	if indexOID == 0 || rows <= 0 {
		return
	}
	indexStats.Lock()
	stats := indexStats.byOID[indexOID]
	stats.IdxTupRead += rows
	stats.IdxTupFetch += rows
	indexStats.byOID[indexOID] = stats
	indexStats.Unlock()
}

// SnapshotIndex returns a copy of the counters for indexOID.
func SnapshotIndex(indexOID uint32) IndexStats {
	if indexOID == 0 {
		return IndexStats{}
	}
	indexStats.RLock()
	stats := indexStats.byOID[indexOID]
	indexStats.RUnlock()
	return stats
}
