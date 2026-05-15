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

package cursorstate

import (
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Row is a wire-format cursor row.
type Row struct {
	Values [][]byte
}

// Cursor describes a session-local SQL cursor.
type Cursor struct {
	Fields []pgproto3.FieldDescription
	Rows   []Row
	Pos    int
	Hold   bool
}

var cursors = struct {
	sync.Mutex
	bySession map[uint32]map[string]*Cursor
}{
	bySession: make(map[uint32]map[string]*Cursor),
}

// Store stores a cursor for the session. It returns false when the cursor already exists.
func Store(sessionID uint32, name string, cursor Cursor) bool {
	cursors.Lock()
	defer cursors.Unlock()
	sessionCursors := cursors.bySession[sessionID]
	if sessionCursors == nil {
		sessionCursors = make(map[string]*Cursor)
		cursors.bySession[sessionID] = sessionCursors
	}
	if _, ok := sessionCursors[name]; ok {
		return false
	}
	copied := cloneCursor(cursor)
	sessionCursors[name] = &copied
	return true
}

// Exists returns whether the named cursor exists for the session.
func Exists(sessionID uint32, name string) bool {
	cursors.Lock()
	defer cursors.Unlock()
	_, ok := cursors.bySession[sessionID][name]
	return ok
}

// Fields returns the cursor field descriptions.
func Fields(sessionID uint32, name string) ([]pgproto3.FieldDescription, bool) {
	cursors.Lock()
	defer cursors.Unlock()
	cursor, ok := cursors.bySession[sessionID][name]
	if !ok {
		return nil, false
	}
	return cloneCursorFields(cursor.Fields), true
}

// Fetch fetches rows from a cursor and advances its position.
func Fetch(sessionID uint32, name string, all bool) ([]pgproto3.FieldDescription, []Row, bool) {
	cursors.Lock()
	defer cursors.Unlock()
	cursor, ok := cursors.bySession[sessionID][name]
	if !ok {
		return nil, nil, false
	}
	fields := cloneCursorFields(cursor.Fields)
	if cursor.Pos >= len(cursor.Rows) {
		return fields, nil, true
	}
	var rows []Row
	if all {
		rows = cloneCursorRows(cursor.Rows[cursor.Pos:])
		cursor.Pos = len(cursor.Rows)
	} else {
		rows = cloneCursorRows(cursor.Rows[cursor.Pos : cursor.Pos+1])
		cursor.Pos++
	}
	return fields, rows, true
}

// Close closes one cursor for the session.
func Close(sessionID uint32, name string) bool {
	cursors.Lock()
	defer cursors.Unlock()
	sessionCursors := cursors.bySession[sessionID]
	if _, ok := sessionCursors[name]; !ok {
		return false
	}
	delete(sessionCursors, name)
	return true
}

// CloseAll closes all cursors for the session.
func CloseAll(sessionID uint32) {
	cursors.Lock()
	defer cursors.Unlock()
	delete(cursors.bySession, sessionID)
}

// CloseNonHold closes all non-hold cursors for the session.
func CloseNonHold(sessionID uint32) {
	cursors.Lock()
	defer cursors.Unlock()
	for name, cursor := range cursors.bySession[sessionID] {
		if !cursor.Hold {
			delete(cursors.bySession[sessionID], name)
		}
	}
}

func cloneCursor(cursor Cursor) Cursor {
	return Cursor{
		Fields: cloneCursorFields(cursor.Fields),
		Rows:   cloneCursorRows(cursor.Rows),
		Pos:    cursor.Pos,
		Hold:   cursor.Hold,
	}
}

func cloneCursorFields(fields []pgproto3.FieldDescription) []pgproto3.FieldDescription {
	if fields == nil {
		return nil
	}
	cloned := make([]pgproto3.FieldDescription, len(fields))
	copy(cloned, fields)
	for i := range cloned {
		cloned[i].Name = append([]byte(nil), fields[i].Name...)
	}
	return cloned
}

func cloneCursorRows(rows []Row) []Row {
	if rows == nil {
		return nil
	}
	cloned := make([]Row, len(rows))
	for i, row := range rows {
		cloned[i].Values = cloneCursorValues(row.Values)
	}
	return cloned
}

func cloneCursorValues(values [][]byte) [][]byte {
	if values == nil {
		return nil
	}
	cloned := make([][]byte, len(values))
	for i, value := range values {
		cloned[i] = append([]byte(nil), value...)
	}
	return cloned
}
