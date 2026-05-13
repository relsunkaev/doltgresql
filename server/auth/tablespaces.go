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

package auth

import (
	"sort"

	"github.com/dolthub/doltgresql/utils"
)

const firstUserTablespaceOID = uint32(16384)

// Tablespace stores PostgreSQL tablespace catalog metadata.
type Tablespace struct {
	Name  string
	Owner string
	Oid   uint32
}

// Tablespaces contains user-created PostgreSQL tablespace metadata keyed by name.
type Tablespaces struct {
	Data map[string]Tablespace
}

// NewTablespaces returns a new *Tablespaces.
func NewTablespaces() *Tablespaces {
	return &Tablespaces{Data: make(map[string]Tablespace)}
}

// SetTablespace stores a user-created tablespace.
func SetTablespace(tablespace Tablespace) {
	if tablespace.Name == "" {
		return
	}
	if tablespace.Oid == 0 {
		tablespace.Oid = nextTablespaceOID()
	}
	globalDatabase.tablespaces.Data[tablespace.Name] = tablespace
}

// GetTablespace returns a user-created tablespace by name.
func GetTablespace(name string) (Tablespace, bool) {
	tablespace, ok := globalDatabase.tablespaces.Data[name]
	return tablespace, ok
}

// GetAllTablespaces returns user-created tablespaces in deterministic order.
func GetAllTablespaces() []Tablespace {
	tablespaces := make([]Tablespace, 0, len(globalDatabase.tablespaces.Data))
	for _, tablespace := range globalDatabase.tablespaces.Data {
		tablespaces = append(tablespaces, tablespace)
	}
	sort.Slice(tablespaces, func(i, j int) bool {
		return tablespaces[i].Name < tablespaces[j].Name
	})
	return tablespaces
}

func nextTablespaceOID() uint32 {
	next := firstUserTablespaceOID
	for _, tablespace := range globalDatabase.tablespaces.Data {
		if tablespace.Oid >= next {
			next = tablespace.Oid + 1
		}
	}
	return next
}

func (tablespaces *Tablespaces) serialize(writer *utils.Writer) {
	ordered := make([]Tablespace, 0, len(tablespaces.Data))
	for _, tablespace := range tablespaces.Data {
		ordered = append(ordered, tablespace)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Name < ordered[j].Name
	})
	writer.Uint64(uint64(len(ordered)))
	for _, tablespace := range ordered {
		writer.String(tablespace.Name)
		writer.String(tablespace.Owner)
		writer.Uint32(tablespace.Oid)
	}
}

func (tablespaces *Tablespaces) deserialize(version uint32, reader *utils.Reader) {
	tablespaces.Data = make(map[string]Tablespace)
	if version == 0 {
		return
	}
	count := reader.Uint64()
	for idx := uint64(0); idx < count; idx++ {
		tablespace := Tablespace{
			Name:  reader.String(),
			Owner: reader.String(),
			Oid:   reader.Uint32(),
		}
		if tablespace.Name != "" {
			tablespaces.Data[tablespace.Name] = tablespace
		}
	}
}
