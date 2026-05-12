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

import "github.com/dolthub/doltgresql/utils"

// SchemaOwners contains explicit PostgreSQL schema owners keyed by schema name.
type SchemaOwners struct {
	Data map[string]string
}

// NewSchemaOwners returns a new *SchemaOwners.
func NewSchemaOwners() *SchemaOwners {
	return &SchemaOwners{Data: make(map[string]string)}
}

// GetSchemaOwner returns the owner for the schema, falling back to PostgreSQL-compatible defaults.
func GetSchemaOwner(schema string) string {
	if owner := globalDatabase.schemaOwners.Data[schema]; owner != "" {
		return owner
	}
	if schema == "public" {
		return "pg_database_owner"
	}
	return "postgres"
}

// SetSchemaOwner records the owner for a schema.
func SetSchemaOwner(schema string, owner string) {
	if schema == "" || owner == "" {
		return
	}
	globalDatabase.schemaOwners.Data[schema] = owner
}

// RemoveSchemaOwner removes explicit ownership metadata for a schema.
func RemoveSchemaOwner(schema string) {
	delete(globalDatabase.schemaOwners.Data, schema)
}

// SchemaOwnedByRole returns whether roleName owns schema.
func SchemaOwnedByRole(schema string, roleName string) bool {
	return roleName != "" && GetSchemaOwner(schema) == roleName
}

// serialize writes the SchemaOwners to the given writer.
func (owners *SchemaOwners) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(owners.Data)))
	for schema, owner := range owners.Data {
		writer.String(schema)
		writer.String(owner)
	}
}

// deserialize reads the SchemaOwners from the given reader.
func (owners *SchemaOwners) deserialize(version uint32, reader *utils.Reader) {
	owners.Data = make(map[string]string)
	if version == 0 {
		return
	}
	dataCount := reader.Uint64()
	for idx := uint64(0); idx < dataCount; idx++ {
		schema := reader.String()
		owner := reader.String()
		if schema != "" && owner != "" {
			owners.Data[schema] = owner
		}
	}
}
