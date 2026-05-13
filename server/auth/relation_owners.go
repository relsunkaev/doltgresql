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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/doltgresql/utils"
)

// RelationOwners contains explicit PostgreSQL relation owners keyed by schema and relation name.
type RelationOwners struct {
	Data map[doltdb.TableName]string
}

// NewRelationOwners returns a new *RelationOwners.
func NewRelationOwners() *RelationOwners {
	return &RelationOwners{Data: make(map[doltdb.TableName]string)}
}

// GetRelationOwner returns the explicit owner for the relation, if any.
func GetRelationOwner(relation doltdb.TableName) string {
	return globalDatabase.relationOwners.Data[relation]
}

// SetRelationOwner records the explicit owner for a relation.
func SetRelationOwner(relation doltdb.TableName, owner string) {
	if relation.Name == "" || owner == "" {
		return
	}
	globalDatabase.relationOwners.Data[relation] = owner
}

// RemoveRelationOwner removes explicit ownership metadata for a relation.
func RemoveRelationOwner(relation doltdb.TableName) {
	delete(globalDatabase.relationOwners.Data, relation)
}

// RenameRelationOwner moves explicit ownership metadata to a renamed relation.
func RenameRelationOwner(oldRelation doltdb.TableName, newRelation doltdb.TableName) {
	owner := GetRelationOwner(oldRelation)
	if owner == "" {
		return
	}
	delete(globalDatabase.relationOwners.Data, oldRelation)
	SetRelationOwner(newRelation, owner)
}

// RelationOwnedByRole returns whether roleName owns relation.
func RelationOwnedByRole(relation doltdb.TableName, roleName string) bool {
	return roleName != "" && GetRelationOwner(relation) == roleName
}

// serialize writes the RelationOwners to the given writer.
func (owners *RelationOwners) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(owners.Data)))
	for relation, owner := range owners.Data {
		writer.String(relation.Schema)
		writer.String(relation.Name)
		writer.String(owner)
	}
}

// deserialize reads the RelationOwners from the given reader.
func (owners *RelationOwners) deserialize(version uint32, reader *utils.Reader) {
	owners.Data = make(map[doltdb.TableName]string)
	if version == 0 {
		return
	}
	dataCount := reader.Uint64()
	for idx := uint64(0); idx < dataCount; idx++ {
		schema := reader.String()
		name := reader.String()
		owner := reader.String()
		if name != "" && owner != "" {
			owners.Data[doltdb.TableName{Name: name, Schema: schema}] = owner
		}
	}
}
