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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/doltgresql/utils"
)

// DefaultPrivileges contains ALTER DEFAULT PRIVILEGES entries.
type DefaultPrivileges struct {
	Data map[DefaultPrivilegeKey]DefaultPrivilegeValue
}

// DefaultPrivilegeKey points to a default ACL entry for objects created by Owner.
type DefaultPrivilegeKey struct {
	Owner   RoleID
	Schema  string
	Object  PrivilegeObject
	Grantee RoleID
}

// DefaultPrivilegeValue is the value associated with the DefaultPrivilegeKey.
type DefaultPrivilegeValue struct {
	Key        DefaultPrivilegeKey
	Privileges map[Privilege]map[GrantedPrivilege]bool
}

// DefaultPrivilegeACL is a pg_default_acl-shaped aggregate of default privileges.
type DefaultPrivilegeACL struct {
	OwnerName string
	Schema    string
	Object    PrivilegeObject
	ACLItems  []string
}

// NewDefaultPrivileges returns a new *DefaultPrivileges.
func NewDefaultPrivileges() *DefaultPrivileges {
	return &DefaultPrivileges{Data: make(map[DefaultPrivilegeKey]DefaultPrivilegeValue)}
}

// AddDefaultPrivilege adds the given default privilege to the global database.
func AddDefaultPrivilege(key DefaultPrivilegeKey, privilege GrantedPrivilege, withGrantOption bool) {
	value, ok := globalDatabase.defaultPrivileges.Data[key]
	if !ok {
		value = DefaultPrivilegeValue{
			Key:        key,
			Privileges: make(map[Privilege]map[GrantedPrivilege]bool),
		}
		globalDatabase.defaultPrivileges.Data[key] = value
	}
	privilegeMap, ok := value.Privileges[privilege.Privilege]
	if !ok {
		privilegeMap = make(map[GrantedPrivilege]bool)
		value.Privileges[privilege.Privilege] = privilegeMap
	}
	privilegeMap[privilege] = withGrantOption
}

// RemoveDefaultPrivilege removes the given default privilege from the global database.
func RemoveDefaultPrivilege(key DefaultPrivilegeKey, privilege GrantedPrivilege, grantOptionOnly bool) {
	if value, ok := globalDatabase.defaultPrivileges.Data[key]; ok {
		if privilegeMap, ok := value.Privileges[privilege.Privilege]; ok {
			if grantOptionOnly {
				if privilege.GrantedBy.IsValid() {
					if _, ok = privilegeMap[privilege]; ok {
						privilegeMap[privilege] = false
					}
				} else {
					for privilegeMapKey := range privilegeMap {
						privilegeMap[privilegeMapKey] = false
					}
				}
			} else {
				if privilege.GrantedBy.IsValid() {
					delete(privilegeMap, privilege)
				} else {
					privilegeMap = nil
				}
				if len(privilegeMap) == 0 {
					delete(value.Privileges, privilege.Privilege)
				}
			}
		}
		if len(value.Privileges) == 0 {
			delete(globalDatabase.defaultPrivileges.Data, key)
		}
	}
}

// ApplyDefaultPrivilegesToTable grants matching default privileges to a newly-created table.
func ApplyDefaultPrivilegesToTable(ownerName string, schema string, table string) error {
	var err error
	LockWrite(func() {
		owner := GetRole(ownerName)
		if !owner.IsValid() {
			return
		}
		for _, value := range matchingDefaultPrivileges(owner.ID(), schema, PrivilegeObject_TABLE) {
			for privilege, grantedMap := range value.Privileges {
				for grantedPrivilege, withGrantOption := range grantedMap {
					AddTablePrivilege(TablePrivilegeKey{
						Role:  value.Key.Grantee,
						Table: doltdb.TableName{Name: table, Schema: schema},
					}, GrantedPrivilege{
						Privilege: privilege,
						GrantedBy: grantedPrivilege.GrantedBy,
					}, withGrantOption)
				}
			}
		}
		err = PersistChanges()
	})
	return err
}

// ApplyDefaultPrivilegesToSequence grants matching default privileges to a newly-created sequence.
func ApplyDefaultPrivilegesToSequence(ownerName string, schema string, sequence string) error {
	var err error
	LockWrite(func() {
		owner := GetRole(ownerName)
		if !owner.IsValid() {
			return
		}
		for _, value := range matchingDefaultPrivileges(owner.ID(), schema, PrivilegeObject_SEQUENCE) {
			for privilege, grantedMap := range value.Privileges {
				for grantedPrivilege, withGrantOption := range grantedMap {
					AddSequencePrivilege(SequencePrivilegeKey{
						Role:   value.Key.Grantee,
						Schema: schema,
						Name:   sequence,
					}, GrantedPrivilege{
						Privilege: privilege,
						GrantedBy: grantedPrivilege.GrantedBy,
					}, withGrantOption)
				}
			}
		}
		err = PersistChanges()
	})
	return err
}

// ApplyDefaultPrivilegesToRoutine grants matching default privileges to a newly-created function or procedure.
func ApplyDefaultPrivilegesToRoutine(ownerName string, schema string, routine string) error {
	var err error
	LockWrite(func() {
		owner := GetRole(ownerName)
		if !owner.IsValid() {
			return
		}
		for _, value := range matchingDefaultPrivileges(owner.ID(), schema, PrivilegeObject_FUNCTION) {
			for privilege, grantedMap := range value.Privileges {
				for grantedPrivilege, withGrantOption := range grantedMap {
					AddRoutinePrivilege(RoutinePrivilegeKey{
						Role:   value.Key.Grantee,
						Schema: schema,
						Name:   routine,
					}, GrantedPrivilege{
						Privilege: privilege,
						GrantedBy: grantedPrivilege.GrantedBy,
					}, withGrantOption)
				}
			}
		}
		err = PersistChanges()
	})
	return err
}

func matchingDefaultPrivileges(owner RoleID, schema string, object PrivilegeObject) []DefaultPrivilegeValue {
	values := make([]DefaultPrivilegeValue, 0)
	for key, value := range globalDatabase.defaultPrivileges.Data {
		if key.Owner == owner && key.Object == object && (key.Schema == "" || key.Schema == schema) {
			values = append(values, value)
		}
	}
	return values
}

// GetDefaultPrivilegeACLs returns default ACL rows grouped the same way pg_default_acl stores them.
func GetDefaultPrivilegeACLs() []DefaultPrivilegeACL {
	type groupKey struct {
		Owner  RoleID
		Schema string
		Object PrivilegeObject
	}
	grouped := make(map[groupKey][]string)
	LockRead(func() {
		for _, value := range globalDatabase.defaultPrivileges.Data {
			key := groupKey{
				Owner:  value.Key.Owner,
				Schema: value.Key.Schema,
				Object: value.Key.Object,
			}
			grouped[key] = append(grouped[key], aclItemsForRolePrivileges(value.Key.Grantee, value.Privileges)...)
		}
	})
	rows := make([]DefaultPrivilegeACL, 0, len(grouped))
	for key, items := range grouped {
		ownerName := ""
		LockRead(func() {
			if owner, ok := globalDatabase.rolesByID[key.Owner]; ok {
				ownerName = owner.Name
			}
		})
		if ownerName == "" {
			continue
		}
		sort.Strings(items)
		rows = append(rows, DefaultPrivilegeACL{
			OwnerName: ownerName,
			Schema:    key.Schema,
			Object:    key.Object,
			ACLItems:  items,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].OwnerName != rows[j].OwnerName {
			return rows[i].OwnerName < rows[j].OwnerName
		}
		if rows[i].Schema != rows[j].Schema {
			return rows[i].Schema < rows[j].Schema
		}
		return rows[i].Object < rows[j].Object
	})
	return rows
}

// DefaultPrivilegeObjectType returns PostgreSQL's pg_default_acl.defaclobjtype code.
func DefaultPrivilegeObjectType(object PrivilegeObject) string {
	switch object {
	case PrivilegeObject_TABLE:
		return "r"
	case PrivilegeObject_SEQUENCE:
		return "S"
	case PrivilegeObject_FUNCTION:
		return "f"
	case PrivilegeObject_TYPE:
		return "T"
	case PrivilegeObject_SCHEMA:
		return "n"
	case PrivilegeObject_LARGE_OBJECT:
		return "L"
	default:
		return ""
	}
}

// serialize writes the DefaultPrivileges to the given writer.
func (dp *DefaultPrivileges) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(dp.Data)))
	for _, value := range dp.Data {
		writer.Uint64(uint64(value.Key.Owner))
		writer.String(value.Key.Schema)
		writer.Uint8(uint8(value.Key.Object))
		writer.Uint64(uint64(value.Key.Grantee))
		writer.Uint64(uint64(len(value.Privileges)))
		for privilege, privilegeMap := range value.Privileges {
			writer.String(string(privilege))
			writer.Uint32(uint32(len(privilegeMap)))
			for grantedPrivilege, withGrantOption := range privilegeMap {
				writer.Uint64(uint64(grantedPrivilege.GrantedBy))
				writer.Bool(withGrantOption)
			}
		}
	}
}

// deserialize reads the DefaultPrivileges from the given reader.
func (dp *DefaultPrivileges) deserialize(version uint32, reader *utils.Reader) {
	dp.Data = make(map[DefaultPrivilegeKey]DefaultPrivilegeValue)
	if version == 0 {
		return
	}
	dataCount := reader.Uint64()
	for dataIdx := uint64(0); dataIdx < dataCount; dataIdx++ {
		value := DefaultPrivilegeValue{Privileges: make(map[Privilege]map[GrantedPrivilege]bool)}
		value.Key.Owner = RoleID(reader.Uint64())
		value.Key.Schema = reader.String()
		value.Key.Object = PrivilegeObject(reader.Uint8())
		value.Key.Grantee = RoleID(reader.Uint64())
		privilegeCount := reader.Uint64()
		for privilegeIdx := uint64(0); privilegeIdx < privilegeCount; privilegeIdx++ {
			privilege := Privilege(reader.String())
			grantedCount := reader.Uint32()
			grantedMap := make(map[GrantedPrivilege]bool)
			for grantedIdx := uint32(0); grantedIdx < grantedCount; grantedIdx++ {
				grantedPrivilege := GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: RoleID(reader.Uint64()),
				}
				grantedMap[grantedPrivilege] = reader.Bool()
			}
			value.Privileges[privilege] = grantedMap
		}
		dp.Data[value.Key] = value
	}
}
