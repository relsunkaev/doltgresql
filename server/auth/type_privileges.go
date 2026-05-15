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

// TypePrivileges contains the privileges given to a role on a type.
type TypePrivileges struct {
	Data map[TypePrivilegeKey]TypePrivilegeValue
}

// TypePrivilegeKey points to a specific type object.
type TypePrivilegeKey struct {
	Role   RoleID
	Schema string
	Name   string
}

// TypePrivilegeValue is the value associated with the TypePrivilegeKey.
type TypePrivilegeValue struct {
	Key        TypePrivilegeKey
	Privileges map[Privilege]map[GrantedPrivilege]bool
}

// NewTypePrivileges returns a new *TypePrivileges.
func NewTypePrivileges() *TypePrivileges {
	return &TypePrivileges{Data: make(map[TypePrivilegeKey]TypePrivilegeValue)}
}

// AddTypePrivilege adds the given type privilege to the global database.
func AddTypePrivilege(key TypePrivilegeKey, privilege GrantedPrivilege, withGrantOption bool) {
	value, ok := globalDatabase.typePrivileges.Data[key]
	if !ok {
		value = TypePrivilegeValue{
			Key:        key,
			Privileges: make(map[Privilege]map[GrantedPrivilege]bool),
		}
		globalDatabase.typePrivileges.Data[key] = value
	}
	privilegeMap, ok := value.Privileges[privilege.Privilege]
	if !ok {
		privilegeMap = make(map[GrantedPrivilege]bool)
		value.Privileges[privilege.Privilege] = privilegeMap
	}
	privilegeMap[privilege] = withGrantOption
}

// HasTypePrivilege checks whether the user has the given privilege on the associated type.
func HasTypePrivilege(key TypePrivilegeKey, privilege Privilege) bool {
	if IsSuperUser(key.Role) {
		return true
	}
	if value, ok := globalDatabase.typePrivileges.Data[key]; ok {
		if privilegeMap, ok := value.Privileges[privilege]; ok && len(privilegeMap) > 0 {
			return true
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		if HasTypePrivilege(TypePrivilegeKey{Role: group, Schema: key.Schema, Name: key.Name}, privilege) {
			return true
		}
	}
	return false
}

// HasTypePrivilegeGrantOption checks whether the user has WITH GRANT OPTION for the given type privilege.
func HasTypePrivilegeGrantOption(key TypePrivilegeKey, privilege Privilege) RoleID {
	if IsSuperUser(key.Role) {
		return key.Role
	}
	if value, ok := globalDatabase.typePrivileges.Data[key]; ok {
		if privilegeMap, ok := value.Privileges[privilege]; ok {
			for _, withGrantOption := range privilegeMap {
				if withGrantOption {
					return key.Role
				}
			}
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		if returnedID := HasTypePrivilegeGrantOption(TypePrivilegeKey{Role: group, Schema: key.Schema, Name: key.Name}, privilege); returnedID.IsValid() {
			return returnedID
		}
	}
	return 0
}

// RoleHasTypePrivilege checks effective type privileges for a role. PostgreSQL
// gives PUBLIC implicit USAGE on newly-created types until the ACL is first
// materialized, usually by GRANT or REVOKE.
func RoleHasTypePrivilege(role Role, schema string, name string, owner string, privilege Privilege) bool {
	if RoleOwnsType(role, owner) {
		return true
	}
	if HasTypePrivilege(TypePrivilegeKey{Role: role.ID(), Schema: schema, Name: name}, privilege) {
		return true
	}
	publicRole := GetRole("public")
	if publicRole.IsValid() && HasTypePrivilege(TypePrivilegeKey{Role: publicRole.ID(), Schema: schema, Name: name}, privilege) {
		return true
	}
	return privilege == Privilege_USAGE && !hasExplicitTypePrivileges(schema, name)
}

// RoleHasTypePrivilegeGrantOption checks whether a role may grant or revoke a
// type privilege. Type owners have implicit grant option.
func RoleHasTypePrivilegeGrantOption(role Role, schema string, name string, owner string, privilege Privilege) RoleID {
	if RoleOwnsType(role, owner) {
		return role.ID()
	}
	return HasTypePrivilegeGrantOption(TypePrivilegeKey{Role: role.ID(), Schema: schema, Name: name}, privilege)
}

// RoleOwnsType returns whether the role owns a type, treating empty owner
// metadata as PostgreSQL's bootstrap superuser.
func RoleOwnsType(role Role, owner string) bool {
	if role.IsSuperUser {
		return true
	}
	if owner == "" {
		owner = "postgres"
	}
	return owner == role.Name
}

// EnsureTypeDefaultPrivileges materializes PostgreSQL's default type ACL for
// a type that has not yet had an explicit ACL. This makes later GRANT/REVOKE
// operations visible in pg_type.typacl without showing default ACLs on
// untouched types.
func EnsureTypeDefaultPrivileges(schema string, name string, owner string) {
	if hasExplicitTypePrivileges(schema, name) {
		return
	}
	if owner == "" {
		owner = "postgres"
	}
	ownerRole := GetRole(owner)
	if !ownerRole.IsValid() {
		return
	}
	grant := GrantedPrivilege{
		Privilege: Privilege_USAGE,
		GrantedBy: ownerRole.ID(),
	}
	AddTypePrivilege(TypePrivilegeKey{Role: ownerRole.ID(), Schema: schema, Name: name}, grant, true)
	publicRole := GetRole("public")
	if publicRole.IsValid() {
		AddTypePrivilege(TypePrivilegeKey{Role: publicRole.ID(), Schema: schema, Name: name}, grant, false)
	}
}

func hasExplicitTypePrivileges(schema string, name string) bool {
	for key := range globalDatabase.typePrivileges.Data {
		if key.Schema == schema && key.Name == name {
			return true
		}
	}
	return false
}

// RemoveTypePrivilege removes the privilege from the global database.
func RemoveTypePrivilege(key TypePrivilegeKey, privilege GrantedPrivilege, grantOptionOnly bool) {
	if value, ok := globalDatabase.typePrivileges.Data[key]; ok {
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
			delete(globalDatabase.typePrivileges.Data, key)
		}
	}
}

// RemoveAllTypePrivileges removes explicit privilege entries for a dropped
// type. Schema-wide entries for all types are left intact.
func RemoveAllTypePrivileges(schema, typ string) {
	for key := range globalDatabase.typePrivileges.Data {
		if key.Schema == schema && key.Name == typ {
			delete(globalDatabase.typePrivileges.Data, key)
		}
	}
}

// RenameTypePrivileges renames all explicit privilege entries for a type.
func RenameTypePrivileges(oldSchema, oldType, newSchema, newType string) {
	var renamed []TypePrivilegeValue
	for key, value := range globalDatabase.typePrivileges.Data {
		if key.Schema != oldSchema || key.Name != oldType {
			continue
		}
		delete(globalDatabase.typePrivileges.Data, key)
		key.Schema = newSchema
		key.Name = newType
		value.Key = key
		renamed = append(renamed, value)
	}
	for _, value := range renamed {
		globalDatabase.typePrivileges.Data[value.Key] = value
	}
}

func (tp *TypePrivileges) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(tp.Data)))
	for _, value := range tp.Data {
		writer.Uint64(uint64(value.Key.Role))
		writer.String(value.Key.Schema)
		writer.String(value.Key.Name)
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

func (tp *TypePrivileges) deserialize(version uint32, reader *utils.Reader) {
	tp.Data = make(map[TypePrivilegeKey]TypePrivilegeValue)
	switch version {
	case 0:
	case 1:
		dataCount := reader.Uint64()
		for dataIdx := uint64(0); dataIdx < dataCount; dataIdx++ {
			value := TypePrivilegeValue{Privileges: make(map[Privilege]map[GrantedPrivilege]bool)}
			value.Key.Role = RoleID(reader.Uint64())
			value.Key.Schema = reader.String()
			value.Key.Name = reader.String()
			privilegeCount := reader.Uint64()
			for privilegeIdx := uint64(0); privilegeIdx < privilegeCount; privilegeIdx++ {
				privilege := Privilege(reader.String())
				grantedCount := reader.Uint32()
				grantedMap := make(map[GrantedPrivilege]bool)
				for grantedIdx := uint32(0); grantedIdx < grantedCount; grantedIdx++ {
					grantedPrivilege := GrantedPrivilege{}
					grantedPrivilege.Privilege = privilege
					grantedPrivilege.GrantedBy = RoleID(reader.Uint64())
					grantedMap[grantedPrivilege] = reader.Bool()
				}
				value.Privileges[privilege] = grantedMap
			}
			tp.Data[value.Key] = value
		}
	default:
		panic("unexpected version in TypePrivileges")
	}
}
