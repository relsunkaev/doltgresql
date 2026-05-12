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
	"strings"

	"github.com/dolthub/doltgresql/utils"
)

// ParameterPrivileges contains privileges given to a role on a configuration parameter.
type ParameterPrivileges struct {
	Data map[ParameterPrivilegeKey]ParameterPrivilegeValue
}

// ParameterPrivilegeKey points to a configuration parameter.
type ParameterPrivilegeKey struct {
	Role RoleID
	Name string
}

// ParameterPrivilegeValue is the value associated with the ParameterPrivilegeKey.
type ParameterPrivilegeValue struct {
	Key        ParameterPrivilegeKey
	Privileges map[Privilege]map[GrantedPrivilege]bool
}

// NewParameterPrivileges returns a new *ParameterPrivileges.
func NewParameterPrivileges() *ParameterPrivileges {
	return &ParameterPrivileges{make(map[ParameterPrivilegeKey]ParameterPrivilegeValue)}
}

// AddParameterPrivilege adds the given parameter privilege to the global database.
func AddParameterPrivilege(key ParameterPrivilegeKey, privilege GrantedPrivilege, withGrantOption bool) {
	key.Name = parameterKey(key.Name)
	value, ok := globalDatabase.parameterPrivileges.Data[key]
	if !ok {
		value = ParameterPrivilegeValue{
			Key:        key,
			Privileges: make(map[Privilege]map[GrantedPrivilege]bool),
		}
		globalDatabase.parameterPrivileges.Data[key] = value
	}
	privilegeMap, ok := value.Privileges[privilege.Privilege]
	if !ok {
		privilegeMap = make(map[GrantedPrivilege]bool)
		value.Privileges[privilege.Privilege] = privilegeMap
	}
	privilegeMap[privilege] = withGrantOption
}

// HasParameterPrivilege checks whether the user has the given privilege on the associated parameter.
func HasParameterPrivilege(key ParameterPrivilegeKey, privilege Privilege) bool {
	key.Name = parameterKey(key.Name)
	if IsSuperUser(key.Role) {
		return true
	}
	if value, ok := globalDatabase.parameterPrivileges.Data[key]; ok {
		if privilegeMap, ok := value.Privileges[privilege]; ok && len(privilegeMap) > 0 {
			return true
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		if HasParameterPrivilege(ParameterPrivilegeKey{Role: group, Name: key.Name}, privilege) {
			return true
		}
	}
	return false
}

// HasParameterPrivilegeGrantOption checks whether the user has WITH GRANT OPTION for the given privilege.
func HasParameterPrivilegeGrantOption(key ParameterPrivilegeKey, privilege Privilege) RoleID {
	key.Name = parameterKey(key.Name)
	if IsSuperUser(key.Role) {
		return key.Role
	}
	if value, ok := globalDatabase.parameterPrivileges.Data[key]; ok {
		if privilegeMap, ok := value.Privileges[privilege]; ok {
			for _, withGrantOption := range privilegeMap {
				if withGrantOption {
					return key.Role
				}
			}
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		if returnedID := HasParameterPrivilegeGrantOption(ParameterPrivilegeKey{Role: group, Name: key.Name}, privilege); returnedID.IsValid() {
			return returnedID
		}
	}
	return 0
}

// RemoveParameterPrivilege removes the privilege from the global database.
func RemoveParameterPrivilege(key ParameterPrivilegeKey, privilege GrantedPrivilege, grantOptionOnly bool) {
	key.Name = parameterKey(key.Name)
	if value, ok := globalDatabase.parameterPrivileges.Data[key]; ok {
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
			delete(globalDatabase.parameterPrivileges.Data, key)
		}
	}
}

func parameterKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func (pp *ParameterPrivileges) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(pp.Data)))
	for _, value := range pp.Data {
		writer.Uint64(uint64(value.Key.Role))
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

func (pp *ParameterPrivileges) deserialize(version uint32, reader *utils.Reader) {
	pp.Data = make(map[ParameterPrivilegeKey]ParameterPrivilegeValue)
	switch version {
	case 0:
	case 1:
		dataCount := reader.Uint64()
		for dataIdx := uint64(0); dataIdx < dataCount; dataIdx++ {
			ppv := ParameterPrivilegeValue{Privileges: make(map[Privilege]map[GrantedPrivilege]bool)}
			ppv.Key.Role = RoleID(reader.Uint64())
			ppv.Key.Name = parameterKey(reader.String())
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
				ppv.Privileges[privilege] = grantedMap
			}
			pp.Data[ppv.Key] = ppv
		}
	default:
		panic("unexpected version in ParameterPrivileges")
	}
}
