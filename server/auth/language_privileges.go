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

// LanguagePrivileges contains the privileges given to a role on a language.
type LanguagePrivileges struct {
	Data map[LanguagePrivilegeKey]LanguagePrivilegeValue
}

// LanguagePrivilegeKey points to a language object.
type LanguagePrivilegeKey struct {
	Role RoleID
	Name string
}

// LanguagePrivilegeValue is the value associated with the LanguagePrivilegeKey.
type LanguagePrivilegeValue struct {
	Key        LanguagePrivilegeKey
	Privileges map[Privilege]map[GrantedPrivilege]bool
}

// NewLanguagePrivileges returns a new *LanguagePrivileges.
func NewLanguagePrivileges() *LanguagePrivileges {
	return &LanguagePrivileges{make(map[LanguagePrivilegeKey]LanguagePrivilegeValue)}
}

// AddLanguagePrivilege adds the given language privilege to the global database.
func AddLanguagePrivilege(key LanguagePrivilegeKey, privilege GrantedPrivilege, withGrantOption bool) {
	key.Name = languageKey(key.Name)
	languagePrivilegeValue, ok := globalDatabase.languagePrivileges.Data[key]
	if !ok {
		languagePrivilegeValue = LanguagePrivilegeValue{
			Key:        key,
			Privileges: make(map[Privilege]map[GrantedPrivilege]bool),
		}
		globalDatabase.languagePrivileges.Data[key] = languagePrivilegeValue
	}
	privilegeMap, ok := languagePrivilegeValue.Privileges[privilege.Privilege]
	if !ok {
		privilegeMap = make(map[GrantedPrivilege]bool)
		languagePrivilegeValue.Privileges[privilege.Privilege] = privilegeMap
	}
	privilegeMap[privilege] = withGrantOption
}

// HasLanguagePrivilege checks whether the user has the given privilege on the associated language.
func HasLanguagePrivilege(key LanguagePrivilegeKey, privilege Privilege) bool {
	key.Name = languageKey(key.Name)
	if IsSuperUser(key.Role) {
		return true
	}
	if languagePrivilegeValue, ok := globalDatabase.languagePrivileges.Data[key]; ok {
		if privilegeMap, ok := languagePrivilegeValue.Privileges[privilege]; ok && len(privilegeMap) > 0 {
			return true
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		if HasLanguagePrivilege(LanguagePrivilegeKey{Role: group, Name: key.Name}, privilege) {
			return true
		}
	}
	return false
}

// HasLanguagePrivilegeGrantOption checks whether the user has WITH GRANT OPTION for the given privilege.
func HasLanguagePrivilegeGrantOption(key LanguagePrivilegeKey, privilege Privilege) RoleID {
	key.Name = languageKey(key.Name)
	if IsSuperUser(key.Role) {
		return key.Role
	}
	if languagePrivilegeValue, ok := globalDatabase.languagePrivileges.Data[key]; ok {
		if privilegeMap, ok := languagePrivilegeValue.Privileges[privilege]; ok {
			for _, withGrantOption := range privilegeMap {
				if withGrantOption {
					return key.Role
				}
			}
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		if returnedID := HasLanguagePrivilegeGrantOption(LanguagePrivilegeKey{Role: group, Name: key.Name}, privilege); returnedID.IsValid() {
			return returnedID
		}
	}
	return 0
}

// RemoveLanguagePrivilege removes the privilege from the global database.
func RemoveLanguagePrivilege(key LanguagePrivilegeKey, privilege GrantedPrivilege, grantOptionOnly bool) {
	key.Name = languageKey(key.Name)
	if languagePrivilegeValue, ok := globalDatabase.languagePrivileges.Data[key]; ok {
		if privilegeMap, ok := languagePrivilegeValue.Privileges[privilege.Privilege]; ok {
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
					delete(languagePrivilegeValue.Privileges, privilege.Privilege)
				}
			}
		}
		if len(languagePrivilegeValue.Privileges) == 0 {
			delete(globalDatabase.languagePrivileges.Data, key)
		}
	}
}

// RemoveAllLanguagePrivileges removes all privilege entries for a language.
func RemoveAllLanguagePrivileges(name string) {
	name = languageKey(name)
	for key := range globalDatabase.languagePrivileges.Data {
		if key.Name == name {
			delete(globalDatabase.languagePrivileges.Data, key)
		}
	}
}

// RenameLanguagePrivileges renames all privilege entries for a language.
func RenameLanguagePrivileges(oldName string, newName string) {
	oldName = languageKey(oldName)
	newName = languageKey(newName)
	for key, value := range globalDatabase.languagePrivileges.Data {
		if key.Name != oldName {
			continue
		}
		delete(globalDatabase.languagePrivileges.Data, key)
		key.Name = newName
		value.Key = key
		globalDatabase.languagePrivileges.Data[key] = value
	}
}

func (lp *LanguagePrivileges) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(lp.Data)))
	for _, value := range lp.Data {
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

func (lp *LanguagePrivileges) deserialize(version uint32, reader *utils.Reader) {
	lp.Data = make(map[LanguagePrivilegeKey]LanguagePrivilegeValue)
	switch version {
	case 0:
	case 1:
		dataCount := reader.Uint64()
		for dataIdx := uint64(0); dataIdx < dataCount; dataIdx++ {
			lpv := LanguagePrivilegeValue{Privileges: make(map[Privilege]map[GrantedPrivilege]bool)}
			lpv.Key.Role = RoleID(reader.Uint64())
			lpv.Key.Name = languageKey(reader.String())
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
				lpv.Privileges[privilege] = grantedMap
			}
			lp.Data[lpv.Key] = lpv
		}
	default:
		panic("unexpected version in LanguagePrivileges")
	}
}
