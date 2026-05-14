// Copyright 2024 Dolthub, Inc.
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

// TablePrivileges contains the privileges given to a role on a table.
type TablePrivileges struct {
	Data map[TablePrivilegeKey]TablePrivilegeValue
}

// TablePrivilegeKey points to a specific table object.
type TablePrivilegeKey struct {
	Role  RoleID
	Table doltdb.TableName
	// Column is empty for relation-level privileges. A non-empty value stores
	// column-level GRANT state for the same table.
	Column string
}

// TablePrivilegeValue is the value associated with the TablePrivilegeKey.
type TablePrivilegeValue struct {
	Key        TablePrivilegeKey
	Privileges map[Privilege]map[GrantedPrivilege]bool
}

// NewTablePrivileges returns a new *TablePrivileges.
func NewTablePrivileges() *TablePrivileges {
	return &TablePrivileges{make(map[TablePrivilegeKey]TablePrivilegeValue)}
}

// AddTablePrivilege adds the given table privilege to the global database.
func AddTablePrivilege(key TablePrivilegeKey, privilege GrantedPrivilege, withGrantOption bool) {
	tablePrivilegeValue, ok := globalDatabase.tablePrivileges.Data[key]
	if !ok {
		tablePrivilegeValue = TablePrivilegeValue{
			Key:        key,
			Privileges: make(map[Privilege]map[GrantedPrivilege]bool),
		}
		globalDatabase.tablePrivileges.Data[key] = tablePrivilegeValue
	}
	privilegeMap, ok := tablePrivilegeValue.Privileges[privilege.Privilege]
	if !ok {
		privilegeMap = make(map[GrantedPrivilege]bool)
		tablePrivilegeValue.Privileges[privilege.Privilege] = privilegeMap
	}
	privilegeMap[privilege] = withGrantOption
}

// HasTablePrivilege checks whether the user has the given privilege on the associated table.
func HasTablePrivilege(key TablePrivilegeKey, privilege Privilege) bool {
	if IsSuperUser(key.Role) {
		return true
	}
	if privilege == Privilege_SELECT && HasInheritedRole(key.Role, "pg_read_all_data") {
		return true
	}
	if isWriteAllDataTablePrivilege(privilege) && HasInheritedRole(key.Role, "pg_write_all_data") {
		return true
	}
	if len(key.Column) > 0 {
		tableKey := key
		tableKey.Column = ""
		if HasTablePrivilege(tableKey, privilege) {
			return true
		}
	}
	// If a table name was provided, then we also want to search for privileges provided to all tables in the schema
	// space. Since those are saved with an empty table name, we can easily do another search by removing the table.
	if len(key.Table.Name) > 0 && len(key.Column) == 0 {
		allTablesKey := key
		allTablesKey.Table.Name = ""
		if ok := HasTablePrivilege(allTablesKey, privilege); ok {
			return true
		}
	}
	if tablePrivilegeValue, ok := globalDatabase.tablePrivileges.Data[key]; ok {
		if privilegeMap, ok := tablePrivilegeValue.Privileges[privilege]; ok && len(privilegeMap) > 0 {
			return true
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		groupKey := key
		groupKey.Role = group
		if HasTablePrivilege(groupKey, privilege) {
			return true
		}
	}
	return false
}

func isWriteAllDataTablePrivilege(privilege Privilege) bool {
	switch privilege {
	case Privilege_INSERT, Privilege_UPDATE, Privilege_DELETE, Privilege_TRUNCATE:
		return true
	default:
		return false
	}
}

// HasAnyColumnPrivilege checks whether the user has the given privilege on any column in the associated table.
func HasAnyColumnPrivilege(key TablePrivilegeKey, privilege Privilege) bool {
	if IsSuperUser(key.Role) {
		return true
	}
	tableKey := key
	tableKey.Column = ""
	if HasTablePrivilege(tableKey, privilege) {
		return true
	}
	for privilegeKey, tablePrivilegeValue := range globalDatabase.tablePrivileges.Data {
		if privilegeKey.Role != key.Role || privilegeKey.Column == "" {
			continue
		}
		if !tableNameMatches(privilegeKey.Table, key.Table.Schema, key.Table.Name) {
			continue
		}
		if privilegeMap, ok := tablePrivilegeValue.Privileges[privilege]; ok && len(privilegeMap) > 0 {
			return true
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		groupKey := key
		groupKey.Role = group
		if HasAnyColumnPrivilege(groupKey, privilege) {
			return true
		}
	}
	return false
}

// HasTablePrivilegeGrantOption checks whether the user has WITH GRANT OPTION for the given privilege on the associated
// table. Returns the role that has WITH GRANT OPTION, or an invalid role if WITH GRANT OPTION is not available.
func HasTablePrivilegeGrantOption(key TablePrivilegeKey, privilege Privilege) RoleID {
	if IsSuperUser(key.Role) {
		return key.Role
	}
	if len(key.Column) > 0 {
		tableKey := key
		tableKey.Column = ""
		if returnedID := HasTablePrivilegeGrantOption(tableKey, privilege); returnedID.IsValid() {
			return returnedID
		}
	}
	// If a table name was provided, then we also want to search for privileges provided to all tables in the schema
	// space. Since those are saved with an empty table name, we can easily do another search by removing the table.
	if len(key.Table.Name) > 0 && len(key.Column) == 0 {
		allTablesKey := key
		allTablesKey.Table.Name = ""
		if returnedID := HasTablePrivilegeGrantOption(allTablesKey, privilege); returnedID.IsValid() {
			return returnedID
		}
	}
	if tablePrivilegeValue, ok := globalDatabase.tablePrivileges.Data[key]; ok {
		if privilegeMap, ok := tablePrivilegeValue.Privileges[privilege]; ok {
			for _, withGrantOption := range privilegeMap {
				if withGrantOption {
					return key.Role
				}
			}
		}
	}
	for _, group := range GetAllGroupsWithMember(key.Role, true) {
		groupKey := key
		groupKey.Role = group
		if returnedID := HasTablePrivilegeGrantOption(groupKey, privilege); returnedID.IsValid() {
			return returnedID
		}
	}
	return 0
}

// HasDependentTablePrivilege returns whether key.Role has granted the same table privilege to another role.
func HasDependentTablePrivilege(key TablePrivilegeKey, privilege Privilege) bool {
	for dependentKey, tablePrivilegeValue := range globalDatabase.tablePrivileges.Data {
		if dependentKey.Table != key.Table || dependentKey.Column != key.Column {
			continue
		}
		privilegeMap, ok := tablePrivilegeValue.Privileges[privilege]
		if !ok {
			continue
		}
		for grantedPrivilege := range privilegeMap {
			if grantedPrivilege.GrantedBy == key.Role {
				return true
			}
		}
	}
	return false
}

// RemoveTablePrivilege removes the privilege from the global database. If `grantOptionOnly` is true, then only the WITH
// GRANT OPTION portion is revoked. If `grantOptionOnly` is false, then the full privilege is removed. If the GrantedBy
// field contains a valid RoleID, then only the privilege associated with that granter is removed. Otherwise, the
// privilege is completely removed for the grantee.
func RemoveTablePrivilege(key TablePrivilegeKey, privilege GrantedPrivilege, grantOptionOnly bool) {
	if tablePrivilegeValue, ok := globalDatabase.tablePrivileges.Data[key]; ok {
		if privilegeMap, ok := tablePrivilegeValue.Privileges[privilege.Privilege]; ok {
			if grantOptionOnly {
				// This is provided when we only want to revoke the WITH GRANT OPTION, and not the privilege itself.
				// If a role is provided in GRANTED BY, then we specifically delete the option associated with that role.
				// If no role was given, then we'll remove WITH GRANT OPTION from all of the associated roles.
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
				// If a role is provided in GRANTED BY, then we specifically delete the privilege associated with that role.
				// If no role was given, then we'll delete the privileges granted by all roles.
				if privilege.GrantedBy.IsValid() {
					delete(privilegeMap, privilege)
				} else {
					privilegeMap = nil
				}
				if len(privilegeMap) == 0 {
					delete(tablePrivilegeValue.Privileges, privilege.Privilege)
				}
			}
		}
		if len(tablePrivilegeValue.Privileges) == 0 {
			delete(globalDatabase.tablePrivileges.Data, key)
		}
	}
}

// RenameTablePrivileges renames all explicit privilege entries for a table.
func RenameTablePrivileges(oldTable doltdb.TableName, newTable doltdb.TableName) {
	var renamed []TablePrivilegeValue
	for key, value := range globalDatabase.tablePrivileges.Data {
		if key.Table.Name == "" || key.Table != oldTable {
			continue
		}
		delete(globalDatabase.tablePrivileges.Data, key)
		key.Table = newTable
		value.Key = key
		renamed = append(renamed, value)
	}
	for _, value := range renamed {
		globalDatabase.tablePrivileges.Data[value.Key] = value
	}
}

// RenameTableSchemaPrivileges moves table ACL entries when a schema is renamed.
func RenameTableSchemaPrivileges(oldSchema string, newSchema string) {
	var renamed []TablePrivilegeValue
	for key, value := range globalDatabase.tablePrivileges.Data {
		if key.Table.Schema != oldSchema {
			continue
		}
		delete(globalDatabase.tablePrivileges.Data, key)
		key.Table.Schema = newSchema
		value.Key = key
		renamed = append(renamed, value)
	}
	for _, value := range renamed {
		globalDatabase.tablePrivileges.Data[value.Key] = value
	}
}

// RenameTableColumnPrivileges renames explicit column-level privilege entries for a table column.
func RenameTableColumnPrivileges(table doltdb.TableName, oldColumn string, newColumn string) {
	var renamed []TablePrivilegeValue
	for key, value := range globalDatabase.tablePrivileges.Data {
		if key.Table != table || key.Column != oldColumn {
			continue
		}
		delete(globalDatabase.tablePrivileges.Data, key)
		key.Column = newColumn
		value.Key = key
		renamed = append(renamed, value)
	}
	for _, value := range renamed {
		globalDatabase.tablePrivileges.Data[value.Key] = value
	}
}

// serialize writes the TablePrivileges to the given writer.
func (tp *TablePrivileges) serialize(writer *utils.Writer) {
	// Version 0
	// Write the total number of values
	writer.Uint64(uint64(len(tp.Data)))
	for _, value := range tp.Data {
		// Write the key
		writer.Uint64(uint64(value.Key.Role))
		writer.String(value.Key.Table.Name)
		writer.String(value.Key.Table.Schema)
		writer.String(value.Key.Column)
		// Write the total number of privileges
		writer.Uint64(uint64(len(value.Privileges)))
		for privilege, privilegeMap := range value.Privileges {
			writer.String(string(privilege))
			// Write the number of granted privileges
			writer.Uint32(uint32(len(privilegeMap)))
			for grantedPrivilege, withGrantOption := range privilegeMap {
				writer.Uint64(uint64(grantedPrivilege.GrantedBy))
				writer.Bool(withGrantOption)
			}
		}
	}
}

// deserialize reads the TablePrivileges from the given reader.
func (tp *TablePrivileges) deserialize(version uint32, reader *utils.Reader) {
	tp.Data = make(map[TablePrivilegeKey]TablePrivilegeValue)
	switch version {
	case 0, 1, 2:
		// Read the total number of values
		dataCount := reader.Uint64()
		for dataIdx := uint64(0); dataIdx < dataCount; dataIdx++ {
			// Read the key
			tpv := TablePrivilegeValue{Privileges: make(map[Privilege]map[GrantedPrivilege]bool)}
			tpv.Key.Role = RoleID(reader.Uint64())
			tpv.Key.Table.Name = reader.String()
			tpv.Key.Table.Schema = reader.String()
			if version >= 2 {
				tpv.Key.Column = reader.String()
			}
			// Read the total number of privileges
			privilegeCount := reader.Uint64()
			for privilegeIdx := uint64(0); privilegeIdx < privilegeCount; privilegeIdx++ {
				privilege := Privilege(reader.String())
				// Read the number of granted privileges
				grantedCount := reader.Uint32()
				grantedMap := make(map[GrantedPrivilege]bool)
				for grantedIdx := uint32(0); grantedIdx < grantedCount; grantedIdx++ {
					grantedPrivilege := GrantedPrivilege{}
					grantedPrivilege.Privilege = privilege
					grantedPrivilege.GrantedBy = RoleID(reader.Uint64())
					grantedMap[grantedPrivilege] = reader.Bool()
				}
				tpv.Privileges[privilege] = grantedMap
			}
			tp.Data[tpv.Key] = tpv
		}
	default:
		panic("unexpected version in TablePrivileges")
	}
}
