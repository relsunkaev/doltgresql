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
)

// TableACLItems returns explicit relation-level ACL entries for a table.
func TableACLItems(schema string, table string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.tablePrivileges.Data {
			if key.Column == "" && tableNameMatches(key.Table, schema, table) {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// ColumnACLItems returns explicit column-level ACL entries for a table column.
func ColumnACLItems(schema string, table string, column string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.tablePrivileges.Data {
			if key.Column == column && tableNameMatches(key.Table, schema, table) {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// SequenceACLItems returns explicit ACL entries for a sequence.
func SequenceACLItems(schema string, sequence string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.sequencePrivileges.Data {
			if key.Schema == schema && key.Name == sequence {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// SchemaACLItems returns explicit ACL entries for a schema.
func SchemaACLItems(schema string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.schemaPrivileges.Data {
			if key.Schema == schema {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// DatabaseACLItems returns explicit ACL entries for a database.
func DatabaseACLItems(database string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.databasePrivileges.Data {
			if key.Name == database {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// RoutineACLItems returns explicit ACL entries for a routine.
func RoutineACLItems(schema string, routine string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.routinePrivileges.Data {
			if key.Schema == schema && key.Name == routine {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// LanguageACLItems returns explicit ACL entries for a language.
func LanguageACLItems(language string) []string {
	language = languageKey(language)
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.languagePrivileges.Data {
			if key.Name == language {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

// TypeACLItems returns explicit ACL entries for a type.
func TypeACLItems(schema string, typeName string) []string {
	var items []string
	LockRead(func() {
		for key, value := range globalDatabase.typePrivileges.Data {
			if key.Schema == schema && key.Name == typeName {
				items = append(items, aclItemsForRolePrivileges(key.Role, value.Privileges)...)
			}
		}
	})
	return sortedACLItems(items)
}

func tableNameMatches(table doltdb.TableName, schema string, name string) bool {
	if table.Schema != schema {
		return false
	}
	return table.Name == name || table.Name == ""
}

func aclItemsForRolePrivileges(roleID RoleID, privileges map[Privilege]map[GrantedPrivilege]bool) []string {
	role, ok := globalDatabase.rolesByID[roleID]
	if !ok {
		return nil
	}
	items := make([]string, 0, len(privileges))
	for privilege, grantedPrivileges := range privileges {
		for grantedPrivilege := range grantedPrivileges {
			grantorName := "postgres"
			if grantor, ok := globalDatabase.rolesByID[grantedPrivilege.GrantedBy]; ok {
				grantorName = grantor.Name
			}
			items = append(items, role.Name+"="+privilege.ACLAbbreviation()+"/"+grantorName)
		}
	}
	return items
}

func sortedACLItems(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	sort.Strings(items)
	return items
}
