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

// RemovePrivilegesForRole removes explicit ACL entries that reference roleID
// as either grantee or grantor. Callers must hold the auth write lock.
func RemovePrivilegesForRole(roleID RoleID) {
	if !roleID.IsValid() {
		return
	}
	removeDatabasePrivilegesForRole(roleID)
	removeSchemaPrivilegesForRole(roleID)
	removeTablePrivilegesForRole(roleID)
	removeSequencePrivilegesForRole(roleID)
	removeRoutinePrivilegesForRole(roleID)
	removeTypePrivilegesForRole(roleID)
	removeLanguagePrivilegesForRole(roleID)
	removeParameterPrivilegesForRole(roleID)
	removeDefaultPrivilegesForRole(roleID)
}

func removeDatabasePrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.databasePrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.databasePrivileges.Data, key)
		}
	}
}

func removeSchemaPrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.schemaPrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.schemaPrivileges.Data, key)
		}
	}
}

func removeTablePrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.tablePrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.tablePrivileges.Data, key)
		}
	}
}

func removeSequencePrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.sequencePrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.sequencePrivileges.Data, key)
		}
	}
}

func removeRoutinePrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.routinePrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.routinePrivileges.Data, key)
		}
	}
}

func removeTypePrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.typePrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.typePrivileges.Data, key)
		}
	}
}

func removeLanguagePrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.languagePrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.languagePrivileges.Data, key)
		}
	}
}

func removeParameterPrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.parameterPrivileges.Data {
		if key.Role == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.parameterPrivileges.Data, key)
		}
	}
}

func removeDefaultPrivilegesForRole(roleID RoleID) {
	for key, value := range globalDatabase.defaultPrivileges.Data {
		if key.Owner == roleID || key.Grantee == roleID || removePrivilegesGrantedByRole(value.Privileges, roleID) {
			delete(globalDatabase.defaultPrivileges.Data, key)
		}
	}
}

func removePrivilegesGrantedByRole(privileges map[Privilege]map[GrantedPrivilege]bool, roleID RoleID) bool {
	for privilege, grantedPrivileges := range privileges {
		for grantedPrivilege := range grantedPrivileges {
			if grantedPrivilege.GrantedBy == roleID {
				delete(grantedPrivileges, grantedPrivilege)
			}
		}
		if len(grantedPrivileges) == 0 {
			delete(privileges, privilege)
		}
	}
	return len(privileges) == 0
}
