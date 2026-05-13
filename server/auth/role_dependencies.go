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

// RoleHasDependencies returns whether any auth metadata still references the
// role. Callers must hold the auth lock.
func RoleHasDependencies(role Role) bool {
	if !role.IsValid() {
		return false
	}
	roleID := role.ID()
	roleName := role.Name
	return roleOwnsAuthObject(roleName) ||
		roleHasExplicitPrivileges(roleID) ||
		roleHasDefaultPrivileges(roleID) ||
		roleHasMembershipDependencies(roleID) ||
		roleHasScopedSettings(roleName)
}

func roleOwnsAuthObject(roleName string) bool {
	if roleName == "" {
		return false
	}
	for _, metadata := range globalDatabase.databaseMetadata.Data {
		if metadata.Owner == roleName {
			return true
		}
	}
	for _, owner := range globalDatabase.schemaOwners.Data {
		if owner == roleName {
			return true
		}
	}
	for _, owner := range globalDatabase.relationOwners.Data {
		if owner == roleName {
			return true
		}
	}
	for _, language := range globalDatabase.languages.Data {
		if language.Owner == roleName {
			return true
		}
	}
	return false
}

func roleHasExplicitPrivileges(roleID RoleID) bool {
	for key, value := range globalDatabase.databasePrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.schemaPrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.tablePrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.sequencePrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.routinePrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.typePrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.languagePrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	for key, value := range globalDatabase.parameterPrivileges.Data {
		if key.Role == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	return false
}

func roleHasDefaultPrivileges(roleID RoleID) bool {
	for key, value := range globalDatabase.defaultPrivileges.Data {
		if key.Owner == roleID || key.Grantee == roleID || privilegeMapReferencesRole(roleID, value.Privileges) {
			return true
		}
	}
	return false
}

func roleHasMembershipDependencies(roleID RoleID) bool {
	for member, groupMap := range globalDatabase.roleMembership.Data {
		if member == roleID {
			return true
		}
		for group, membership := range groupMap {
			if group == roleID || membership.Member == roleID || membership.Group == roleID || membership.GrantedBy == roleID {
				return true
			}
		}
	}
	return false
}

func roleHasScopedSettings(roleName string) bool {
	if roleName == "" {
		return false
	}
	for key := range globalDatabase.dbRoleSettings.Data {
		if key.Role == roleName {
			return true
		}
	}
	return false
}

func privilegeMapReferencesRole(roleID RoleID, privileges map[Privilege]map[GrantedPrivilege]bool) bool {
	for _, grants := range privileges {
		for grantedPrivilege := range grants {
			if grantedPrivilege.GrantedBy == roleID {
				return true
			}
		}
	}
	return false
}
