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

// TablePrivilegeInfo describes one explicit table or column privilege entry.
type TablePrivilegeInfo struct {
	Grantee     string
	GranteeID   RoleID
	Table       doltdb.TableName
	Column      string
	Privilege   Privilege
	IsGrantable bool
	GrantorIDs  []RoleID
}

// RoutinePrivilegeInfo describes one explicit routine privilege entry.
type RoutinePrivilegeInfo struct {
	Grantor     string
	GrantorID   RoleID
	Grantee     string
	GranteeID   RoleID
	Schema      string
	Name        string
	ArgTypes    string
	Privilege   Privilege
	IsGrantable bool
}

// GetTablePrivilegeInfo returns explicit table and column grants in a deterministic order.
func GetTablePrivilegeInfo() []TablePrivilegeInfo {
	var infos []TablePrivilegeInfo
	LockRead(func() {
		roleNames := roleNamesByID()
		for _, value := range globalDatabase.tablePrivileges.Data {
			grantee, ok := roleNames[value.Key.Role]
			if !ok {
				continue
			}
			for privilege, grantedPrivileges := range value.Privileges {
				info := TablePrivilegeInfo{
					Grantee:   grantee,
					GranteeID: value.Key.Role,
					Table:     value.Key.Table,
					Column:    value.Key.Column,
					Privilege: privilege,
				}
				grantorIDs := make(map[RoleID]struct{}, len(grantedPrivileges))
				for grantedPrivilege, withGrantOption := range grantedPrivileges {
					if withGrantOption {
						info.IsGrantable = true
					}
					if grantedPrivilege.GrantedBy.IsValid() {
						grantorIDs[grantedPrivilege.GrantedBy] = struct{}{}
					}
				}
				for grantorID := range grantorIDs {
					info.GrantorIDs = append(info.GrantorIDs, grantorID)
				}
				sort.Slice(info.GrantorIDs, func(i, j int) bool {
					return info.GrantorIDs[i] < info.GrantorIDs[j]
				})
				infos = append(infos, info)
			}
		}
	})
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Grantee != infos[j].Grantee {
			return infos[i].Grantee < infos[j].Grantee
		}
		if infos[i].Table.Schema != infos[j].Table.Schema {
			return infos[i].Table.Schema < infos[j].Table.Schema
		}
		if infos[i].Table.Name != infos[j].Table.Name {
			return infos[i].Table.Name < infos[j].Table.Name
		}
		if infos[i].Column != infos[j].Column {
			return infos[i].Column < infos[j].Column
		}
		return infos[i].Privilege.String() < infos[j].Privilege.String()
	})
	return infos
}

// GetRoutinePrivilegeInfo returns explicit routine grants in a deterministic order.
func GetRoutinePrivilegeInfo() []RoutinePrivilegeInfo {
	var infos []RoutinePrivilegeInfo
	LockRead(func() {
		roleNames := roleNamesByID()
		for _, value := range globalDatabase.routinePrivileges.Data {
			grantee, ok := roleNames[value.Key.Role]
			if !ok {
				continue
			}
			for privilege, grantedPrivileges := range value.Privileges {
				for grantedPrivilege, withGrantOption := range grantedPrivileges {
					grantor := roleNames[grantedPrivilege.GrantedBy]
					infos = append(infos, RoutinePrivilegeInfo{
						Grantor:     grantor,
						GrantorID:   grantedPrivilege.GrantedBy,
						Grantee:     grantee,
						GranteeID:   value.Key.Role,
						Schema:      value.Key.Schema,
						Name:        value.Key.Name,
						ArgTypes:    value.Key.ArgTypes,
						Privilege:   privilege,
						IsGrantable: withGrantOption,
					})
				}
			}
		}
	})
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Grantee != infos[j].Grantee {
			return infos[i].Grantee < infos[j].Grantee
		}
		if infos[i].Schema != infos[j].Schema {
			return infos[i].Schema < infos[j].Schema
		}
		if infos[i].Name != infos[j].Name {
			return infos[i].Name < infos[j].Name
		}
		if infos[i].ArgTypes != infos[j].ArgTypes {
			return infos[i].ArgTypes < infos[j].ArgTypes
		}
		if infos[i].Privilege != infos[j].Privilege {
			return infos[i].Privilege.String() < infos[j].Privilege.String()
		}
		return infos[i].Grantor < infos[j].Grantor
	})
	return infos
}

// GetEnabledRoleIDs returns the named role, public, and inherited groups.
func GetEnabledRoleIDs(roleName string) (map[RoleID]struct{}, bool) {
	ids := make(map[RoleID]struct{})
	var superuser bool
	LockRead(func() {
		role := GetRole(roleName)
		if !role.IsValid() {
			return
		}
		ids[role.ID()] = struct{}{}
		superuser = role.IsSuperUser
		if public := GetRole("public"); public.IsValid() {
			ids[public.ID()] = struct{}{}
		}
		for _, groupID := range GetAllGroupsWithMember(role.ID(), true) {
			ids[groupID] = struct{}{}
		}
	})
	return ids, superuser
}

func roleNamesByID() map[RoleID]string {
	roleNames := make(map[RoleID]string, len(globalDatabase.rolesByID))
	for roleID, role := range globalDatabase.rolesByID {
		roleNames[roleID] = role.Name
	}
	return roleNames
}
