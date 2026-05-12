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

package functions

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initPrivilegeInquiry() {
	framework.RegisterFunction(has_table_privilege_text_text_text)
	framework.RegisterFunction(has_column_privilege_text_text_text_text)
	framework.RegisterFunction(has_any_column_privilege_text_text_text)
	framework.RegisterFunction(has_type_privilege_text_text)
	framework.RegisterFunction(has_database_privilege_text_text_text)
	framework.RegisterFunction(has_schema_privilege_text_text_text)
	framework.RegisterFunction(has_sequence_privilege_text_text_text)
	framework.RegisterFunction(has_function_privilege_text_text_text)
	framework.RegisterFunction(pg_get_acl_regclass_regclass_int32)
	framework.RegisterFunction(has_parameter_privilege_text_text)
	framework.RegisterFunction(has_parameter_privilege_text_text_text)
}

var has_table_privilege_text_text_text = framework.Function3{
	Name:       "has_table_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, table any, privilege any) (any, error) {
		return hasTablePrivilege(ctx, role.(string), table.(string), "", privilege.(string))
	},
}

var has_column_privilege_text_text_text_text = framework.Function4{
	Name:       "has_column_privilege",
	Return:     pgtypes.Bool,
	Parameters: [4]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [5]*pgtypes.DoltgresType, role any, table any, column any, privilege any) (any, error) {
		return hasTablePrivilege(ctx, role.(string), table.(string), column.(string), privilege.(string))
	},
}

var has_any_column_privilege_text_text_text = framework.Function3{
	Name:       "has_any_column_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, table any, privilege any) (any, error) {
		authPrivilege, err := privilegeByName(privilege.(string))
		if err != nil {
			return false, err
		}
		schemaName, relationName, err := splitSchemaObjectName(ctx, table.(string))
		if err != nil {
			return false, err
		}
		return roleHasPrivilege(role.(string), func(roleID auth.RoleID) bool {
			return auth.HasAnyColumnPrivilege(auth.TablePrivilegeKey{
				Role:  roleID,
				Table: doltdb.TableName{Name: relationName, Schema: schemaName},
			}, authPrivilege)
		}), nil
	},
}

var has_type_privilege_text_text = framework.Function2{
	Name:       "has_type_privilege",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, typ any, privilege any) (any, error) {
		authPrivilege, err := privilegeByName(privilege.(string))
		if err != nil {
			return false, err
		}
		return authPrivilege == auth.Privilege_USAGE, nil
	},
}

var has_database_privilege_text_text_text = framework.Function3{
	Name:       "has_database_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, database any, privilege any) (any, error) {
		authPrivilege, err := privilegeByName(privilege.(string))
		if err != nil {
			return false, err
		}
		databaseName := database.(string)
		return roleHasPrivilege(role.(string), func(roleID auth.RoleID) bool {
			return auth.HasDatabasePrivilege(auth.DatabasePrivilegeKey{
				Role: roleID,
				Name: databaseName,
			}, authPrivilege)
		}), nil
	},
}

var has_schema_privilege_text_text_text = framework.Function3{
	Name:       "has_schema_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, schema any, privilege any) (any, error) {
		authPrivilege, err := privilegeByName(privilege.(string))
		if err != nil {
			return false, err
		}
		schemaName := schema.(string)
		return roleHasPrivilege(role.(string), func(roleID auth.RoleID) bool {
			return auth.HasSchemaPrivilege(auth.SchemaPrivilegeKey{
				Role:   roleID,
				Schema: schemaName,
			}, authPrivilege)
		}), nil
	},
}

var has_sequence_privilege_text_text_text = framework.Function3{
	Name:       "has_sequence_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, sequence any, privilege any) (any, error) {
		authPrivilege, err := privilegeByName(privilege.(string))
		if err != nil {
			return false, err
		}
		schemaName, sequenceName, err := splitSchemaObjectName(ctx, sequence.(string))
		if err != nil {
			return false, err
		}
		return roleHasPrivilege(role.(string), func(roleID auth.RoleID) bool {
			return auth.HasSequencePrivilege(auth.SequencePrivilegeKey{
				Role:   roleID,
				Schema: schemaName,
				Name:   sequenceName,
			}, authPrivilege)
		}), nil
	},
}

var has_function_privilege_text_text_text = framework.Function3{
	Name:       "has_function_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, routine any, privilege any) (any, error) {
		authPrivilege, err := privilegeByName(privilege.(string))
		if err != nil {
			return false, err
		}
		schemaName, routineName, err := splitSchemaObjectName(ctx, routineNameWithoutArgs(routine.(string)))
		if err != nil {
			return false, err
		}
		return roleHasPrivilege(role.(string), func(roleID auth.RoleID) bool {
			return auth.HasRoutinePrivilege(auth.RoutinePrivilegeKey{
				Role:   roleID,
				Schema: schemaName,
				Name:   routineName,
			}, authPrivilege)
		}), nil
	},
}

var has_parameter_privilege_text_text = framework.Function2{
	Name:       "has_parameter_privilege",
	Return:     pgtypes.Bool,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, parameter any, privilege any) (any, error) {
		return hasParameterPrivilege(ctx.Client().User, parameter.(string), privilege.(string))
	},
}

var has_parameter_privilege_text_text_text = framework.Function3{
	Name:       "has_parameter_privilege",
	Return:     pgtypes.Bool,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, role any, parameter any, privilege any) (any, error) {
		return hasParameterPrivilege(role.(string), parameter.(string), privilege.(string))
	},
}

var pg_get_acl_regclass_regclass_int32 = framework.Function3{
	Name:       "pg_get_acl",
	Return:     pgtypes.Text,
	Parameters: [3]*pgtypes.DoltgresType{pgtypes.Regclass, pgtypes.Regclass, pgtypes.Int32},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, classOID any, objectOID any, subID any) (any, error) {
		return "{}", nil
	},
}

func hasTablePrivilege(ctx *sql.Context, roleName, tableName, columnName, privilegeName string) (bool, error) {
	authPrivilege, err := privilegeByName(privilegeName)
	if err != nil {
		return false, err
	}
	schemaName, relationName, err := splitSchemaObjectName(ctx, tableName)
	if err != nil {
		return false, err
	}
	return roleHasPrivilege(roleName, func(roleID auth.RoleID) bool {
		return auth.HasTablePrivilege(auth.TablePrivilegeKey{
			Role:   roleID,
			Table:  doltdb.TableName{Name: relationName, Schema: schemaName},
			Column: columnName,
		}, authPrivilege)
	}), nil
}

func hasParameterPrivilege(roleName, parameterName, privilegeName string) (bool, error) {
	authPrivilege, err := privilegeByName(privilegeName)
	if err != nil {
		return false, err
	}
	return roleHasPrivilege(roleName, func(roleID auth.RoleID) bool {
		return auth.HasParameterPrivilege(auth.ParameterPrivilegeKey{
			Role: roleID,
			Name: parameterName,
		}, authPrivilege)
	}), nil
}

func roleHasPrivilege(roleName string, check func(auth.RoleID) bool) bool {
	var hasPrivilege bool
	auth.LockRead(func() {
		role := auth.GetRole(roleName)
		public := auth.GetRole("public")
		hasPrivilege = (role.IsValid() && check(role.ID())) || (public.IsValid() && check(public.ID()))
	})
	return hasPrivilege
}

func splitSchemaObjectName(ctx *sql.Context, name string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(name), ".")
	objectName := unquoteIdentifier(parts[len(parts)-1])
	if len(parts) >= 2 {
		return unquoteIdentifier(parts[len(parts)-2]), objectName, nil
	}
	schemaName, err := core.GetSchemaName(ctx, nil, "")
	return schemaName, objectName, err
}

func unquoteIdentifier(name string) string {
	return strings.Trim(strings.TrimSpace(name), `"`)
}

func routineNameWithoutArgs(name string) string {
	if idx := strings.IndexByte(name, '('); idx >= 0 {
		return strings.TrimSpace(name[:idx])
	}
	return name
}

func privilegeByName(name string) (auth.Privilege, error) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SELECT":
		return auth.Privilege_SELECT, nil
	case "INSERT":
		return auth.Privilege_INSERT, nil
	case "UPDATE":
		return auth.Privilege_UPDATE, nil
	case "DELETE":
		return auth.Privilege_DELETE, nil
	case "TRUNCATE":
		return auth.Privilege_TRUNCATE, nil
	case "REFERENCES":
		return auth.Privilege_REFERENCES, nil
	case "TRIGGER":
		return auth.Privilege_TRIGGER, nil
	case "CREATE":
		return auth.Privilege_CREATE, nil
	case "CONNECT":
		return auth.Privilege_CONNECT, nil
	case "TEMPORARY", "TEMP":
		return auth.Privilege_TEMPORARY, nil
	case "EXECUTE":
		return auth.Privilege_EXECUTE, nil
	case "USAGE":
		return auth.Privilege_USAGE, nil
	case "SET":
		return auth.Privilege_SET, nil
	case "ALTER SYSTEM":
		return auth.Privilege_ALTER_SYSTEM, nil
	case "DROP":
		return auth.Privilege_DROP, nil
	default:
		return "", errors.Errorf(`unrecognized privilege type: "%s"`, name)
	}
}
