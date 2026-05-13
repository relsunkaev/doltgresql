// Copyright 2023 Dolthub, Inc.
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

package ast

import (
	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/privilege"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeGrant handles *tree.Grant nodes.
func nodeGrant(ctx *Context, node *tree.Grant) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	var grantTable *pgnodes.GrantTable
	var grantSchema *pgnodes.GrantSchema
	var grantDatabase *pgnodes.GrantDatabase
	var grantSequence *pgnodes.GrantSequence
	var grantRoutine *pgnodes.GrantRoutine
	var grantForeignDataWrapper *pgnodes.GrantForeignDataWrapper
	var grantForeignServer *pgnodes.GrantForeignServer
	var grantLanguage *pgnodes.GrantLanguage
	var grantType *pgnodes.GrantType
	var grantLargeObject *pgnodes.GrantLargeObject
	var grantParameter *pgnodes.GrantParameter
	switch node.Targets.TargetType {
	case privilege.Table:
		tables := make([]doltdb.TableName, 0, len(node.Targets.Tables)+len(node.Targets.InSchema))
		for _, table := range node.Targets.Tables {
			normalizedTable, err := table.NormalizeTablePattern()
			if err != nil {
				return nil, err
			}
			switch normalizedTable := normalizedTable.(type) {
			case *tree.TableName:
				if normalizedTable.ExplicitCatalog {
					return nil, errors.Errorf("granting privileges to other databases is not yet supported")
				}
				tables = append(tables, doltdb.TableName{
					Name:   string(normalizedTable.ObjectName),
					Schema: string(normalizedTable.SchemaName),
				})
			case *tree.AllTablesSelector:
				tables = append(tables, doltdb.TableName{
					Name:   "",
					Schema: string(normalizedTable.SchemaName),
				})
			default:
				return nil, errors.Errorf(`unexpected table type in GRANT: %T`, normalizedTable)
			}
		}
		for _, schema := range node.Targets.InSchema {
			tables = append(tables, doltdb.TableName{
				Name:   "",
				Schema: schema,
			})
		}
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_TABLE, node.Privileges)
		if err != nil {
			return nil, err
		}
		columnPrivileges, err := convertColumnPrivileges(node.PrivsWithCols)
		if err != nil {
			return nil, err
		}
		grantTable = &pgnodes.GrantTable{
			Privileges:       privileges,
			ColumnPrivileges: columnPrivileges,
			Tables:           tables,
		}
	case privilege.Schema:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_SCHEMA, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantSchema = &pgnodes.GrantSchema{
			Privileges: privileges,
			Schemas:    node.Targets.Names,
		}
	case privilege.Database:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_DATABASE, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantDatabase = &pgnodes.GrantDatabase{
			Privileges: privileges,
			Databases:  node.Targets.Databases.ToStrings(),
		}
	case privilege.Sequence:
		sequences := make([]auth.SequencePrivilegeKey, 0, len(node.Targets.Sequences)+len(node.Targets.InSchema))
		for _, seq := range node.Targets.Sequences {
			sequences = append(sequences, auth.SequencePrivilegeKey{
				Schema: sequenceSchema(seq),
				Name:   seq.Parts[0],
			})
		}
		for _, schema := range node.Targets.InSchema {
			sequences = append(sequences, auth.SequencePrivilegeKey{
				Schema: schema,
				Name:   "",
			})
		}
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_SEQUENCE, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantSequence = &pgnodes.GrantSequence{
			Privileges: privileges,
			Sequences:  sequences,
		}
	case privilege.Function, privilege.Procedure, privilege.Routine:
		routines := make([]auth.RoutinePrivilegeKey, 0, len(node.Targets.Routines)+len(node.Targets.InSchema))
		for _, r := range node.Targets.Routines {
			argTypes, err := routineArgTypesKey(ctx, r.Args)
			if err != nil {
				return nil, err
			}
			routines = append(routines, auth.RoutinePrivilegeKey{
				Schema:   routineSchema(r.Name),
				Name:     r.Name.Parts[0],
				ArgTypes: argTypes,
			})
		}
		for _, schema := range node.Targets.InSchema {
			routines = append(routines, auth.RoutinePrivilegeKey{
				Schema: schema,
				Name:   "",
			})
		}
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_FUNCTION, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantRoutine = &pgnodes.GrantRoutine{
			Privileges: privileges,
			Routines:   routines,
		}
	case privilege.ForeignDataWrapper:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_FOREIGN_DATA_WRAPPER, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantForeignDataWrapper = &pgnodes.GrantForeignDataWrapper{
			Privileges: privileges,
			Wrappers:   node.Targets.Names,
		}
	case privilege.ForeignServer:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_FOREIGN_SERVER, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantForeignServer = &pgnodes.GrantForeignServer{
			Privileges: privileges,
			Servers:    node.Targets.Names,
		}
	case privilege.Language:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_LANGUAGE, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantLanguage = &pgnodes.GrantLanguage{
			Privileges: privileges,
			Languages:  node.Targets.Names,
		}
	case privilege.Type:
		types := make([]auth.TypePrivilegeKey, 0, len(node.Targets.Types))
		for _, typ := range node.Targets.Types {
			types = append(types, auth.TypePrivilegeKey{
				Schema: typeSchema(typ),
				Name:   typ.Parts[0],
			})
		}
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_TYPE, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantType = &pgnodes.GrantType{
			Privileges: privileges,
			Types:      types,
		}
	case privilege.LargeObject:
		oids := make([]uint32, 0, len(node.Targets.LargeObjects))
		for _, expr := range node.Targets.LargeObjects {
			oid, err := commentLargeObjectOID(expr)
			if err != nil {
				return nil, err
			}
			oids = append(oids, oid)
		}
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_LARGE_OBJECT, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantLargeObject = &pgnodes.GrantLargeObject{
			Privileges: privileges,
			OIDs:       oids,
		}
	case privilege.Parameter:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_PARAMETER, node.Privileges)
		if err != nil {
			return nil, err
		}
		grantParameter = &pgnodes.GrantParameter{
			Privileges: privileges,
			Parameters: node.Targets.Names,
		}
	default:
		return nil, errors.Errorf("this form of GRANT is not yet supported")
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.Grant{
			GrantTable:              grantTable,
			GrantSchema:             grantSchema,
			GrantDatabase:           grantDatabase,
			GrantSequence:           grantSequence,
			GrantRoutine:            grantRoutine,
			GrantForeignDataWrapper: grantForeignDataWrapper,
			GrantForeignServer:      grantForeignServer,
			GrantLanguage:           grantLanguage,
			GrantType:               grantType,
			GrantLargeObject:        grantLargeObject,
			GrantParameter:          grantParameter,
			GrantRole:               nil,
			ToRoles:                 node.Grantees,
			WithGrantOption:         node.WithGrantOption,
			GrantedBy:               node.GrantedBy,
		},
		Children: nil,
	}, nil
}

func convertColumnPrivileges(privsWithCols []tree.PrivForCols) ([]pgnodes.GrantColumnPrivilege, error) {
	if len(privsWithCols) == 0 {
		return nil, nil
	}
	columnPrivileges := make([]pgnodes.GrantColumnPrivilege, 0, len(privsWithCols))
	for _, privWithCols := range privsWithCols {
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_TABLE_COLUMN, []privilege.Kind{privWithCols.Privilege})
		if err != nil {
			return nil, err
		}
		columns := privWithCols.ColNames.ToStrings()
		for _, priv := range privileges {
			columnPrivileges = append(columnPrivileges, pgnodes.GrantColumnPrivilege{
				Privilege: priv,
				Columns:   columns,
			})
		}
	}
	return columnPrivileges, nil
}

// sequenceSchema returns the schema portion of an UnresolvedObjectName for a sequence.
func sequenceSchema(name *tree.UnresolvedObjectName) string {
	if name.NumParts >= 2 {
		return name.Parts[1]
	}
	return ""
}

// routineSchema returns the schema portion of an UnresolvedObjectName for a routine.
func routineSchema(name *tree.UnresolvedObjectName) string {
	if name.NumParts >= 2 {
		return name.Parts[1]
	}
	return ""
}

// typeSchema returns the schema portion of an UnresolvedObjectName for a type.
func typeSchema(name *tree.UnresolvedObjectName) string {
	if name.NumParts >= 2 {
		return name.Parts[1]
	}
	return ""
}

// routineArgTypesKey builds a canonical string key from a RoutineArgs list using only the argument types.
func routineArgTypesKey(ctx *Context, args tree.RoutineArgs) (string, error) {
	types := make([]id.Type, len(args))
	for i, arg := range args {
		_, dt, err := nodeResolvableTypeReference(ctx, arg.Type, false)
		if err != nil {
			return "", err
		}
		if dt == nil {
			return "", errors.Errorf("routine argument type could not be resolved")
		}
		types[i] = dt.ID
	}
	return auth.RoutineArgTypesKey(types), nil
}

// convertPrivilegeKind converts a privilege from its parser representation to the server representation.
func convertPrivilegeKinds(object auth.PrivilegeObject, kinds []privilege.Kind) ([]auth.Privilege, error) {
	privileges := make([]auth.Privilege, len(kinds))
	for i, kind := range kinds {
		switch kind {
		case privilege.ALL:
			// If we encounter ALL, then we know to return all privileges for this object
			return object.AllPrivileges(), nil
		case privilege.ALTERSYSTEM:
			privileges[i] = auth.Privilege_ALTER_SYSTEM
		case privilege.CONNECT:
			privileges[i] = auth.Privilege_CONNECT
		case privilege.CREATE:
			privileges[i] = auth.Privilege_CREATE
		case privilege.DELETE:
			privileges[i] = auth.Privilege_DELETE
		case privilege.DROP:
			privileges[i] = auth.Privilege_DROP
		case privilege.EXECUTE:
			privileges[i] = auth.Privilege_EXECUTE
		case privilege.INSERT:
			privileges[i] = auth.Privilege_INSERT
		case privilege.MAINTAIN:
			privileges[i] = auth.Privilege_MAINTAIN
		case privilege.REFERENCES:
			privileges[i] = auth.Privilege_REFERENCES
		case privilege.SELECT:
			privileges[i] = auth.Privilege_SELECT
		case privilege.SET:
			privileges[i] = auth.Privilege_SET
		case privilege.TEMPORARY:
			privileges[i] = auth.Privilege_TEMPORARY
		case privilege.TRIGGER:
			privileges[i] = auth.Privilege_TRIGGER
		case privilege.TRUNCATE:
			privileges[i] = auth.Privilege_TRUNCATE
		case privilege.UPDATE:
			privileges[i] = auth.Privilege_UPDATE
		case privilege.USAGE:
			privileges[i] = auth.Privilege_USAGE
		default:
			// This shouldn't be possible unless we update our list of supported privileges
			return nil, errors.Errorf("unknown privilege kind: %v", kind)
		}
	}
	for _, p := range privileges {
		if !object.IsValid(p) {
			return nil, errors.Errorf("invalid privilege type %s for relation", p.String())
		}
	}
	return privileges, nil
}
