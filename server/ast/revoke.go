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

	"github.com/dolthub/doltgresql/postgres/parser/privilege"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeRevoke handles *tree.Revoke nodes.
func nodeRevoke(ctx *Context, node *tree.Revoke) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	var revokeTable *pgnodes.RevokeTable
	var revokeSchema *pgnodes.RevokeSchema
	var revokeDatabase *pgnodes.RevokeDatabase
	var revokeSequence *pgnodes.RevokeSequence
	var revokeRoutine *pgnodes.RevokeRoutine
	var revokeType *pgnodes.RevokeType
	var revokeForeignDataWrapper *pgnodes.RevokeForeignDataWrapper
	var revokeForeignServer *pgnodes.RevokeForeignServer
	var revokeLanguage *pgnodes.RevokeLanguage
	var revokeParameter *pgnodes.RevokeParameter
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
					return nil, errors.Errorf("revoking privileges from other databases is not yet supported")
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
				return nil, errors.Errorf(`unexpected table type in REVOKE: %T`, normalizedTable)
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
		revokeTable = &pgnodes.RevokeTable{
			Privileges: privileges,
			Tables:     tables,
		}
	case privilege.Schema:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_SCHEMA, node.Privileges)
		if err != nil {
			return nil, err
		}
		revokeSchema = &pgnodes.RevokeSchema{
			Privileges: privileges,
			Schemas:    node.Targets.Names,
		}
	case privilege.Database:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_DATABASE, node.Privileges)
		if err != nil {
			return nil, err
		}
		revokeDatabase = &pgnodes.RevokeDatabase{
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
		revokeSequence = &pgnodes.RevokeSequence{
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
		revokeRoutine = &pgnodes.RevokeRoutine{
			Privileges: privileges,
			Routines:   routines,
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
		revokeType = &pgnodes.RevokeType{
			Privileges: privileges,
			Types:      types,
		}
	case privilege.ForeignDataWrapper:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_FOREIGN_DATA_WRAPPER, node.Privileges)
		if err != nil {
			return nil, err
		}
		revokeForeignDataWrapper = &pgnodes.RevokeForeignDataWrapper{
			Privileges: privileges,
			Wrappers:   node.Targets.Names,
		}
	case privilege.ForeignServer:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_FOREIGN_SERVER, node.Privileges)
		if err != nil {
			return nil, err
		}
		revokeForeignServer = &pgnodes.RevokeForeignServer{
			Privileges: privileges,
			Servers:    node.Targets.Names,
		}
	case privilege.Language:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_LANGUAGE, node.Privileges)
		if err != nil {
			return nil, err
		}
		revokeLanguage = &pgnodes.RevokeLanguage{
			Privileges: privileges,
			Languages:  node.Targets.Names,
		}
	case privilege.Parameter:
		privileges, err := convertPrivilegeKinds(auth.PrivilegeObject_PARAMETER, node.Privileges)
		if err != nil {
			return nil, err
		}
		revokeParameter = &pgnodes.RevokeParameter{
			Privileges: privileges,
			Parameters: node.Targets.Names,
		}
	default:
		return nil, errors.Errorf("this form of REVOKE is not yet supported")
	}
	return vitess.InjectedStatement{
		Statement: &pgnodes.Revoke{
			RevokeTable:              revokeTable,
			RevokeSchema:             revokeSchema,
			RevokeDatabase:           revokeDatabase,
			RevokeSequence:           revokeSequence,
			RevokeRoutine:            revokeRoutine,
			RevokeType:               revokeType,
			RevokeForeignDataWrapper: revokeForeignDataWrapper,
			RevokeForeignServer:      revokeForeignServer,
			RevokeLanguage:           revokeLanguage,
			RevokeParameter:          revokeParameter,
			RevokeRole:               nil,
			FromRoles:                node.Grantees,
			GrantedBy:                node.GrantedBy,
			GrantOptionFor:           node.GrantOptionFor,
			Cascade:                  node.DropBehavior == tree.DropCascade,
		},
		Children: nil,
	}, nil
}
