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

package node

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/largeobject"
)

// Grant handles all of the GRANT statements.
type Grant struct {
	GrantTable              *GrantTable
	GrantSchema             *GrantSchema
	GrantDatabase           *GrantDatabase
	GrantSequence           *GrantSequence
	GrantRoutine            *GrantRoutine
	GrantForeignDataWrapper *GrantForeignDataWrapper
	GrantForeignServer      *GrantForeignServer
	GrantLanguage           *GrantLanguage
	GrantType               *GrantType
	GrantLargeObject        *GrantLargeObject
	GrantParameter          *GrantParameter
	GrantRole               *GrantRole
	ToRoles                 []string
	WithGrantOption         bool // This is "WITH ADMIN OPTION" for GrantRole only
	GrantedBy               string
}

// GrantTable specifically handles the GRANT ... ON TABLE statement.
type GrantTable struct {
	Privileges       []auth.Privilege
	ColumnPrivileges []GrantColumnPrivilege
	Tables           []doltdb.TableName
}

// GrantColumnPrivilege handles a GRANT privilege(column, ...) entry.
type GrantColumnPrivilege struct {
	Privilege auth.Privilege
	Columns   []string
}

// GrantSchema specifically handles the GRANT ... ON SCHEMA statement.
type GrantSchema struct {
	Privileges []auth.Privilege
	Schemas    []string
}

// GrantDatabase specifically handles the GRANT ... ON DATABASE statement.
type GrantDatabase struct {
	Privileges []auth.Privilege
	Databases  []string
}

// GrantSequence specifically handles the GRANT ... ON SEQUENCE statement.
type GrantSequence struct {
	Privileges []auth.Privilege
	Sequences  []auth.SequencePrivilegeKey
}

// GrantRoutine specifically handles the GRANT ... ON FUNCTION/PROCEDURE/ROUTINE statement.
type GrantRoutine struct {
	Privileges []auth.Privilege
	Routines   []auth.RoutinePrivilegeKey
}

// GrantForeignDataWrapper specifically handles the GRANT ... ON FOREIGN DATA WRAPPER statement.
type GrantForeignDataWrapper struct {
	Privileges []auth.Privilege
	Wrappers   []string
}

// GrantForeignServer specifically handles the GRANT ... ON FOREIGN SERVER statement.
type GrantForeignServer struct {
	Privileges []auth.Privilege
	Servers    []string
}

// GrantLanguage specifically handles the GRANT ... ON LANGUAGE statement.
type GrantLanguage struct {
	Privileges []auth.Privilege
	Languages  []string
}

// GrantType specifically handles the GRANT ... ON TYPE statement.
type GrantType struct {
	Privileges []auth.Privilege
	Types      []auth.TypePrivilegeKey
}

// GrantLargeObject specifically handles the GRANT ... ON LARGE OBJECT statement.
type GrantLargeObject struct {
	Privileges []auth.Privilege
	OIDs       []uint32
}

// GrantParameter specifically handles the GRANT ... ON PARAMETER statement.
type GrantParameter struct {
	Privileges []auth.Privilege
	Parameters []string
}

// GrantRole specifically handles the GRANT <roles> TO <roles> statement.
type GrantRole struct {
	Groups []string
}

var _ sql.ExecSourceRel = (*Grant)(nil)
var _ vitess.Injectable = (*Grant)(nil)

// Children implements the interface sql.ExecSourceRel.
func (g *Grant) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (g *Grant) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (g *Grant) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (g *Grant) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	var err error
	auth.LockWrite(func() {
		switch {
		case g.GrantTable != nil:
			if err = g.grantTable(ctx); err != nil {
				return
			}
		case g.GrantSchema != nil:
			if err = g.grantSchema(ctx); err != nil {
				return
			}
		case g.GrantDatabase != nil:
			if err = g.grantDatabase(ctx); err != nil {
				return
			}
		case g.GrantSequence != nil:
			if err = g.grantSequence(ctx); err != nil {
				return
			}
		case g.GrantRoutine != nil:
			if err = g.grantRoutine(ctx); err != nil {
				return
			}
		case g.GrantForeignDataWrapper != nil:
			if err = g.grantForeignDataWrapper(ctx); err != nil {
				return
			}
		case g.GrantForeignServer != nil:
			if err = g.grantForeignServer(ctx); err != nil {
				return
			}
		case g.GrantLanguage != nil:
			if err = g.grantLanguage(ctx); err != nil {
				return
			}
		case g.GrantType != nil:
			if err = g.grantType(ctx); err != nil {
				return
			}
		case g.GrantLargeObject != nil:
			if err = g.grantLargeObject(ctx); err != nil {
				return
			}
		case g.GrantParameter != nil:
			if err = g.grantParameter(ctx); err != nil {
				return
			}
		case g.GrantRole != nil:
			if err = g.grantRole(ctx); err != nil {
				return
			}
		default:
			err = errors.Errorf("GRANT statement is not yet supported")
			return
		}
		err = auth.PersistChanges()
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements the interface sql.ExecSourceRel.
func (g *Grant) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (g *Grant) String() string {
	switch {
	case g.GrantTable != nil:
		return "GRANT TABLE"
	default:
		return "GRANT"
	}
}

// WithChildren implements the interface sql.ExecSourceRel.
func (g *Grant) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(g, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (g *Grant) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return g, nil
}

// common handles the initial logic for each GRANT statement. `roles` are the `ToRoles`. `userRole` is the role of the
// session's selected user.
func (g *Grant) common(ctx *sql.Context) (roles []auth.Role, userRole auth.Role, err error) {
	roles = make([]auth.Role, len(g.ToRoles))
	// First we'll verify that all of the roles exist
	for i, roleName := range g.ToRoles {
		roles[i] = auth.GetRole(roleName)
		if !roles[i].IsValid() {
			return nil, auth.Role{}, errors.Errorf(`role "%s" does not exist`, roleName)
		}
	}
	// Then we'll check that the role that is granting the privileges exists
	userRole = auth.GetRole(ctx.Client().User)
	if !userRole.IsValid() {
		return nil, auth.Role{}, errors.Errorf(`role "%s" does not exist`, ctx.Client().User)
	}
	if len(g.GrantedBy) != 0 {
		grantedByRole := auth.GetRole(g.GrantedBy)
		if !grantedByRole.IsValid() {
			return nil, auth.Role{}, errors.Errorf(`role "%s" does not exist`, g.GrantedBy)
		}
		if userRole.ID() != grantedByRole.ID() {
			// TODO: grab the actual error message
			return nil, auth.Role{}, errors.New("GRANTED BY may only be set to the calling user")
		}
	}
	return roles, userRole, nil
}

// grantTable handles *GrantTable from within RowIter.
func (g *Grant) grantTable(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	resolvedTables := make([]doltdb.TableName, len(g.GrantTable.Tables))
	for i, table := range g.GrantTable.Tables {
		schemaName, err := validateACLTableTarget(ctx, table)
		if err != nil {
			return err
		}
		resolvedTables[i] = doltdb.TableName{Name: table.Name, Schema: schemaName}
	}
	for _, role := range roles {
		for _, table := range resolvedTables {
			key := auth.TablePrivilegeKey{
				Role:  userRole.ID(),
				Table: table,
			}
			for _, privilege := range g.GrantTable.Privileges {
				grantedBy := auth.HasTablePrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					if auth.HasTablePrivilege(key, privilege) {
						ctx.Warn(0, "no privileges were granted for %s", table.Name)
						continue
					}
					// TODO: grab the actual error message
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddTablePrivilege(auth.TablePrivilegeKey{
					Role:  role.ID(),
					Table: table,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
			for _, columnPrivilege := range g.GrantTable.ColumnPrivileges {
				for _, column := range columnPrivilege.Columns {
					columnKey := auth.TablePrivilegeKey{
						Role:   userRole.ID(),
						Table:  table,
						Column: column,
					}
					grantedBy := auth.HasTablePrivilegeGrantOption(columnKey, columnPrivilege.Privilege)
					if !grantedBy.IsValid() {
						if auth.HasTablePrivilege(columnKey, columnPrivilege.Privilege) {
							ctx.Warn(0, "no privileges were granted for %s", table.Name)
							continue
						}
						// TODO: grab the actual error message
						return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
					}
					auth.AddTablePrivilege(auth.TablePrivilegeKey{
						Role:   role.ID(),
						Table:  table,
						Column: column,
					}, auth.GrantedPrivilege{
						Privilege: columnPrivilege.Privilege,
						GrantedBy: grantedBy,
					}, g.WithGrantOption)
				}
			}
		}
	}
	return nil
}

// grantSchema handles *GrantSchema from within RowIter.
func (g *Grant) grantSchema(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	for _, schema := range g.GrantSchema.Schemas {
		if err := validateACLSchemaTarget(ctx, schema); err != nil {
			return err
		}
	}
	for _, role := range roles {
		for _, schema := range g.GrantSchema.Schemas {
			key := auth.SchemaPrivilegeKey{
				Role:   userRole.ID(),
				Schema: schema,
			}
			for _, privilege := range g.GrantSchema.Privileges {
				grantedBy := auth.HasSchemaPrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					// TODO: grab the actual error message
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddSchemaPrivilege(auth.SchemaPrivilegeKey{
					Role:   role.ID(),
					Schema: schema,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

// grantDatabase handles *GrantDatabase from within RowIter.
func (g *Grant) grantDatabase(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	for _, database := range g.GrantDatabase.Databases {
		if err := validateACLDatabaseTarget(ctx, database); err != nil {
			return err
		}
	}
	for _, role := range roles {
		for _, database := range g.GrantDatabase.Databases {
			key := auth.DatabasePrivilegeKey{
				Role: userRole.ID(),
				Name: database,
			}
			for _, privilege := range g.GrantDatabase.Privileges {
				grantedBy := auth.HasDatabasePrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					// TODO: grab the actual error message
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddDatabasePrivilege(auth.DatabasePrivilegeKey{
					Role: role.ID(),
					Name: database,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

// grantSequence handles *GrantSequence from within RowIter.
func (g *Grant) grantSequence(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	resolvedSequences := make([]auth.SequencePrivilegeKey, len(g.GrantSequence.Sequences))
	for i, seq := range g.GrantSequence.Sequences {
		schemaName, err := validateACLSequenceTarget(ctx, seq)
		if err != nil {
			return err
		}
		resolvedSequences[i] = auth.SequencePrivilegeKey{
			Schema: schemaName,
			Name:   seq.Name,
		}
	}
	for _, role := range roles {
		for _, seq := range resolvedSequences {
			key := auth.SequencePrivilegeKey{
				Role:   userRole.ID(),
				Schema: seq.Schema,
				Name:   seq.Name,
			}
			for _, privilege := range g.GrantSequence.Privileges {
				grantedBy := auth.HasSequencePrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					// TODO: grab the actual error message
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddSequencePrivilege(auth.SequencePrivilegeKey{
					Role:   role.ID(),
					Schema: seq.Schema,
					Name:   seq.Name,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

// grantRoutine handles *GrantRoutine from within RowIter.
func (g *Grant) grantRoutine(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	resolvedRoutines := make([]auth.RoutinePrivilegeKey, len(g.GrantRoutine.Routines))
	for i, routine := range g.GrantRoutine.Routines {
		schemaName, err := validateACLRoutineTarget(ctx, routine)
		if err != nil {
			return err
		}
		resolvedRoutines[i] = auth.RoutinePrivilegeKey{
			Schema:   schemaName,
			Name:     routine.Name,
			ArgTypes: routine.ArgTypes,
		}
	}
	for _, role := range roles {
		for _, routine := range resolvedRoutines {
			key := auth.RoutinePrivilegeKey{
				Role:     userRole.ID(),
				Schema:   routine.Schema,
				Name:     routine.Name,
				ArgTypes: routine.ArgTypes,
			}
			for _, privilege := range g.GrantRoutine.Privileges {
				grantedBy := auth.HasRoutinePrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					// TODO: grab the actual error message
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddRoutinePrivilege(auth.RoutinePrivilegeKey{
					Role:     role.ID(),
					Schema:   routine.Schema,
					Name:     routine.Name,
					ArgTypes: routine.ArgTypes,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

// grantForeignDataWrapper handles *GrantForeignDataWrapper from within RowIter.
func (g *Grant) grantForeignDataWrapper(ctx *sql.Context) error {
	if _, _, err := g.common(ctx); err != nil {
		return err
	}
	for _, wrapper := range g.GrantForeignDataWrapper.Wrappers {
		if _, ok := auth.GetForeignDataWrapper(wrapper); !ok {
			return errors.Errorf(`foreign-data wrapper "%s" does not exist`, wrapper)
		}
	}
	return errors.Errorf("GRANT on foreign data wrappers is not yet supported")
}

// grantForeignServer handles *GrantForeignServer from within RowIter.
func (g *Grant) grantForeignServer(ctx *sql.Context) error {
	if _, _, err := g.common(ctx); err != nil {
		return err
	}
	for _, server := range g.GrantForeignServer.Servers {
		if _, ok := auth.GetForeignServer(server); !ok {
			return errors.Errorf(`server "%s" does not exist`, server)
		}
	}
	return errors.Errorf("GRANT on foreign servers is not yet supported")
}

// grantLanguage handles *GrantLanguage from within RowIter.
func (g *Grant) grantLanguage(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	for _, role := range roles {
		for _, language := range g.GrantLanguage.Languages {
			if _, ok := auth.GetLanguage(language); !ok {
				return errors.Errorf(`language "%s" does not exist`, language)
			}
			key := auth.LanguagePrivilegeKey{
				Role: userRole.ID(),
				Name: language,
			}
			for _, privilege := range g.GrantLanguage.Privileges {
				grantedBy := auth.HasLanguagePrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddLanguagePrivilege(auth.LanguagePrivilegeKey{
					Role: role.ID(),
					Name: language,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

func validateACLSchemaTarget(ctx *sql.Context, schema string) error {
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return err
	}
	if _, ok, err := schemaDatabase.GetSchema(ctx, schema); err != nil {
		return err
	} else if !ok {
		return errors.Errorf(`schema "%s" does not exist`, schema)
	}
	return nil
}

func resolveExistingACLSchema(ctx *sql.Context, schema string) (string, error) {
	schemaName, err := core.GetSchemaName(ctx, nil, schema)
	if err != nil {
		return "", err
	}
	if err = validateACLSchemaTarget(ctx, schemaName); err != nil {
		return "", err
	}
	return schemaName, nil
}

func validateACLDatabaseTarget(ctx *sql.Context, database string) error {
	db, err := core.GetSqlDatabaseFromContext(ctx, database)
	if err != nil {
		return err
	}
	if db == nil {
		return errors.Errorf(`database "%s" does not exist`, database)
	}
	return nil
}

func validateACLTableTarget(ctx *sql.Context, table doltdb.TableName) (string, error) {
	schemaName, err := resolveExistingACLSchema(ctx, table.Schema)
	if err != nil {
		return "", err
	}
	if table.Name == "" {
		return schemaName, nil
	}
	relationType, err := core.GetRelationType(ctx, schemaName, table.Name)
	if err != nil {
		return "", err
	}
	if relationType == core.RelationType_DoesNotExist {
		exists, err := aclViewExists(ctx, schemaName, table.Name)
		if err != nil {
			return "", err
		}
		if !exists {
			return "", errors.Errorf(`relation "%s" does not exist`, table.Name)
		}
	}
	return schemaName, nil
}

func aclViewExists(ctx *sql.Context, schemaName string, viewName string) (bool, error) {
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return false, err
	}
	schema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
	if err != nil || !ok {
		return false, err
	}
	viewDatabase, ok := schema.(sql.ViewDatabase)
	if !ok {
		return false, nil
	}
	_, exists, err := viewDatabase.GetViewDefinition(ctx, viewName)
	return exists, err
}

func validateACLSequenceTarget(ctx *sql.Context, seq auth.SequencePrivilegeKey) (string, error) {
	schemaName, err := resolveExistingACLSchema(ctx, seq.Schema)
	if err != nil {
		return "", err
	}
	if seq.Name == "" {
		return schemaName, nil
	}
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return "", err
	}
	if !collection.HasSequence(ctx, id.NewSequence(schemaName, seq.Name)) {
		return "", errors.Errorf(`sequence "%s" does not exist`, seq.Name)
	}
	return schemaName, nil
}

func validateACLRoutineTarget(ctx *sql.Context, routine auth.RoutinePrivilegeKey) (string, error) {
	schemaName, err := resolveExistingACLSchema(ctx, routine.Schema)
	if err != nil {
		return "", err
	}
	if routine.Name == "" {
		return schemaName, nil
	}
	exists, err := aclRoutineExists(ctx, schemaName, routine.Name, routine.ArgTypes)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", errors.Errorf(`routine "%s" does not exist`, routine.Name)
	}
	return schemaName, nil
}

func aclRoutineExists(ctx *sql.Context, schema string, name string, argTypes string) (bool, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return false, err
	}
	funcs, err := funcColl.GetFunctionOverloads(ctx, id.NewFunction(schema, name))
	if err != nil {
		return false, err
	}
	for _, fn := range funcs {
		if argTypes == "" || aclRoutineArgTypesKey(fn.ID.Parameters()) == argTypes {
			return true, nil
		}
	}

	procColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return false, err
	}
	procs, err := procColl.GetProcedureOverloads(ctx, id.NewProcedure(schema, name))
	if err != nil {
		return false, err
	}
	for _, proc := range procs {
		if argTypes == "" || aclRoutineArgTypesKey(proc.ID.Parameters()) == argTypes {
			return true, nil
		}
	}
	return false, nil
}

func aclRoutineArgTypesKey(argTypes []id.Type) string {
	parts := make([]string, len(argTypes))
	for i, argType := range argTypes {
		parts[i] = argType.TypeName()
	}
	return strings.Join(parts, ",")
}

// grantType handles *GrantType from within RowIter.
func (g *Grant) grantType(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return err
	}
	for _, role := range roles {
		for _, typ := range g.GrantType.Types {
			schemaName, err := core.GetSchemaName(ctx, nil, typ.Schema)
			if err != nil {
				return err
			}
			resolvedType, err := typeCollection.GetType(ctx, id.NewType(schemaName, typ.Name))
			if err != nil {
				return err
			}
			if resolvedType == nil {
				return errors.Errorf(`type "%s" does not exist`, typ.Name)
			}
			key := auth.TypePrivilegeKey{
				Role:   userRole.ID(),
				Schema: schemaName,
				Name:   typ.Name,
			}
			for _, privilege := range g.GrantType.Privileges {
				grantedBy := auth.HasTypePrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddTypePrivilege(auth.TypePrivilegeKey{
					Role:   role.ID(),
					Schema: schemaName,
					Name:   typ.Name,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

// grantLargeObject handles *GrantLargeObject from within RowIter.
func (g *Grant) grantLargeObject(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	database := ctx.GetCurrentDatabase()
	for _, oid := range g.GrantLargeObject.OIDs {
		owner, ok := largeobject.Owner(database, oid)
		if !ok {
			return errors.Errorf("large object %d does not exist", oid)
		}
		if !userRole.IsSuperUser && userRole.Name != owner {
			return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
		}
		for _, role := range roles {
			for _, privilege := range g.GrantLargeObject.Privileges {
				aclPrivilege := privilege.ACLAbbreviation()
				if g.WithGrantOption {
					aclPrivilege += "*"
				}
				item := strings.Join([]string{role.Name, "=", aclPrivilege, "/", userRole.Name}, "")
				largeobject.TrackMutation(uint32(ctx.Session.ID()))
				if err := largeobject.AddACLItem(database, oid, item); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// grantParameter handles *GrantParameter from within RowIter.
func (g *Grant) grantParameter(ctx *sql.Context) error {
	roles, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	for _, role := range roles {
		for _, parameter := range g.GrantParameter.Parameters {
			key := auth.ParameterPrivilegeKey{
				Role: userRole.ID(),
				Name: parameter,
			}
			for _, privilege := range g.GrantParameter.Privileges {
				grantedBy := auth.HasParameterPrivilegeGrantOption(key, privilege)
				if !grantedBy.IsValid() {
					return errors.Errorf(`role "%s" does not have permission to grant this privilege`, userRole.Name)
				}
				auth.AddParameterPrivilege(auth.ParameterPrivilegeKey{
					Role: role.ID(),
					Name: parameter,
				}, auth.GrantedPrivilege{
					Privilege: privilege,
					GrantedBy: grantedBy,
				}, g.WithGrantOption)
			}
		}
	}
	return nil
}

// grantRole handles *GrantRole from within RowIter.
func (g *Grant) grantRole(ctx *sql.Context) error {
	members, userRole, err := g.common(ctx)
	if err != nil {
		return err
	}
	groups := make([]auth.Role, len(g.GrantRole.Groups))
	for i, groupName := range g.GrantRole.Groups {
		groups[i] = auth.GetRole(groupName)
		if !groups[i].IsValid() {
			return errors.Errorf(`role "%s" does not exist`, groupName)
		}
	}
	for _, member := range members {
		for _, group := range groups {
			if member.ID() == group.ID() {
				return errors.New("role cannot be a member of itself")
			}
			// Superusers behave as members of every role for privilege checks, but
			// that virtual membership is not a catalog edge and does not make
			// granting a superuser role circular.
			if groupID, _, _ := auth.IsRoleAMember(group.ID(), member.ID()); groupID.IsValid() && !group.IsSuperUser {
				return errors.New("role memberships cannot be circular")
			}
			memberGroupID, _, withAdminOption := auth.IsRoleAMember(userRole.ID(), group.ID())
			if !memberGroupID.IsValid() || !withAdminOption {
				// TODO: grab the actual error message
				return errors.Errorf(`role "%s" does not have permission to grant role "%s"`, userRole.Name, group.Name)
			}
			auth.AddMemberToGroup(member.ID(), group.ID(), g.WithGrantOption, memberGroupID)
		}
	}
	return nil
}
