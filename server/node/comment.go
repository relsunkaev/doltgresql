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

package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/accessmethod"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/indexmetadata"
	"github.com/dolthub/doltgresql/server/largeobject"
	"github.com/dolthub/doltgresql/server/settings"
)

type CommentTargetKind string

const (
	CommentTargetTable    CommentTargetKind = "table"
	CommentTargetColumn   CommentTargetKind = "column"
	CommentTargetView     CommentTargetKind = "view"
	CommentTargetSeq      CommentTargetKind = "sequence"
	CommentTargetSchema   CommentTargetKind = "schema"
	CommentTargetFunc     CommentTargetKind = "function"
	CommentTargetProc     CommentTargetKind = "procedure"
	CommentTargetRoutine  CommentTargetKind = "routine"
	CommentTargetType     CommentTargetKind = "type"
	CommentTargetDomain   CommentTargetKind = "domain"
	CommentTargetLang     CommentTargetKind = "language"
	CommentTargetDB       CommentTargetKind = "database"
	CommentTargetRole     CommentTargetKind = "role"
	CommentTargetExt      CommentTargetKind = "extension"
	CommentTargetAM       CommentTargetKind = "access method"
	CommentTargetPub      CommentTargetKind = "publication"
	CommentTargetSub      CommentTargetKind = "subscription"
	CommentTargetTSConfig CommentTargetKind = "text search configuration"
	CommentTargetTSDict   CommentTargetKind = "text search dictionary"
	CommentTargetTSParser CommentTargetKind = "text search parser"
	CommentTargetTSTmpl   CommentTargetKind = "text search template"
	CommentTargetIndex    CommentTargetKind = "index"
	CommentTargetConstr   CommentTargetKind = "constraint"
	CommentTargetTrigger  CommentTargetKind = "trigger"
	CommentTargetColl     CommentTargetKind = "collation"
	CommentTargetOperator CommentTargetKind = "operator"
	CommentTargetPolicy   CommentTargetKind = "policy"
	CommentTargetLargeObj CommentTargetKind = "large object"
	CommentTargetTblspace CommentTargetKind = "tablespace"
)

type Comment struct {
	Kind        CommentTargetKind
	Relation    vitess.TableName
	Column      string
	Name        string
	SchemaName  string
	TableName   string
	LeftType    id.Type
	RightType   id.Type
	OID         uint32
	Routine     *RoutineWithParams
	Description *string
}

var _ vitess.Injectable = Comment{}
var _ sql.ExecSourceRel = Comment{}

func NewCommentOnTable(relation vitess.TableName, description *string) Comment {
	return Comment{Kind: CommentTargetTable, Relation: relation, Description: description}
}

func NewCommentOnView(relation vitess.TableName, description *string) Comment {
	return Comment{Kind: CommentTargetView, Relation: relation, Description: description}
}

func NewCommentOnSequence(relation vitess.TableName, description *string) Comment {
	return Comment{Kind: CommentTargetSeq, Relation: relation, Description: description}
}

func NewCommentOnSchema(name string, description *string) Comment {
	return Comment{Kind: CommentTargetSchema, Name: name, Description: description}
}

func NewCommentOnFunction(routine *RoutineWithParams, description *string) Comment {
	return Comment{Kind: CommentTargetFunc, Routine: routine, Description: description}
}

func NewCommentOnProcedure(routine *RoutineWithParams, description *string) Comment {
	return Comment{Kind: CommentTargetProc, Routine: routine, Description: description}
}

func NewCommentOnRoutine(routine *RoutineWithParams, description *string) Comment {
	return Comment{Kind: CommentTargetRoutine, Routine: routine, Description: description}
}

func NewCommentOnType(relation vitess.TableName, description *string) Comment {
	return Comment{Kind: CommentTargetType, Relation: relation, Description: description}
}

func NewCommentOnDomain(relation vitess.TableName, description *string) Comment {
	return Comment{Kind: CommentTargetDomain, Relation: relation, Description: description}
}

func NewCommentOnLanguage(name string, description *string) Comment {
	return Comment{Kind: CommentTargetLang, Name: name, Description: description}
}

func NewCommentOnDatabase(name string, description *string) Comment {
	return Comment{Kind: CommentTargetDB, Name: name, Description: description}
}

func NewCommentOnRole(name string, description *string) Comment {
	return Comment{Kind: CommentTargetRole, Name: name, Description: description}
}

func NewCommentOnExtension(name string, description *string) Comment {
	return Comment{Kind: CommentTargetExt, Name: name, Description: description}
}

func NewCommentOnAccessMethod(name string, description *string) Comment {
	return Comment{Kind: CommentTargetAM, Name: name, Description: description}
}

func NewCommentOnPublication(name string, description *string) Comment {
	return Comment{Kind: CommentTargetPub, Name: name, Description: description}
}

func NewCommentOnSubscription(name string, description *string) Comment {
	return Comment{Kind: CommentTargetSub, Name: name, Description: description}
}

func NewCommentOnTextSearchConfiguration(name string, description *string) Comment {
	return Comment{Kind: CommentTargetTSConfig, Name: name, Description: description}
}

func NewCommentOnTextSearchDictionary(name string, description *string) Comment {
	return Comment{Kind: CommentTargetTSDict, Name: name, Description: description}
}

func NewCommentOnTextSearchParser(name string, description *string) Comment {
	return Comment{Kind: CommentTargetTSParser, Name: name, Description: description}
}

func NewCommentOnTextSearchTemplate(name string, description *string) Comment {
	return Comment{Kind: CommentTargetTSTmpl, Name: name, Description: description}
}

func NewCommentOnIndex(schemaName string, tableName string, indexName string, description *string) Comment {
	return Comment{Kind: CommentTargetIndex, SchemaName: schemaName, TableName: tableName, Name: indexName, Description: description}
}

func NewCommentOnConstraint(relation vitess.TableName, name string, description *string) Comment {
	return Comment{Kind: CommentTargetConstr, Relation: relation, Name: name, Description: description}
}

func NewCommentOnTrigger(relation vitess.TableName, name string, description *string) Comment {
	return Comment{Kind: CommentTargetTrigger, Relation: relation, Name: name, Description: description}
}

func NewCommentOnCollation(name string, description *string) Comment {
	return Comment{Kind: CommentTargetColl, Name: name, Description: description}
}

func NewCommentOnOperator(name string, leftType id.Type, rightType id.Type, description *string) Comment {
	return Comment{Kind: CommentTargetOperator, Name: name, LeftType: leftType, RightType: rightType, Description: description}
}

func NewCommentOnPolicy(relation vitess.TableName, name string, description *string) Comment {
	return Comment{Kind: CommentTargetPolicy, Relation: relation, Name: name, Description: description}
}

func NewCommentOnLargeObject(oid uint32, description *string) Comment {
	return Comment{Kind: CommentTargetLargeObj, OID: oid, Description: description}
}

func NewCommentOnTablespace(name string, description *string) Comment {
	return Comment{Kind: CommentTargetTblspace, Name: name, Description: description}
}

func NewCommentOnColumn(relation vitess.TableName, column string, description *string) Comment {
	return Comment{Kind: CommentTargetColumn, Relation: relation, Column: column, Description: description}
}

func (c Comment) Resolved() bool { return true }

func (c Comment) String() string { return "COMMENT ON " + string(c.Kind) }

func (c Comment) Schema(ctx *sql.Context) sql.Schema { return nil }

func (c Comment) Children() []sql.Node { return nil }

func (c Comment) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 0)
	}
	return c, nil
}

func (c Comment) IsReadOnly() bool { return false }

func (c Comment) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

func (c Comment) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	key, err := c.commentKey(ctx)
	if err != nil {
		return nil, err
	}
	if err = c.checkPrivileges(ctx); err != nil {
		return nil, err
	}
	comments.Set(key, c.Description)
	return sql.RowsToRowIter(), nil
}

func (c Comment) commentKey(ctx *sql.Context) (comments.Key, error) {
	switch c.Kind {
	case CommentTargetSchema:
		oid, err := resolveCommentSchema(ctx, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_namespace", 0), nil
	case CommentTargetFunc:
		oid, err := resolveCommentFunction(ctx, c.Routine)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_proc", 0), nil
	case CommentTargetProc:
		oid, err := resolveCommentProcedure(ctx, c.Routine)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_proc", 0), nil
	case CommentTargetRoutine:
		oid, err := resolveCommentRoutine(ctx, c.Routine)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_proc", 0), nil
	case CommentTargetType, CommentTargetDomain:
		oid, err := resolveCommentType(ctx, c.Relation)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_type", 0), nil
	case CommentTargetLang:
		oid, err := resolveCommentLanguage(c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_language", 0), nil
	case CommentTargetDB:
		oid, err := resolveCommentDatabase(ctx, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_database", 0), nil
	case CommentTargetRole:
		oid, err := resolveCommentRole(c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_authid", 0), nil
	case CommentTargetExt:
		oid, err := resolveCommentExtension(ctx, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_extension", 0), nil
	case CommentTargetAM:
		oid, err := resolveCommentAccessMethod(c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_am", 0), nil
	case CommentTargetPub:
		oid, err := resolveCommentPublication(ctx, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_publication", 0), nil
	case CommentTargetSub:
		oid, err := resolveCommentSubscription(ctx, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_subscription", 0), nil
	case CommentTargetTSConfig:
		oid, err := resolveCommentTextSearchConfig(c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_ts_config", 0), nil
	case CommentTargetTSDict:
		oid, err := resolveBuiltInTextSearchObject(c.Name, id.Section_TextSearchDictionary, "dictionary", "simple")
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_ts_dict", 0), nil
	case CommentTargetTSParser:
		oid, err := resolveBuiltInTextSearchObject(c.Name, id.Section_TextSearchParser, "parser", "default")
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_ts_parser", 0), nil
	case CommentTargetTSTmpl:
		oid, err := resolveBuiltInTextSearchObject(c.Name, id.Section_TextSearchTemplate, "template", "simple")
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_ts_template", 0), nil
	case CommentTargetIndex:
		oid, err := resolveCommentIndex(ctx, c.SchemaName, c.TableName, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_class", 0), nil
	case CommentTargetConstr:
		oid, err := resolveCommentConstraint(ctx, c.Relation, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_constraint", 0), nil
	case CommentTargetTrigger:
		oid, err := resolveCommentTrigger(ctx, c.Relation, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_trigger", 0), nil
	case CommentTargetColl:
		oid, err := resolveCommentCollation(ctx, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_collation", 0), nil
	case CommentTargetOperator:
		oid, err := resolveCommentOperator(c.Name, c.LeftType, c.RightType)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_operator", 0), nil
	case CommentTargetPolicy:
		oid, err := resolveCommentPolicy(ctx, c.Relation, c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_policy", 0), nil
	case CommentTargetLargeObj:
		oid, err := resolveCommentLargeObject(c.OID)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_largeobject_metadata", 0), nil
	case CommentTargetTblspace:
		oid, err := resolveCommentTablespace(c.Name)
		if err != nil {
			return comments.Key{}, err
		}
		return commentObjectKey(oid, "pg_tablespace", 0), nil
	}

	relationOID, schema, err := c.resolveObjectID(ctx)
	if err != nil {
		return comments.Key{}, err
	}
	key := commentObjectKey(relationOID, "pg_class", 0)
	if c.Kind == CommentTargetColumn {
		idx := schema.IndexOfColName(c.Column)
		if idx < 0 {
			return comments.Key{}, errors.Errorf(`column "%s" of relation "%s" does not exist`, c.Column, c.Relation.Name.String())
		}
		key.ObjSubID = int32(idx + 1)
	}
	return key, nil
}

func (c Comment) checkPrivileges(ctx *sql.Context) error {
	if c.requiresSuperuser() {
		if commentUserIsSuperuser(ctx) {
			return nil
		}
		return c.privilegeError()
	}

	owner, err := c.owner(ctx)
	if err != nil {
		return err
	}
	if commentUserIsOwnerOrSuperuser(ctx, owner) {
		return nil
	}
	return c.privilegeError()
}

func (c Comment) requiresSuperuser() bool {
	switch c.Kind {
	case CommentTargetTSParser, CommentTargetTSTmpl, CommentTargetAM, CommentTargetTblspace:
		return true
	default:
		return false
	}
}

func (c Comment) privilegeError() error {
	switch c.Kind {
	case CommentTargetProc, CommentTargetRoutine, CommentTargetDomain, CommentTargetLang,
		CommentTargetExt, CommentTargetColl, CommentTargetOperator, CommentTargetTSConfig,
		CommentTargetTSDict:
		return errors.New("must be owner")
	case CommentTargetTSParser, CommentTargetTSTmpl, CommentTargetAM, CommentTargetTblspace:
		return errors.New("must be superuser")
	default:
		return errors.New("permission denied")
	}
}

func (c Comment) owner(ctx *sql.Context) (string, error) {
	switch c.Kind {
	case CommentTargetFunc:
		return commentFunctionOwner(ctx, c.Routine)
	case CommentTargetProc:
		return commentProcedureOwner(ctx, c.Routine)
	case CommentTargetRoutine:
		if owner, err := commentFunctionOwner(ctx, c.Routine); err == nil {
			return owner, nil
		}
		return commentProcedureOwner(ctx, c.Routine)
	case CommentTargetLang:
		var (
			lang auth.Language
			ok   bool
		)
		auth.LockRead(func() {
			lang, ok = auth.GetLanguage(c.Name)
		})
		if !ok {
			return "", errors.Errorf(`language "%s" does not exist`, c.Name)
		}
		return lang.Owner, nil
	default:
		owner, _ := auth.GetSuperUserAndPassword()
		if owner == "" {
			owner = "postgres"
		}
		return owner, nil
	}
}

func commentFunctionOwner(ctx *sql.Context, routine *RoutineWithParams) (string, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return "", err
	}
	funcID, err := resolveFunctionID(ctx, funcColl, routine)
	if err != nil {
		return "", err
	}
	function, err := funcColl.GetFunction(ctx, funcID)
	if err != nil {
		return "", err
	}
	if !function.ID.IsValid() {
		return "", fmt.Errorf(`function "%s" does not exist`, routine.RoutineName)
	}
	return function.Owner, nil
}

func commentProcedureOwner(ctx *sql.Context, routine *RoutineWithParams) (string, error) {
	procColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return "", err
	}
	schema, err := core.GetSchemaName(ctx, nil, routine.SchemaName)
	if err != nil {
		return "", err
	}
	procID := id.NewProcedure(schema, routine.RoutineName)
	if len(routine.Args) == 0 {
		procs, err := procColl.GetProcedureOverloads(ctx, procID)
		if err != nil {
			return "", err
		}
		if len(procs) == 1 {
			procID = procs[0].ID
		} else if len(procs) > 1 && !procColl.HasProcedure(ctx, procID) {
			return "", fmt.Errorf(`procedure name "%s" is not unique`, routine.RoutineName)
		}
	} else {
		argTypes := make([]id.Type, len(routine.Args))
		for i, arg := range routine.Args {
			argTypes[i] = arg.Type.ID
		}
		procID = id.NewProcedure(schema, routine.RoutineName, argTypes...)
	}
	procedure, err := procColl.GetProcedure(ctx, procID)
	if err != nil {
		return "", err
	}
	if !procedure.ID.IsValid() {
		return "", fmt.Errorf(`procedure "%s" does not exist`, routine.RoutineName)
	}
	return procedure.Owner, nil
}

func commentUserIsOwnerOrSuperuser(ctx *sql.Context, owner string) bool {
	if owner == "" || owner == ctx.Client().User {
		return true
	}
	return commentUserIsSuperuser(ctx)
}

func commentUserIsSuperuser(ctx *sql.Context) bool {
	var userRole auth.Role
	auth.LockRead(func() {
		userRole = auth.GetRole(ctx.Client().User)
	})
	return userRole.IsValid() && userRole.IsSuperUser
}

func commentObjectKey(objID id.Id, className string, objSubID int32) comments.Key {
	return comments.Key{
		ObjOID:   id.Cache().ToOID(objID),
		ClassOID: comments.ClassOID(className),
		ObjSubID: objSubID,
	}
}

func (c Comment) resolveObjectID(ctx *sql.Context) (id.Id, sql.Schema, error) {
	switch c.Kind {
	case CommentTargetView:
		oid, err := resolveCommentView(ctx, c.Relation)
		return oid, nil, err
	case CommentTargetSeq:
		oid, err := resolveCommentSequence(ctx, c.Relation)
		return oid, nil, err
	default:
		return resolveCommentRelation(ctx, c.Relation)
	}
}

func resolveCommentSchema(ctx *sql.Context, schemaName string) (id.Id, error) {
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return id.Null, err
	}
	if _, ok, err := schemaDatabase.GetSchema(ctx, schemaName); err != nil {
		return id.Null, err
	} else if !ok {
		return id.Null, fmt.Errorf(`schema "%s" does not exist`, schemaName)
	}
	return id.NewNamespace(schemaName).AsId(), nil
}

func resolveCommentFunction(ctx *sql.Context, routine *RoutineWithParams) (id.Id, error) {
	funcColl, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	funcID, err := resolveFunctionID(ctx, funcColl, routine)
	if err != nil {
		return id.Null, err
	}
	if !funcColl.HasFunction(ctx, funcID) {
		return id.Null, fmt.Errorf(`function "%s" does not exist`, routine.RoutineName)
	}
	return funcID.AsId(), nil
}

func resolveCommentProcedure(ctx *sql.Context, routine *RoutineWithParams) (id.Id, error) {
	procColl, err := core.GetProceduresCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	schema, err := core.GetSchemaName(ctx, nil, routine.SchemaName)
	if err != nil {
		return id.Null, err
	}
	procID := id.NewProcedure(schema, routine.RoutineName)
	if len(routine.Args) == 0 {
		procs, err := procColl.GetProcedureOverloads(ctx, procID)
		if err != nil {
			return id.Null, err
		}
		if len(procs) == 1 {
			procID = procs[0].ID
		} else if len(procs) > 1 && !procColl.HasProcedure(ctx, procID) {
			return id.Null, fmt.Errorf(`procedure name "%s" is not unique`, routine.RoutineName)
		}
	} else {
		argTypes := make([]id.Type, len(routine.Args))
		for i, arg := range routine.Args {
			argTypes[i] = arg.Type.ID
		}
		procID = id.NewProcedure(schema, routine.RoutineName, argTypes...)
	}
	if !procColl.HasProcedure(ctx, procID) {
		return id.Null, fmt.Errorf(`procedure "%s" does not exist`, routine.RoutineName)
	}
	return procID.AsId(), nil
}

func resolveCommentRoutine(ctx *sql.Context, routine *RoutineWithParams) (id.Id, error) {
	if oid, err := resolveCommentFunction(ctx, routine); err == nil {
		return oid, nil
	}
	return resolveCommentProcedure(ctx, routine)
}

func resolveCommentType(ctx *sql.Context, relation vitess.TableName) (id.Id, error) {
	typeName := relation.Name.String()
	searchSchemas, err := commentSearchSchemas(ctx, relation)
	if err != nil {
		return id.Null, err
	}
	typeCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	for _, schemaName := range searchSchemas {
		typeID := id.NewType(schemaName, typeName)
		if typeCollection.HasType(ctx, typeID) {
			return typeID.AsId(), nil
		}
	}
	return id.Null, fmt.Errorf(`type "%s" does not exist`, typeName)
}

func resolveCommentLanguage(name string) (id.Id, error) {
	var exists bool
	auth.LockRead(func() {
		_, exists = auth.GetLanguage(name)
	})
	if !exists {
		return id.Null, fmt.Errorf(`language "%s" does not exist`, name)
	}
	return id.NewId(id.Section_FunctionLanguage, name), nil
}

func resolveCommentDatabase(ctx *sql.Context, name string) (id.Id, error) {
	doltSession, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return id.Null, fmt.Errorf("expected Dolt session")
	}
	if _, err := doltSession.Provider().Database(ctx, name); err != nil {
		return id.Null, err
	}
	return id.NewDatabase(name).AsId(), nil
}

func resolveCommentRole(name string) (id.Id, error) {
	var exists bool
	auth.LockRead(func() {
		exists = auth.RoleExists(name)
	})
	if !exists {
		return id.Null, fmt.Errorf(`role "%s" does not exist`, name)
	}
	return id.NewId(id.Section_User, name), nil
}

func resolveCommentExtension(ctx *sql.Context, name string) (id.Id, error) {
	extCollection, err := core.GetExtensionsCollectionFromContext(ctx, "")
	if err != nil {
		return id.Null, err
	}
	extID := id.NewExtension(name)
	if !extCollection.HasLoadedExtension(ctx, extID) {
		return id.Null, fmt.Errorf(`extension "%s" does not exist`, name)
	}
	return extID.AsId(), nil
}

func resolveCommentAccessMethod(name string) (id.Id, error) {
	switch name {
	case "heap", "btree", "hash", "gist", "gin", "spgist", "brin":
		return id.NewAccessMethod(name).AsId(), nil
	}
	for _, entry := range accessmethod.Snapshot() {
		if entry.Name == name {
			return id.NewAccessMethod(name).AsId(), nil
		}
	}
	return id.Null, fmt.Errorf(`access method "%s" does not exist`, name)
}

func resolveCommentPublication(ctx *sql.Context, name string) (id.Id, error) {
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	pubID := id.NewPublication(name)
	if !collection.HasPublication(ctx, pubID) {
		return id.Null, fmt.Errorf(`publication "%s" does not exist`, name)
	}
	return pubID.AsId(), nil
}

func resolveCommentSubscription(ctx *sql.Context, name string) (id.Id, error) {
	collection, err := core.GetSubscriptionsCollectionFromContext(ctx)
	if err != nil {
		return id.Null, err
	}
	subID := id.NewSubscription(name)
	if !collection.HasSubscription(ctx, subID) {
		return id.Null, fmt.Errorf(`subscription "%s" does not exist`, name)
	}
	return subID.AsId(), nil
}

func resolveCommentTextSearchConfig(name string) (id.Id, error) {
	if name == "simple" {
		return id.NewId(id.Section_TextSearchConfig, "pg_catalog", name), nil
	}
	var found bool
	var foundID id.Id
	auth.LockRead(func() {
		for _, config := range auth.GetAllTextSearchConfigs() {
			if config.Name == name {
				found = true
				foundID = id.NewId(id.Section_TextSearchConfig, config.Namespace.SchemaName(), config.Name)
				return
			}
		}
	})
	if found {
		return foundID, nil
	}
	return id.Null, fmt.Errorf(`text search configuration "%s" does not exist`, name)
}

func resolveBuiltInTextSearchObject(name string, section id.Section, label string, builtInName string) (id.Id, error) {
	if name != builtInName {
		return id.Null, fmt.Errorf(`text search %s "%s" does not exist`, label, name)
	}
	return id.NewId(section, "pg_catalog", name), nil
}

func resolveCommentIndex(ctx *sql.Context, schemaName string, tableName string, indexName string) (id.Id, error) {
	searchSchemas := []string{schemaName}
	if schemaName == "" {
		var err error
		searchSchemas, err = settings.GetCurrentSchemas(ctx)
		if err != nil {
			return id.Null, err
		}
	}
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return id.Null, err
	}
	var found id.Id
	for _, searchSchema := range searchSchemas {
		schema, ok, err := schemaDatabase.GetSchema(ctx, searchSchema)
		if err != nil {
			return id.Null, err
		}
		if !ok {
			continue
		}
		tableNames := []string{tableName}
		if tableName == "" {
			tableNames, err = schema.GetTableNames(ctx)
			if err != nil {
				return id.Null, err
			}
		}
		for _, tblName := range tableNames {
			table, ok, err := schema.GetTableInsensitive(ctx, tblName)
			if err != nil {
				return id.Null, err
			}
			if !ok {
				continue
			}
			indexedTable, ok := table.(sql.IndexAddressable)
			if !ok {
				continue
			}
			indexes, err := indexedTable.GetIndexes(ctx)
			if err != nil {
				return id.Null, err
			}
			for _, index := range indexes {
				if indexName != "" && indexmetadata.DisplayNameForTable(index, table) != indexName {
					continue
				}
				if indexName == "" && index.ID() != "PRIMARY" {
					continue
				}
				oid := id.NewIndex(schema.SchemaName(), table.Name(), index.ID()).AsId()
				if found.IsValid() {
					return id.Null, fmt.Errorf(`index name "%s" is not unique`, indexName)
				}
				found = oid
			}
		}
	}
	if !found.IsValid() {
		return id.Null, fmt.Errorf(`index "%s" does not exist`, indexName)
	}
	return found, nil
}

func resolveCommentConstraint(ctx *sql.Context, relation vitess.TableName, constraintName string) (id.Id, error) {
	relationID, schema, err := resolveCommentRelation(ctx, relation)
	if err != nil {
		return id.Null, err
	}
	if checkTable, ok := schemaTable(ctx, relation, schema).(sql.CheckTable); ok {
		checks, err := checkTable.GetChecks(ctx)
		if err != nil {
			return id.Null, err
		}
		for _, check := range checks {
			if check.Name == constraintName {
				tableID := id.Table(relationID)
				return id.NewCheck(tableID.SchemaName(), tableID.TableName(), constraintName).AsId(), nil
			}
		}
	}
	return id.Null, fmt.Errorf(`constraint "%s" for relation "%s" does not exist`, constraintName, relation.Name.String())
}

func resolveCommentTrigger(ctx *sql.Context, relation vitess.TableName, triggerName string) (id.Id, error) {
	relationID, _, err := resolveCommentRelation(ctx, relation)
	if err != nil {
		return id.Null, err
	}
	collection, err := core.GetTriggersCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return id.Null, err
	}
	tableID := id.Table(relationID)
	triggerID := id.NewTrigger(tableID.SchemaName(), tableID.TableName(), triggerName)
	if !collection.HasTrigger(ctx, triggerID) {
		return id.Null, fmt.Errorf(`trigger "%s" for relation "%s" does not exist`, triggerName, relation.Name.String())
	}
	return triggerID.AsId(), nil
}

func schemaTable(ctx *sql.Context, relation vitess.TableName, schema sql.Schema) sql.Table {
	relationName := relation.Name.String()
	searchSchemas, err := commentSearchSchemas(ctx, relation)
	if err != nil {
		return nil
	}
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return nil
	}
	for _, schemaName := range searchSchemas {
		dbSchema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
		if err != nil || !ok {
			continue
		}
		table, ok, err := dbSchema.GetTableInsensitive(ctx, relationName)
		if err != nil || !ok {
			continue
		}
		if len(schema) == len(table.Schema(ctx)) {
			return table
		}
	}
	return nil
}

func resolveCommentCollation(ctx *sql.Context, name string) (id.Id, error) {
	schemaName := "pg_catalog"
	collationName := name
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		schemaName = strings.Trim(name[:idx], `"`)
		collationName = name[idx+1:]
	}
	collationName = strings.Trim(collationName, `"`)
	collationID := id.NewCollation(schemaName, collationName)
	if !id.Cache().Exists(collationID.AsId()) {
		return id.Null, fmt.Errorf(`collation "%s" does not exist`, name)
	}
	return collationID.AsId(), nil
}

func resolveCommentOperator(name string, leftType id.Type, rightType id.Type) (id.Id, error) {
	if name == "+" && leftType == id.NewType("pg_catalog", "int4") && rightType == id.NewType("pg_catalog", "int4") {
		return id.NewId(id.Section_Operator, name, string(leftType), string(rightType)), nil
	}
	var found id.Id
	auth.LockRead(func() {
		for _, operator := range auth.GetAllOperators() {
			if operator.Name == name && operator.LeftType == leftType && operator.RightType == rightType {
				found = id.NewId(id.Section_Operator, operator.Namespace.SchemaName(), operator.Name, string(operator.LeftType), string(operator.RightType))
				return
			}
		}
	})
	if found.IsValid() {
		return found, nil
	}
	return id.Null, fmt.Errorf(`operator "%s" does not exist`, name)
}

func resolveCommentPolicy(ctx *sql.Context, relation vitess.TableName, policyName string) (id.Id, error) {
	relationID, _, err := resolveCommentRelation(ctx, relation)
	if err != nil {
		return id.Null, err
	}
	tableID := id.Table(relationID)
	return id.Null, fmt.Errorf(`policy "%s" for relation "%s" does not exist`, policyName, tableID.TableName())
}

func resolveCommentLargeObject(oid uint32) (id.Id, error) {
	if !largeobject.Exists(oid) {
		return id.Null, fmt.Errorf("large object %d does not exist", oid)
	}
	return id.NewOID(oid).AsId(), nil
}

func resolveCommentTablespace(name string) (id.Id, error) {
	switch name {
	case "pg_default":
		return id.NewOID(1663).AsId(), nil
	case "pg_global":
		return id.NewOID(1664).AsId(), nil
	default:
		return id.Null, fmt.Errorf(`tablespace "%s" does not exist`, name)
	}
}

type schemaGetter interface {
	GetSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error)
}

func currentSchemaDatabase(ctx *sql.Context) (schemaGetter, error) {
	doltSession, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return nil, fmt.Errorf("expected Dolt session")
	}
	database, err := doltSession.Provider().Database(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return nil, err
	}
	schemaDatabase, ok := database.(sql.SchemaDatabase)
	if !ok {
		if schema, ok := database.(sql.DatabaseSchema); ok {
			return singleSchemaDatabase{schema: schema}, nil
		}
		return nil, fmt.Errorf("current database does not support schemas")
	}
	return schemaDatabase, nil
}

type singleSchemaDatabase struct {
	schema sql.DatabaseSchema
}

func (s singleSchemaDatabase) GetSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error) {
	if schemaName == "" || s.schema.SchemaName() == schemaName {
		return s.schema, true, nil
	}
	return nil, false, nil
}

func commentSearchSchemas(ctx *sql.Context, relation vitess.TableName) ([]string, error) {
	searchSchemas := []string{relation.SchemaQualifier.String()}
	if searchSchemas[0] != "" {
		return searchSchemas, nil
	}
	return settings.GetCurrentSchemas(ctx)
}

func resolveCommentRelation(ctx *sql.Context, relation vitess.TableName) (id.Id, sql.Schema, error) {
	relationName := relation.Name.String()
	searchSchemas, err := commentSearchSchemas(ctx, relation)
	if err != nil {
		return id.Null, nil, err
	}
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return id.Null, nil, err
	}
	for _, schemaName := range searchSchemas {
		schema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
		if err != nil {
			return id.Null, nil, err
		}
		if !ok {
			continue
		}
		table, found, err := schema.GetTableInsensitive(ctx, relationName)
		if err != nil {
			return id.Null, nil, err
		}
		if !found {
			continue
		}
		return id.NewTable(schema.SchemaName(), table.Name()).AsId(), table.Schema(ctx), nil
	}
	return id.Null, nil, fmt.Errorf(`relation "%s" does not exist`, relationName)
}

func resolveCommentView(ctx *sql.Context, relation vitess.TableName) (id.Id, error) {
	relationName := relation.Name.String()
	searchSchemas, err := commentSearchSchemas(ctx, relation)
	if err != nil {
		return id.Null, err
	}
	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return id.Null, err
	}
	for _, schemaName := range searchSchemas {
		schema, ok, err := schemaDatabase.GetSchema(ctx, schemaName)
		if err != nil {
			return id.Null, err
		}
		if !ok {
			continue
		}
		viewDatabase, ok := schema.(sql.ViewDatabase)
		if !ok {
			continue
		}
		views, err := viewDatabase.AllViews(ctx)
		if err != nil {
			return id.Null, err
		}
		for _, view := range views {
			if view.Name == relationName {
				return id.NewView(schema.SchemaName(), view.Name).AsId(), nil
			}
		}
	}
	return id.Null, fmt.Errorf(`relation "%s" does not exist`, relationName)
}

func resolveCommentSequence(ctx *sql.Context, relation vitess.TableName) (id.Id, error) {
	relationName := relation.Name.String()
	searchSchemas, err := commentSearchSchemas(ctx, relation)
	if err != nil {
		return id.Null, err
	}
	collection, err := core.GetSequencesCollectionFromContext(ctx, ctx.GetCurrentDatabase())
	if err != nil {
		return id.Null, err
	}
	for _, schemaName := range searchSchemas {
		sequenceID := id.NewSequence(schemaName, relationName)
		if collection.HasSequence(ctx, sequenceID) {
			return sequenceID.AsId(), nil
		}
	}
	return id.Null, fmt.Errorf(`relation "%s" does not exist`, relationName)
}
