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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/comments"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func initPgObjectAddress() {
	framework.RegisterFunction(pg_describe_object_oid_oid_int32)
	framework.RegisterFunction(pg_describe_object_qualified_oid_oid_int32)

	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions,
		newPgGetObjectAddressTableFunction("pg_get_object_address"),
		newPgGetObjectAddressTableFunction(qualifiedPgCatalogFunctionName("pg_get_object_address")),
	)
}

func qualifiedPgCatalogFunctionName(name string) string {
	return "__doltgres_qualified_function__" + "\x1f" +
		hex.EncodeToString([]byte("pg_catalog")) + "\x1f" +
		hex.EncodeToString([]byte(name))
}

type pgGetObjectAddressTableFunction struct {
	db    sql.Database
	name  string
	exprs []sql.Expression
}

func newPgGetObjectAddressTableFunction(name string) *pgGetObjectAddressTableFunction {
	return &pgGetObjectAddressTableFunction{name: name}
}

var _ sql.TableFunction = (*pgGetObjectAddressTableFunction)(nil)
var _ sql.ExecSourceRel = (*pgGetObjectAddressTableFunction)(nil)

func (p *pgGetObjectAddressTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New(p.Name(), 3, len(args))
	}
	return &pgGetObjectAddressTableFunction{db: db, name: p.name, exprs: args}, nil
}

func (p *pgGetObjectAddressTableFunction) Name() string {
	return p.name
}

func (p *pgGetObjectAddressTableFunction) String() string {
	args := make([]string, len(p.exprs))
	for i, expr := range p.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", p.Name(), strings.Join(args, ", "))
}

func (p *pgGetObjectAddressTableFunction) Resolved() bool {
	for _, expr := range p.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (p *pgGetObjectAddressTableFunction) Expressions() []sql.Expression {
	return p.exprs
}

func (p *pgGetObjectAddressTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), 3)
	}
	np := *p
	np.exprs = exprs
	return &np, nil
}

func (p *pgGetObjectAddressTableFunction) Database() sql.Database {
	return p.db
}

func (p *pgGetObjectAddressTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	np := *p
	np.db = db
	return &np, nil
}

func (p *pgGetObjectAddressTableFunction) IsReadOnly() bool {
	return true
}

func (p *pgGetObjectAddressTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if p.db != nil {
		dbName = p.db.Name()
	}
	return sql.Schema{
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "classid", Type: pgtypes.Oid, Nullable: false},
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "objid", Type: pgtypes.Oid, Nullable: false},
		&sql.Column{DatabaseSource: dbName, Source: p.Name(), Name: "objsubid", Type: pgtypes.Int32, Nullable: false},
	}
}

func (p *pgGetObjectAddressTableFunction) Children() []sql.Node {
	return nil
}

func (p *pgGetObjectAddressTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}
	return p, nil
}

func (p *pgGetObjectAddressTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	objectTypeVal, err := p.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	objectNamesVal, err := p.exprs[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	objectArgsVal, err := p.exprs[2].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if objectTypeVal == nil || objectNamesVal == nil || objectArgsVal == nil {
		return sql.RowsToRowIter(), nil
	}
	objectType := strings.ToLower(objectTypeVal.(string))
	objectNames, err := objectAddressTextArrayArg(ctx, "object_names", objectNamesVal)
	if err != nil {
		return nil, err
	}
	if _, err = objectAddressTextArrayArg(ctx, "object_args", objectArgsVal); err != nil {
		return nil, err
	}

	switch objectType {
	case "table":
		tableID, err := resolveObjectAddressTable(ctx, objectNames)
		if err != nil {
			return nil, err
		}
		return sql.RowsToRowIter(sql.Row{
			id.NewTable("pg_catalog", "pg_class").AsId(),
			tableID,
			int32(0),
		}), nil
	default:
		return nil, errors.Errorf("unsupported object type: %s", objectType)
	}
}

func (p *pgGetObjectAddressTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (p *pgGetObjectAddressTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

func objectAddressTextArrayArg(ctx *sql.Context, name string, val any) ([]string, error) {
	unwrapped, err := sql.UnwrapAny(ctx, val)
	if err != nil {
		return nil, err
	}
	values, ok := unwrapped.([]any)
	if !ok {
		return nil, errors.Errorf("%s must be text[]", name)
	}
	result := make([]string, len(values))
	for i, value := range values {
		if value == nil {
			return nil, errors.Errorf("%s must not contain nulls", name)
		}
		result[i] = value.(string)
	}
	return result, nil
}

func resolveObjectAddressTable(ctx *sql.Context, objectNames []string) (id.Id, error) {
	var (
		searchSchemas []string
		tableName     string
		err           error
	)
	switch len(objectNames) {
	case 1:
		searchSchemas, err = core.SearchPath(ctx)
		if err != nil {
			return id.Null, err
		}
		tableName = objectNames[0]
	case 2:
		searchSchemas = []string{objectNames[0]}
		tableName = objectNames[1]
	default:
		return id.Null, errors.Errorf("table object address requires one or two names")
	}

	var tableID id.Id
	err = IterateCurrentDatabase(ctx, Callbacks{
		SearchSchemas: searchSchemas,
		Table: func(ctx *sql.Context, schema ItemSchema, table ItemTable) (cont bool, err error) {
			if table.Item.Name() != tableName {
				return true, nil
			}
			tableID = table.OID.AsId()
			return false, nil
		},
	})
	if err != nil {
		return id.Null, err
	}
	if !tableID.IsValid() {
		return id.Null, errors.Errorf(`table "%s" does not exist`, strings.Join(objectNames, "."))
	}
	return tableID, nil
}

var pg_describe_object_oid_oid_int32 = framework.Function3{
	Name:               "pg_describe_object",
	Return:             pgtypes.Text,
	Parameters:         [3]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Oid, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, classid any, objid any, objsubid any) (any, error) {
		return describeObject(ctx, classid.(id.Id), objid.(id.Id), objsubid.(int32))
	},
}

var pg_describe_object_qualified_oid_oid_int32 = framework.Function3{
	Name:               qualifiedPgCatalogFunctionName("pg_describe_object"),
	Return:             pgtypes.Text,
	Parameters:         [3]*pgtypes.DoltgresType{pgtypes.Oid, pgtypes.Oid, pgtypes.Int32},
	IsNonDeterministic: true,
	Strict:             true,
	Callable: func(ctx *sql.Context, _ [4]*pgtypes.DoltgresType, classid any, objid any, objsubid any) (any, error) {
		return describeObject(ctx, classid.(id.Id), objid.(id.Id), objsubid.(int32))
	},
}

func describeObject(ctx *sql.Context, classID id.Id, objID id.Id, objSubID int32) (string, error) {
	if id.Cache().ToOID(classID) != comments.PgClassOID() {
		return fmt.Sprintf("unrecognized object %d", id.Cache().ToOID(objID)), nil
	}
	if objSubID != 0 {
		return fmt.Sprintf("column %d of relation %d", objSubID, id.Cache().ToOID(objID)), nil
	}
	var description string
	err := RunCallback(ctx, objID, Callbacks{
		Table: func(ctx *sql.Context, schema ItemSchema, table ItemTable) (cont bool, err error) {
			description = fmt.Sprintf(`table "%s.%s"`, schema.Item.SchemaName(), table.Item.Name())
			return false, nil
		},
		View: func(ctx *sql.Context, schema ItemSchema, view ItemView) (cont bool, err error) {
			description = fmt.Sprintf(`view "%s.%s"`, schema.Item.SchemaName(), view.Item.Name)
			return false, nil
		},
		Sequence: func(ctx *sql.Context, schema ItemSchema, sequence ItemSequence) (cont bool, err error) {
			description = fmt.Sprintf(`sequence "%s.%s"`, schema.Item.SchemaName(), sequence.Item.Id.SequenceName())
			return false, nil
		},
		Index: func(ctx *sql.Context, schema ItemSchema, table ItemTable, index ItemIndex) (cont bool, err error) {
			description = fmt.Sprintf(`index "%s.%s"`, schema.Item.SchemaName(), index.Item.ID())
			return false, nil
		},
	})
	if err != nil {
		return "", err
	}
	if description == "" {
		return fmt.Sprintf("unrecognized object %d", id.Cache().ToOID(objID)), nil
	}
	return description, nil
}
