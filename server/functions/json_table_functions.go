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
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/go-mysql-server/sql"

	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var _ sql.TableFunction = (*jsonEachTableFunction)(nil)
var _ sql.ExecSourceRel = (*jsonEachTableFunction)(nil)

func initJsonTableFunctions() {
	dtablefunctions.DoltTableFunctions = append(dtablefunctions.DoltTableFunctions,
		newJsonEachTableFunction("json_each", pgtypes.Json, pgtypes.Json, false),
		newJsonEachTableFunction("json_each_text", pgtypes.Json, pgtypes.Text, true),
		newJsonEachTableFunction("jsonb_each", pgtypes.JsonB, pgtypes.JsonB, false),
		newJsonEachTableFunction("jsonb_each_text", pgtypes.JsonB, pgtypes.Text, true),
		&jsonbPopulateRecordTableFunction{},
		newJsonToRecordTableFunction("doltgres_json_to_record", pgtypes.Json, false),
		newJsonToRecordTableFunction("doltgres_json_to_recordset", pgtypes.Json, true),
		newJsonToRecordTableFunction("doltgres_jsonb_to_record", pgtypes.JsonB, false),
		newJsonToRecordTableFunction("doltgres_jsonb_to_recordset", pgtypes.JsonB, true),
	)
}

func newJsonEachTableFunction(name string, inputType *pgtypes.DoltgresType, valueType *pgtypes.DoltgresType, textOutput bool) *jsonEachTableFunction {
	return &jsonEachTableFunction{
		name:       name,
		inputType:  inputType,
		valueType:  valueType,
		textOutput: textOutput,
	}
}

type jsonEachTableFunction struct {
	db         sql.Database
	name       string
	inputType  *pgtypes.DoltgresType
	valueType  *pgtypes.DoltgresType
	textOutput bool
	exprs      []sql.Expression
}

// NewInstance creates a new instance of the JSON table function.
func (j *jsonEachTableFunction) NewInstance(ctx *sql.Context, db sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New(j.Name(), 1, len(args))
	}
	nt := *j
	nt.db = db
	nt.exprs = args
	return &nt, nil
}

// Name implements the sql.Nameable interface.
func (j *jsonEachTableFunction) Name() string {
	return j.name
}

// String implements fmt.Stringer.
func (j *jsonEachTableFunction) String() string {
	args := make([]string, len(j.exprs))
	for i, expr := range j.exprs {
		args[i] = expr.String()
	}
	return fmt.Sprintf("%s(%s)", j.Name(), strings.Join(args, ", "))
}

// Resolved implements the sql.Resolvable interface.
func (j *jsonEachTableFunction) Resolved() bool {
	for _, expr := range j.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

// Expressions implements sql.Expressioner.
func (j *jsonEachTableFunction) Expressions() []sql.Expression {
	return j.exprs
}

// WithExpressions implements sql.Expressioner.
func (j *jsonEachTableFunction) WithExpressions(ctx *sql.Context, exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(exprs), 1)
	}
	nt := *j
	nt.exprs = exprs
	return &nt, nil
}

// Database implements sql.Databaser.
func (j *jsonEachTableFunction) Database() sql.Database {
	return j.db
}

// WithDatabase implements sql.Databaser.
func (j *jsonEachTableFunction) WithDatabase(db sql.Database) (sql.Node, error) {
	nt := *j
	nt.db = db
	return &nt, nil
}

// IsReadOnly implements sql.Node.
func (j *jsonEachTableFunction) IsReadOnly() bool {
	return true
}

// Schema implements sql.Node.
func (j *jsonEachTableFunction) Schema(ctx *sql.Context) sql.Schema {
	var dbName string
	if j.db != nil {
		dbName = j.db.Name()
	}
	return sql.Schema{
		&sql.Column{
			DatabaseSource: dbName,
			Source:         j.Name(),
			Name:           "key",
			Type:           pgtypes.Text,
			Nullable:       false,
		},
		&sql.Column{
			DatabaseSource: dbName,
			Source:         j.Name(),
			Name:           "value",
			Type:           j.valueType,
			Nullable:       j.textOutput,
		},
	}
}

// Children implements sql.Node.
func (j *jsonEachTableFunction) Children() []sql.Node {
	return nil
}

// WithChildren implements sql.Node.
func (j *jsonEachTableFunction) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 0)
	}
	return j, nil
}

// RowIter implements sql.Node.
func (j *jsonEachTableFunction) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	value, err := j.exprs[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return sql.RowsToRowIter(), nil
	}
	doc, err := jsonDocumentFromFunctionValue(ctx, j.inputType, value)
	if err != nil {
		return nil, err
	}
	object, err := jsonValueAsObjectForKeys(j.Name(), doc.Value)
	if err != nil {
		return nil, err
	}
	return &jsonEachTableRowIter{
		object:     object,
		valueType:  j.valueType,
		textOutput: j.textOutput,
	}, nil
}

// CollationCoercibility implements sql.CollationCoercible.
func (j *jsonEachTableFunction) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

// Collation implements sql.Table.
func (j *jsonEachTableFunction) Collation() sql.CollationID {
	return sql.Collation_Default
}

type jsonEachTableRowIter struct {
	object     pgtypes.JsonValueObject
	valueType  *pgtypes.DoltgresType
	textOutput bool
	idx        int
}

var _ sql.RowIter = (*jsonEachTableRowIter)(nil)

func (j *jsonEachTableRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if j.idx >= len(j.object.Items) {
		return nil, io.EOF
	}
	item := j.object.Items[j.idx]
	j.idx++
	var value any
	var err error
	if j.textOutput {
		value, err = jsonValueAsText(ctx, item.Value)
	} else if j.valueType == pgtypes.JsonB {
		value, err = jsonbValueToOutput(ctx, item.Value)
	} else {
		value, err = jsonValueToOutput(ctx, item.Value)
	}
	if err != nil {
		return nil, err
	}
	return sql.Row{item.Key, value}, nil
}

func (j *jsonEachTableRowIter) Close(ctx *sql.Context) error {
	return nil
}
