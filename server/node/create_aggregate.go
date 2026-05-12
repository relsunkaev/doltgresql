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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/functions"
	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// CreateAggregate implements CREATE AGGREGATE.
type CreateAggregate struct {
	AggregateName string
	SchemaName    string
	Replace       bool
	Parameters    []RoutineParam
	StateType     *pgtypes.DoltgresType
	SFuncName     string
	SFuncSchema   string
	InitCond      string
	Definition    string
}

var _ sql.ExecSourceRel = (*CreateAggregate)(nil)
var _ vitess.Injectable = (*CreateAggregate)(nil)

// NewCreateAggregate returns a new *CreateAggregate.
func NewCreateAggregate(
	aggregateName string,
	schemaName string,
	replace bool,
	params []RoutineParam,
	stateType *pgtypes.DoltgresType,
	sFuncName string,
	sFuncSchema string,
	initCond string,
	definition string,
) *CreateAggregate {
	return &CreateAggregate{
		AggregateName: aggregateName,
		SchemaName:    schemaName,
		Replace:       replace,
		Parameters:    params,
		StateType:     stateType,
		SFuncName:     sFuncName,
		SFuncSchema:   sFuncSchema,
		InitCond:      initCond,
		Definition:    definition,
	}
}

// Children implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) Children() []sql.Node {
	return nil
}

// IsReadOnly implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) Resolved() bool {
	return true
}

// RowIter implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	funcCollection, err := core.GetFunctionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	schemaName, err := core.GetSchemaName(ctx, nil, c.SchemaName)
	if err != nil {
		return nil, err
	}
	sFuncSchema, err := core.GetSchemaName(ctx, nil, c.SFuncSchema)
	if err != nil {
		return nil, err
	}

	stateType, err := resolveCreateAggregateType(ctx, c.StateType)
	if err != nil {
		return nil, err
	}
	paramTypes := make([]id.Type, len(c.Parameters))
	for i, param := range c.Parameters {
		paramType, err := resolveCreateAggregateType(ctx, param.Type)
		if err != nil {
			return nil, err
		}
		paramTypes[i] = paramType.ID
	}
	transitionParamTypes := make([]id.Type, 0, len(paramTypes)+1)
	transitionParamTypes = append(transitionParamTypes, stateType.ID)
	transitionParamTypes = append(transitionParamTypes, paramTypes...)
	transitionID := id.NewFunction(sFuncSchema, c.SFuncName, transitionParamTypes...)
	transitionFunction, err := funcCollection.GetFunction(ctx, transitionID)
	if err != nil {
		return nil, err
	}
	if !transitionFunction.ID.IsValid() {
		overloads, err := funcCollection.GetFunctionOverloads(ctx, id.NewFunction(sFuncSchema, c.SFuncName))
		if err != nil {
			return nil, err
		}
		for _, overload := range overloads {
			if functionParameterTypesMatch(overload.ParameterTypes, transitionParamTypes) {
				transitionFunction = overload
				transitionID = overload.ID
				break
			}
		}
	}
	if !transitionFunction.ID.IsValid() && len(c.SFuncSchema) == 0 {
		err = funcCollection.IterateFunctions(ctx, func(f functions.Function) (stop bool, err error) {
			if !strings.EqualFold(f.ID.FunctionName(), c.SFuncName) {
				return false, nil
			}
			if !functionParameterTypesMatch(f.ParameterTypes, transitionParamTypes) {
				return false, nil
			}
			transitionFunction = f
			transitionID = f.ID
			return true, nil
		})
		if err != nil {
			return nil, err
		}
	}
	if !transitionFunction.ID.IsValid() {
		return nil, errors.Errorf("CREATE AGGREGATE is not yet supported")
	}
	if len(transitionFunction.SQLDefinition) == 0 {
		return nil, errors.Errorf("CREATE AGGREGATE is not yet supported")
	}

	aggregateID := id.NewFunction(schemaName, c.AggregateName, paramTypes...)
	if c.Replace && funcCollection.HasFunction(ctx, aggregateID) {
		if err = funcCollection.DropFunction(ctx, aggregateID); err != nil {
			return nil, err
		}
	}
	err = funcCollection.AddFunction(ctx, functions.Function{
		ID:                 aggregateID,
		ReturnType:         stateType.ID,
		ParameterTypes:     paramTypes,
		IsNonDeterministic: true,
		Definition:         c.Definition,
		Aggregate:          true,
		AggregateStateType: stateType.ID,
		AggregateSFunc:     transitionID,
		AggregateInitCond:  c.InitCond,
		Owner:              ctx.Client().User,
	})
	if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func resolveCreateAggregateType(ctx *sql.Context, typ *pgtypes.DoltgresType) (*pgtypes.DoltgresType, error) {
	if typ == nil || typ.IsResolvedType() {
		return typ, nil
	}
	if resolved := resolveCreateAggregateBuiltInType(typ); resolved != nil {
		return resolved, nil
	}

	typesCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}

	schemaName, err := core.GetSchemaName(ctx, nil, typ.ID.SchemaName())
	if err != nil {
		return nil, err
	}
	resolved, err := typesCollection.GetType(ctx, id.NewType(schemaName, typ.ID.TypeName()))
	if err != nil {
		return nil, err
	}
	if resolved == nil && len(typ.ID.SchemaName()) == 0 {
		resolved, err = typesCollection.GetType(ctx, id.NewType("pg_catalog", typ.ID.TypeName()))
		if err != nil {
			return nil, err
		}
	}
	if resolved == nil {
		return nil, pgtypes.ErrTypeDoesNotExist.New(typ.Name())
	}
	return resolved.WithAttTypMod(typ.GetAttTypMod()), nil
}

func resolveCreateAggregateBuiltInType(typ *pgtypes.DoltgresType) *pgtypes.DoltgresType {
	if len(typ.ID.SchemaName()) > 0 && typ.ID.SchemaName() != "pg_catalog" {
		return nil
	}
	typeName := strings.ToLower(strings.Trim(typ.ID.TypeName(), `"`))
	switch typeName {
	case "int", "integer":
		typeName = "int4"
	case "smallint":
		typeName = "int2"
	case "bigint":
		typeName = "int8"
	case "double precision":
		typeName = "float8"
	case "real":
		typeName = "float4"
	case "boolean":
		typeName = "bool"
	case "character varying":
		typeName = "varchar"
	}
	internalID, ok := pgtypes.NameToInternalID[typeName]
	if !ok {
		return nil
	}
	resolved := pgtypes.GetTypeByID(internalID)
	if resolved == nil {
		return nil
	}
	return resolved.WithAttTypMod(typ.GetAttTypMod())
}

func functionParameterTypesMatch(left []id.Type, right []id.Type) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] == right[i] {
			continue
		}
		if !strings.EqualFold(left[i].TypeName(), right[i].TypeName()) {
			return false
		}
		leftSchema := left[i].SchemaName()
		rightSchema := right[i].SchemaName()
		if len(leftSchema) > 0 && len(rightSchema) > 0 && !strings.EqualFold(leftSchema, rightSchema) {
			return false
		}
	}
	return true
}

// Schema implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) String() string {
	return "CREATE AGGREGATE"
}

// WithChildren implements the interface sql.ExecSourceRel.
func (c *CreateAggregate) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (c *CreateAggregate) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}
