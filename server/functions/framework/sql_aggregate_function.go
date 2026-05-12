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

package framework

import (
	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// SQLAggregateFunction is the runtime representation of a user-defined
// aggregate whose transition function is implemented in LANGUAGE SQL.
type SQLAggregateFunction struct {
	ID                 id.Function
	ReturnType         *pgtypes.DoltgresType
	ParameterTypes     []*pgtypes.DoltgresType
	StateType          *pgtypes.DoltgresType
	TransitionFunction SQLFunction
	InitCond           string
	IsNonDeterministic bool
}

var _ AggregateFunctionInterface = SQLAggregateFunction{}

// GetExpectedParameterCount implements the interface FunctionInterface.
func (agg SQLAggregateFunction) GetExpectedParameterCount() int {
	return len(agg.ParameterTypes)
}

// GetName implements the interface FunctionInterface.
func (agg SQLAggregateFunction) GetName() string {
	return agg.ID.FunctionName()
}

// GetParameters implements the interface FunctionInterface.
func (agg SQLAggregateFunction) GetParameters() []*pgtypes.DoltgresType {
	return agg.ParameterTypes
}

// GetReturn implements the interface FunctionInterface.
func (agg SQLAggregateFunction) GetReturn() *pgtypes.DoltgresType {
	return agg.ReturnType
}

// InternalID implements the interface FunctionInterface.
func (agg SQLAggregateFunction) InternalID() id.Id {
	return agg.ID.AsId()
}

// IsStrict implements the interface FunctionInterface.
func (agg SQLAggregateFunction) IsStrict() bool {
	return false
}

// NonDeterministic implements the interface FunctionInterface.
func (agg SQLAggregateFunction) NonDeterministic() bool {
	return agg.IsNonDeterministic
}

// IsCVariadic implements the FunctionInterface interface.
func (agg SQLAggregateFunction) IsCVariadic() bool {
	return false
}

// VariadicIndex implements the interface FunctionInterface.
func (agg SQLAggregateFunction) VariadicIndex() int {
	return -1
}

// IsSRF implements the interface FunctionInterface.
func (agg SQLAggregateFunction) IsSRF() bool {
	return false
}

// NewBuffer implements the AggregateFunctionInterface interface.
func (agg SQLAggregateFunction) NewBuffer(ctx *sql.Context, runner sql.StatementRunner, exprs []sql.Expression) (sql.AggregationBuffer, error) {
	if runner == nil {
		return nil, errors.Errorf("statement runner is not available for aggregate %s", agg.ID.FunctionName())
	}
	buffer := &sqlAggregateBuffer{
		agg:    agg,
		runner: runner,
		exprs:  exprs,
	}
	if len(agg.InitCond) > 0 {
		state, err := agg.StateType.IoInput(ctx, agg.InitCond)
		if err != nil {
			return nil, err
		}
		buffer.state = state
		buffer.hasState = true
	}
	return buffer, nil
}

// enforceInterfaceInheritance implements the interface FunctionInterface.
func (agg SQLAggregateFunction) enforceInterfaceInheritance(error) {}

type sqlAggregateBuffer struct {
	agg      SQLAggregateFunction
	runner   sql.StatementRunner
	exprs    []sql.Expression
	state    any
	hasState bool
	sawRow   bool
}

var _ sql.AggregationBuffer = (*sqlAggregateBuffer)(nil)

// Dispose implements the sql.AggregationBuffer interface.
func (b *sqlAggregateBuffer) Dispose(ctx *sql.Context) {
	for _, expr := range b.exprs {
		expression.Dispose(ctx, expr)
	}
}

// Eval implements the sql.AggregationBuffer interface.
func (b *sqlAggregateBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if b.hasState {
		return b.state, nil
	}
	if !b.sawRow {
		return nil, nil
	}
	return b.state, nil
}

// Update implements the sql.AggregationBuffer interface.
func (b *sqlAggregateBuffer) Update(ctx *sql.Context, row sql.Row) error {
	args := make([]any, 0, len(b.exprs)+1)
	args = append(args, b.state)
	for _, expr := range b.exprs {
		val, err := expr.Eval(ctx, row)
		if err != nil {
			return err
		}
		args = append(args, val)
	}
	nextState, err := CallSqlFunction(ctx, b.agg.TransitionFunction, b.runner, args)
	if err != nil {
		return err
	}
	b.state = nextState
	b.hasState = true
	b.sawRow = true
	return nil
}
