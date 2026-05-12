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

	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// Operator stores user-defined operator metadata.
type Operator struct {
	Name           string
	Namespace      id.Namespace
	LeftType       id.Type
	RightType      id.Type
	ResultType     id.Type
	Function       string
	FunctionSchema string
}

// Operators contains user-defined operators keyed by name and operand types.
type Operators struct {
	Data map[string]Operator
}

// NewOperators returns a new *Operators.
func NewOperators() *Operators {
	return &Operators{Data: make(map[string]Operator)}
}

// CreateOperator creates or replaces a user-defined operator.
func CreateOperator(operator Operator) error {
	if operator.Name == "" {
		return errors.New("operator name cannot be empty")
	}
	if !operator.Namespace.IsValid() || !operator.LeftType.IsValid() || !operator.RightType.IsValid() || !operator.ResultType.IsValid() {
		return errors.New("operator namespace and types must be valid")
	}
	if operator.Function == "" {
		return errors.New("operator function cannot be empty")
	}
	globalDatabase.operators.Data[operatorKey(operator.Namespace, operator.Name, operator.LeftType, operator.RightType)] = operator
	return nil
}

// DropOperator drops a user-defined operator. It returns false when the operator did not exist.
func DropOperator(namespace id.Namespace, name string, leftType id.Type, rightType id.Type) bool {
	key := operatorKey(namespace, name, leftType, rightType)
	if _, ok := globalDatabase.operators.Data[key]; !ok {
		return false
	}
	delete(globalDatabase.operators.Data, key)
	return true
}

// GetAllOperators returns all operators in deterministic order.
func GetAllOperators() []Operator {
	operators := make([]Operator, 0, len(globalDatabase.operators.Data))
	for _, operator := range globalDatabase.operators.Data {
		operators = append(operators, operator)
	}
	sort.Slice(operators, func(i, j int) bool {
		if operators[i].Namespace != operators[j].Namespace {
			return operators[i].Namespace < operators[j].Namespace
		}
		if operators[i].Name != operators[j].Name {
			return operators[i].Name < operators[j].Name
		}
		if operators[i].LeftType != operators[j].LeftType {
			return operators[i].LeftType < operators[j].LeftType
		}
		return operators[i].RightType < operators[j].RightType
	})
	return operators
}

func operatorKey(namespace id.Namespace, name string, leftType id.Type, rightType id.Type) string {
	return string(namespace) + "\x00" + name + "\x00" + string(leftType) + "\x00" + string(rightType)
}

func (operators *Operators) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(operators.Data)))
	for _, operator := range GetAllOperators() {
		writer.String(operator.Name)
		writer.Id(id.Id(operator.Namespace))
		writer.Id(operator.LeftType.AsId())
		writer.Id(operator.RightType.AsId())
		writer.Id(operator.ResultType.AsId())
		writer.String(operator.Function)
		writer.String(operator.FunctionSchema)
	}
}

func (operators *Operators) deserialize(version uint32, reader *utils.Reader) {
	operators.Data = make(map[string]Operator)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			operator := Operator{
				Name:           reader.String(),
				Namespace:      id.Namespace(reader.Id()),
				LeftType:       id.Type(reader.Id()),
				RightType:      id.Type(reader.Id()),
				ResultType:     id.Type(reader.Id()),
				Function:       reader.String(),
				FunctionSchema: reader.String(),
			}
			operators.Data[operatorKey(operator.Namespace, operator.Name, operator.LeftType, operator.RightType)] = operator
		}
	default:
		panic("unexpected version in Operators")
	}
}
