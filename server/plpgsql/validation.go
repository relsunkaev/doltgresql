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

package plpgsql

import (
	"strings"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// ValidateReturnStatements checks PL/pgSQL RETURN statements against the
// declared function result shape.
func ValidateReturnStatements(ops []InterpreterOperation, returnType *pgtypes.DoltgresType, returnsSet, hasOutputParams bool) error {
	if returnType == nil {
		return nil
	}

	isVoid := isReturnTypeNamed(returnType, pgtypes.Void, "void")
	isEventTrigger := isReturnTypeNamed(returnType, pgtypes.EventTrigger, "event_trigger")
	allowsBareReturn := returnsSet || hasOutputParams || isVoid || isEventTrigger
	for _, op := range ops {
		if op.OpCode != OpCode_Return {
			continue
		}
		hasExpression := strings.TrimSpace(op.PrimaryData) != ""
		if hasExpression {
			if isVoid {
				return pgerror.New(pgcode.DatatypeMismatch, "RETURN cannot have a parameter in function returning void")
			}
			if hasOutputParams {
				return pgerror.New(pgcode.DatatypeMismatch, "RETURN cannot have a parameter in function with OUT parameters")
			}
			continue
		}
		if !allowsBareReturn {
			return pgerror.New(pgcode.Syntax, "RETURN statement in a function returning non-void requires an expression")
		}
	}
	return nil
}

func isReturnTypeNamed(returnType, resolvedType *pgtypes.DoltgresType, name string) bool {
	return returnType.ID == resolvedType.ID || strings.EqualFold(returnType.ID.TypeName(), name)
}
