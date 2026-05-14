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
	"testing"

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

func TestValidateReturnStatements(t *testing.T) {
	tests := []struct {
		name            string
		ops             []InterpreterOperation
		returnType      *pgtypes.DoltgresType
		returnsSet      bool
		hasOutputParams bool
		expectedCode    pgcode.Code
	}{
		{
			name:         "non-void bare return",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:   pgtypes.Int32,
			expectedCode: pgcode.Syntax,
		},
		{
			name:         "void return expression",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return, PrimaryData: "SELECT 5;"}},
			returnType:   pgtypes.Void,
			expectedCode: pgcode.DatatypeMismatch,
		},
		{
			name:         "unresolved void return expression",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return, PrimaryData: "SELECT 5;"}},
			returnType:   pgtypes.NewUnresolvedDoltgresType("", "void"),
			expectedCode: pgcode.DatatypeMismatch,
		},
		{
			name:         "non-void return expression",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return, PrimaryData: "SELECT 5;"}},
			returnType:   pgtypes.Int32,
			expectedCode: pgcode.Uncategorized,
		},
		{
			name:         "void bare return",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:   pgtypes.Void,
			expectedCode: pgcode.Uncategorized,
		},
		{
			name:         "set-returning bare return",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:   pgtypes.Int32,
			returnsSet:   true,
			expectedCode: pgcode.Uncategorized,
		},
		{
			name:            "output parameter bare return",
			ops:             []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:      pgtypes.Int32,
			hasOutputParams: true,
			expectedCode:    pgcode.Uncategorized,
		},
		{
			name:            "output parameter return expression",
			ops:             []InterpreterOperation{{OpCode: OpCode_Return, PrimaryData: "SELECT 5;"}},
			returnType:      pgtypes.Int32,
			hasOutputParams: true,
			expectedCode:    pgcode.DatatypeMismatch,
		},
		{
			name:         "event trigger bare return",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:   pgtypes.EventTrigger,
			expectedCode: pgcode.Uncategorized,
		},
		{
			name:         "trigger bare return",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:   pgtypes.Trigger,
			expectedCode: pgcode.Uncategorized,
		},
		{
			name:         "unresolved trigger bare return",
			ops:          []InterpreterOperation{{OpCode: OpCode_Return}},
			returnType:   pgtypes.NewUnresolvedDoltgresType("", "trigger"),
			expectedCode: pgcode.Uncategorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReturnStatements(tt.ops, tt.returnType, tt.returnsSet, tt.hasOutputParams)
			if tt.expectedCode == pgcode.Uncategorized {
				if err != nil {
					t.Fatalf("expected no error, found %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected %s error, found nil", tt.expectedCode)
			}
			if code := pgerror.GetPGCode(err); code != tt.expectedCode {
				t.Fatalf("expected SQLSTATE %s, found %s: %v", tt.expectedCode, code, err)
			}
		})
	}
}
