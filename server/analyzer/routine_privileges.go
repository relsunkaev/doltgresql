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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/server/auth"
	"github.com/dolthub/doltgresql/server/functions/framework"
)

func checkResolvedRoutineExecutePrivilege(ctx *sql.Context, compiledFunction *framework.CompiledFunction) error {
	routineID, owner, ok := compiledFunction.ResolvedRoutine()
	if !ok {
		return nil
	}
	var schemaName string
	var routineName string
	var params []id.Type
	switch routineID.Section() {
	case id.Section_Function:
		functionID := id.Function(routineID)
		schemaName = functionID.SchemaName()
		routineName = functionID.FunctionName()
		params = functionID.Parameters()
	case id.Section_Procedure:
		procedureID := id.Procedure(routineID)
		schemaName = procedureID.SchemaName()
		routineName = procedureID.ProcedureName()
		params = procedureID.Parameters()
	default:
		return nil
	}
	if strings.EqualFold(schemaName, "pg_catalog") {
		return nil
	}
	return auth.CheckRoutineExecutePrivilege(ctx, schemaName, routineName, auth.RoutineArgTypesKey(params), owner)
}
