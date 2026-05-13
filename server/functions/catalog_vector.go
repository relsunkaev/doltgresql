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
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

func catalogVectorSend(ctx *sql.Context, vectorType *pgtypes.DoltgresType, val any) ([]byte, error) {
	if wrapper, ok := val.(sql.AnyWrapper); ok {
		var err error
		val, err = wrapper.UnwrapAny(ctx)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
	}
	vals := val.([]any)
	dimensions := arrayBinaryDimensions(vals)
	hasNull, err := validateArrayBinaryShape(vals, dimensions, 0)
	if err != nil {
		return nil, err
	}
	writer := utils.NewWireWriter()
	writer.WriteInt32(int32(len(dimensions)))
	if hasNull {
		writer.WriteInt32(1)
	} else {
		writer.WriteInt32(0)
	}
	baseType, err := vectorType.ResolveArrayBaseType(ctx)
	if err != nil {
		return nil, err
	}
	writer.WriteUint32(id.Cache().ToOID(baseType.ID.AsId()))
	for _, dimension := range dimensions {
		writer.WriteInt32(dimension)
		writer.WriteInt32(0)
	}
	if err := writeArrayBinaryElements(ctx, writer, vals, dimensions, 0, baseType); err != nil {
		return nil, err
	}
	return writer.BufferData(), nil
}
