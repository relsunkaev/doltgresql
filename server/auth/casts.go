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

// Cast stores user-defined cast metadata.
type Cast struct {
	SourceType id.Type
	TargetType id.Type
	Function   string
}

// Casts contains user-defined casts keyed by source and target type.
type Casts struct {
	Data map[string]Cast
}

// NewCasts returns a new *Casts.
func NewCasts() *Casts {
	return &Casts{Data: make(map[string]Cast)}
}

// CreateCast creates or replaces a user-defined cast.
func CreateCast(cast Cast) error {
	if !cast.SourceType.IsValid() || !cast.TargetType.IsValid() {
		return errors.New("cast source and target types must be valid")
	}
	if cast.Function == "" {
		return errors.New("cast function cannot be empty")
	}
	globalDatabase.casts.Data[castKey(cast.SourceType, cast.TargetType)] = cast
	return nil
}

// DropCast drops a cast. It returns false when the cast did not exist.
func DropCast(sourceType id.Type, targetType id.Type) bool {
	key := castKey(sourceType, targetType)
	if _, ok := globalDatabase.casts.Data[key]; !ok {
		return false
	}
	delete(globalDatabase.casts.Data, key)
	return true
}

// GetCast returns the user-defined cast with the given source and target.
func GetCast(sourceType id.Type, targetType id.Type) (Cast, bool) {
	cast, ok := globalDatabase.casts.Data[castKey(sourceType, targetType)]
	return cast, ok
}

// GetAllCasts returns all casts in deterministic order.
func GetAllCasts() []Cast {
	casts := make([]Cast, 0, len(globalDatabase.casts.Data))
	for _, cast := range globalDatabase.casts.Data {
		casts = append(casts, cast)
	}
	sort.Slice(casts, func(i, j int) bool {
		if casts[i].SourceType != casts[j].SourceType {
			return casts[i].SourceType < casts[j].SourceType
		}
		return casts[i].TargetType < casts[j].TargetType
	})
	return casts
}

func castKey(sourceType id.Type, targetType id.Type) string {
	return string(sourceType) + "\x00" + string(targetType)
}

func (casts *Casts) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(casts.Data)))
	for _, cast := range GetAllCasts() {
		writer.Id(cast.SourceType.AsId())
		writer.Id(cast.TargetType.AsId())
		writer.String(cast.Function)
	}
}

func (casts *Casts) deserialize(version uint32, reader *utils.Reader) {
	casts.Data = make(map[string]Cast)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			cast := Cast{
				SourceType: id.Type(reader.Id()),
				TargetType: id.Type(reader.Id()),
				Function:   reader.String(),
			}
			casts.Data[castKey(cast.SourceType, cast.TargetType)] = cast
		}
	default:
		panic("unexpected version in Casts")
	}
}
