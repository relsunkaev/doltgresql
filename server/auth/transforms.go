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

// Transform stores catalog metadata for CREATE TRANSFORM.
type Transform struct {
	TypeID  id.Id
	Lang    string
	FromSQL string
	ToSQL   string
}

// Transforms contains transforms keyed by type and language.
type Transforms struct {
	Data map[string]Transform
}

// NewTransforms returns a new *Transforms.
func NewTransforms() *Transforms {
	return &Transforms{Data: make(map[string]Transform)}
}

// GetAllTransforms returns all transforms in a deterministic order.
func GetAllTransforms() []Transform {
	transforms := make([]Transform, 0, len(globalDatabase.transforms.Data))
	for _, transform := range globalDatabase.transforms.Data {
		transforms = append(transforms, transform)
	}
	sort.Slice(transforms, func(i, j int) bool {
		if transforms[i].TypeID != transforms[j].TypeID {
			return transforms[i].TypeID < transforms[j].TypeID
		}
		return transforms[i].Lang < transforms[j].Lang
	})
	return transforms
}

// CreateTransform creates or replaces the transform.
func CreateTransform(transform Transform) error {
	if !transform.TypeID.IsValid() {
		return errors.New("transform type cannot be empty")
	}
	transform.Lang = languageKey(transform.Lang)
	if len(transform.Lang) == 0 {
		return errors.New("transform language cannot be empty")
	}
	if _, ok := GetLanguage(transform.Lang); !ok {
		return errors.Errorf(`language "%s" does not exist`, transform.Lang)
	}
	globalDatabase.transforms.Data[transformKey(transform.TypeID, transform.Lang)] = transform
	return nil
}

func transformKey(typeID id.Id, lang string) string {
	return string(typeID) + "\x00" + languageKey(lang)
}

func (transforms *Transforms) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(transforms.Data)))
	for _, transform := range GetAllTransforms() {
		writer.Id(transform.TypeID)
		writer.String(transform.Lang)
		writer.String(transform.FromSQL)
		writer.String(transform.ToSQL)
	}
}

func (transforms *Transforms) deserialize(version uint32, reader *utils.Reader) {
	transforms.Data = make(map[string]Transform)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			transform := Transform{
				TypeID:  reader.Id(),
				Lang:    reader.String(),
				FromSQL: reader.String(),
				ToSQL:   reader.String(),
			}
			transforms.Data[transformKey(transform.TypeID, transform.Lang)] = transform
		}
	default:
		panic("unexpected version in Transforms")
	}
}
