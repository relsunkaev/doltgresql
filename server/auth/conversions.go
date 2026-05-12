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
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/utils"
)

// Conversion stores catalog metadata for CREATE CONVERSION.
type Conversion struct {
	Name        string
	Namespace   string
	Owner       string
	ForEncoding int32
	ToEncoding  int32
	Proc        string
	Default     bool
}

// Conversions contains conversions keyed by namespace and name.
type Conversions struct {
	Data map[string]Conversion
}

// NewConversions returns a new *Conversions.
func NewConversions() *Conversions {
	return &Conversions{Data: make(map[string]Conversion)}
}

// GetAllConversions returns all conversions in a deterministic order.
func GetAllConversions() []Conversion {
	conversions := make([]Conversion, 0, len(globalDatabase.conversions.Data))
	for _, conversion := range globalDatabase.conversions.Data {
		conversions = append(conversions, conversion)
	}
	sort.Slice(conversions, func(i, j int) bool {
		if conversions[i].Namespace != conversions[j].Namespace {
			return conversions[i].Namespace < conversions[j].Namespace
		}
		return conversions[i].Name < conversions[j].Name
	})
	return conversions
}

// CreateConversion creates or replaces the conversion.
func CreateConversion(conversion Conversion) error {
	conversion.Name = conversionNameKey(conversion.Name)
	conversion.Namespace = conversionNameKey(conversion.Namespace)
	if len(conversion.Name) == 0 {
		return errors.New("conversion name cannot be empty")
	}
	if len(conversion.Namespace) == 0 {
		conversion.Namespace = "public"
	}
	globalDatabase.conversions.Data[conversionKey(conversion.Namespace, conversion.Name)] = conversion
	return nil
}

// DropConversion drops the conversion. It returns false when the conversion did not exist.
func DropConversion(namespace string, name string) bool {
	namespace = conversionNameKey(namespace)
	name = conversionNameKey(name)
	if len(namespace) == 0 {
		namespace = "public"
	}
	key := conversionKey(namespace, name)
	if _, ok := globalDatabase.conversions.Data[key]; !ok {
		return false
	}
	delete(globalDatabase.conversions.Data, key)
	return true
}

func conversionKey(namespace string, name string) string {
	return conversionNameKey(namespace) + "\x00" + conversionNameKey(name)
}

func conversionNameKey(name string) string {
	name = strings.TrimSpace(name)
	if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
		return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	return strings.ToLower(name)
}

func (conversions *Conversions) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(conversions.Data)))
	for _, conversion := range GetAllConversions() {
		writer.String(conversion.Name)
		writer.String(conversion.Namespace)
		writer.String(conversion.Owner)
		writer.Int32(conversion.ForEncoding)
		writer.Int32(conversion.ToEncoding)
		writer.String(conversion.Proc)
		writer.Bool(conversion.Default)
	}
}

func (conversions *Conversions) deserialize(version uint32, reader *utils.Reader) {
	conversions.Data = make(map[string]Conversion)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			conversion := Conversion{
				Name:        reader.String(),
				Namespace:   reader.String(),
				Owner:       reader.String(),
				ForEncoding: reader.Int32(),
				ToEncoding:  reader.Int32(),
				Proc:        reader.String(),
				Default:     reader.Bool(),
			}
			conversions.Data[conversionKey(conversion.Namespace, conversion.Name)] = conversion
		}
	default:
		panic("unexpected version in Conversions")
	}
}
