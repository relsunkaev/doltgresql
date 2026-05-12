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

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// TextSearchConfig stores user-defined text-search configuration metadata.
type TextSearchConfig struct {
	Name      string
	Namespace id.Namespace
}

// TextSearchConfigs contains user-defined text-search configurations.
type TextSearchConfigs struct {
	Data map[string]TextSearchConfig
}

// NewTextSearchConfigs returns a new *TextSearchConfigs.
func NewTextSearchConfigs() *TextSearchConfigs {
	return &TextSearchConfigs{Data: make(map[string]TextSearchConfig)}
}

// CreateTextSearchConfig creates or replaces a user-defined text-search configuration.
func CreateTextSearchConfig(config TextSearchConfig) {
	globalDatabase.textSearchConfigs.Data[textSearchConfigKey(config.Namespace, config.Name)] = config
}

// GetAllTextSearchConfigs returns all text-search configurations in deterministic order.
func GetAllTextSearchConfigs() []TextSearchConfig {
	configs := make([]TextSearchConfig, 0, len(globalDatabase.textSearchConfigs.Data))
	for _, config := range globalDatabase.textSearchConfigs.Data {
		configs = append(configs, config)
	}
	sort.Slice(configs, func(i, j int) bool {
		if configs[i].Namespace != configs[j].Namespace {
			return configs[i].Namespace < configs[j].Namespace
		}
		return configs[i].Name < configs[j].Name
	})
	return configs
}

func textSearchConfigKey(namespace id.Namespace, name string) string {
	return string(namespace) + "\x00" + name
}

func (configs *TextSearchConfigs) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(configs.Data)))
	for _, config := range GetAllTextSearchConfigs() {
		writer.String(config.Name)
		writer.Id(config.Namespace.AsId())
	}
}

func (configs *TextSearchConfigs) deserialize(version uint32, reader *utils.Reader) {
	configs.Data = make(map[string]TextSearchConfig)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			config := TextSearchConfig{
				Name:      reader.String(),
				Namespace: id.Namespace(reader.Id()),
			}
			configs.Data[textSearchConfigKey(config.Namespace, config.Name)] = config
		}
	default:
		panic("unexpected version in TextSearchConfigs")
	}
}
