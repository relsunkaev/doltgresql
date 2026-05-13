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

	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/utils"
)

// ForeignDataWrapper stores catalog metadata for a foreign-data wrapper.
type ForeignDataWrapper struct {
	Name      string
	Owner     string
	Handler   string
	Validator string
	Options   []string
}

// ForeignServer stores catalog metadata for a foreign server.
type ForeignServer struct {
	Name    string
	Owner   string
	Wrapper string
	Type    string
	Version string
	Options []string
}

// UserMapping stores catalog metadata for a foreign-server user mapping.
type UserMapping struct {
	User    string
	Server  string
	Options []string
}

// ForeignDataWrappers contains user-defined foreign-data wrappers.
type ForeignDataWrappers struct {
	Data map[string]ForeignDataWrapper
}

// ForeignServers contains user-defined foreign servers.
type ForeignServers struct {
	Data map[string]ForeignServer
}

// UserMappings contains user mappings keyed by user and foreign server.
type UserMappings struct {
	Data map[string]UserMapping
}

// NewForeignDataWrappers returns a new *ForeignDataWrappers.
func NewForeignDataWrappers() *ForeignDataWrappers {
	return &ForeignDataWrappers{Data: make(map[string]ForeignDataWrapper)}
}

// NewForeignServers returns a new *ForeignServers.
func NewForeignServers() *ForeignServers {
	return &ForeignServers{Data: make(map[string]ForeignServer)}
}

// NewUserMappings returns a new *UserMappings.
func NewUserMappings() *UserMappings {
	return &UserMappings{Data: make(map[string]UserMapping)}
}

// CreateForeignDataWrapper creates a foreign-data wrapper.
func CreateForeignDataWrapper(wrapper ForeignDataWrapper) error {
	wrapper.Name = foreignDataNameKey(wrapper.Name)
	if wrapper.Name == "" {
		return errors.New("foreign-data wrapper name cannot be empty")
	}
	if _, ok := globalDatabase.foreignDataWrappers.Data[wrapper.Name]; ok {
		return errors.Errorf(`foreign-data wrapper "%s" already exists`, wrapper.Name)
	}
	globalDatabase.foreignDataWrappers.Data[wrapper.Name] = wrapper
	return nil
}

// GetForeignDataWrapper returns the named foreign-data wrapper.
func GetForeignDataWrapper(name string) (ForeignDataWrapper, bool) {
	wrapper, ok := globalDatabase.foreignDataWrappers.Data[foreignDataNameKey(name)]
	return wrapper, ok
}

// GetAllForeignDataWrappers returns all foreign-data wrappers in deterministic order.
func GetAllForeignDataWrappers() []ForeignDataWrapper {
	wrappers := make([]ForeignDataWrapper, 0, len(globalDatabase.foreignDataWrappers.Data))
	for _, wrapper := range globalDatabase.foreignDataWrappers.Data {
		wrappers = append(wrappers, wrapper)
	}
	sort.Slice(wrappers, func(i, j int) bool {
		return wrappers[i].Name < wrappers[j].Name
	})
	return wrappers
}

// DropForeignDataWrapper drops a foreign-data wrapper.
func DropForeignDataWrapper(name string) bool {
	name = foreignDataNameKey(name)
	if _, ok := globalDatabase.foreignDataWrappers.Data[name]; !ok {
		return false
	}
	delete(globalDatabase.foreignDataWrappers.Data, name)
	for serverName, server := range globalDatabase.foreignServers.Data {
		if server.Wrapper == name {
			delete(globalDatabase.foreignServers.Data, serverName)
			removeUserMappingsForServer(serverName)
		}
	}
	return true
}

// CreateForeignServer creates a foreign server.
func CreateForeignServer(server ForeignServer) error {
	server.Name = foreignDataNameKey(server.Name)
	server.Wrapper = foreignDataNameKey(server.Wrapper)
	if server.Name == "" {
		return errors.New("foreign server name cannot be empty")
	}
	if _, ok := globalDatabase.foreignServers.Data[server.Name]; ok {
		return errors.Errorf(`server "%s" already exists`, server.Name)
	}
	if _, ok := globalDatabase.foreignDataWrappers.Data[server.Wrapper]; !ok {
		return foreignDataWrapperDoesNotExistError(server.Wrapper)
	}
	globalDatabase.foreignServers.Data[server.Name] = server
	return nil
}

// GetForeignServer returns the named foreign server.
func GetForeignServer(name string) (ForeignServer, bool) {
	server, ok := globalDatabase.foreignServers.Data[foreignDataNameKey(name)]
	return server, ok
}

// GetAllForeignServers returns all foreign servers in deterministic order.
func GetAllForeignServers() []ForeignServer {
	servers := make([]ForeignServer, 0, len(globalDatabase.foreignServers.Data))
	for _, server := range globalDatabase.foreignServers.Data {
		servers = append(servers, server)
	}
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].Name < servers[j].Name
	})
	return servers
}

// AlterForeignServerVersion updates a foreign server version.
func AlterForeignServerVersion(name string, version string) error {
	name = foreignDataNameKey(name)
	server, ok := globalDatabase.foreignServers.Data[name]
	if !ok {
		return foreignServerDoesNotExistError(name)
	}
	server.Version = version
	globalDatabase.foreignServers.Data[name] = server
	return nil
}

// DropForeignServer drops a foreign server.
func DropForeignServer(name string) bool {
	name = foreignDataNameKey(name)
	if _, ok := globalDatabase.foreignServers.Data[name]; !ok {
		return false
	}
	delete(globalDatabase.foreignServers.Data, name)
	removeUserMappingsForServer(name)
	return true
}

// CreateUserMapping creates a user mapping for a foreign server.
func CreateUserMapping(mapping UserMapping) error {
	mapping.User = strings.TrimSpace(mapping.User)
	mapping.Server = foreignDataNameKey(mapping.Server)
	if _, ok := globalDatabase.foreignServers.Data[mapping.Server]; !ok {
		return foreignServerDoesNotExistError(mapping.Server)
	}
	key := userMappingKey(mapping.User, mapping.Server)
	if _, ok := globalDatabase.userMappings.Data[key]; ok {
		return errors.Errorf(`user mapping for "%s" already exists for server "%s"`, mapping.User, mapping.Server)
	}
	globalDatabase.userMappings.Data[key] = mapping
	return nil
}

// GetAllUserMappings returns all user mappings in deterministic order.
func GetAllUserMappings() []UserMapping {
	mappings := make([]UserMapping, 0, len(globalDatabase.userMappings.Data))
	for _, mapping := range globalDatabase.userMappings.Data {
		mappings = append(mappings, mapping)
	}
	sort.Slice(mappings, func(i, j int) bool {
		if mappings[i].Server != mappings[j].Server {
			return mappings[i].Server < mappings[j].Server
		}
		return mappings[i].User < mappings[j].User
	})
	return mappings
}

// AlterUserMapping validates that a user mapping target server exists.
func AlterUserMapping(user string, server string) error {
	server = foreignDataNameKey(server)
	if _, ok := globalDatabase.foreignServers.Data[server]; !ok {
		return foreignServerDoesNotExistError(server)
	}
	if _, ok := globalDatabase.userMappings.Data[userMappingKey(user, server)]; !ok {
		return errors.Errorf(`user mapping for "%s" does not exist for server "%s"`, user, server)
	}
	return nil
}

// DropUserMapping drops a user mapping.
func DropUserMapping(user string, server string) error {
	server = foreignDataNameKey(server)
	if _, ok := globalDatabase.foreignServers.Data[server]; !ok {
		return foreignServerDoesNotExistError(server)
	}
	key := userMappingKey(user, server)
	if _, ok := globalDatabase.userMappings.Data[key]; !ok {
		return errors.Errorf(`user mapping for "%s" does not exist for server "%s"`, user, server)
	}
	delete(globalDatabase.userMappings.Data, key)
	return nil
}

func removeUserMappingsForServer(server string) {
	for key, mapping := range globalDatabase.userMappings.Data {
		if mapping.Server == server {
			delete(globalDatabase.userMappings.Data, key)
		}
	}
}

func foreignDataNameKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func userMappingKey(user string, server string) string {
	return strings.TrimSpace(user) + "\x00" + foreignDataNameKey(server)
}

func foreignDataWrapperDoesNotExistError(name string) error {
	return pgerror.Newf(pgcode.UndefinedObject, `foreign-data wrapper "%s" does not exist`, name)
}

func foreignServerDoesNotExistError(name string) error {
	return pgerror.Newf(pgcode.UndefinedObject, `server "%s" does not exist`, name)
}

func (wrappers *ForeignDataWrappers) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(wrappers.Data)))
	for _, wrapper := range GetAllForeignDataWrappers() {
		writer.String(wrapper.Name)
		writer.String(wrapper.Owner)
		writer.String(wrapper.Handler)
		writer.String(wrapper.Validator)
		writer.StringSlice(wrapper.Options)
	}
}

func (wrappers *ForeignDataWrappers) deserialize(version uint32, reader *utils.Reader) {
	wrappers.Data = make(map[string]ForeignDataWrapper)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			wrapper := ForeignDataWrapper{
				Name:      reader.String(),
				Owner:     reader.String(),
				Handler:   reader.String(),
				Validator: reader.String(),
				Options:   reader.StringSlice(),
			}
			wrappers.Data[foreignDataNameKey(wrapper.Name)] = wrapper
		}
	default:
		panic("unexpected version in ForeignDataWrappers")
	}
}

func (servers *ForeignServers) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(servers.Data)))
	for _, server := range GetAllForeignServers() {
		writer.String(server.Name)
		writer.String(server.Owner)
		writer.String(server.Wrapper)
		writer.String(server.Type)
		writer.String(server.Version)
		writer.StringSlice(server.Options)
	}
}

func (servers *ForeignServers) deserialize(version uint32, reader *utils.Reader) {
	servers.Data = make(map[string]ForeignServer)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			server := ForeignServer{
				Name:    reader.String(),
				Owner:   reader.String(),
				Wrapper: reader.String(),
				Type:    reader.String(),
				Version: reader.String(),
				Options: reader.StringSlice(),
			}
			servers.Data[foreignDataNameKey(server.Name)] = server
		}
	default:
		panic("unexpected version in ForeignServers")
	}
}

func (mappings *UserMappings) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(mappings.Data)))
	for _, mapping := range GetAllUserMappings() {
		writer.String(mapping.User)
		writer.String(mapping.Server)
		writer.StringSlice(mapping.Options)
	}
}

func (mappings *UserMappings) deserialize(version uint32, reader *utils.Reader) {
	mappings.Data = make(map[string]UserMapping)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			mapping := UserMapping{
				User:    reader.String(),
				Server:  reader.String(),
				Options: reader.StringSlice(),
			}
			mappings.Data[userMappingKey(mapping.User, mapping.Server)] = mapping
		}
	default:
		panic("unexpected version in UserMappings")
	}
}
