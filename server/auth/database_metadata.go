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

	"github.com/dolthub/doltgresql/utils"
)

// DatabaseMetadataValue contains PostgreSQL catalog metadata for a database.
type DatabaseMetadataValue struct {
	Owner            string
	AllowConnections bool
	ConnectionLimit  int32
	IsTemplate       bool
	Oid              uint32
	LocaleProvider   string
	Collate          string
	CType            string
	IcuLocale        string
	IcuRules         string
	CollationVersion string
}

// DatabaseMetadataUpdate contains optional ALTER DATABASE metadata updates.
type DatabaseMetadataUpdate struct {
	Owner            *string
	AllowConnections *bool
	ConnectionLimit  *int32
	IsTemplate       *bool
	Oid              *uint32
	LocaleProvider   *string
	Collate          *string
	CType            *string
	IcuLocale        *string
	IcuRules         *string
	CollationVersion *string
}

// DatabaseMetadata contains explicit PostgreSQL database metadata keyed by database name.
type DatabaseMetadata struct {
	Data map[string]DatabaseMetadataValue
}

// NewDatabaseMetadata returns a new *DatabaseMetadata.
func NewDatabaseMetadata() *DatabaseMetadata {
	return &DatabaseMetadata{Data: make(map[string]DatabaseMetadataValue)}
}

// DefaultDatabaseMetadata returns PostgreSQL-compatible defaults for database catalog metadata.
func DefaultDatabaseMetadata() DatabaseMetadataValue {
	owner, _ := GetSuperUserAndPassword()
	if owner == "" {
		owner = "postgres"
	}
	return DatabaseMetadataValue{
		Owner:            owner,
		AllowConnections: true,
		ConnectionLimit:  -1,
		IsTemplate:       false,
		LocaleProvider:   "i",
	}
}

// GetDatabaseMetadata returns database metadata, filling unspecified values from PostgreSQL-compatible defaults.
func GetDatabaseMetadata(database string) DatabaseMetadataValue {
	metadata := DefaultDatabaseMetadata()
	if explicit, ok := globalDatabase.databaseMetadata.Data[database]; ok {
		if explicit.Owner != "" {
			metadata.Owner = explicit.Owner
		}
		metadata.AllowConnections = explicit.AllowConnections
		metadata.ConnectionLimit = explicit.ConnectionLimit
		metadata.IsTemplate = explicit.IsTemplate
		metadata.Oid = explicit.Oid
		if explicit.LocaleProvider != "" {
			metadata.LocaleProvider = explicit.LocaleProvider
		}
		metadata.Collate = explicit.Collate
		metadata.CType = explicit.CType
		metadata.IcuLocale = explicit.IcuLocale
		metadata.IcuRules = explicit.IcuRules
		metadata.CollationVersion = explicit.CollationVersion
	}
	return metadata
}

// UpdateDatabaseMetadata applies optional metadata fields for a database.
func UpdateDatabaseMetadata(database string, update DatabaseMetadataUpdate) {
	if database == "" {
		return
	}
	metadata := GetDatabaseMetadata(database)
	if update.Owner != nil {
		metadata.Owner = *update.Owner
	}
	if update.AllowConnections != nil {
		metadata.AllowConnections = *update.AllowConnections
	}
	if update.ConnectionLimit != nil {
		metadata.ConnectionLimit = *update.ConnectionLimit
	}
	if update.IsTemplate != nil {
		metadata.IsTemplate = *update.IsTemplate
	}
	if update.Oid != nil {
		metadata.Oid = *update.Oid
	}
	if update.LocaleProvider != nil {
		metadata.LocaleProvider = *update.LocaleProvider
	}
	if update.Collate != nil {
		metadata.Collate = *update.Collate
	}
	if update.CType != nil {
		metadata.CType = *update.CType
	}
	if update.IcuLocale != nil {
		metadata.IcuLocale = *update.IcuLocale
	}
	if update.IcuRules != nil {
		metadata.IcuRules = *update.IcuRules
	}
	if update.CollationVersion != nil {
		metadata.CollationVersion = *update.CollationVersion
	}
	globalDatabase.databaseMetadata.Data[database] = metadata
}

// RenameDatabaseMetadata moves stored database metadata to a new database name.
func RenameDatabaseMetadata(oldName string, newName string) {
	if oldName == "" || newName == "" || oldName == newName {
		return
	}
	if metadata, ok := globalDatabase.databaseMetadata.Data[oldName]; ok {
		globalDatabase.databaseMetadata.Data[newName] = metadata
		delete(globalDatabase.databaseMetadata.Data, oldName)
	}
	globalDatabase.dbRoleSettings.renameDatabase(oldName, newName)
}

// RemoveDatabaseMetadata removes explicit database metadata.
func RemoveDatabaseMetadata(database string) {
	delete(globalDatabase.databaseMetadata.Data, database)
	globalDatabase.dbRoleSettings.removeDatabase(database)
}

// serialize writes the DatabaseMetadata to the given writer.
func (metadata *DatabaseMetadata) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(metadata.Data)))
	for database, value := range metadata.Data {
		writer.String(database)
		writer.String(value.Owner)
		writer.Bool(value.AllowConnections)
		writer.Int32(value.ConnectionLimit)
		writer.Bool(value.IsTemplate)
		writer.Uint32(value.Oid)
		writer.String(value.LocaleProvider)
		writer.String(value.Collate)
		writer.String(value.CType)
		writer.String(value.IcuLocale)
		writer.String(value.IcuRules)
		writer.String(value.CollationVersion)
	}
}

// deserialize reads the DatabaseMetadata from the given reader.
func (metadata *DatabaseMetadata) deserialize(version uint32, reader *utils.Reader) {
	metadata.Data = make(map[string]DatabaseMetadataValue)
	if version == 0 {
		return
	}
	dataCount := reader.Uint64()
	for idx := uint64(0); idx < dataCount; idx++ {
		database := reader.String()
		value := DatabaseMetadataValue{
			Owner:            reader.String(),
			AllowConnections: reader.Bool(),
			ConnectionLimit:  reader.Int32(),
			IsTemplate:       reader.Bool(),
		}
		if version >= 2 {
			value.Oid = reader.Uint32()
			value.LocaleProvider = reader.String()
			value.Collate = reader.String()
			value.CType = reader.String()
			value.IcuLocale = reader.String()
			value.IcuRules = reader.String()
			value.CollationVersion = reader.String()
		}
		if database != "" {
			metadata.Data[database] = value
		}
	}
}

// DbRoleSettingKey points to pg_db_role_setting row metadata.
type DbRoleSettingKey struct {
	Database string
	Role     string
}

// DbRoleSettingValue contains a pg_db_role_setting row.
type DbRoleSettingValue struct {
	Key    DbRoleSettingKey
	Config []string
}

// DbRoleSettings contains PostgreSQL database/role configuration metadata.
type DbRoleSettings struct {
	Data map[DbRoleSettingKey]DbRoleSettingValue
}

// NewDbRoleSettings returns a new *DbRoleSettings.
func NewDbRoleSettings() *DbRoleSettings {
	return &DbRoleSettings{Data: make(map[DbRoleSettingKey]DbRoleSettingValue)}
}

// SetDbRoleSetting stores a configuration setting for a database and/or role scope.
func SetDbRoleSetting(database string, role string, name string, value string) {
	if name == "" {
		return
	}
	key := DbRoleSettingKey{Database: database, Role: role}
	setting := globalDatabase.dbRoleSettings.Data[key]
	setting.Key = key
	prefix := name + "="
	replacement := prefix + value
	for idx, config := range setting.Config {
		if len(config) >= len(prefix) && config[:len(prefix)] == prefix {
			setting.Config[idx] = replacement
			globalDatabase.dbRoleSettings.Data[key] = setting
			return
		}
	}
	setting.Config = append(setting.Config, replacement)
	sort.Strings(setting.Config)
	globalDatabase.dbRoleSettings.Data[key] = setting
}

// ResetDbRoleSetting removes a configuration setting for a database and/or role scope.
func ResetDbRoleSetting(database string, role string, name string) {
	key := DbRoleSettingKey{Database: database, Role: role}
	setting, ok := globalDatabase.dbRoleSettings.Data[key]
	if !ok {
		return
	}
	if name == "" {
		delete(globalDatabase.dbRoleSettings.Data, key)
		return
	}
	prefix := name + "="
	config := setting.Config[:0]
	for _, entry := range setting.Config {
		if len(entry) < len(prefix) || entry[:len(prefix)] != prefix {
			config = append(config, entry)
		}
	}
	if len(config) == 0 {
		delete(globalDatabase.dbRoleSettings.Data, key)
		return
	}
	setting.Config = config
	globalDatabase.dbRoleSettings.Data[key] = setting
}

// GetDbRoleSettings returns all pg_db_role_setting rows in deterministic order.
func GetDbRoleSettings() []DbRoleSettingValue {
	settings := make([]DbRoleSettingValue, 0, len(globalDatabase.dbRoleSettings.Data))
	for _, value := range globalDatabase.dbRoleSettings.Data {
		copied := DbRoleSettingValue{
			Key:    value.Key,
			Config: append([]string(nil), value.Config...),
		}
		settings = append(settings, copied)
	}
	sort.Slice(settings, func(i, j int) bool {
		if settings[i].Key.Database != settings[j].Key.Database {
			return settings[i].Key.Database < settings[j].Key.Database
		}
		return settings[i].Key.Role < settings[j].Key.Role
	})
	return settings
}

func (settings *DbRoleSettings) renameDatabase(oldName string, newName string) {
	for key, value := range settings.Data {
		if key.Database != oldName {
			continue
		}
		delete(settings.Data, key)
		key.Database = newName
		value.Key = key
		settings.Data[key] = value
	}
}

func (settings *DbRoleSettings) removeDatabase(database string) {
	for key := range settings.Data {
		if key.Database == database {
			delete(settings.Data, key)
		}
	}
}

// serialize writes the DbRoleSettings to the given writer.
func (settings *DbRoleSettings) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(settings.Data)))
	for _, value := range settings.Data {
		writer.String(value.Key.Database)
		writer.String(value.Key.Role)
		writer.Uint64(uint64(len(value.Config)))
		for _, config := range value.Config {
			writer.String(config)
		}
	}
}

// deserialize reads the DbRoleSettings from the given reader.
func (settings *DbRoleSettings) deserialize(version uint32, reader *utils.Reader) {
	settings.Data = make(map[DbRoleSettingKey]DbRoleSettingValue)
	if version == 0 {
		return
	}
	dataCount := reader.Uint64()
	for idx := uint64(0); idx < dataCount; idx++ {
		key := DbRoleSettingKey{
			Database: reader.String(),
			Role:     reader.String(),
		}
		configCount := reader.Uint64()
		config := make([]string, 0, configCount)
		for configIdx := uint64(0); configIdx < configCount; configIdx++ {
			config = append(config, reader.String())
		}
		settings.Data[key] = DbRoleSettingValue{
			Key:    key,
			Config: config,
		}
	}
}
