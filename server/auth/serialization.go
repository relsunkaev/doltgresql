// Copyright 2024 Dolthub, Inc.
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
	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/utils"
)

// PersistChanges will save the state of the global database to disk (assuming we are not using the pure in-memory
// implementation).
func PersistChanges() error {
	if fileSystem != nil {
		return fileSystem.WriteFile(authFileName, globalDatabase.serialize(), 0644)
	}
	return nil
}

// serialize returns the Database as a byte slice.
func (db *Database) serialize() []byte {
	writer := utils.NewWriter(16384)
	// Write the version
	writer.Uint32(18)
	// Write the roles
	writer.Uint32(uint32(len(db.rolesByID)))
	for _, role := range db.rolesByID {
		role.serialize(writer)
	}
	// Write database metadata
	db.databaseMetadata.serialize(writer)
	// Write the database privileges
	db.databasePrivileges.serialize(writer)
	// Write the schema privileges
	db.schemaPrivileges.serialize(writer)
	// Write the schema owners
	db.schemaOwners.serialize(writer)
	// Write the relation owners
	db.relationOwners.serialize(writer)
	// Write default privileges
	db.defaultPrivileges.serialize(writer)
	// Write the table privileges
	db.tablePrivileges.serialize(writer)
	// Write the sequence privileges
	db.sequencePrivileges.serialize(writer)
	// Write the routine privileges
	db.routinePrivileges.serialize(writer)
	// Write the type privileges
	db.typePrivileges.serialize(writer)
	// Write the languages
	db.languages.serialize(writer)
	// Write the language privileges
	db.languagePrivileges.serialize(writer)
	// Write the parameter privileges
	db.parameterPrivileges.serialize(writer)
	// Write the transforms
	db.transforms.serialize(writer)
	// Write the conversions
	db.conversions.serialize(writer)
	// Write the casts
	db.casts.serialize(writer)
	// Write the operators
	db.operators.serialize(writer)
	// Write the text-search configurations
	db.textSearchConfigs.serialize(writer)
	// Write tablespace metadata
	db.tablespaces.serialize(writer)
	// Write foreign-data metadata
	db.foreignDataWrappers.serialize(writer)
	db.foreignServers.serialize(writer)
	db.userMappings.serialize(writer)
	// Write the role chain
	db.roleMembership.serialize(writer)
	// Write database/role settings
	db.dbRoleSettings.serialize(writer)
	return writer.Data()
}

// deserialize creates a Database from a byte slice.
func (db *Database) deserialize(data []byte) error {
	if len(data) < 4 {
		return errors.New("invalid auth database format")
	}
	reader := utils.NewReader(data)
	version := reader.Uint32()
	switch version {
	case 0:
		return db.deserializeV0(reader)
	case 1:
		return db.deserializeV1(reader)
	case 2:
		return db.deserializeV2(reader)
	case 3:
		return db.deserializeV3(reader)
	case 4:
		return db.deserializeV4(reader)
	case 5:
		return db.deserializeV5(reader)
	case 6:
		return db.deserializeV6(reader)
	case 7:
		return db.deserializeV7(reader)
	case 8:
		return db.deserializeV8(reader)
	case 9:
		return db.deserializeV9(reader)
	case 10:
		return db.deserializeV10(reader)
	case 11:
		return db.deserializeV11(reader)
	case 12:
		return db.deserializeV12(reader)
	case 13:
		return db.deserializeV13(reader)
	case 14:
		return db.deserializeV14(reader)
	case 15:
		return db.deserializeV15(reader)
	case 16:
		return db.deserializeV16(reader)
	case 17:
		return db.deserializeV17(reader)
	case 18:
		return db.deserializeV18(reader)
	default:
		return errors.Errorf("Authorization database format %d is not supported, please upgrade Doltgres", version)
	}
}

// deserializeV0 creates a Database from a byte slice. Expects a reader that has already read the version.
func (db *Database) deserializeV0(reader *utils.Reader) error {
	// Read the roles
	clear(db.rolesByName)
	clear(db.rolesByID)
	roleCount := reader.Uint32()
	for i := uint32(0); i < roleCount; i++ {
		r := Role{}
		r.deserialize(0, reader)
		db.rolesByName[r.Name] = r.id
		db.rolesByID[r.id] = r
	}
	// Read the database privileges
	db.databaseMetadata.deserialize(0, reader)
	db.databasePrivileges.deserialize(0, reader)
	// Read the schema privileges
	db.schemaPrivileges.deserialize(0, reader)
	db.schemaOwners.deserialize(0, reader)
	db.relationOwners.deserialize(0, reader)
	// Read the table privileges
	db.tablePrivileges.deserialize(0, reader)
	// Read the role chain
	db.roleMembership.deserialize(0, reader)
	// Read the routine privileges
	db.routinePrivileges.deserialize(0, reader)
	db.typePrivileges.deserialize(0, reader)
	// Read the sequence privileges
	db.sequencePrivileges.deserialize(0, reader)
	db.languages.deserialize(0, reader)
	db.languagePrivileges.deserialize(0, reader)
	db.parameterPrivileges.deserialize(0, reader)
	db.transforms.deserialize(0, reader)
	db.conversions.deserialize(0, reader)
	db.casts.deserialize(0, reader)
	db.operators.deserialize(0, reader)
	db.textSearchConfigs.deserialize(0, reader)
	ensurePredefinedRoles()
	dbInitDefaultLanguages()
	return nil
}

// deserializeV1 creates a Database from a byte slice. Expects a reader that has already read the version.
func (db *Database) deserializeV1(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 1)
}

// deserializeV2 creates a Database from a byte slice. Expects a reader that has already read the version.
func (db *Database) deserializeV2(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 2)
}

// deserializeV3 creates a Database from a byte slice. Expects a reader that has already read the version.
func (db *Database) deserializeV3(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 3)
}

// deserializeV4 creates a Database from a byte slice. Expects a reader that has already read the version.
func (db *Database) deserializeV4(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 4)
}

func (db *Database) deserializeV5(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 5)
}

func (db *Database) deserializeV6(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 6)
}

func (db *Database) deserializeV7(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 7)
}

func (db *Database) deserializeV8(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 8)
}

func (db *Database) deserializeV9(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 9)
}

func (db *Database) deserializeV10(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 10)
}

func (db *Database) deserializeV11(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 11)
}

func (db *Database) deserializeV12(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 12)
}

func (db *Database) deserializeV13(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 13)
}

func (db *Database) deserializeV14(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 14)
}

func (db *Database) deserializeV15(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 15)
}

func (db *Database) deserializeV16(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 16)
}

func (db *Database) deserializeV17(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 17)
}

func (db *Database) deserializeV18(reader *utils.Reader) error {
	return db.deserializeCurrent(reader, 18)
}

func (db *Database) deserializeCurrent(reader *utils.Reader, version uint32) error {
	// Read the roles
	clear(db.rolesByName)
	clear(db.rolesByID)
	roleCount := reader.Uint32()
	for i := uint32(0); i < roleCount; i++ {
		r := Role{}
		r.deserialize(1, reader)
		db.rolesByName[r.Name] = r.id
		db.rolesByID[r.id] = r
	}
	if version >= 16 {
		// Read database metadata
		db.databaseMetadata.deserialize(2, reader)
	} else if version >= 12 {
		// Read database metadata
		db.databaseMetadata.deserialize(1, reader)
	} else {
		db.databaseMetadata.deserialize(0, reader)
	}
	// Read the database privileges
	db.databasePrivileges.deserialize(1, reader)
	// Read the schema privileges
	db.schemaPrivileges.deserialize(1, reader)
	if version >= 11 {
		db.schemaOwners.deserialize(1, reader)
	} else {
		db.schemaOwners.deserialize(0, reader)
	}
	if version >= 13 {
		db.relationOwners.deserialize(1, reader)
	} else {
		db.relationOwners.deserialize(0, reader)
	}
	if version >= 14 {
		db.defaultPrivileges.deserialize(1, reader)
	} else {
		db.defaultPrivileges.deserialize(0, reader)
	}
	if version >= 3 {
		// Read the table privileges
		db.tablePrivileges.deserialize(2, reader)
		// Read the sequence privileges
		db.sequencePrivileges.deserialize(1, reader)
		// Read the routine privileges
		db.routinePrivileges.deserialize(1, reader)
		if version >= 10 {
			// Read the type privileges
			db.typePrivileges.deserialize(1, reader)
		} else {
			db.typePrivileges.deserialize(0, reader)
		}
		// Read the languages
		db.languages.deserialize(1, reader)
		// Read the language privileges
		db.languagePrivileges.deserialize(1, reader)
		if version >= 4 {
			// Read the parameter privileges
			db.parameterPrivileges.deserialize(1, reader)
		} else {
			db.parameterPrivileges.deserialize(0, reader)
		}
		if version >= 5 {
			// Read the transforms
			db.transforms.deserialize(1, reader)
		} else {
			db.transforms.deserialize(0, reader)
		}
		if version >= 6 {
			// Read the conversions
			db.conversions.deserialize(1, reader)
		} else {
			db.conversions.deserialize(0, reader)
		}
		if version >= 7 {
			// Read the casts
			db.casts.deserialize(1, reader)
		} else {
			db.casts.deserialize(0, reader)
		}
		if version >= 8 {
			// Read the operators
			db.operators.deserialize(1, reader)
		} else {
			db.operators.deserialize(0, reader)
		}
		if version >= 9 {
			// Read the text-search configurations
			db.textSearchConfigs.deserialize(1, reader)
		} else {
			db.textSearchConfigs.deserialize(0, reader)
		}
		if version >= 17 {
			// Read tablespace metadata
			db.tablespaces.deserialize(1, reader)
		} else {
			db.tablespaces.deserialize(0, reader)
		}
		if version >= 15 {
			// Read foreign-data metadata
			db.foreignDataWrappers.deserialize(1, reader)
			db.foreignServers.deserialize(1, reader)
			db.userMappings.deserialize(1, reader)
		} else {
			db.foreignDataWrappers.deserialize(0, reader)
			db.foreignServers.deserialize(0, reader)
			db.userMappings.deserialize(0, reader)
		}
		// Read the role chain
		if version >= 18 {
			db.roleMembership.deserialize(2, reader)
		} else {
			db.roleMembership.deserialize(1, reader)
		}
		if version >= 12 {
			// Read database/role settings
			db.dbRoleSettings.deserialize(1, reader)
		} else {
			db.dbRoleSettings.deserialize(0, reader)
		}
	} else {
		// Read the table privileges
		db.tablePrivileges.deserialize(version, reader)
		// Read the role chain
		db.roleMembership.deserialize(1, reader)
		// Read the routine privileges
		db.routinePrivileges.deserialize(1, reader)
		db.typePrivileges.deserialize(0, reader)
		// Read the sequence privileges
		db.sequencePrivileges.deserialize(1, reader)
		db.languages.deserialize(0, reader)
		db.languagePrivileges.deserialize(0, reader)
		db.parameterPrivileges.deserialize(0, reader)
		db.transforms.deserialize(0, reader)
		db.conversions.deserialize(0, reader)
		db.casts.deserialize(0, reader)
		db.operators.deserialize(0, reader)
		db.textSearchConfigs.deserialize(0, reader)
		db.tablespaces.deserialize(0, reader)
		db.foreignDataWrappers.deserialize(0, reader)
		db.foreignServers.deserialize(0, reader)
		db.userMappings.deserialize(0, reader)
		db.dbRoleSettings.deserialize(0, reader)
		ensurePredefinedRoles()
		dbInitDefaultLanguages()
	}
	if version >= 3 {
		ensurePredefinedRoles()
	}
	return nil
}
