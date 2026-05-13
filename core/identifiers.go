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

package core

import (
	"encoding/hex"
	"strings"
)

const (
	physicalColumnNamePrefix = "__doltgres_column_name_"
	physicalIndexNamePrefix  = "__doltgres_index_name_"
	physicalViewNamePrefix   = "__doltgres_view_name_"
	physicalConstraintPrefix = "__doltgres_constraint_name_"
)

// IsValidPostgresIdentifier returns true according to Postgres quoted identifier rules.
// Quoted identifiers can contain any character except the null character (code zero),
// including supplementary Unicode (emoji, code points above U+FFFF) unlike MySQL.
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
func IsValidPostgresIdentifier(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		if c == 0x0000 {
			return false
		}
	}
	return true
}

// EncodePhysicalColumnName maps PostgreSQL's case-sensitive column identifiers
// onto GMS column names, which are otherwise validated case-insensitively.
func EncodePhysicalColumnName(name string) string {
	if name == strings.ToLower(name) && !strings.HasPrefix(name, physicalColumnNamePrefix) {
		return name
	}
	return physicalColumnNamePrefix + hex.EncodeToString([]byte(name))
}

// DecodePhysicalColumnName returns the PostgreSQL-facing column name for a
// column that was encoded by EncodePhysicalColumnName.
func DecodePhysicalColumnName(name string) string {
	if !strings.HasPrefix(name, physicalColumnNamePrefix) {
		return name
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(name, physicalColumnNamePrefix))
	if err != nil {
		return name
	}
	return string(decoded)
}

// EncodePhysicalIndexName maps PostgreSQL's case-sensitive index identifiers
// onto GMS index names, which are otherwise validated case-insensitively.
func EncodePhysicalIndexName(name string) string {
	if name == strings.ToLower(name) && !strings.HasPrefix(name, physicalIndexNamePrefix) {
		return name
	}
	return physicalIndexNamePrefix + hex.EncodeToString([]byte(name))
}

// DecodePhysicalIndexName returns the PostgreSQL-facing index name for an
// index that was encoded by EncodePhysicalIndexName.
func DecodePhysicalIndexName(name string) string {
	if !strings.HasPrefix(name, physicalIndexNamePrefix) {
		return name
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(name, physicalIndexNamePrefix))
	if err != nil {
		return name
	}
	return string(decoded)
}

// EncodePhysicalViewName maps PostgreSQL's case-sensitive view identifiers
// onto Dolt schema-fragment names, which are keyed case-insensitively.
func EncodePhysicalViewName(name string) string {
	if name == strings.ToLower(name) && !strings.HasPrefix(name, physicalViewNamePrefix) {
		return name
	}
	return physicalViewNamePrefix + hex.EncodeToString([]byte(name))
}

// DecodePhysicalViewName returns the PostgreSQL-facing view name for a view
// that was encoded by EncodePhysicalViewName.
func DecodePhysicalViewName(name string) string {
	if !strings.HasPrefix(name, physicalViewNamePrefix) {
		return name
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(name, physicalViewNamePrefix))
	if err != nil {
		return name
	}
	return string(decoded)
}

// EncodePhysicalConstraintName maps PostgreSQL's case-sensitive constraint
// identifiers onto GMS constraint names, which are otherwise validated
// case-insensitively.
func EncodePhysicalConstraintName(name string) string {
	if name == strings.ToLower(name) && !strings.HasPrefix(name, physicalConstraintPrefix) {
		return name
	}
	return physicalConstraintPrefix + hex.EncodeToString([]byte(name))
}

// DecodePhysicalConstraintName returns the PostgreSQL-facing constraint name
// for a constraint encoded by EncodePhysicalConstraintName.
func DecodePhysicalConstraintName(name string) string {
	if !strings.HasPrefix(name, physicalConstraintPrefix) {
		return name
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(name, physicalConstraintPrefix))
	if err != nil {
		return name
	}
	return string(decoded)
}
