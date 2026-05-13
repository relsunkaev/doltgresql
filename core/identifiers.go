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

const physicalColumnNamePrefix = "__doltgres_column_name_"

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
