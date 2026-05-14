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

package id

// This adds all of the built-in operator class IDs to the cache.
func init() {
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "bit_ops"), 10002)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "bool_ops"), 10003)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "bpchar_ops"), 10004)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "bpchar_pattern_ops"), 4219)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "bytea_ops"), 10006)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "char_ops"), 10007)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "date_ops"), 3122)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "float4_ops"), 10012)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "float8_ops"), 3123)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "int2_ops"), 1979)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "int4_ops"), 1978)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "int8_ops"), 3124)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "interval_ops"), 10022)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "jsonb_ops"), 10088)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "name_ops"), 10028)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "numeric_ops"), 3125)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "oid_ops"), 1981)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "oidvector_ops"), 10032)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "pg_lsn_ops"), 10067)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "text_ops"), 3126)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "text_pattern_ops"), 4217)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "time_ops"), 10038)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "timestamp_ops"), 3128)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "timestamptz_ops"), 3127)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "timetz_ops"), 10041)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "uuid_ops"), 10065)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "varbit_ops"), 10043)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "varchar_ops"), 10044)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "btree", "varchar_pattern_ops"), 4218)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "hash", "jsonb_ops"), 10089)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "gin", "jsonb_ops"), 10090)
	globalCache.setBuiltIn(NewId(Section_OperatorClass, "gin", "jsonb_path_ops"), 10091)
}
