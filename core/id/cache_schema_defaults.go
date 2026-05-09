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

package id

// This adds all of the built-in schemas to the cache.
func init() {
	globalCache.setBuiltIn(NewId(Section_Namespace, "pg_catalog"), 11)
	globalCache.setBuiltIn(NewId(Section_Namespace, "pg_toast"), 99)
	globalCache.setBuiltIn(NewId(Section_Namespace, "public"), 2200)
	globalCache.setBuiltIn(NewId(Section_Namespace, "information_schema"), 13183)

	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_class"), 1259)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_proc"), 1255)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_type"), 1247)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_namespace"), 2615)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_extension"), 3079)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_language"), 2612)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_operator"), 2617)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_am"), 2601)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_opclass"), 2616)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_opfamily"), 2753)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_ts_dict"), 3600)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_ts_parser"), 3601)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_ts_config"), 3602)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_ts_config_map"), 3603)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_ts_template"), 3764)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_foreign_server"), 1417)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_user_mapping"), 1418)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_foreign_data_wrapper"), 2328)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_foreign_table"), 3118)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_default_acl"), 826)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_collation"), 3456)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_conversion"), 2607)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_cast"), 2605)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_attrdef"), 2604)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_constraint"), 2606)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_rewrite"), 2618)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_trigger"), 2620)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_statistic_ext"), 3381)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_transform"), 3576)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_event_trigger"), 3466)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_policy"), 3256)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_publication"), 6104)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_publication_rel"), 6106)
	globalCache.setBuiltIn(NewId(Section_Table, "pg_catalog", "pg_publication_namespace"), 6237)
}
