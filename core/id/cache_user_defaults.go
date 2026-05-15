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

func init() {
	globalCache.setBuiltIn(NewId(Section_User, "pg_monitor"), 3373)
	globalCache.setBuiltIn(NewId(Section_User, "pg_read_all_settings"), 3374)
	globalCache.setBuiltIn(NewId(Section_User, "pg_read_all_stats"), 3375)
	globalCache.setBuiltIn(NewId(Section_User, "pg_stat_scan_tables"), 3377)
	globalCache.setBuiltIn(NewId(Section_User, "pg_signal_backend"), 4200)
	globalCache.setBuiltIn(NewId(Section_User, "pg_checkpoint"), 4544)
	globalCache.setBuiltIn(NewId(Section_User, "pg_use_reserved_connections"), 4550)
	globalCache.setBuiltIn(NewId(Section_User, "pg_read_server_files"), 4569)
	globalCache.setBuiltIn(NewId(Section_User, "pg_write_server_files"), 4570)
	globalCache.setBuiltIn(NewId(Section_User, "pg_execute_server_program"), 4571)
	globalCache.setBuiltIn(NewId(Section_User, "pg_database_owner"), 6171)
	globalCache.setBuiltIn(NewId(Section_User, "pg_read_all_data"), 6181)
	globalCache.setBuiltIn(NewId(Section_User, "pg_write_all_data"), 6182)
	globalCache.setBuiltIn(NewId(Section_User, "pg_create_subscription"), 6304)
}
