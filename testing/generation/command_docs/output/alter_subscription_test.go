// Copyright 2023 Dolthub, Inc.
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

package output

import "testing"

func TestAlterSubscription(t *testing.T) {
	tests := []QueryParses{
		Converts("ALTER SUBSCRIPTION name CONNECTION ' conninfo '"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name WITH ( publication_option )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name WITH ( publication_option )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name WITH ( publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name WITH ( publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name WITH ( publication_option , publication_option )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name WITH ( publication_option , publication_option )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name WITH ( publication_option = value , publication_option )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name WITH ( publication_option = value , publication_option )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name WITH ( publication_option , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name WITH ( publication_option , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name WITH ( publication_option = value , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name SET PUBLICATION publication_name , publication_name WITH ( publication_option = value , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name WITH ( publication_option )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name WITH ( publication_option )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name WITH ( publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name WITH ( publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name WITH ( publication_option , publication_option )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name WITH ( publication_option , publication_option )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name WITH ( publication_option = value , publication_option )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name WITH ( publication_option = value , publication_option )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name WITH ( publication_option , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name WITH ( publication_option , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name WITH ( publication_option = value , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name ADD PUBLICATION publication_name , publication_name WITH ( publication_option = value , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name WITH ( publication_option )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name WITH ( publication_option )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name WITH ( publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name WITH ( publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name WITH ( publication_option , publication_option )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name WITH ( publication_option , publication_option )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name WITH ( publication_option = value , publication_option )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name WITH ( publication_option = value , publication_option )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name WITH ( publication_option , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name WITH ( publication_option , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name WITH ( publication_option = value , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name DROP PUBLICATION publication_name , publication_name WITH ( publication_option = value , publication_option = value )"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION WITH ( refresh_option )"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION WITH ( refresh_option = value )"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION WITH ( refresh_option , refresh_option )"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION WITH ( refresh_option = value , refresh_option )"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION WITH ( refresh_option , refresh_option = value )"),
		Converts("ALTER SUBSCRIPTION name REFRESH PUBLICATION WITH ( refresh_option = value , refresh_option = value )"),
		Converts("ALTER SUBSCRIPTION name ENABLE"),
		Converts("ALTER SUBSCRIPTION name DISABLE"),
		Converts("ALTER SUBSCRIPTION name SET ( subscription_parameter )"),
		Converts("ALTER SUBSCRIPTION name SET ( subscription_parameter = value )"),
		Converts("ALTER SUBSCRIPTION name SET ( subscription_parameter , subscription_parameter )"),
		Converts("ALTER SUBSCRIPTION name SET ( subscription_parameter = value , subscription_parameter )"),
		Converts("ALTER SUBSCRIPTION name SET ( subscription_parameter , subscription_parameter = value )"),
		Converts("ALTER SUBSCRIPTION name SET ( subscription_parameter = value , subscription_parameter = value )"),
		Converts("ALTER SUBSCRIPTION name SKIP ( skip_option = value )"),
		Converts("ALTER SUBSCRIPTION name OWNER TO new_owner"),
		Converts("ALTER SUBSCRIPTION name OWNER TO CURRENT_ROLE"),
		Converts("ALTER SUBSCRIPTION name OWNER TO CURRENT_USER"),
		Converts("ALTER SUBSCRIPTION name OWNER TO SESSION_USER"),
		Converts("ALTER SUBSCRIPTION name RENAME TO new_name"),
	}
	RunTests(t, tests)
}
