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

import vitess "github.com/dolthub/vitess/go/vt/sqlparser"

// AdditionalAuthChecks stores supplemental authorization checks on a Vitess auth
// node when one SQL statement needs more than one privilege kind.
type AdditionalAuthChecks []vitess.AuthInformation

// AppendAdditionalAuth appends supplemental checks to an AuthInformation value.
func AppendAdditionalAuth(authInfo *vitess.AuthInformation, checks ...vitess.AuthInformation) {
	if authInfo == nil || len(checks) == 0 {
		return
	}
	existing, _ := authInfo.Extra.(AdditionalAuthChecks)
	additional := make(AdditionalAuthChecks, 0, len(existing)+len(checks))
	additional = append(additional, existing...)
	additional = append(additional, checks...)
	authInfo.Extra = additional
}

// GetAdditionalAuth returns supplemental checks, if any.
func GetAdditionalAuth(authInfo vitess.AuthInformation) AdditionalAuthChecks {
	additional, _ := authInfo.Extra.(AdditionalAuthChecks)
	return additional
}
