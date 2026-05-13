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
	"testing"

	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
)

func TestAppendAdditionalAuth(t *testing.T) {
	base := vitess.AuthInformation{AuthType: AuthType_INSERT}
	selectAuth := vitess.AuthInformation{AuthType: AuthType_SELECT}
	updateAuth := vitess.AuthInformation{AuthType: AuthType_UPDATE}

	AppendAdditionalAuth(&base, selectAuth)
	AppendAdditionalAuth(&base, updateAuth)

	require.Equal(t, AdditionalAuthChecks{selectAuth, updateAuth}, GetAdditionalAuth(base))
}

func TestGetAdditionalAuthIgnoresOtherExtraValues(t *testing.T) {
	authInfo := vitess.AuthInformation{
		AuthType: AuthType_EXECUTE,
		Extra:    "function-signature",
	}

	require.Nil(t, GetAdditionalAuth(authInfo))
}
