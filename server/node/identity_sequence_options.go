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

package node

import (
	"encoding/json"
	"strings"
)

// TableOptionIdentitySequenceOptions carries parsed identity sequence options
// through go-mysql-server's CREATE TABLE plan until Doltgres creates the owned
// backing sequences.
const TableOptionIdentitySequenceOptions = "doltgres_identity_sequence_options"

// EncodeIdentitySequenceOptions encodes identity sequence options by column.
func EncodeIdentitySequenceOptions(options map[string][]AlterSequenceOption) string {
	encoded, _ := json.Marshal(options)
	return string(encoded)
}

// DecodeIdentitySequenceOptions decodes identity sequence options from CREATE
// TABLE options. Malformed values are ignored so normal CREATE TABLE handling
// can continue without treating this internal carrier as user input.
func DecodeIdentitySequenceOptions(tableOpts map[string]any) map[string][]AlterSequenceOption {
	if tableOpts == nil {
		return nil
	}
	raw, _ := tableOpts[TableOptionIdentitySequenceOptions].(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var options map[string][]AlterSequenceOption
	if err := json.Unmarshal([]byte(raw), &options); err != nil {
		return nil
	}
	if len(options) == 0 {
		return nil
	}
	return options
}
