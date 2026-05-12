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

package ast

import "testing"

func TestIsPostgresEncodingName(t *testing.T) {
	tests := []struct {
		name     string
		encoding string
		want     bool
	}{
		{name: "utf8", encoding: "UTF8", want: true},
		{name: "quoted utf8", encoding: "'UTF8'", want: true},
		{name: "case insensitive", encoding: "latin1", want: true},
		{name: "unknown", encoding: "notexist", want: false},
		{name: "empty", encoding: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPostgresEncodingName(tt.encoding); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}
