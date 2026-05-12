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

package parser

import (
	"strings"
	"testing"
)

func TestParseTableSampleSystemHundred(t *testing.T) {
	statements, err := Parse(`SELECT count(*) FROM sample_items TABLESAMPLE SYSTEM (100);`)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(statements))
	}
}

func TestParseTableSampleRejectsUnsupportedPercentage(t *testing.T) {
	_, err := Parse(`SELECT count(*) FROM sample_items TABLESAMPLE SYSTEM (50);`)
	if err == nil {
		t.Fatal("expected unsupported TABLESAMPLE percentage to fail")
	}
	if !strings.Contains(err.Error(), "TABLESAMPLE only supports SYSTEM (100)") {
		t.Fatalf("expected TABLESAMPLE unsupported percentage error, got %v", err)
	}
}
