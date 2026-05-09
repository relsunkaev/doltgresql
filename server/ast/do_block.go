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

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/plpgsql"
)

// nodeDoBlock handles *tree.DoBlock nodes.
func nodeDoBlock(ctx *Context, node *tree.DoBlock) (vitess.Statement, error) {
	if node == nil {
		return nil, nil
	}
	language := node.Language
	if language == "" {
		language = "plpgsql"
	}
	if !strings.EqualFold(language, "plpgsql") {
		return nil, errors.Errorf("DO only supports LANGUAGE plpgsql")
	}

	wrapped := fmt.Sprintf("CREATE FUNCTION __doltgres_do_block() RETURNS void AS %s LANGUAGE plpgsql;", node.Code)
	parsedBody, err := plpgsql.Parse(wrapped)
	if err != nil {
		return nil, err
	}
	return vitess.InjectedStatement{
		Statement: pgnodes.NewDoBlock(ctx.originalQuery, parsedBody),
	}, nil
}
