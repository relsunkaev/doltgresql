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
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// CopyTo handles the COPY ... TO ... statement.
type CopyTo struct {
	DatabaseName string
	TableName    doltdb.TableName
	Stdout       bool
	Columns      tree.NameList
	CopyOptions  tree.CopyOptions
}

var _ vitess.Injectable = (*CopyTo)(nil)

// NewCopyTo returns a new *CopyTo.
func NewCopyTo(databaseName string, tableName doltdb.TableName, stdout bool, columns tree.NameList, options tree.CopyOptions) *CopyTo {
	return &CopyTo{
		DatabaseName: databaseName,
		TableName:    tableName,
		Stdout:       stdout,
		Columns:      columns,
		CopyOptions:  options,
	}
}

// String implements the interface vitess.Injectable.
func (ct *CopyTo) String() string {
	target := "STDOUT"
	if !ct.Stdout {
		target = "<unsupported>"
	}
	return fmt.Sprintf("COPY TO %s", target)
}

// WithResolvedChildren implements the interface vitess.Injectable.
func (ct *CopyTo) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return ct, nil
}
