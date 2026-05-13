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
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
)

// AlterViewOption is a PostgreSQL view option from ALTER VIEW SET/RESET.
type AlterViewOption struct {
	Name     string
	CheckOpt string
	Security bool
}

// AlterViewOptions handles ALTER VIEW ... SET/RESET (...) reloptions.
type AlterViewOptions struct {
	ifExists bool
	schema   string
	view     string
	reset    bool
	options  []AlterViewOption
}

var _ sql.ExecSourceRel = (*AlterViewOptions)(nil)
var _ vitess.Injectable = (*AlterViewOptions)(nil)

// NewAlterViewOptions returns a new *AlterViewOptions.
func NewAlterViewOptions(ifExists bool, schema string, view string, reset bool, options []AlterViewOption) *AlterViewOptions {
	return &AlterViewOptions{
		ifExists: ifExists,
		schema:   schema,
		view:     view,
		reset:    reset,
		options:  append([]AlterViewOption(nil), options...),
	}
}

// Children implements sql.ExecSourceRel.
func (a *AlterViewOptions) Children() []sql.Node {
	return nil
}

// IsReadOnly implements sql.ExecSourceRel.
func (a *AlterViewOptions) IsReadOnly() bool {
	return false
}

// Resolved implements sql.ExecSourceRel.
func (a *AlterViewOptions) Resolved() bool {
	return true
}

// RowIter implements sql.ExecSourceRel.
func (a *AlterViewOptions) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	resolver := AlterRelationOwner{
		kind:     relationOwnerKindView,
		ifExists: a.ifExists,
		schema:   a.schema,
		name:     a.view,
	}
	viewName, err := resolver.resolveView(ctx)
	if err != nil {
		return nil, err
	}
	if viewName.Name == "" {
		return sql.RowsToRowIter(), nil
	}
	if err = resolver.checkOwnership(ctx, viewName); err != nil {
		return nil, errors.Wrap(err, "permission denied")
	}

	schemaDatabase, err := currentSchemaDatabase(ctx)
	if err != nil {
		return nil, err
	}
	schema, ok, err := schemaDatabase.GetSchema(ctx, viewName.Schema)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseSchemaNotFound.New(viewName.Schema)
	}
	viewDatabase, ok := schema.(sql.ViewDatabase)
	if !ok {
		return nil, errors.Errorf("schema %s does not support views", viewName.Schema)
	}
	view, exists, err := viewDatabase.GetViewDefinition(ctx, viewName.Name)
	if err != nil {
		return nil, err
	}
	if !exists {
		if a.ifExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`relation "%s" does not exist`, a.view)
	}

	textDefinition, createViewStatement, err := alteredViewOptionDefinitions(view, viewName, a.options, a.reset)
	if err != nil {
		return nil, err
	}
	if err = viewDatabase.DropView(ctx, viewName.Name); err != nil {
		return nil, err
	}
	if err = viewDatabase.CreateView(ctx, viewName.Name, textDefinition, createViewStatement); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// Schema implements sql.ExecSourceRel.
func (a *AlterViewOptions) Schema(ctx *sql.Context) sql.Schema {
	return nil
}

// String implements sql.ExecSourceRel.
func (a *AlterViewOptions) String() string {
	return "ALTER VIEW OPTIONS"
}

// WithChildren implements sql.ExecSourceRel.
func (a *AlterViewOptions) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

// WithResolvedChildren implements vitess.Injectable.
func (a *AlterViewOptions) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

func alteredViewOptionDefinitions(view sql.ViewDefinition, viewName doltdb.TableName, changes []AlterViewOption, reset bool) (string, string, error) {
	textDefinition, columnNames, options, err := parsedViewDefinitionParts(view)
	if err != nil {
		return "", "", err
	}
	options, err = applyAlterViewOptions(options, changes, reset)
	if err != nil {
		return "", "", err
	}
	createViewStatement := buildCreateViewStatement(viewName.Name, columnNames, options, textDefinition)
	return textDefinition, createViewStatement, nil
}

func parsedViewDefinitionParts(view sql.ViewDefinition) (string, []string, []AlterViewOption, error) {
	textDefinition := strings.TrimSpace(view.TextDefinition)
	if textDefinition == "" {
		textDefinition = strings.TrimSpace(view.CreateViewStatement)
	}
	if textDefinition == "" {
		return "", nil, nil, sql.ErrViewCreateStatementInvalid.New(view.CreateViewStatement)
	}

	stmts, err := parser.Parse(view.CreateViewStatement)
	if err != nil || len(stmts) == 0 {
		return textDefinition, nil, nil, nil
	}
	createView, ok := stmts[0].AST.(*tree.CreateView)
	if !ok {
		return textDefinition, nil, nil, nil
	}

	if createView.AsSource != nil {
		textDefinition = strings.TrimSpace(tree.AsString(createView.AsSource))
	}
	columnNames := make([]string, len(createView.ColumnNames))
	for i, name := range createView.ColumnNames {
		columnNames[i] = string(name)
	}
	options := make([]AlterViewOption, 0, len(createView.Options)+1)
	for _, opt := range createView.Options {
		options = append(options, AlterViewOption{
			Name:     opt.Name,
			CheckOpt: opt.CheckOpt,
			Security: opt.Security,
		})
	}
	switch createView.CheckOption {
	case tree.ViewCheckOptionCascaded:
		options = upsertViewOption(options, AlterViewOption{Name: "check_option", CheckOpt: "cascaded"})
	case tree.ViewCheckOptionLocal:
		options = upsertViewOption(options, AlterViewOption{Name: "check_option", CheckOpt: "local"})
	}
	return textDefinition, columnNames, options, nil
}

func applyAlterViewOptions(options []AlterViewOption, changes []AlterViewOption, reset bool) ([]AlterViewOption, error) {
	for _, change := range changes {
		name := strings.ToLower(change.Name)
		switch name {
		case "security_invoker", "security_barrier":
			change.Name = name
		case "check_option":
			change.Name = name
			change.CheckOpt = strings.ToLower(change.CheckOpt)
			if !reset && change.CheckOpt != "local" && change.CheckOpt != "cascaded" {
				return nil, errors.Errorf(`invalid value for view option "check_option": %s`, change.CheckOpt)
			}
		default:
			return nil, errors.Errorf(`unrecognized view option "%s"`, change.Name)
		}
		if reset {
			options = removeViewOption(options, change.Name)
			continue
		}
		options = upsertViewOption(options, change)
	}
	return options, nil
}

func removeViewOption(options []AlterViewOption, name string) []AlterViewOption {
	name = strings.ToLower(name)
	filtered := options[:0]
	for _, opt := range options {
		if strings.ToLower(opt.Name) == name {
			continue
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

func upsertViewOption(options []AlterViewOption, option AlterViewOption) []AlterViewOption {
	return append(removeViewOption(options, option.Name), option)
}

func buildCreateViewStatement(viewName string, columnNames []string, options []AlterViewOption, textDefinition string) string {
	var builder strings.Builder
	builder.WriteString("CREATE VIEW ")
	builder.WriteString(quoteSQLIdentifier(viewName))
	if len(columnNames) > 0 {
		builder.WriteString(" (")
		for i, columnName := range columnNames {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteString(quoteSQLIdentifier(columnName))
		}
		builder.WriteString(")")
	}
	if len(options) > 0 {
		builder.WriteString(" WITH (")
		for i, opt := range options {
			if i > 0 {
				builder.WriteString(", ")
			}
			switch strings.ToLower(opt.Name) {
			case "check_option":
				builder.WriteString("check_option = ")
				builder.WriteString(strings.ToLower(opt.CheckOpt))
			case "security_barrier", "security_invoker":
				builder.WriteString(strings.ToLower(opt.Name))
				if opt.Security {
					builder.WriteString(" = true")
				} else {
					builder.WriteString(" = false")
				}
			}
		}
		builder.WriteString(")")
	}
	builder.WriteString(" AS ")
	builder.WriteString(textDefinition)
	return builder.String()
}
