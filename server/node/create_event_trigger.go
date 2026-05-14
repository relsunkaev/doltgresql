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
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions/framework"
	"github.com/dolthub/doltgresql/server/plpgsql"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// CreateEventTrigger handles the supported subset of CREATE EVENT TRIGGER.
type CreateEventTrigger struct {
	Name     string
	Event    string
	Tags     []string
	Function id.Function
}

type registeredEventTrigger struct {
	Name     string
	Event    string
	Tags     []string
	Function id.Function
}

var eventTriggerRegistry = struct {
	sync.Mutex
	triggers map[string][]registeredEventTrigger
}{
	triggers: make(map[string][]registeredEventTrigger),
}

var _ sql.ExecSourceRel = (*CreateEventTrigger)(nil)
var _ vitess.Injectable = (*CreateEventTrigger)(nil)

// NewCreateEventTrigger returns a new CREATE EVENT TRIGGER node.
func NewCreateEventTrigger(name string, event string, tags []string, function id.Function) *CreateEventTrigger {
	normalizedTags := make([]string, len(tags))
	for i, tag := range tags {
		normalizedTags[i] = strings.ToUpper(tag)
	}
	return &CreateEventTrigger{
		Name:     name,
		Event:    strings.ToLower(event),
		Tags:     normalizedTags,
		Function: function,
	}
}

func (c *CreateEventTrigger) Children() []sql.Node { return nil }
func (c *CreateEventTrigger) IsReadOnly() bool     { return false }
func (c *CreateEventTrigger) Resolved() bool       { return true }
func (c *CreateEventTrigger) Schema(ctx *sql.Context) sql.Schema {
	return nil
}
func (c *CreateEventTrigger) String() string { return "CREATE EVENT TRIGGER" }

func (c *CreateEventTrigger) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	if c.Event != "ddl_command_end" {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "only ddl_command_end event triggers are supported")
	}
	function, err := loadFunction(ctx, nil, c.Function)
	if err != nil {
		return nil, err
	}
	if !function.ID.IsValid() {
		return nil, pgerror.Newf(pgcode.UndefinedFunction, `function %s() does not exist`, c.Function.FunctionName())
	}
	if function.ReturnType != pgtypes.EventTrigger.ID {
		return nil, pgerror.Newf(pgcode.InvalidObjectDefinition, `function %s must return type event_trigger`, function.ID.FunctionName())
	}

	database := ctx.GetCurrentDatabase()
	eventTriggerRegistry.Lock()
	defer eventTriggerRegistry.Unlock()
	triggers := eventTriggerRegistry.triggers[database]
	for _, trigger := range triggers {
		if strings.EqualFold(trigger.Name, c.Name) {
			return nil, pgerror.Newf(pgcode.DuplicateObject, `event trigger "%s" already exists`, c.Name)
		}
	}
	eventTriggerRegistry.triggers[database] = append(triggers, registeredEventTrigger{
		Name:     c.Name,
		Event:    c.Event,
		Tags:     append([]string(nil), c.Tags...),
		Function: function.ID,
	})
	return sql.RowsToRowIter(), nil
}

func (c *CreateEventTrigger) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (c *CreateEventTrigger) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// FireDDLCommandEndEventTriggers fires registered ddl_command_end triggers matching the statement tag.
func FireDDLCommandEndEventTriggers(ctx *sql.Context, runner sql.StatementRunner, tag string) error {
	database := ctx.GetCurrentDatabase()
	normalizedTag := strings.ToUpper(tag)

	eventTriggerRegistry.Lock()
	triggers := append([]registeredEventTrigger(nil), eventTriggerRegistry.triggers[database]...)
	eventTriggerRegistry.Unlock()

	for _, trigger := range triggers {
		if trigger.Event != "ddl_command_end" || !slices.Contains(trigger.Tags, normalizedTag) {
			continue
		}
		function, err := loadFunction(ctx, nil, trigger.Function)
		if err != nil {
			return err
		}
		if !function.ID.IsValid() {
			pruneEventTrigger(database, trigger.Name)
			continue
		}
		if function.ReturnType != pgtypes.EventTrigger.ID {
			return errors.Errorf("function %s must return type event_trigger", function.ID.FunctionName())
		}

		iFunc := framework.InterpretedFunction{
			ID:                 function.ID,
			ReturnType:         pgtypes.EventTrigger,
			ParameterNames:     nil,
			ParameterTypes:     nil,
			Variadic:           function.Variadic,
			IsNonDeterministic: function.IsNonDeterministic,
			Strict:             function.Strict,
			Statements:         function.Operations,
			Owner:              function.Owner,
			SecurityDefiner:    function.SecurityDefiner,
		}
		_, err = plpgsql.TriggerCall(ctx, iFunc, runner, nil, nil, nil, map[string]any{
			"TG_NAME":  trigger.Name,
			"TG_WHEN":  "AFTER",
			"TG_LEVEL": "STATEMENT",
			"TG_EVENT": "ddl_command_end",
			"TG_TAG":   tag,
			"TG_NARGS": int32(0),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// HasDDLCommandEndEventTriggers returns whether any registered event trigger matches the statement tag.
func HasDDLCommandEndEventTriggers(tag string) bool {
	normalizedTag := strings.ToUpper(tag)
	eventTriggerRegistry.Lock()
	defer eventTriggerRegistry.Unlock()
	for _, triggers := range eventTriggerRegistry.triggers {
		for _, trigger := range triggers {
			if trigger.Event == "ddl_command_end" && slices.Contains(trigger.Tags, normalizedTag) {
				return true
			}
		}
	}
	return false
}

func pruneEventTrigger(database string, name string) {
	eventTriggerRegistry.Lock()
	defer eventTriggerRegistry.Unlock()
	triggers := eventTriggerRegistry.triggers[database]
	for i, trigger := range triggers {
		if strings.EqualFold(trigger.Name, name) {
			eventTriggerRegistry.triggers[database] = append(triggers[:i], triggers[i+1:]...)
			return
		}
	}
}
