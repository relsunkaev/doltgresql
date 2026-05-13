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

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/publications"
	"github.com/dolthub/doltgresql/core/subscriptions"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/functions"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// PublicationTableSpec is the parsed table membership of a publication.
type PublicationTableSpec struct {
	Schema    string
	Name      string
	Columns   []string
	RowFilter string
}

// CreatePublication handles CREATE PUBLICATION.
type CreatePublication struct {
	Name      string
	AllTables bool
	Tables    []PublicationTableSpec
	Schemas   []string
	Options   map[string]string
}

var _ sql.ExecSourceRel = (*CreatePublication)(nil)
var _ vitess.Injectable = (*CreatePublication)(nil)

func (c *CreatePublication) Children() []sql.Node               { return nil }
func (c *CreatePublication) IsReadOnly() bool                   { return false }
func (c *CreatePublication) Resolved() bool                     { return true }
func (c *CreatePublication) Schema(ctx *sql.Context) sql.Schema { return nil }
func (c *CreatePublication) String() string                     { return "CREATE PUBLICATION" }

func (c *CreatePublication) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	if strings.HasPrefix(strings.ToLower(c.Name), "dolt") {
		return nil, errors.Errorf("publications cannot be prefixed with 'dolt'")
	}
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pub := publications.NewPublication(c.Name)
	pub.Owner = id.NewId(id.Section_User, ctx.Client().User)
	pub.AllTables = c.AllTables
	if err = applyPublicationOptions(&pub, c.Options); err != nil {
		return nil, err
	}
	if collection.HasPublication(ctx, pub.ID) {
		return nil, pgerror.Newf(pgcode.DuplicateObject, `publication "%s" already exists`, c.Name)
	}
	pub.Tables, err = resolvePublicationTables(ctx, c.Tables)
	if err != nil {
		return nil, err
	}
	pub.Schemas, err = resolvePublicationSchemas(ctx, c.Schemas)
	if err != nil {
		return nil, err
	}
	if err = validatePublicationSchemaMembership(pub, publicationSchemaRestrictionColumnList); err != nil {
		return nil, err
	}
	if err = collection.AddPublication(ctx, pub); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreatePublication) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (c *CreatePublication) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// PublicationAlterAction is the action in ALTER PUBLICATION.
type PublicationAlterAction string

const (
	PublicationAlterAddTables   PublicationAlterAction = "add_tables"
	PublicationAlterSetTables   PublicationAlterAction = "set_tables"
	PublicationAlterDropTables  PublicationAlterAction = "drop_tables"
	PublicationAlterAddSchemas  PublicationAlterAction = "add_schemas"
	PublicationAlterSetSchemas  PublicationAlterAction = "set_schemas"
	PublicationAlterDropSchemas PublicationAlterAction = "drop_schemas"
	PublicationAlterSetOptions  PublicationAlterAction = "set_options"
	PublicationAlterRename      PublicationAlterAction = "rename"
)

// AlterPublication handles ALTER PUBLICATION.
type AlterPublication struct {
	Name      string
	Action    PublicationAlterAction
	NewName   string
	Tables    []PublicationTableSpec
	Schemas   []string
	Options   map[string]string
	AllTables bool
}

var _ sql.ExecSourceRel = (*AlterPublication)(nil)
var _ vitess.Injectable = (*AlterPublication)(nil)

func (a *AlterPublication) Children() []sql.Node               { return nil }
func (a *AlterPublication) IsReadOnly() bool                   { return false }
func (a *AlterPublication) Resolved() bool                     { return true }
func (a *AlterPublication) Schema(ctx *sql.Context) sql.Schema { return nil }
func (a *AlterPublication) String() string                     { return "ALTER PUBLICATION" }

func (a *AlterPublication) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pubID := id.NewPublication(a.Name)
	pub, err := collection.GetPublication(ctx, pubID)
	if err != nil {
		return nil, err
	}
	if !pub.ID.IsValid() {
		return nil, errors.Errorf(`publication "%s" does not exist`, a.Name)
	}
	switch a.Action {
	case PublicationAlterAddTables:
		tables, err := resolvePublicationTables(ctx, a.Tables)
		if err != nil {
			return nil, err
		}
		schemas, err := resolvePublicationSchemas(ctx, a.Schemas)
		if err != nil {
			return nil, err
		}
		combinedSchemas := append(slices.Clone(pub.Schemas), schemas...)
		if err = validatePublicationSchemaMembership(publications.Publication{
			ID:      pub.ID,
			Tables:  tables,
			Schemas: combinedSchemas,
		}, publicationSchemaRestrictionColumnList); err != nil {
			return nil, err
		}
		if len(schemas) > 0 && publicationHasRestrictedTable(pub.Tables) {
			return nil, publicationAddSchemaRestrictionError(pub.ID.PublicationName())
		}
		if err = addPublicationTables(&pub, tables); err != nil {
			return nil, err
		}
		if err = addPublicationSchemas(&pub, schemas); err != nil {
			return nil, err
		}
	case PublicationAlterSetTables:
		pub.AllTables = false
		pub.Tables, err = resolvePublicationTables(ctx, a.Tables)
		if err != nil {
			return nil, err
		}
		pub.Schemas, err = resolvePublicationSchemas(ctx, a.Schemas)
		if err != nil {
			return nil, err
		}
		if err = validatePublicationSchemaMembership(pub, publicationSchemaRestrictionColumnList); err != nil {
			return nil, err
		}
	case PublicationAlterDropTables:
		tables, err := resolvePublicationTables(ctx, a.Tables)
		if err != nil {
			return nil, err
		}
		if err = dropPublicationTables(&pub, tables); err != nil {
			return nil, err
		}
		schemas, err := resolvePublicationSchemas(ctx, a.Schemas)
		if err != nil {
			return nil, err
		}
		if err = dropPublicationSchemas(&pub, schemas); err != nil {
			return nil, err
		}
	case PublicationAlterAddSchemas:
		schemas, err := resolvePublicationSchemas(ctx, a.Schemas)
		if err != nil {
			return nil, err
		}
		if err = addPublicationSchemas(&pub, schemas); err != nil {
			return nil, err
		}
		if err = validatePublicationSchemaMembership(pub, publicationSchemaRestrictionAddSchema); err != nil {
			return nil, err
		}
	case PublicationAlterSetSchemas:
		pub.AllTables = false
		pub.Schemas, err = resolvePublicationSchemas(ctx, a.Schemas)
		if err != nil {
			return nil, err
		}
		if err = validatePublicationSchemaMembership(pub, publicationSchemaRestrictionAddSchema); err != nil {
			return nil, err
		}
	case PublicationAlterDropSchemas:
		schemas, err := resolvePublicationSchemas(ctx, a.Schemas)
		if err != nil {
			return nil, err
		}
		if err = dropPublicationSchemas(&pub, schemas); err != nil {
			return nil, err
		}
	case PublicationAlterSetOptions:
		if err = applyPublicationOptions(&pub, a.Options); err != nil {
			return nil, err
		}
	case PublicationAlterRename:
		if a.NewName == "" {
			return nil, errors.New("missing publication rename target")
		}
		newID := id.NewPublication(a.NewName)
		if collection.HasPublication(ctx, newID) {
			return nil, pgerror.Newf(pgcode.DuplicateObject, `publication "%s" already exists`, a.NewName)
		}
		if err = collection.DropPublication(ctx, pubID); err != nil {
			return nil, err
		}
		pub.ID = newID
		return sql.RowsToRowIter(), collection.AddPublication(ctx, pub)
	default:
		return nil, errors.Errorf("unknown ALTER PUBLICATION action: %s", a.Action)
	}
	if err = collection.UpdatePublication(ctx, pub); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterPublication) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterPublication) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// DropPublication handles DROP PUBLICATION.
type DropPublication struct {
	Names    []string
	IfExists bool
	Cascade  bool
}

var _ sql.ExecSourceRel = (*DropPublication)(nil)
var _ vitess.Injectable = (*DropPublication)(nil)

func (d *DropPublication) Children() []sql.Node               { return nil }
func (d *DropPublication) IsReadOnly() bool                   { return false }
func (d *DropPublication) Resolved() bool                     { return true }
func (d *DropPublication) Schema(ctx *sql.Context) sql.Schema { return nil }
func (d *DropPublication) String() string                     { return "DROP PUBLICATION" }

func (d *DropPublication) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	collection, err := core.GetPublicationsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	for _, name := range d.Names {
		pubID := id.NewPublication(name)
		if !collection.HasPublication(ctx, pubID) {
			if d.IfExists {
				continue
			}
			return nil, errors.Errorf(`publication "%s" does not exist`, name)
		}
		if err = collection.DropPublication(ctx, pubID); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}

func (d *DropPublication) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

func (d *DropPublication) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

// CreateSubscription handles CREATE SUBSCRIPTION metadata. Remote apply workers are intentionally not started here.
type CreateSubscription struct {
	Name         string
	ConnInfo     string
	Publications []string
	Options      map[string]string
}

var _ sql.ExecSourceRel = (*CreateSubscription)(nil)
var _ vitess.Injectable = (*CreateSubscription)(nil)

func (c *CreateSubscription) Children() []sql.Node               { return nil }
func (c *CreateSubscription) IsReadOnly() bool                   { return false }
func (c *CreateSubscription) Resolved() bool                     { return true }
func (c *CreateSubscription) Schema(ctx *sql.Context) sql.Schema { return nil }
func (c *CreateSubscription) String() string                     { return "CREATE SUBSCRIPTION" }

func (c *CreateSubscription) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	if strings.HasPrefix(strings.ToLower(c.Name), "dolt") {
		return nil, errors.Errorf("subscriptions cannot be prefixed with 'dolt'")
	}
	if len(c.Publications) == 0 {
		return nil, errors.New("CREATE SUBSCRIPTION requires at least one publication")
	}
	connect, err := optionBool(c.Options, "connect", true)
	if err != nil {
		return nil, err
	}
	if connect {
		return nil, errors.New("subscription publisher connections are not yet supported; use WITH (connect=false)")
	}
	if err = validateMetadataOnlySubscriptionCreateOptions(c.Options); err != nil {
		return nil, err
	}
	collection, err := core.GetSubscriptionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sub := subscriptions.NewSubscription(c.Name)
	sub.ConnInfo = c.ConnInfo
	sub.Publications = slices.Clone(c.Publications)
	sub.SlotName = c.Name
	if err = applySubscriptionOptions(&sub, c.Options); err != nil {
		return nil, err
	}
	if err = collection.AddSubscription(ctx, sub); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (c *CreateSubscription) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(c, children...)
}

func (c *CreateSubscription) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return c, nil
}

// SubscriptionAlterAction is the action in ALTER SUBSCRIPTION.
type SubscriptionAlterAction string

const (
	SubscriptionAlterConnection      SubscriptionAlterAction = "connection"
	SubscriptionAlterSetPublication  SubscriptionAlterAction = "set_publication"
	SubscriptionAlterAddPublication  SubscriptionAlterAction = "add_publication"
	SubscriptionAlterDropPublication SubscriptionAlterAction = "drop_publication"
	SubscriptionAlterRefresh         SubscriptionAlterAction = "refresh"
	SubscriptionAlterEnable          SubscriptionAlterAction = "enable"
	SubscriptionAlterDisable         SubscriptionAlterAction = "disable"
	SubscriptionAlterSetOptions      SubscriptionAlterAction = "set_options"
	SubscriptionAlterSkip            SubscriptionAlterAction = "skip"
	SubscriptionAlterRename          SubscriptionAlterAction = "rename"
	SubscriptionAlterOwner           SubscriptionAlterAction = "owner"
)

// AlterSubscription handles ALTER SUBSCRIPTION.
type AlterSubscription struct {
	Name         string
	Action       SubscriptionAlterAction
	NewName      string
	Owner        string
	ConnInfo     string
	Publications []string
	Options      map[string]string
}

var _ sql.ExecSourceRel = (*AlterSubscription)(nil)
var _ vitess.Injectable = (*AlterSubscription)(nil)

func (a *AlterSubscription) Children() []sql.Node               { return nil }
func (a *AlterSubscription) IsReadOnly() bool                   { return false }
func (a *AlterSubscription) Resolved() bool                     { return true }
func (a *AlterSubscription) Schema(ctx *sql.Context) sql.Schema { return nil }
func (a *AlterSubscription) String() string                     { return "ALTER SUBSCRIPTION" }

func (a *AlterSubscription) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	collection, err := core.GetSubscriptionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	subID := id.NewSubscription(a.Name)
	sub, err := collection.GetSubscription(ctx, subID)
	if err != nil {
		return nil, err
	}
	if !sub.ID.IsValid() {
		return nil, errors.Errorf(`subscription "%s" does not exist`, a.Name)
	}
	switch a.Action {
	case SubscriptionAlterConnection:
		sub.ConnInfo = a.ConnInfo
	case SubscriptionAlterSetPublication:
		if err = validateSubscriptionPublicationAlterOptions(sub, a.Options); err != nil {
			return nil, err
		}
		sub.Publications = slices.Clone(a.Publications)
		if err = applySubscriptionOptions(&sub, a.Options); err != nil {
			return nil, err
		}
	case SubscriptionAlterAddPublication:
		if err = validateSubscriptionPublicationAlterOptions(sub, a.Options); err != nil {
			return nil, err
		}
		for _, publication := range a.Publications {
			if slices.Contains(sub.Publications, publication) {
				return nil, errors.Errorf(`publication "%s" is already in subscription "%s"`, publication, a.Name)
			}
			sub.Publications = append(sub.Publications, publication)
		}
		if err = applySubscriptionOptions(&sub, a.Options); err != nil {
			return nil, err
		}
	case SubscriptionAlterDropPublication:
		if err = validateSubscriptionPublicationAlterOptions(sub, a.Options); err != nil {
			return nil, err
		}
		for _, publication := range a.Publications {
			idx := slices.Index(sub.Publications, publication)
			if idx < 0 {
				return nil, errors.Errorf(`publication "%s" is not in subscription "%s"`, publication, a.Name)
			}
			sub.Publications = slices.Delete(sub.Publications, idx, idx+1)
		}
		if err = applySubscriptionOptions(&sub, a.Options); err != nil {
			return nil, err
		}
	case SubscriptionAlterRefresh:
		if !sub.Enabled {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions")
		}
		return nil, errors.New("subscription refresh requires publisher connections, which are not yet supported")
	case SubscriptionAlterEnable:
		if sub.SlotName == "" {
			return nil, errors.New("cannot enable subscription that does not have a slot name")
		}
		sub.Enabled = true
	case SubscriptionAlterDisable:
		sub.Enabled = false
	case SubscriptionAlterSetOptions:
		if err = applySubscriptionOptions(&sub, a.Options); err != nil {
			return nil, err
		}
	case SubscriptionAlterSkip:
		lsn, ok := a.Options["lsn"]
		if !ok {
			return nil, errors.New(`ALTER SUBSCRIPTION SKIP requires "lsn"`)
		}
		if _, err = pgtypes.ParsePgLsn(lsn); err != nil {
			return nil, err
		}
		sub.SkipLSN = pgtypes.FormatPgLsn(mustParsePgLsn(lsn))
	case SubscriptionAlterRename:
		if a.NewName == "" {
			return nil, errors.New("missing subscription rename target")
		}
		newID := id.NewSubscription(a.NewName)
		if collection.HasSubscription(ctx, newID) {
			return nil, errors.Errorf(`subscription "%s" already exists`, a.NewName)
		}
		if err = collection.DropSubscription(ctx, subID); err != nil {
			return nil, err
		}
		sub.ID = newID
		return sql.RowsToRowIter(), collection.AddSubscription(ctx, sub)
	case SubscriptionAlterOwner:
		// Object ownership is not modeled in Doltgres catalogs yet. Accepting
		// this statement after existence validation preserves PostgreSQL client compatibility.
	default:
		return nil, errors.Errorf("unknown ALTER SUBSCRIPTION action: %s", a.Action)
	}
	if err = collection.UpdateSubscription(ctx, sub); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (a *AlterSubscription) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(a, children...)
}

func (a *AlterSubscription) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return a, nil
}

// DropSubscription handles DROP SUBSCRIPTION.
type DropSubscription struct {
	Name     string
	IfExists bool
	Cascade  bool
}

var _ sql.ExecSourceRel = (*DropSubscription)(nil)
var _ vitess.Injectable = (*DropSubscription)(nil)

func (d *DropSubscription) Children() []sql.Node               { return nil }
func (d *DropSubscription) IsReadOnly() bool                   { return false }
func (d *DropSubscription) Resolved() bool                     { return true }
func (d *DropSubscription) Schema(ctx *sql.Context) sql.Schema { return nil }
func (d *DropSubscription) String() string                     { return "DROP SUBSCRIPTION" }

func (d *DropSubscription) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	collection, err := core.GetSubscriptionsCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	subID := id.NewSubscription(d.Name)
	if !collection.HasSubscription(ctx, subID) {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		}
		return nil, errors.Errorf(`subscription "%s" does not exist`, d.Name)
	}
	if err = collection.DropSubscription(ctx, subID); err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

func (d *DropSubscription) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	return plan.NillaryWithChildren(d, children...)
}

func (d *DropSubscription) WithResolvedChildren(ctx context.Context, children []any) (any, error) {
	if len(children) != 0 {
		return nil, ErrVitessChildCount.New(0, len(children))
	}
	return d, nil
}

func resolvePublicationTables(ctx *sql.Context, specs []PublicationTableSpec) ([]publications.PublicationRelation, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	relations := make([]publications.PublicationRelation, 0, len(specs))
	seen := make(map[id.Table]struct{}, len(specs))
	for _, spec := range specs {
		schema, err := core.GetSchemaName(ctx, nil, spec.Schema)
		if err != nil {
			return nil, err
		}
		table, err := core.GetSqlTableFromContext(ctx, "", doltdb.TableName{Name: spec.Name, Schema: schema})
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, errors.Errorf(`relation "%s" does not exist`, doltdb.TableName{Name: spec.Name, Schema: schema}.String())
		}
		relationID := id.NewTable(schema, spec.Name)
		if _, ok := seen[relationID]; ok {
			return nil, errors.Errorf(`table "%s" is specified more than once`, doltdb.TableName{Name: spec.Name, Schema: schema}.String())
		}
		seen[relationID] = struct{}{}
		columns, err := validatePublicationColumns(table.Schema(ctx), spec.Columns)
		if err != nil {
			return nil, err
		}
		relations = append(relations, publications.PublicationRelation{
			Table:     relationID,
			Columns:   columns,
			RowFilter: spec.RowFilter,
		})
	}
	return relations, nil
}

func validatePublicationColumns(tableSchema sql.Schema, columns []string) ([]string, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	resolved := make([]string, len(columns))
	for i, column := range columns {
		found := false
		for _, tableColumn := range tableSchema {
			if tableColumn.Name == column || strings.EqualFold(tableColumn.Name, column) {
				resolved[i] = tableColumn.Name
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf(`column "%s" does not exist`, column)
		}
	}
	return resolved, nil
}

func resolvePublicationSchemas(ctx *sql.Context, schemaNames []string) ([]string, error) {
	if len(schemaNames) == 0 {
		return nil, nil
	}
	schemas := make([]string, 0, len(schemaNames))
	for _, schemaName := range schemaNames {
		schema, err := core.GetSchemaName(ctx, nil, schemaName)
		if err != nil {
			return nil, err
		}
		exists, err := publicationSchemaExists(ctx, schema)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.Errorf(`schema "%s" does not exist`, schema)
		}
		schemas = append(schemas, schema)
	}
	slices.Sort(schemas)
	return slices.Compact(schemas), nil
}

func publicationSchemaExists(ctx *sql.Context, schema string) (bool, error) {
	exists := false
	err := functions.IterateCurrentDatabase(ctx, functions.Callbacks{
		Schema: func(ctx *sql.Context, item functions.ItemSchema) (cont bool, err error) {
			if strings.EqualFold(item.Item.SchemaName(), schema) {
				exists = true
				return false, nil
			}
			return true, nil
		},
	})
	return exists, err
}

func applyPublicationOptions(pub *publications.Publication, options map[string]string) error {
	for key, raw := range options {
		value := strings.TrimSpace(raw)
		switch key {
		case "publish":
			if value == "" || strings.EqualFold(value, "true") {
				return errors.New(`publication option "publish" requires a comma-separated action list`)
			}
			pub.PublishInsert = false
			pub.PublishUpdate = false
			pub.PublishDelete = false
			pub.PublishTruncate = false
			for _, action := range strings.Split(value, ",") {
				switch strings.ToLower(strings.TrimSpace(action)) {
				case "insert":
					pub.PublishInsert = true
				case "update":
					pub.PublishUpdate = true
				case "delete":
					pub.PublishDelete = true
				case "truncate":
					pub.PublishTruncate = true
				default:
					return errors.Errorf(`unrecognized publication publish action "%s"`, strings.TrimSpace(action))
				}
			}
		case "publish_via_partition_root":
			parsed, err := parseReplicationBoolOption(key, value)
			if err != nil {
				return err
			}
			pub.PublishViaPartition = parsed
		default:
			return errors.Errorf(`unrecognized publication option "%s"`, key)
		}
	}
	return nil
}

type publicationSchemaRestrictionError string

const (
	publicationSchemaRestrictionAddSchema  publicationSchemaRestrictionError = "add_schema"
	publicationSchemaRestrictionColumnList publicationSchemaRestrictionError = "column_list"
)

func validatePublicationSchemaMembership(pub publications.Publication, errorKind publicationSchemaRestrictionError) error {
	if len(pub.Schemas) == 0 {
		return nil
	}
	for _, table := range pub.Tables {
		if len(table.Columns) == 0 && strings.TrimSpace(table.RowFilter) == "" {
			continue
		}
		if errorKind == publicationSchemaRestrictionAddSchema {
			return publicationAddSchemaRestrictionError(pub.ID.PublicationName())
		}
		if len(table.Columns) > 0 {
			return pgerror.New(pgcode.InvalidParameterValue,
				"cannot use column list in publication that publishes tables in schemas")
		}
		return pgerror.New(pgcode.InvalidParameterValue,
			"cannot use row filter in publication that publishes tables in schemas")
	}
	return nil
}

func publicationHasRestrictedTable(tables []publications.PublicationRelation) bool {
	for _, table := range tables {
		if len(table.Columns) > 0 || strings.TrimSpace(table.RowFilter) != "" {
			return true
		}
	}
	return false
}

func publicationAddSchemaRestrictionError(publicationName string) error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		`cannot add schema to publication "%s" because it contains a table where a row filter or column list is specified`,
		publicationName)
}

func addPublicationTables(pub *publications.Publication, tables []publications.PublicationRelation) error {
	for _, table := range tables {
		if slices.ContainsFunc(pub.Tables, func(existing publications.PublicationRelation) bool {
			return existing.Table == table.Table
		}) {
			return errors.Errorf(`relation "%s" is already member of publication "%s"`, table.Table, pub.ID.PublicationName())
		}
		pub.Tables = append(pub.Tables, table)
	}
	return nil
}

func dropPublicationTables(pub *publications.Publication, tables []publications.PublicationRelation) error {
	for _, table := range tables {
		idx := slices.IndexFunc(pub.Tables, func(existing publications.PublicationRelation) bool {
			return existing.Table == table.Table
		})
		if idx < 0 {
			return errors.Errorf(`relation "%s" is not member of publication "%s"`, table.Table, pub.ID.PublicationName())
		}
		pub.Tables = slices.Delete(pub.Tables, idx, idx+1)
	}
	return nil
}

func addPublicationSchemas(pub *publications.Publication, schemas []string) error {
	for _, schema := range schemas {
		if slices.ContainsFunc(pub.Schemas, func(existing string) bool {
			return strings.EqualFold(existing, schema)
		}) {
			return errors.Errorf(`schema "%s" is already member of publication "%s"`, schema, pub.ID.PublicationName())
		}
		pub.Schemas = append(pub.Schemas, schema)
	}
	return nil
}

func dropPublicationSchemas(pub *publications.Publication, schemas []string) error {
	for _, schema := range schemas {
		idx := slices.IndexFunc(pub.Schemas, func(existing string) bool {
			return strings.EqualFold(existing, schema)
		})
		if idx < 0 {
			return errors.Errorf(`schema "%s" is not member of publication "%s"`, schema, pub.ID.PublicationName())
		}
		pub.Schemas = slices.Delete(pub.Schemas, idx, idx+1)
	}
	return nil
}

func applySubscriptionOptions(sub *subscriptions.Subscription, options map[string]string) error {
	for key, raw := range options {
		value := strings.TrimSpace(raw)
		switch key {
		case "connect":
			if optionBoolDefault(options, "connect", true) {
				return errors.New("subscription publisher connections are not yet supported; use connect=false")
			}
		case "create_slot":
			if optionBoolDefault(options, "create_slot", false) {
				return errors.New("creating remote subscription slots is not yet supported")
			}
		case "enabled":
			parsed, err := parseReplicationBoolOption(key, value)
			if err != nil {
				return err
			}
			sub.Enabled = parsed
		case "binary":
			parsed, err := parseReplicationBoolOption(key, value)
			if err != nil {
				return err
			}
			sub.Binary = parsed
		case "streaming":
			sub.Stream = parseStreamingOption(value)
		case "two_phase":
			parsed, err := parseReplicationBoolOption(key, value)
			if err != nil {
				return err
			}
			if parsed {
				sub.TwoPhaseState = "p"
			} else {
				sub.TwoPhaseState = "d"
			}
		case "disable_on_error":
			parsed, err := parseReplicationBoolOption(key, value)
			if err != nil {
				return err
			}
			sub.DisableOnError = parsed
		case "slot_name":
			if strings.EqualFold(value, "none") {
				sub.SlotName = ""
			} else {
				sub.SlotName = value
			}
		case "synchronous_commit":
			sub.SyncCommit = value
		case "copy_data":
			if _, err := parseReplicationBoolOption(key, value); err != nil {
				return err
			}
		case "refresh":
			if _, err := parseReplicationBoolOption(key, value); err != nil {
				return err
			}
		case "origin", "run_as_owner", "password_required":
			// These options affect remote apply behavior. They are accepted so metadata-only
			// subscriptions can round-trip PgDog setup, but no local worker is started.
		case "lsn":
			if _, err := pgtypes.ParsePgLsn(value); err != nil {
				return err
			}
			sub.SkipLSN = pgtypes.FormatPgLsn(mustParsePgLsn(value))
		default:
			return errors.Errorf(`unrecognized subscription option "%s"`, key)
		}
	}
	return nil
}

func validateSubscriptionPublicationAlterOptions(sub subscriptions.Subscription, options map[string]string) error {
	// copy_data only affects refresh worker behavior, which Doltgres does not
	// run yet. Parsing it here preserves PostgreSQL boolean validation before
	// any publication membership is mutated.
	if _, _, err := explicitBoolOption(options, "copy_data"); err != nil {
		return err
	}
	refresh, err := optionBool(options, "refresh", true)
	if err != nil {
		return err
	}
	if !refresh {
		return nil
	}
	if !sub.Enabled {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions")
	}
	return errors.New("subscription refresh requires publisher connections, which are not yet supported")
}

func validateMetadataOnlySubscriptionCreateOptions(options map[string]string) error {
	for _, key := range []string{"create_slot", "enabled", "copy_data"} {
		value, ok, err := explicitBoolOption(options, key)
		if err != nil {
			return err
		}
		if ok && value {
			return errors.Errorf("connect = false and %s = true are mutually exclusive options", key)
		}
	}
	return nil
}

func optionBool(options map[string]string, key string, fallback bool) (bool, error) {
	value, ok := options[key]
	if !ok {
		return fallback, nil
	}
	return parseReplicationBoolOption(key, value)
}

func explicitBoolOption(options map[string]string, key string) (bool, bool, error) {
	value, ok := options[key]
	if !ok {
		return false, false, nil
	}
	parsed, err := parseReplicationBoolOption(key, value)
	return parsed, true, err
}

func optionBoolDefault(options map[string]string, key string, fallback bool) bool {
	value, ok := options[key]
	if !ok {
		return fallback
	}
	parsed, err := parseReplicationBoolOption(key, value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseReplicationBoolOption(key string, value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "true", "on", "yes", "1":
		return true, nil
	case "false", "off", "no", "0":
		return false, nil
	default:
		return false, errors.Errorf(`invalid boolean value for option "%s": "%s"`, key, value)
	}
}

func parseStreamingOption(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "on", "yes", "1", "parallel":
		return true
	default:
		return false
	}
}

func mustParsePgLsn(value string) uint64 {
	lsn, _ := pgtypes.ParsePgLsn(value)
	return lsn
}
