// Copyright 2025 Dolthub, Inc.
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

package functions

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"

	"github.com/dolthub/doltgresql/core/id"
	pgmerge "github.com/dolthub/doltgresql/core/merge"
	"github.com/dolthub/doltgresql/core/rootobject/objinterface"
	"github.com/dolthub/doltgresql/server/plpgsql"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

const (
	FIELD_NAME_PARAMETER_NAMES   = "parameter_names"
	FIELD_NAME_RETURN_TYPE       = "return_type"
	FIELD_NAME_NON_DETERMINISTIC = "non_deterministic"
	FIELD_NAME_STRICT            = "strict"
	FIELD_NAME_SECURITY_DEFINER  = "security_definer"
	FIELD_NAME_LEAK_PROOF        = "leak_proof"
	FIELD_NAME_VOLATILITY        = "volatility"
	FIELD_NAME_PARALLEL          = "parallel"
	FIELD_NAME_COST              = "cost"
	FIELD_NAME_ROWS              = "rows"
	FIELD_NAME_DEFINITION        = "definition"
	FIELD_NAME_EXTENSION_NAME    = "extension_name"
	FIELD_NAME_EXTENSION_SYMBOL  = "extension_symbol"
	FIELD_NAME_EXTENSION_DEPS    = "extension_deps"
	FIELD_NAME_SQL_DEFINITION    = "sql_definition"
	FIELD_NAME_SET_OF            = "set_of"
	FIELD_NAME_OWNER             = "owner"
	FIELD_NAME_AGGREGATE         = "aggregate"
	FIELD_NAME_AGGREGATE_STYPE   = "aggregate_state_type"
	FIELD_NAME_AGGREGATE_SFUNC   = "aggregate_sfunc"
	FIELD_NAME_AGGREGATE_INIT    = "aggregate_initcond"
)

// DeserializeRootObject implements the interface objinterface.Collection.
func (pgf *Collection) DeserializeRootObject(ctx context.Context, data []byte) (objinterface.RootObject, error) {
	return DeserializeFunction(ctx, data)
}

// DiffRootObjects implements the interface objinterface.Collection.
func (pgf *Collection) DiffRootObjects(ctx context.Context, fromHash string, o objinterface.RootObject, t objinterface.RootObject, a objinterface.RootObject) ([]objinterface.RootObjectDiff, objinterface.RootObject, error) {
	// We ignore many fields when diffing, as differences in these fields would result in a different function due to overloading
	// For example, "func_name(text)" and "func_name(varchar)" cannot produce a conflict as they're different functions
	ours := o.(Function)
	theirs := t.(Function)
	ancestor, hasAncestor := a.(Function)
	var diffs []objinterface.RootObjectDiff
	{
		ourParamNames := strings.Join(ours.ParameterNames, ",")
		theirParamNames := strings.Join(theirs.ParameterNames, ",")
		ancParamNames := strings.Join(ancestor.ParameterNames, ",")
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_PARAMETER_NAMES,
		}
		if pgmerge.DiffValues(&diff, ourParamNames, theirParamNames, ancParamNames, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.ParameterNames = strings.Split(diff.OurValue.(string), ",")
		}
	}
	if ours.ReturnType != theirs.ReturnType {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_RETURN_TYPE,
		}
		if pgmerge.DiffValues(&diff, ours.ReturnType.TypeName(), theirs.ReturnType.TypeName(), ancestor.ReturnType.TypeName(), hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.ReturnType = id.NewType(ours.ReturnType.SchemaName(), diff.OurValue.(string))
		}
	}
	if ours.IsNonDeterministic != theirs.IsNonDeterministic {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Bool,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_NON_DETERMINISTIC,
		}
		if pgmerge.DiffValues(&diff, ours.IsNonDeterministic, theirs.IsNonDeterministic, ancestor.IsNonDeterministic, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.IsNonDeterministic = diff.OurValue.(bool)
		}
	}
	if ours.Strict != theirs.Strict {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Bool,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_STRICT,
		}
		if pgmerge.DiffValues(&diff, ours.Strict, theirs.Strict, ancestor.Strict, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Strict = diff.OurValue.(bool)
		}
	}
	if ours.SecurityDefiner != theirs.SecurityDefiner {
		diff := objinterface.RootObjectDiff{Type: pgtypes.Bool, FromHash: fromHash, FieldName: FIELD_NAME_SECURITY_DEFINER}
		if pgmerge.DiffValues(&diff, ours.SecurityDefiner, theirs.SecurityDefiner, ancestor.SecurityDefiner, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.SecurityDefiner = diff.OurValue.(bool)
		}
	}
	if ours.LeakProof != theirs.LeakProof {
		diff := objinterface.RootObjectDiff{Type: pgtypes.Bool, FromHash: fromHash, FieldName: FIELD_NAME_LEAK_PROOF}
		if pgmerge.DiffValues(&diff, ours.LeakProof, theirs.LeakProof, ancestor.LeakProof, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.LeakProof = diff.OurValue.(bool)
		}
	}
	if ours.Volatility != theirs.Volatility {
		diff := objinterface.RootObjectDiff{Type: pgtypes.Text, FromHash: fromHash, FieldName: FIELD_NAME_VOLATILITY}
		if pgmerge.DiffValues(&diff, ours.Volatility, theirs.Volatility, ancestor.Volatility, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Volatility = diff.OurValue.(string)
		}
	}
	if ours.Parallel != theirs.Parallel {
		diff := objinterface.RootObjectDiff{Type: pgtypes.Text, FromHash: fromHash, FieldName: FIELD_NAME_PARALLEL}
		if pgmerge.DiffValues(&diff, ours.Parallel, theirs.Parallel, ancestor.Parallel, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Parallel = diff.OurValue.(string)
		}
	}
	if ours.Cost != theirs.Cost {
		diff := objinterface.RootObjectDiff{Type: pgtypes.Float32, FromHash: fromHash, FieldName: FIELD_NAME_COST}
		if pgmerge.DiffValues(&diff, ours.Cost, theirs.Cost, ancestor.Cost, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Cost = diff.OurValue.(float32)
		}
	}
	if ours.Rows != theirs.Rows {
		diff := objinterface.RootObjectDiff{Type: pgtypes.Float32, FromHash: fromHash, FieldName: FIELD_NAME_ROWS}
		if pgmerge.DiffValues(&diff, ours.Rows, theirs.Rows, ancestor.Rows, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Rows = diff.OurValue.(float32)
		}
	}
	if ours.Definition != theirs.Definition {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_DEFINITION,
		}
		if pgmerge.DiffValues(&diff, ours.GetInnerDefinition(), theirs.GetInnerDefinition(), ancestor.GetInnerDefinition(), hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Definition = ours.ReplaceDefinition(diff.OurValue.(string))
		}
	}
	if ours.ExtensionName != theirs.ExtensionName {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_EXTENSION_NAME,
		}
		if pgmerge.DiffValues(&diff, ours.ExtensionName, theirs.ExtensionName, ancestor.ExtensionName, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.ExtensionName = diff.OurValue.(string)
		}
	}
	if ours.ExtensionSymbol != theirs.ExtensionSymbol {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_EXTENSION_SYMBOL,
		}
		if pgmerge.DiffValues(&diff, ours.ExtensionSymbol, theirs.ExtensionSymbol, ancestor.ExtensionSymbol, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.ExtensionSymbol = diff.OurValue.(string)
		}
	}
	{
		ourExtensionDeps := strings.Join(ours.ExtensionDeps, ",")
		theirExtensionDeps := strings.Join(theirs.ExtensionDeps, ",")
		ancestorExtensionDeps := strings.Join(ancestor.ExtensionDeps, ",")
		diff := objinterface.RootObjectDiff{Type: pgtypes.Text, FromHash: fromHash, FieldName: FIELD_NAME_EXTENSION_DEPS}
		if pgmerge.DiffValues(&diff, ourExtensionDeps, theirExtensionDeps, ancestorExtensionDeps, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.ExtensionDeps = splitFunctionExtensionDeps(diff.OurValue.(string))
		}
	}
	if ours.SQLDefinition != theirs.SQLDefinition {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_SQL_DEFINITION,
		}
		if pgmerge.DiffValues(&diff, ours.SQLDefinition, theirs.SQLDefinition, ancestor.SQLDefinition, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.SQLDefinition = diff.OurValue.(string)
		}
	}
	if ours.SetOf != theirs.SetOf {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Bool,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_SET_OF,
		}
		if pgmerge.DiffValues(&diff, ours.SetOf, theirs.SetOf, ancestor.SetOf, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.SetOf = diff.OurValue.(bool)
		}
	}
	if ours.Owner != theirs.Owner {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_OWNER,
		}
		if pgmerge.DiffValues(&diff, ours.Owner, theirs.Owner, ancestor.Owner, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Owner = diff.OurValue.(string)
		}
	}
	if ours.Aggregate != theirs.Aggregate {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Bool,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_AGGREGATE,
		}
		if pgmerge.DiffValues(&diff, ours.Aggregate, theirs.Aggregate, ancestor.Aggregate, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.Aggregate = diff.OurValue.(bool)
		}
	}
	if ours.AggregateStateType != theirs.AggregateStateType {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_AGGREGATE_STYPE,
		}
		if pgmerge.DiffValues(&diff, ours.AggregateStateType.TypeName(), theirs.AggregateStateType.TypeName(), ancestor.AggregateStateType.TypeName(), hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.AggregateStateType = id.NewType(ours.AggregateStateType.SchemaName(), diff.OurValue.(string))
		}
	}
	if ours.AggregateSFunc != theirs.AggregateSFunc {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_AGGREGATE_SFUNC,
		}
		if pgmerge.DiffValues(&diff, string(ours.AggregateSFunc), string(theirs.AggregateSFunc), string(ancestor.AggregateSFunc), hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.AggregateSFunc = id.Function(diff.OurValue.(string))
		}
	}
	if ours.AggregateInitCond != theirs.AggregateInitCond {
		diff := objinterface.RootObjectDiff{
			Type:      pgtypes.Text,
			FromHash:  fromHash,
			FieldName: FIELD_NAME_AGGREGATE_INIT,
		}
		if pgmerge.DiffValues(&diff, ours.AggregateInitCond, theirs.AggregateInitCond, ancestor.AggregateInitCond, hasAncestor) {
			diffs = append(diffs, diff)
		} else {
			ours.AggregateInitCond = diff.OurValue.(string)
		}
	}
	return diffs, ours, nil
}

// DropRootObject implements the interface objinterface.Collection.
func (pgf *Collection) DropRootObject(ctx context.Context, identifier id.Id) error {
	if identifier.Section() != id.Section_Function {
		return errors.Errorf(`function %s does not exist`, identifier.String())
	}
	return pgf.DropFunction(ctx, id.Function(identifier))
}

// GetFieldType implements the interface objinterface.Collection.
func (pgf *Collection) GetFieldType(ctx context.Context, fieldName string) *pgtypes.DoltgresType {
	switch fieldName {
	case FIELD_NAME_PARAMETER_NAMES:
		return pgtypes.Text
	case FIELD_NAME_RETURN_TYPE:
		return pgtypes.Text
	case FIELD_NAME_NON_DETERMINISTIC:
		return pgtypes.Bool
	case FIELD_NAME_STRICT:
		return pgtypes.Bool
	case FIELD_NAME_SECURITY_DEFINER:
		return pgtypes.Bool
	case FIELD_NAME_LEAK_PROOF:
		return pgtypes.Bool
	case FIELD_NAME_VOLATILITY:
		return pgtypes.Text
	case FIELD_NAME_PARALLEL:
		return pgtypes.Text
	case FIELD_NAME_COST:
		return pgtypes.Float32
	case FIELD_NAME_ROWS:
		return pgtypes.Float32
	case FIELD_NAME_DEFINITION:
		return pgtypes.Text
	case FIELD_NAME_EXTENSION_NAME:
		return pgtypes.Text
	case FIELD_NAME_EXTENSION_SYMBOL:
		return pgtypes.Text
	case FIELD_NAME_EXTENSION_DEPS:
		return pgtypes.Text
	case FIELD_NAME_SQL_DEFINITION:
		return pgtypes.Text
	case FIELD_NAME_SET_OF:
		return pgtypes.Bool
	case FIELD_NAME_OWNER:
		return pgtypes.Text
	case FIELD_NAME_AGGREGATE:
		return pgtypes.Bool
	case FIELD_NAME_AGGREGATE_STYPE:
		return pgtypes.Text
	case FIELD_NAME_AGGREGATE_SFUNC:
		return pgtypes.Text
	case FIELD_NAME_AGGREGATE_INIT:
		return pgtypes.Text
	default:
		return nil
	}
}

// GetID implements the interface objinterface.Collection.
func (pgf *Collection) GetID() objinterface.RootObjectID {
	return objinterface.RootObjectID_Functions
}

// GetRootObject implements the interface objinterface.Collection.
func (pgf *Collection) GetRootObject(ctx context.Context, identifier id.Id) (objinterface.RootObject, bool, error) {
	if identifier.Section() != id.Section_Function {
		return nil, false, nil
	}
	f, err := pgf.GetFunction(ctx, id.Function(identifier))
	return f, err == nil && f.ID.IsValid(), err
}

// HasRootObject implements the interface objinterface.Collection.
func (pgf *Collection) HasRootObject(ctx context.Context, identifier id.Id) (bool, error) {
	if identifier.Section() != id.Section_Function {
		return false, nil
	}
	return pgf.HasFunction(ctx, id.Function(identifier)), nil
}

// IDToTableName implements the interface objinterface.Collection.
func (pgf *Collection) IDToTableName(identifier id.Id) doltdb.TableName {
	if identifier.Section() != id.Section_Function {
		return doltdb.TableName{}
	}
	return FunctionIDToTableName(id.Function(identifier))
}

// IterAll implements the interface objinterface.Collection.
func (pgf *Collection) IterAll(ctx context.Context, callback func(rootObj objinterface.RootObject) (stop bool, err error)) error {
	return pgf.IterateFunctions(ctx, func(f Function) (stop bool, err error) {
		return callback(f)
	})
}

// IterIDs implements the interface objinterface.Collection.
func (pgf *Collection) IterIDs(ctx context.Context, callback func(identifier id.Id) (stop bool, err error)) error {
	return pgf.iterateIDs(ctx, func(funcID id.Function) (stop bool, err error) {
		return callback(funcID.AsId())
	})
}

// PutRootObject implements the interface objinterface.Collection.
func (pgf *Collection) PutRootObject(ctx context.Context, rootObj objinterface.RootObject) error {
	f, ok := rootObj.(Function)
	if !ok {
		return errors.Newf("invalid function root object: %T", rootObj)
	}
	return pgf.AddFunction(ctx, f)
}

// RenameRootObject implements the interface objinterface.Collection.
func (pgf *Collection) RenameRootObject(ctx context.Context, oldName id.Id, newName id.Id) error {
	if !oldName.IsValid() || !newName.IsValid() || oldName.Section() != newName.Section() || oldName.Section() != id.Section_Function {
		return errors.New("cannot rename function due to invalid name")
	}
	oldFuncName := id.Function(oldName)
	newFuncName := id.Function(newName)
	if oldFuncName.ParameterCount() != newFuncName.ParameterCount() {
		return errors.Newf(`old function id had "%d" parameters, new function id has "%d" parameters`,
			oldFuncName.ParameterCount(), newFuncName.ParameterCount())
	}
	f, err := pgf.GetFunction(ctx, oldFuncName)
	if err != nil {
		return err
	}
	if err = pgf.DropFunction(ctx, oldFuncName); err != nil {
		return err
	}
	f.ID = newFuncName
	return pgf.AddFunction(ctx, f)
}

// ResolveName implements the interface objinterface.Collection.
func (pgf *Collection) ResolveName(ctx context.Context, name doltdb.TableName) (doltdb.TableName, id.Id, error) {
	rawID, err := pgf.resolveName(ctx, name.Schema, name.Name)
	if err != nil || !rawID.IsValid() {
		return doltdb.TableName{}, id.Null, err
	}
	return FunctionIDToTableName(rawID), rawID.AsId(), nil
}

// TableNameToID implements the interface objinterface.Collection.
func (pgf *Collection) TableNameToID(name doltdb.TableName) id.Id {
	return pgf.tableNameToID(name.Schema, name.Name).AsId()
}

// UpdateField implements the interface objinterface.Collection.
func (pgf *Collection) UpdateField(ctx context.Context, rootObject objinterface.RootObject, fieldName string, newValue any) (objinterface.RootObject, error) {
	function := rootObject.(Function)
	switch fieldName {
	case FIELD_NAME_PARAMETER_NAMES:
		function.ParameterNames = strings.Split(newValue.(string), ",")
	case FIELD_NAME_RETURN_TYPE:
		function.ReturnType = id.NewType(function.ReturnType.SchemaName(), newValue.(string))
	case FIELD_NAME_NON_DETERMINISTIC:
		function.IsNonDeterministic = newValue.(bool)
	case FIELD_NAME_STRICT:
		function.Strict = newValue.(bool)
	case FIELD_NAME_SECURITY_DEFINER:
		function.SecurityDefiner = newValue.(bool)
	case FIELD_NAME_LEAK_PROOF:
		function.LeakProof = newValue.(bool)
	case FIELD_NAME_VOLATILITY:
		function.Volatility = newValue.(string)
	case FIELD_NAME_PARALLEL:
		function.Parallel = newValue.(string)
	case FIELD_NAME_COST:
		function.Cost = newValue.(float32)
	case FIELD_NAME_ROWS:
		function.Rows = newValue.(float32)
	case FIELD_NAME_DEFINITION:
		function.Definition = function.ReplaceDefinition(newValue.(string))
		parsedBody, err := plpgsql.Parse(function.Definition)
		if err != nil {
			return nil, err
		}
		function.Operations = parsedBody
	case FIELD_NAME_EXTENSION_NAME:
		function.ExtensionName = newValue.(string)
	case FIELD_NAME_EXTENSION_SYMBOL:
		function.ExtensionSymbol = newValue.(string)
	case FIELD_NAME_EXTENSION_DEPS:
		function.ExtensionDeps = splitFunctionExtensionDeps(newValue.(string))
	case FIELD_NAME_SQL_DEFINITION:
		function.SQLDefinition = newValue.(string)
	case FIELD_NAME_SET_OF:
		function.SetOf = newValue.(bool)
	case FIELD_NAME_OWNER:
		function.Owner = newValue.(string)
	case FIELD_NAME_AGGREGATE:
		function.Aggregate = newValue.(bool)
	case FIELD_NAME_AGGREGATE_STYPE:
		function.AggregateStateType = id.NewType(function.AggregateStateType.SchemaName(), newValue.(string))
	case FIELD_NAME_AGGREGATE_SFUNC:
		function.AggregateSFunc = id.Function(newValue.(string))
	case FIELD_NAME_AGGREGATE_INIT:
		function.AggregateInitCond = newValue.(string)
	default:
		return nil, errors.Newf("unknown field name: `%s`", fieldName)
	}
	return function, nil
}

func splitFunctionExtensionDeps(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
