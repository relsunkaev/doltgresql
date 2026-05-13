// Copyright 2024 Dolthub, Inc.
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

package analyzer

import (
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/core/sequences"
	"github.com/dolthub/doltgresql/postgres/parser/parser"
	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/ast"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/functions/framework"
	pgnodes "github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/tablemetadata"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

// maxSequenceAutoNames is the maximum number of otherwise-identical sequence names that can be generated before
// resulting in an error. Under normal operation, sequence names should be automatically cleaned up when their table
// gets dropped, so except in extremely unusual circumstances this limit should never be reached. If it is, it's
// probably an indicator of a bug in sequence cleanup (or an extremely large schema).
const maxSequenceAutoNames = 10_000

// ReplaceSerial replaces a CreateTable node containing a SERIAL type with a node that can create sequences alongside
// the table.
func ReplaceSerial(ctx *sql.Context, a *analyzer.Analyzer, node sql.Node, scope *plan.Scope, selector analyzer.RuleSelector, qFlags *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	createTable, ok := node.(*plan.CreateTable)
	if !ok {
		return node, transform.SameTree, nil
	}

	var ctSequences []*pgnodes.CreateSequence
	identitySequenceOptions := pgnodes.DecodeIdentitySequenceOptions(createTable.TableOpts)
	for _, col := range createTable.PkSchema().Schema {
		doltgresType, isDoltgresType := col.Type.(*pgtypes.DoltgresType)
		if !isDoltgresType || !doltgresType.IsSerial {
			continue
		}

		// For always-generated columns we insert a placeholder sequence to be replaced by the actual sequence name. We
		// detect that here and treat these generated columns differently than other generated columns on serial types.
		isGeneratedFromSequence := false
		if col.Generated != nil {
			seenNextVal := false
			transform.InspectExpr(ctx, col.Generated, func(ctx *sql.Context, expr sql.Expression) bool {
				switch e := expr.(type) {
				case *framework.CompiledFunction:
					if strings.ToLower(e.Name) == "nextval" {
						seenNextVal = true
					}
				case *expression.Literal:
					placeholderName := fmt.Sprintf("'%s'", ast.DoltCreateTablePlaceholderSequenceName)
					if e.String() == placeholderName {
						isGeneratedFromSequence = true
					}
				}
				return false
			})

			if !seenNextVal && !isGeneratedFromSequence {
				continue
			}
		}

		schemaName, err := core.GetSchemaName(ctx, createTable.Db, "")
		if err != nil {
			return nil, false, err
		}

		sequenceName, err := generateSequenceName(ctx, createTable, col, schemaName)
		if err != nil {
			return nil, transform.NewTree, err
		}

		// TODO: need better way to detect sequence usage
		err = authCheckSequence(ctx, a.Catalog.AuthHandler, schemaName, sequenceName)
		if err != nil {
			return nil, transform.SameTree, err
		}

		databaseName := databaseNameForSQLDatabase(createTable.Db)
		seqName := sequenceDefaultName(ctx, databaseName, schemaName, sequenceName)
		nextVal, isDoltgresType, err := framework.GetFunction(ctx, "nextval", pgexprs.NewTextLiteral(seqName))
		if err != nil {
			return nil, transform.NewTree, err
		}
		if !isDoltgresType {
			return nil, transform.NewTree, errors.Errorf(`function "nextval" could not be found for SERIAL default`)
		}

		nextValExpr := &sql.ColumnDefaultValue{
			Expr:          nextVal,
			OutType:       pgtypes.Int64,
			Literal:       false,
			ReturnNil:     false,
			Parenthesized: false,
		}

		if isGeneratedFromSequence {
			col.Generated = nextValExpr
		} else {
			col.Default = nextValExpr
		}

		var maxValue int64
		switch doltgresType.Name() {
		case "smallserial":
			col.Type = pgtypes.Int16
			maxValue = 32767
		case "serial":
			col.Type = pgtypes.Int32
			maxValue = 2147483647
		case "bigserial":
			col.Type = pgtypes.Int64
			maxValue = 9223372036854775807
		}

		persistence := sequences.Persistence_Permanent
		if createTable.Temporary() {
			persistence = sequences.Persistence_Temporary
		}
		seq := &sequences.Sequence{
			Id:          id.NewSequence("", sequenceName),
			DataTypeID:  col.Type.(*pgtypes.DoltgresType).ID,
			Persistence: persistence,
			Start:       1,
			Current:     1,
			Increment:   1,
			Minimum:     1,
			Maximum:     maxValue,
			Cache:       1,
			Cycle:       false,
			IsAtEnd:     false,
			OwnerTable:  id.NewTable("", createTable.Name()),
			OwnerColumn: col.Name,
		}
		if options := identitySequenceOptions[col.Name]; len(options) > 0 {
			if err = applyIdentitySequenceOptions(seq, options); err != nil {
				return nil, transform.NewTree, err
			}
		}
		ctSequences = append(ctSequences, pgnodes.NewCreateSequence(false, databaseName, "", false, seq))
	}
	return pgnodes.NewCreateTable(createTable, ctSequences, inheritedTablesForCreate(ctx.Query(), createTable.Name())...), transform.NewTree, nil
}

func inheritedTablesForCreate(query string, tableName string) []tablemetadata.InheritedTable {
	if query == "" {
		return nil
	}
	statements, err := parser.Parse(query)
	if err != nil {
		return nil
	}
	for _, statement := range statements {
		createTable, ok := statement.AST.(*tree.CreateTable)
		if !ok || len(createTable.Inherits) == 0 || !strings.EqualFold(string(createTable.Table.ObjectName), tableName) {
			continue
		}
		inheritedTables := make([]tablemetadata.InheritedTable, 0, len(createTable.Inherits))
		for _, parent := range createTable.Inherits {
			inherited := tablemetadata.InheritedTable{Name: string(parent.ObjectName)}
			if parent.ExplicitSchema {
				inherited.Schema = string(parent.SchemaName)
			}
			inheritedTables = append(inheritedTables, inherited)
		}
		return inheritedTables
	}
	return nil
}

func applyIdentitySequenceOptions(seq *sequences.Sequence, options []pgnodes.AlterSequenceOption) error {
	minValueLimit, maxValueLimit := sequenceTypeBounds(seq.DataTypeID)
	increment := seq.Increment
	minValue := seq.Minimum
	maxValue := seq.Maximum
	start := seq.Start
	minValueSet := false
	maxValueSet := false
	startSet := false

	for _, option := range options {
		switch option.Name {
		case pgnodes.AlterSequenceOptionStart:
			start = *option.IntVal
			startSet = true
		case pgnodes.AlterSequenceOptionIncrement:
			increment = *option.IntVal
			if increment == 0 {
				return errors.Errorf("INCREMENT must not be zero")
			}
		case pgnodes.AlterSequenceOptionMinValue:
			if option.IntVal != nil {
				minValue = *option.IntVal
				minValueSet = true
			}
		case pgnodes.AlterSequenceOptionMaxValue:
			if option.IntVal != nil {
				maxValue = *option.IntVal
				maxValueSet = true
			}
		case pgnodes.AlterSequenceOptionCycle:
			seq.Cycle = true
		case pgnodes.AlterSequenceOptionNoCycle:
			seq.Cycle = false
		default:
			return errors.Errorf(`unsupported identity sequence option "%s"`, option.Name)
		}
	}
	if minValueSet {
		if minValue < minValueLimit || minValue > maxValueLimit {
			return errors.Errorf("MINVALUE (%d) is out of range for sequence data type", minValue)
		}
	} else if increment > 0 {
		minValue = 1
	} else {
		minValue = minValueLimit
	}
	if maxValueSet {
		if maxValue < minValueLimit || maxValue > maxValueLimit {
			return errors.Errorf("MAXVALUE (%d) is out of range for sequence data type", maxValue)
		}
	} else if increment > 0 {
		maxValue = maxValueLimit
	} else {
		maxValue = -1
	}
	if startSet {
		if start < minValue {
			return errors.Errorf("START value (%d) cannot be less than MINVALUE (%d))", start, minValue)
		}
		if start > maxValue {
			return errors.Errorf("START value (%d) cannot be greater than MAXVALUE (%d)", start, maxValue)
		}
	} else if increment > 0 {
		start = minValue
	} else {
		start = maxValue
	}
	if minValue > maxValue {
		return errors.Errorf("MINVALUE must be less than or equal to MAXVALUE")
	}

	seq.Start = start
	seq.Current = start
	seq.Increment = increment
	seq.Minimum = minValue
	seq.Maximum = maxValue
	seq.IsAtEnd = false
	seq.IsCalled = false
	return nil
}

func sequenceTypeBounds(dataTypeID id.Type) (int64, int64) {
	switch dataTypeID {
	case pgtypes.Int16.ID:
		return math.MinInt16, math.MaxInt16
	case pgtypes.Int32.ID:
		return math.MinInt32, math.MaxInt32
	default:
		return math.MinInt64, math.MaxInt64
	}
}

func hasDoltgresTableMetadata(tableOpts map[string]any) bool {
	if tableOpts == nil {
		return false
	}
	comment, ok := tableOpts["comment"].(string)
	if !ok {
		return false
	}
	_, ok = tablemetadata.DecodeComment(comment)
	return ok
}

type revisionQualifiedDatabase interface {
	RevisionQualifiedName() string
}

func databaseNameForSQLDatabase(db sql.Database) string {
	if db == nil {
		return ""
	}
	if revisionDb, ok := db.(revisionQualifiedDatabase); ok {
		return revisionDb.RevisionQualifiedName()
	}
	return db.Name()
}

func sequenceDefaultName(ctx *sql.Context, databaseName string, schemaName string, sequenceName string) string {
	if databaseName == "" || databaseName == ctx.GetCurrentDatabase() {
		return doltdb.TableName{Name: sequenceName, Schema: schemaName}.String()
	}
	return quoteIdentifier(databaseName) + "." + quoteIdentifier(schemaName) + "." + quoteIdentifier(sequenceName)
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// generateSequenceName generates a unique sequence name for a SERIAL column in the table given
func generateSequenceName(ctx *sql.Context, createTable *plan.CreateTable, col *sql.Column, schemaName string) (string, error) {
	baseSequenceName := fmt.Sprintf("%s_%s_seq", createTable.Name(), col.Name)
	sequenceName := baseSequenceName
	relationType, err := core.GetRelationType(ctx, schemaName, baseSequenceName)
	if err != nil {
		return "", err
	}
	if relationType != core.RelationType_DoesNotExist {
		seqIndex := 1
		for ; seqIndex <= maxSequenceAutoNames; seqIndex++ {
			sequenceName = fmt.Sprintf("%s%d", baseSequenceName, seqIndex)
			relationType, err = core.GetRelationType(ctx, schemaName, sequenceName)
			if err != nil {
				return "", err
			}
			if relationType == core.RelationType_DoesNotExist {
				break
			}
		}
		if seqIndex > maxSequenceAutoNames {
			return "", errors.Errorf("SERIAL sequence name reached max iterations")
		}
	}
	return sequenceName, nil
}

// authCheckSequenceFromExpr checks authorization of sequence being used.
// It parses schema and sequence names out of given expression.
// There can be only one argument expression of string type.
func authCheckSequenceFromExpr(ctx *sql.Context, ah sql.AuthorizationHandler, arg sql.Expression) error {
	// Prefer Eval over String(): the analyzer may have wrapped the literal
	// in an Alias for projection naming, in which case arg.String() leaks
	// the alias suffix into the relation name and the sequence privilege
	// lookup misses.
	rawName, err := evalAsRelationName(ctx, arg)
	if err != nil {
		return err
	}

	schemaName, seqName, err := functions.ParseRelationName(ctx, rawName)
	if err != nil {
		return err
	}

	return authCheckSequence(ctx, ah, schemaName, seqName)
}

// evalAsRelationName extracts the literal string the sequence name was
// expressed as. Falls back to a String()-based parse when the expression
// cannot be evaluated outside row context (e.g. CASE expressions that take
// the sequence-name argument from a column).
func evalAsRelationName(ctx *sql.Context, arg sql.Expression) (string, error) {
	if arg == nil {
		return "", nil
	}
	if val, err := arg.Eval(ctx, nil); err == nil {
		switch v := val.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		}
	}
	return strings.Trim(arg.String(), "'"), nil
}

// authCheckSequence checks authorization of sequence being used. We cannot check it during parsing because we cannot
// detect sequence currently, so we try to catch any sequence being used and check authorization here.
func authCheckSequence(ctx *sql.Context, ah sql.AuthorizationHandler, schemaName, seqName string) error {
	if err := ah.HandleAuth(ctx, ah.NewQueryState(ctx), sqlparser.AuthInformation{
		AuthType:    auth.AuthType_USAGE,
		TargetType:  auth.AuthTargetType_SequenceIdentifiers,
		TargetNames: []string{schemaName, seqName},
	}); err != nil {
		return err
	}
	return nil
}
