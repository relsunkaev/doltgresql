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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/memo"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	goerrors "gopkg.in/src-d/go-errors.v1"

	corefunctions "github.com/dolthub/doltgresql/core/functions"
	pgexpression "github.com/dolthub/doltgresql/server/expression"
)

// IDs are basically arbitrary, we just need to ensure that they do not conflict with existing IDs
// Comments are to match the Stringer formatting rules in the original rule definition file, but we can't generate
// human-readable strings for these extended types because they are in another package.
const (
	ruleId_TypeSanitizer                            analyzer.RuleId = iota + 1000 // typeSanitizer
	ruleId_AddDomainConstraints                                                   // addDomainConstraints
	ruleId_AddDomainConstraintsToCasts                                            // addDomainConstraintsToCasts
	ruleId_ApplyTablesForAnalyzeAllTables                                         // applyTablesForAnalyzeAllTables
	ruleId_ApplyIdentityOverride                                                  // applyIdentityOverride
	ruleId_AssignInsertCasts                                                      // assignInsertCasts
	ruleId_AssignJsonbGinLookups                                                  // assignJsonbGinLookups
	ruleId_AssignJsonbGinMaintainers                                              // assignJsonbGinMaintainers
	ruleId_AssignBtreePlannerBoundaries                                           // assignBtreePlannerBoundaries
	ruleId_AssignBatchedIndexLookups                                              // assignBatchedIndexLookups
	ruleId_InferInnerJoinPredicates                                               // inferInnerJoinPredicates
	ruleId_AssignSelectiveLookupJoinHints                                         // assignSelectiveLookupJoinHints
	ruleId_PreserveLateralLeftJoin                                                // preserveLateralLeftJoin
	ruleId_AssignTriggers                                                         // assignTriggers
	ruleId_AssignUpdateCasts                                                      // assignUpdateCasts
	ruleId_UseSchemaAwareStatsProvider                                            // useSchemaAwareStatsProvider
	ruleId_ConvertDropPrimaryKeyConstraint                                        // convertDropPrimaryKeyConstraint
	ruleId_WrapPrimaryKeyMetadata                                                 // wrapPrimaryKeyMetadata
	ruleId_GenerateForeignKeyName                                                 // generateForeignKeyName
	ruleId_ReplaceIndexedTables                                                   // replaceIndexedTables
	ruleId_ReplaceNode                                                            // replaceNode
	ruleId_ReplaceSerial                                                          // replaceSerial
	ruleId_InsertContextRootFinalizer                                             // insertContextRootFinalizer
	ruleId_ResolveType                                                            // resolveType
	ruleId_ReplaceArithmeticExpressions                                           // replaceArithmeticExpressions
	ruleId_OptimizeFunctions                                                      // optimizeFunctions
	ruleId_ValidateColumnDefaults                                                 // validateColumnDefaults
	ruleId_ValidateCreateTable                                                    // validateCreateTable
	ruleId_ValidateCreateSchema                                                   // validateCreateSchema
	ruleId_ResolveAlterColumn                                                     // resolveAlterColumn
	ruleId_ValidateCreateFunction                                                 // validateCreateFunction
	ruleId_ResolveValuesTypes                                                     // resolveValuesTypes
	ruleId_ResolveProcedureDefaults                                               // resolveProcedureDefaults
	ruleId_ValidateGroupBy                                                        // validateGroupBy
	ruleId_ValidateDropConstraintOwnership                                        // validateDropConstraintOwnership
	ruleId_ValidateOnConflictArbiter                                              // validateOnConflictArbiter
	ruleId_AssignNullsNotDistinctUniqueChecks                                     // assignNullsNotDistinctUniqueChecks
	ruleId_AssignRowLevelLocking                                                  // assignRowLevelLocking
	ruleId_SuppressReplicaRoleForeignKeys                                         // suppressReplicaRoleForeignKeys
	ruleId_SuppressDeferrableForeignKeys                                          // suppressDeferrableForeignKeys
	ruleId_UnwrapTableCopierCreateTable                                           // unwrapTableCopierCreateTable
	ruleId_PreserveTableMetadata                                                  // preserveTableMetadata
	ruleId_AssignUnpopulatedMatviewScans                                          // assignUnpopulatedMaterializedViewScans
	ruleId_AssignIndexStats                                                       // assignIndexStats
	ruleId_ClearUncorrelatedSubqueryAliasVisibility                               // clearUncorrelatedSubqueryAliasVisibility
	ruleId_PruneNotNullSortProbes                                                 // pruneNotNullSortProbes
	ruleId_PreferOrderedSortOptionIndexes                                         // preferOrderedSortOptionIndexes
	ruleId_ResolveDropColumnIfExists                                              // resolveDropColumnIfExists
	ruleId_ValidateOrderBy                                                        // validateOrderBy
)

// Init adds additional rules to the analyzer to handle Doltgres-specific functionality.
func Init() {
	// OnceBeforeDefault runs before AlwaysBeforeDefault in GMS
	analyzer.OnceBeforeDefault = append([]analyzer.Rule{
		// resolveDropColumnIfExists must run before any rule that validates
		// DropColumn against the table schema, otherwise the "column not found"
		// error masks the IF EXISTS no-op semantics.
		{Id: ruleId_ResolveDropColumnIfExists, Apply: resolveDropColumnIfExists},
		{Id: ruleId_ResolveType, Apply: ResolveType}, // ResolveType rule must run before simplifyFilters rule in GMS
		{Id: ruleId_ApplyTablesForAnalyzeAllTables, Apply: applyTablesForAnalyzeAllTables},
		{Id: ruleId_ValidateDropConstraintOwnership, Apply: validateDropConstraintOwnership},
		{Id: ruleId_ConvertDropPrimaryKeyConstraint, Apply: convertDropPrimaryKeyConstraint}},
		analyzer.OnceBeforeDefault...)

	analyzer.AlwaysBeforeDefault = append(analyzer.AlwaysBeforeDefault,
		// ResolveType rule must run in this batch in addition to OnceBeforeDefault batch
		// because of custom batch set optimization in GMS skipping OnceBeforeDefault batch for some nodes.
		analyzer.Rule{Id: ruleId_ResolveType, Apply: ResolveType},
		analyzer.Rule{Id: ruleId_TypeSanitizer, Apply: TypeSanitizer},
		analyzer.Rule{Id: ruleId_ResolveValuesTypes, Apply: ResolveValuesTypes},
		analyzer.Rule{Id: ruleId_GenerateForeignKeyName, Apply: generateForeignKeyName},
		analyzer.Rule{Id: ruleId_AddDomainConstraints, Apply: AddDomainConstraints},
		analyzer.Rule{Id: ruleId_ValidateColumnDefaults, Apply: ValidateColumnDefaults},
		analyzer.Rule{Id: ruleId_ApplyIdentityOverride, Apply: ApplyIdentityOverride},
		analyzer.Rule{Id: ruleId_AssignInsertCasts, Apply: AssignInsertCasts},
		analyzer.Rule{Id: ruleId_AssignUpdateCasts, Apply: AssignUpdateCasts},
		analyzer.Rule{Id: ruleId_UseSchemaAwareStatsProvider, Apply: UseSchemaAwareStatsProvider},
		analyzer.Rule{Id: ruleId_AssignJsonbGinMaintainers, Apply: AssignJsonbGinMaintainers},
		analyzer.Rule{Id: ruleId_AssignNullsNotDistinctUniqueChecks, Apply: AssignNullsNotDistinctUniqueChecks},
		analyzer.Rule{Id: ruleId_AssignBtreePlannerBoundaries, Apply: AssignBtreePlannerBoundaries},
		analyzer.Rule{Id: ruleId_AssignRowLevelLocking, Apply: AssignRowLevelLocking},
		analyzer.Rule{Id: ruleId_AssignTriggers, Apply: AssignTriggers},
		analyzer.Rule{Id: ruleId_ValidateCreateFunction, Apply: ValidateCreateFunction},
		analyzer.Rule{Id: ruleId_ValidateCreateSchema, Apply: ValidateCreateSchema},
		analyzer.Rule{Id: ruleId_ResolveProcedureDefaults, Apply: ResolveProcedureDefaults},
	)

	// We remove several validation rules and substitute our own
	analyzer.OnceBeforeDefault = insertAnalyzerRules(analyzer.OnceBeforeDefault, analyzer.ValidateCreateTableId, true,
		analyzer.Rule{Id: ruleId_ValidateCreateTable, Apply: validateCreateTable})
	analyzer.OnceBeforeDefault = insertAnalyzerRules(analyzer.OnceBeforeDefault, analyzer.ResolveAlterColumnId, true,
		analyzer.Rule{Id: ruleId_ResolveAlterColumn, Apply: resolveAlterColumn})
	analyzer.OnceBeforeDefault = replaceAnalyzerRuleByName(analyzer.OnceBeforeDefault, "validateGroupBy",
		analyzer.Rule{Id: ruleId_ValidateGroupBy, Apply: ValidateGroupBy})

	analyzer.OnceBeforeDefault = removeAnalyzerRules(
		analyzer.OnceBeforeDefault,
		analyzer.ValidateColumnDefaultsId,
		analyzer.ValidateCreateTableId,
		analyzer.ResolveAlterColumnId,
	)

	// Remove all other validation rules that do not apply to Postgres
	analyzer.DefaultValidationRules = removeAnalyzerRules(analyzer.DefaultValidationRules, analyzer.ValidateOperandsId)

	analyzer.OnceAfterDefault = insertAnalyzerRulesByName(analyzer.OnceAfterDefault, "optimizeJoins", true,
		analyzer.Rule{Id: ruleId_PreserveLateralLeftJoin, Apply: PreserveLateralLeftJoin},
		analyzer.Rule{Id: ruleId_AssignBtreePlannerBoundaries, Apply: AssignBtreePlannerBoundaries},
		analyzer.Rule{Id: ruleId_AssignUnpopulatedMatviewScans, Apply: AssignUnpopulatedMaterializedViewScans},
		analyzer.Rule{Id: ruleId_AssignBatchedIndexLookups, Apply: AssignBatchedIndexLookups},
		analyzer.Rule{Id: ruleId_AssignJsonbGinLookups, Apply: AssignJsonbGinLookups},
		analyzer.Rule{Id: ruleId_InferInnerJoinPredicates, Apply: InferInnerJoinPredicates},
		analyzer.Rule{Id: ruleId_AssignSelectiveLookupJoinHints, Apply: AssignSelectiveLookupJoinHints},
		analyzer.Rule{Id: ruleId_PreferOrderedSortOptionIndexes, Apply: PreferOrderedSortOptionIndexes},
	)

	analyzer.OnceAfterDefault = append(analyzer.OnceAfterDefault,
		analyzer.Rule{Id: ruleId_WrapPrimaryKeyMetadata, Apply: wrapPrimaryKeyMetadata},
		analyzer.Rule{Id: ruleId_PreserveTableMetadata, Apply: preserveTableMetadata},
		analyzer.Rule{Id: ruleId_ReplaceSerial, Apply: ReplaceSerial},
		analyzer.Rule{Id: ruleId_UnwrapTableCopierCreateTable, Apply: UnwrapTableCopierCreateTable},
		analyzer.Rule{Id: ruleId_ReplaceArithmeticExpressions, Apply: ReplaceArithmeticExpressions},
		analyzer.Rule{Id: ruleId_ValidateOrderBy, Apply: ValidateOrderBy},
		analyzer.Rule{Id: ruleId_ValidateOnConflictArbiter, Apply: ValidateOnConflictArbiter},
	)
	analyzer.OnceAfterDefault = insertAnalyzerRulesByName(analyzer.OnceAfterDefault, "replaceIdxSort", true,
		analyzer.Rule{Id: ruleId_PruneNotNullSortProbes, Apply: PruneNotNullSortProbes})

	// The auto-commit rule writes the contents of the context, so we need to insert our finalizer before that.
	// We also should optimize functions last, since other rules may change the underlying expressions, potentially changing their return types.
	analyzer.OnceAfterAll = insertAnalyzerRulesByName(analyzer.OnceAfterAll, "assignExecIndexes", true,
		analyzer.Rule{Id: ruleId_ClearUncorrelatedSubqueryAliasVisibility, Apply: ClearUncorrelatedSubqueryAliasVisibility})
	analyzer.OnceAfterAll = insertAnalyzerRules(analyzer.OnceAfterAll, analyzer.QuoteDefaultColumnValueNamesId, false,
		analyzer.Rule{Id: ruleId_OptimizeFunctions, Apply: OptimizeFunctions},
		// AddDomainConstraintsToCasts needs to run after 'assignExecIndexes' rule in GMS.
		analyzer.Rule{Id: ruleId_AddDomainConstraintsToCasts, Apply: AddDomainConstraintsToCasts},
		analyzer.Rule{Id: ruleId_AssignIndexStats, Apply: AssignIndexStats},
		analyzer.Rule{Id: ruleId_SuppressDeferrableForeignKeys, Apply: SuppressDeferrableForeignKeys},
		analyzer.Rule{Id: ruleId_SuppressReplicaRoleForeignKeys, Apply: SuppressReplicaRoleForeignKeys},
		analyzer.Rule{Id: ruleId_ReplaceNode, Apply: ReplaceNode},
		analyzer.Rule{Id: ruleId_InsertContextRootFinalizer, Apply: InsertContextRootFinalizer},
	)

	initEngine()
}

// TODO: introduce a real pluggable architecture for this instead of swapping function pointers
func initEngine() {
	// This technically takes place at execution time rather than as part of analysis, but we don't have a better
	// place to put it. Our foreign key validation logic is different from MySQL's, and since it's not an analyzer rule
	// we can't swap out a rule like the rest of the logic in this package, we have to do a function swap.
	plan.ValidateForeignKeyDefinition = validateForeignKeyDefinition

	planbuilder.IsAggregateFunc = IsAggregateFunc
	sql.ErrFunctionNotFound = goerrors.NewKind("function: '%s' not found; function does not exist")
	sql.ErrTableNotFound = goerrors.NewKind("table not found: %s; relation does not exist")

	expression.DefaultExpressionFactory = pgexpression.PostgresExpressionFactory{}

	// There are a couple places during analysis where SplitConjunction in GMS cannot correctly split up
	// Doltgres expressions, so we need to override the default function used.
	analyzer.SplitConjunction = SplitConjunction
	memo.SplitConjunction = SplitConjunction
}

// IsAggregateFunc checks if the given function name is an aggregate function. This is the entire set supported by
// MySQL plus some postgres specific ones.
func IsAggregateFunc(name string) bool {
	if planbuilder.IsMySQLAggregateFuncName(name) {
		return true
	}
	if corefunctions.IsAggregateName(name) {
		return true
	}

	switch name {
	case "array_agg", "bool_and", "bool_or", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg":
		return true
	}

	return false
}

// insertAnalyzerRules inserts the given rule(s) before or after the given analyzer.RuleId, returning an updated slice.
func insertAnalyzerRules(rules []analyzer.Rule, id analyzer.RuleId, before bool, additionalRules ...analyzer.Rule) []analyzer.Rule {
	inserted := false
	newRules := make([]analyzer.Rule, len(rules)+len(additionalRules))
	for i, rule := range rules {
		if rule.Id == id {
			inserted = true
			if before {
				copy(newRules, rules[:i])
				copy(newRules[i:], additionalRules)
				copy(newRules[i+len(additionalRules):], rules[i:])
			} else {
				copy(newRules, rules[:i+1])
				copy(newRules[i+1:], additionalRules)
				copy(newRules[i+1+len(additionalRules):], rules[i+1:])
			}
			break
		}
	}

	if !inserted {
		panic("no rules were inserted")
	}

	return newRules
}

// insertAnalyzerRulesByName inserts rules relative to a GMS analyzer rule whose
// ID is not exported by the dependency.
func insertAnalyzerRulesByName(rules []analyzer.Rule, name string, before bool, additionalRules ...analyzer.Rule) []analyzer.Rule {
	inserted := false
	newRules := make([]analyzer.Rule, len(rules)+len(additionalRules))
	for i, rule := range rules {
		if rule.Id.String() == name {
			inserted = true
			if before {
				copy(newRules, rules[:i])
				copy(newRules[i:], additionalRules)
				copy(newRules[i+len(additionalRules):], rules[i:])
			} else {
				copy(newRules, rules[:i+1])
				copy(newRules[i+1:], additionalRules)
				copy(newRules[i+1+len(additionalRules):], rules[i+1:])
			}
			break
		}
	}

	if !inserted {
		panic("no rules were inserted")
	}

	return newRules
}

// replaceAnalyzerRuleByName replaces a GMS analyzer rule whose ID is not
// exported by the dependency.
func replaceAnalyzerRuleByName(rules []analyzer.Rule, name string, replacement analyzer.Rule) []analyzer.Rule {
	replaced := false
	newRules := make([]analyzer.Rule, len(rules))
	for i, rule := range rules {
		if rule.Id.String() == name {
			replaced = true
			newRules[i] = replacement
		} else {
			newRules[i] = rule
		}
	}

	if !replaced {
		panic("one or more rules were not replaced, this is a bug")
	}

	return newRules
}

// removeAnalyzerRules removes the given analyzer.RuleId(s), returning an updated slice.
func removeAnalyzerRules(rules []analyzer.Rule, remove ...analyzer.RuleId) []analyzer.Rule {
	ids := make(map[analyzer.RuleId]struct{})
	for _, removal := range remove {
		ids[removal] = struct{}{}
	}

	removedIds := 0
	var newRules []analyzer.Rule
	for _, rule := range rules {
		if _, ok := ids[rule.Id]; !ok {
			newRules = append(newRules, rule)
		} else {
			removedIds++
		}
	}

	if removedIds < len(remove) {
		panic("one or more rules were not removed, this is a bug")
	}

	return newRules
}
