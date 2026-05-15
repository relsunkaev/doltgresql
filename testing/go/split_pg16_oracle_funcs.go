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

//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type migrationFile struct {
	GeneratedBy       string               `json:"generatedBy"`
	SourceFile        string               `json:"sourceFile"`
	DefaultOracle     string               `json:"defaultOracle"`
	AssertionKeyStyle string               `json:"assertionKeyStyle"`
	AssertionFields   []string             `json:"assertionFields"`
	Assertions        []migrationAssertion `json:"assertions"`
}

type migrationAssertion struct {
	Key             string            `json:"key"`
	Source          string            `json:"source"`
	Ordinal         int               `json:"ordinal"`
	ScriptName      string            `json:"scriptName,omitempty"`
	Oracle          string            `json:"oracle"`
	PostgresID      string            `json:"postgresId,omitempty"`
	SuggestedID     string            `json:"suggestedId"`
	Compare         string            `json:"compare,omitempty"`
	ColumnModes     []string          `json:"columnModes,omitempty"`
	ExpectedKind    string            `json:"expectedKind"`
	SQLState        string            `json:"sqlstate,omitempty"`
	ErrorSeverity   string            `json:"errorSeverity,omitempty"`
	Username        string            `json:"username,omitempty"`
	Query           string            `json:"query,omitempty"`
	QuerySHA256     string            `json:"querySha256,omitempty"`
	BindVars        []oracleBindVar   `json:"bindVars,omitempty"`
	ExpectedRows    json.RawMessage   `json:"expectedRows,omitempty"`
	ExpectedTag     *string           `json:"expectedTag,omitempty"`
	NonLiteral      []string          `json:"nonLiteral,omitempty"`
	Cleanup         []string          `json:"cleanup,omitempty"`
	Variables       map[string]string `json:"variables,omitempty"`
	NeedsCleanup    bool              `json:"needsCleanup"`
	CleanupProvided bool              `json:"cleanupProvided"`
}

type oracleBindVar struct {
	Kind   string   `json:"kind"`
	Value  string   `json:"value,omitempty"`
	Values []string `json:"values,omitempty"`
	Null   bool     `json:"null,omitempty"`
}

type span struct {
	start int
	end   int
	name  string
}

type filePlan struct {
	sourcePath string
	mapPath    string
	mapFile    migrationFile
	funcs      map[string]bool
	scripts    map[string]map[string]bool
	assertions map[string]map[int]migrationAssertion
}

func main() {
	sourceDir := flag.String("source-dir", "testing/go", "directory containing top-level Go tests")
	targetDir := flag.String("target-dir", "testing/go/postgres16", "directory for PostgreSQL 16 oracle-backed tests")
	mapDir := flag.String("map-dir", "testing/go/testdata/postgres_oracle_migrations", "top-level oracle-map directory")
	targetMapDir := flag.String("target-map-dir", "testing/go/postgres16/testdata/postgres_oracle_migrations", "PostgreSQL 16 oracle-map directory")
	filesFlag := flag.String("files", "", "comma-separated test basenames to consider, for example foo_test,bar_test")
	splitScripts := flag.Bool("split-scripts", false, "split pure PostgreSQL ScriptTest cases inside mixed RunScripts functions instead of whole test functions")
	splitAssertions := flag.Bool("split-assertions", false, "split leading PostgreSQL assertions inside mixed ScriptTest cases")
	dryRun := flag.Bool("dry-run", false, "print the planned split without writing files")
	flag.Parse()

	files := parseCSV(*filesFlag)
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "--files is required")
		os.Exit(1)
	}

	if *splitScripts && *splitAssertions {
		fmt.Fprintln(os.Stderr, "--split-scripts and --split-assertions cannot be combined")
		os.Exit(1)
	}

	plans, err := buildPlans(*sourceDir, *mapDir, files, *splitScripts, *splitAssertions)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(plans) == 0 {
		if *splitAssertions {
			fmt.Fprintln(os.Stderr, "no PostgreSQL row/tag assertion groups found")
		} else if *splitScripts {
			fmt.Fprintln(os.Stderr, "no pure PostgreSQL ScriptTest groups found")
		} else {
			fmt.Fprintln(os.Stderr, "no pure PostgreSQL function groups found")
		}
		return
	}

	for _, plan := range plans {
		if *splitAssertions {
			names := describeAssertions(plan.assertions)
			fmt.Printf("%s: split %d assertions: %s\n", filepath.Base(plan.sourcePath), countAssertions(plan.assertions), strings.Join(names, ", "))
			if *dryRun {
				continue
			}
			if err := splitAssertionSourceFile(plan.sourcePath, *targetDir, plan.assertions); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := splitAssertionMapFile(plan, *targetMapDir); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		} else if *splitScripts {
			names := describeScripts(plan.scripts)
			fmt.Printf("%s: split %d scripts: %s\n", filepath.Base(plan.sourcePath), countScripts(plan.scripts), strings.Join(names, ", "))
			if *dryRun {
				continue
			}
			if err := splitScriptSourceFile(plan.sourcePath, *targetDir, plan.scripts); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := splitScriptMapFile(plan, *targetMapDir); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		} else {
			names := sortedKeys(plan.funcs)
			fmt.Printf("%s: split %d funcs: %s\n", filepath.Base(plan.sourcePath), len(names), strings.Join(names, ", "))
			if *dryRun {
				continue
			}
			if err := splitSourceFile(plan.sourcePath, *targetDir, plan.funcs); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if err := splitMapFile(plan, *targetMapDir); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}
}

func parseCSV(raw string) map[string]bool {
	out := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out[part] = true
	}
	return out
}

func buildPlans(sourceDir, mapDir string, files map[string]bool, splitScripts bool, splitAssertions bool) ([]filePlan, error) {
	var plans []filePlan
	for base := range files {
		mapPath := filepath.Join(mapDir, base+".oracle-map.json")
		data, err := os.ReadFile(mapPath)
		if err != nil {
			return nil, err
		}
		var mf migrationFile
		if err := json.Unmarshal(data, &mf); err != nil {
			return nil, fmt.Errorf("%s: %w", mapPath, err)
		}
		sourcePath := filepath.Join(sourceDir, base+".go")
		if mf.SourceFile != filepath.ToSlash(sourcePath) {
			return nil, fmt.Errorf("%s: sourceFile is %q, expected %q", mapPath, mf.SourceFile, filepath.ToSlash(sourcePath))
		}

		type counts struct {
			postgres int
			internal int
		}
		byFunc := make(map[string]*counts)
		type scriptKey struct {
			function string
			name     string
		}
		byScript := make(map[scriptKey]*counts)
		for _, assertion := range mf.Assertions {
			fn, ok := functionName(assertion.Source)
			if !ok {
				return nil, fmt.Errorf("%s: cannot parse source %q", mapPath, assertion.Source)
			}
			c := byFunc[fn]
			if c == nil {
				c = new(counts)
				byFunc[fn] = c
			}
			if assertion.Oracle == "postgres" {
				c.postgres++
			} else {
				c.internal++
			}
			if splitScripts && assertion.ScriptName != "" {
				key := scriptKey{function: fn, name: assertion.ScriptName}
				sc := byScript[key]
				if sc == nil {
					sc = new(counts)
					byScript[key] = sc
				}
				if assertion.Oracle == "postgres" {
					sc.postgres++
				} else {
					sc.internal++
				}
			}
		}

		if splitAssertions {
			type groupedAssertions struct {
				function   string
				scriptName string
				assertions []migrationAssertion
			}
			byScriptAssertions := make(map[scriptKey]*groupedAssertions)
			for _, assertion := range mf.Assertions {
				if assertion.ScriptName == "" {
					continue
				}
				fn, ok := functionName(assertion.Source)
				if !ok {
					return nil, fmt.Errorf("%s: cannot parse source %q", mapPath, assertion.Source)
				}
				key := scriptKey{function: fn, name: assertion.ScriptName}
				group := byScriptAssertions[key]
				if group == nil {
					group = &groupedAssertions{function: fn, scriptName: assertion.ScriptName}
					byScriptAssertions[key] = group
				}
				group.assertions = append(group.assertions, assertion)
			}
			assertions := make(map[string]map[int]migrationAssertion)
			for _, group := range byScriptAssertions {
				sort.Slice(group.assertions, func(i, j int) bool {
					return group.assertions[i].Ordinal < group.assertions[j].Ordinal
				})
				var prefix []migrationAssertion
				for _, assertion := range group.assertions {
					if assertion.Oracle != "postgres" {
						break
					}
					prefix = append(prefix, assertion)
				}
				if len(prefix) == 0 || len(prefix) == len(group.assertions) {
					continue
				}
				if !prefixAssertionsAreSafe(prefix) {
					continue
				}
				if assertions[group.function] == nil {
					assertions[group.function] = make(map[int]migrationAssertion)
				}
				for _, assertion := range prefix {
					assertions[group.function][assertion.Ordinal] = assertion
				}
			}
			if countAssertions(assertions) == 0 {
				continue
			}
			plans = append(plans, filePlan{
				sourcePath: sourcePath,
				mapPath:    mapPath,
				mapFile:    mf,
				assertions: assertions,
			})
		} else if splitScripts {
			scripts := make(map[string]map[string]bool)
			for key, c := range byScript {
				if c.postgres > 0 && c.internal == 0 {
					if scripts[key.function] == nil {
						scripts[key.function] = make(map[string]bool)
					}
					scripts[key.function][key.name] = true
				}
			}
			if countScripts(scripts) == 0 {
				continue
			}
			plans = append(plans, filePlan{
				sourcePath: sourcePath,
				mapPath:    mapPath,
				mapFile:    mf,
				scripts:    scripts,
			})
		} else {
			funcs := make(map[string]bool)
			for fn, c := range byFunc {
				if c.postgres > 0 && c.internal == 0 {
					funcs[fn] = true
				}
			}
			if len(funcs) == 0 {
				continue
			}
			plans = append(plans, filePlan{
				sourcePath: sourcePath,
				mapPath:    mapPath,
				mapFile:    mf,
				funcs:      funcs,
			})
		}
	}
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].sourcePath < plans[j].sourcePath
	})
	return plans, nil
}

func splitScriptSourceFile(sourcePath, targetDir string, moveScripts map[string]map[string]bool) error {
	src, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourcePath, src, parser.ParseComments)
	if err != nil {
		return err
	}

	var moveSpans []span
	movedByFunc := make(map[string][]string)
	callByFunc := make(map[string]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || len(moveScripts[fn.Name.Name]) == 0 {
			continue
		}
		callName, scriptsLit, err := runScriptsLiteral(fn)
		if err != nil {
			return fmt.Errorf("%s:%s: %w", sourcePath, fn.Name.Name, err)
		}
		callByFunc[fn.Name.Name] = callName
		for _, elt := range scriptsLit.Elts {
			scriptName, ok := scriptTestName(elt)
			if !ok || !moveScripts[fn.Name.Name][scriptName] {
				continue
			}
			start := lineStart(src, fset.Position(elt.Pos()).Offset)
			end := includeTrailingCommaLine(src, fset.Position(elt.End()).Offset)
			moveSpans = append(moveSpans, span{start: start, end: end, name: fn.Name.Name + "/" + scriptName})
			moved := strings.TrimSpace(string(src[fset.Position(elt.Pos()).Offset:end]))
			if !strings.HasSuffix(moved, ",") {
				moved += ","
			}
			movedByFunc[fn.Name.Name] = append(movedByFunc[fn.Name.Name], moved)
		}
	}

	expected := countScripts(moveScripts)
	if len(moveSpans) != expected {
		found := make(map[string]bool)
		for _, s := range moveSpans {
			found[s.name] = true
		}
		var missing []string
		for fn, names := range moveScripts {
			for name := range names {
				key := fn + "/" + name
				if !found[key] {
					missing = append(missing, key)
				}
			}
		}
		sort.Strings(missing)
		return fmt.Errorf("%s: missing ScriptTest declarations: %s", sourcePath, strings.Join(missing, ", "))
	}

	sort.Slice(moveSpans, func(i, j int) bool { return moveSpans[i].start > moveSpans[j].start })
	topSrc := append([]byte(nil), src...)
	for _, s := range moveSpans {
		topSrc = append(topSrc[:s.start], topSrc[s.end:]...)
	}
	topSrc, err = pruneAndFormat(topSrc)
	if err != nil {
		return fmt.Errorf("%s top-level script rewrite: %w", sourcePath, err)
	}

	var movedFuncs []string
	for _, fn := range sortedScriptFunctions(moveScripts) {
		callName := callByFunc[fn]
		var body strings.Builder
		body.WriteString("func ")
		body.WriteString(fn)
		body.WriteString("(t *testing.T) {\n")
		body.WriteString("\t")
		body.WriteString(callName)
		body.WriteString("(\n\t\tt,\n\t\t[]ScriptTest{\n")
		for _, elt := range movedByFunc[fn] {
			body.WriteString(indentBlock(elt, "\t\t\t"))
			body.WriteString("\n")
		}
		body.WriteString("\t\t},\n\t)\n}")
		movedFuncs = append(movedFuncs, body.String())
	}

	header := fileHeader(src, fset.Position(file.Package).Offset)
	pgSrc := []byte(header + "package postgres16\n\n" + importBlockForMoved(src, fset, file, nil) + "\n\n" + strings.Join(movedFuncs, "\n\n") + "\n")
	pgSrc, err = pruneAndFormat(pgSrc)
	if err != nil {
		return fmt.Errorf("%s pg16 script rewrite: %w", sourcePath, err)
	}

	targetPath := filepath.Join(targetDir, filepath.Base(sourcePath))
	if _, err := os.Stat(targetPath); err == nil {
		existing, err := os.ReadFile(targetPath)
		if err != nil {
			return err
		}
		combined := []byte(strings.TrimRight(string(existing), " \t\r\n") + "\n\n" + strings.Join(movedFuncs, "\n\n") + "\n")
		combined, err = pruneAndFormat(combined)
		if err != nil {
			return fmt.Errorf("%s pg16 append rewrite: %w", sourcePath, err)
		}
		if err := os.WriteFile(sourcePath, topSrc, 0o644); err != nil {
			return err
		}
		return os.WriteFile(targetPath, combined, 0o644)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}
	if sourceHasDeclarations(topSrc) {
		if err := os.WriteFile(sourcePath, topSrc, 0o644); err != nil {
			return err
		}
	} else if err := os.Remove(sourcePath); err != nil {
		return err
	}
	return os.WriteFile(targetPath, pgSrc, 0o644)
}

type movedScriptSnippet struct {
	source string
}

type runScriptsTarget struct {
	name string
	lit  *ast.CompositeLit
}

func splitAssertionSourceFile(sourcePath, targetDir string, moveAssertions map[string]map[int]migrationAssertion) error {
	src, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourcePath, src, parser.ParseComments)
	if err != nil {
		return err
	}

	packageScripts := packageScriptTestSlices(file)
	movedByFunc := make(map[string][]movedScriptSnippet)
	callByFunc := make(map[string]string)
	found := make(map[string]bool)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || len(moveAssertions[fn.Name.Name]) == 0 {
			continue
		}
		targets, err := runScriptsTargets(fn, packageScripts)
		if err != nil {
			return fmt.Errorf("%s:%s: %w", sourcePath, fn.Name.Name, err)
		}
		ordinal := 0
		for _, target := range targets {
			if callByFunc[fn.Name.Name] == "" {
				callByFunc[fn.Name.Name] = target.name
			} else if callByFunc[fn.Name.Name] != target.name {
				return fmt.Errorf("%s:%s: mixed RunScripts helpers are not supported", sourcePath, fn.Name.Name)
			}
			for _, scriptExpr := range target.lit.Elts {
				scriptLit, ok := scriptExpr.(*ast.CompositeLit)
				if !ok {
					continue
				}
				assertionsLit, ok := compositeField(scriptLit, "Assertions").(*ast.CompositeLit)
				if !ok {
					continue
				}
				var movedAssertionSources []string
				var setupQueries []ast.Expr
				var keptAssertions []ast.Expr
				for _, assertionExpr := range assertionsLit.Elts {
					assertionLit, ok := assertionExpr.(*ast.CompositeLit)
					if !ok {
						keptAssertions = append(keptAssertions, assertionExpr)
						continue
					}
					if !assertionOrdinalAnchor(assertionLit) {
						keptAssertions = append(keptAssertions, assertionExpr)
						continue
					}
					ordinal++
					moveAssertion, ok := moveAssertions[fn.Name.Name][ordinal]
					if !ok {
						keptAssertions = append(keptAssertions, assertionExpr)
						continue
					}
					key := fmt.Sprintf("%s#%04d", fn.Name.Name, ordinal)
					found[key] = true
					movedAssertionSources = append(movedAssertionSources, strings.TrimSpace(sourceForExpr(src, fset, assertionLit)))
					if moveAssertion.ExpectedKind != "error" {
						queryExpr := compositeField(assertionLit, "Query")
						if queryExpr == nil {
							return fmt.Errorf("%s:%s: assertion %04d has no Query field", sourcePath, fn.Name.Name, ordinal)
						}
						setupQueries = append(setupQueries, queryExpr)
					}
				}
				if len(movedAssertionSources) == 0 {
					continue
				}
				movedByFunc[fn.Name.Name] = append(movedByFunc[fn.Name.Name], movedScriptSnippet{
					source: movedScriptSource(src, fset, scriptLit, movedAssertionSources),
				})
				if len(setupQueries) > 0 {
					setupLit, err := ensureSetupScriptLiteral(scriptLit)
					if err != nil {
						return fmt.Errorf("%s:%s: %w", sourcePath, fn.Name.Name, err)
					}
					setupLit.Elts = append(setupLit.Elts, setupQueries...)
				}
				assertionsLit.Elts = keptAssertions
			}
		}
	}

	expected := countAssertions(moveAssertions)
	if len(found) != expected {
		var missing []string
		for fn, ordinals := range moveAssertions {
			for ordinal := range ordinals {
				key := fmt.Sprintf("%s#%04d", fn, ordinal)
				if !found[key] {
					missing = append(missing, key)
				}
			}
		}
		sort.Strings(missing)
		return fmt.Errorf("%s: missing assertion declarations: %s", sourcePath, strings.Join(missing, ", "))
	}

	var top bytes.Buffer
	if err := format.Node(&top, fset, file); err != nil {
		return fmt.Errorf("%s top-level assertion rewrite: %w", sourcePath, err)
	}
	topSrc, err := pruneAndFormat(top.Bytes())
	if err != nil {
		return fmt.Errorf("%s top-level assertion rewrite: %w", sourcePath, err)
	}

	var movedFuncs []string
	for _, fn := range sortedAssertionFunctions(moveAssertions) {
		callName := callByFunc[fn]
		pgFn := postgresAssertionFunctionName(fn)
		var body strings.Builder
		body.WriteString("func ")
		body.WriteString(pgFn)
		body.WriteString("(t *testing.T) {\n")
		body.WriteString("\t")
		body.WriteString(callName)
		body.WriteString("(\n\t\tt,\n\t\t[]ScriptTest{\n")
		for _, moved := range movedByFunc[fn] {
			body.WriteString(indentBlock(ensureTrailingComma(moved.source), "\t\t\t"))
			body.WriteString("\n")
		}
		body.WriteString("\t\t},\n\t)\n}")
		movedFuncs = append(movedFuncs, body.String())
	}

	header := fileHeader(src, fset.Position(file.Package).Offset)
	pgSrc := []byte(header + "package postgres16\n\n" + importBlockForMoved(src, fset, file, nil) + "\n\n" + strings.Join(movedFuncs, "\n\n") + "\n")
	pgSrc, err = pruneAndFormat(pgSrc)
	if err != nil {
		return fmt.Errorf("%s pg16 assertion rewrite: %w", sourcePath, err)
	}

	targetPath := filepath.Join(targetDir, filepath.Base(sourcePath))
	if _, err := os.Stat(targetPath); err == nil {
		existing, err := os.ReadFile(targetPath)
		if err != nil {
			return err
		}
		combined := []byte(strings.TrimRight(string(existing), " \t\r\n") + "\n\n" + strings.Join(movedFuncs, "\n\n") + "\n")
		combined, err = pruneAndFormat(combined)
		if err != nil {
			return fmt.Errorf("%s pg16 assertion append rewrite: %w", sourcePath, err)
		}
		if err := os.WriteFile(sourcePath, topSrc, 0o644); err != nil {
			return err
		}
		return os.WriteFile(targetPath, combined, 0o644)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}
	if sourceHasDeclarations(topSrc) {
		if err := os.WriteFile(sourcePath, topSrc, 0o644); err != nil {
			return err
		}
	} else if err := os.Remove(sourcePath); err != nil {
		return err
	}
	return os.WriteFile(targetPath, pgSrc, 0o644)
}

func runScriptsLiteral(fn *ast.FuncDecl) (string, *ast.CompositeLit, error) {
	var matches []struct {
		name string
		lit  *ast.CompositeLit
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok || (ident.Name != "RunScripts" && ident.Name != "RunScriptsWithoutNormalization") {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.CompositeLit)
			if ok && isScriptTestSlice(lit.Type) {
				matches = append(matches, struct {
					name string
					lit  *ast.CompositeLit
				}{name: ident.Name, lit: lit})
				return true
			}
		}
		return true
	})
	if len(matches) != 1 {
		return "", nil, fmt.Errorf("expected one RunScripts ScriptTest literal, found %d", len(matches))
	}
	return matches[0].name, matches[0].lit, nil
}

func runScriptsTargets(fn *ast.FuncDecl, packageScripts map[string]*ast.CompositeLit) ([]runScriptsTarget, error) {
	var matches []runScriptsTarget
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok || (ident.Name != "RunScripts" && ident.Name != "RunScriptsWithoutNormalization") {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.CompositeLit)
			if ok && isScriptTestSlice(lit.Type) {
				matches = append(matches, runScriptsTarget{name: ident.Name, lit: lit})
				return true
			}
			argIdent, ok := arg.(*ast.Ident)
			if ok {
				if lit := packageScripts[argIdent.Name]; lit != nil {
					matches = append(matches, runScriptsTarget{name: ident.Name, lit: lit})
					return true
				}
			}
		}
		return true
	})
	if len(matches) == 0 {
		return nil, fmt.Errorf("expected at least one RunScripts ScriptTest literal or package variable")
	}
	return matches, nil
}

func packageScriptTestSlices(file *ast.File) map[string]*ast.CompositeLit {
	slices := make(map[string]*ast.CompositeLit)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valueSpec.Names {
				if i >= len(valueSpec.Values) {
					continue
				}
				lit, ok := valueSpec.Values[i].(*ast.CompositeLit)
				if ok && isScriptTestSlice(lit.Type) {
					slices[name.Name] = lit
				}
			}
		}
	}
	return slices
}

func isScriptTestSlice(expr ast.Expr) bool {
	arrayType, ok := expr.(*ast.ArrayType)
	if !ok {
		return false
	}
	ident, ok := arrayType.Elt.(*ast.Ident)
	return ok && ident.Name == "ScriptTest"
}

func scriptTestName(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.CompositeLit)
	if !ok {
		return "", false
	}
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name != "Name" {
			continue
		}
		value, ok := kv.Value.(*ast.BasicLit)
		if !ok || value.Kind != token.STRING {
			return "", false
		}
		name, err := strconv.Unquote(value.Value)
		if err != nil {
			return "", false
		}
		return name, true
	}
	return "", false
}

func prefixAssertionsAreSafe(assertions []migrationAssertion) bool {
	for _, assertion := range assertions {
		if assertion.Username != "" || len(assertion.BindVars) > 0 || assertion.ExpectedKind == "error" || assertion.SQLState != "" || assertion.ErrorSeverity != "" {
			return false
		}
		if assertion.Query == "" {
			return false
		}
	}
	return true
}

func compositeField(lit *ast.CompositeLit, name string) ast.Expr {
	for _, elt := range lit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if ok && key.Name == name {
			return kv.Value
		}
	}
	return nil
}

func assertionOrdinalAnchor(lit *ast.CompositeLit) bool {
	for _, field := range []string{"Expected", "ExpectedRaw", "ExpectedErr", "ExpectedTag", "ExpectedColNames", "ExpectedColTypes", "ExpectedNotices", "PostgresOracle"} {
		if compositeField(lit, field) != nil {
			return true
		}
	}
	return false
}

func movedScriptSource(src []byte, fset *token.FileSet, scriptLit *ast.CompositeLit, movedAssertions []string) string {
	var body strings.Builder
	body.WriteString("{\n")
	for _, elt := range scriptLit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if !ok || key.Name == "Assertions" || key.Name == "Focus" || key.Name == "Skip" {
			continue
		}
		body.WriteString("\t")
		body.WriteString(key.Name)
		body.WriteString(": ")
		body.WriteString(sourceForExpr(src, fset, kv.Value))
		body.WriteString(",\n")
	}
	body.WriteString("\tAssertions: []ScriptTestAssertion{\n")
	for _, assertion := range movedAssertions {
		body.WriteString(indentBlock(ensureTrailingComma(assertion), "\t\t"))
		body.WriteString("\n")
	}
	body.WriteString("\t},\n")
	body.WriteString("}")
	return body.String()
}

func ensureSetupScriptLiteral(scriptLit *ast.CompositeLit) (*ast.CompositeLit, error) {
	if expr := compositeField(scriptLit, "SetUpScript"); expr != nil {
		lit, ok := expr.(*ast.CompositeLit)
		if !ok {
			return nil, fmt.Errorf("SetUpScript is not a literal")
		}
		return lit, nil
	}

	lit := &ast.CompositeLit{
		Type: &ast.ArrayType{Elt: ast.NewIdent("string")},
	}
	setupField := &ast.KeyValueExpr{
		Key:   ast.NewIdent("SetUpScript"),
		Value: lit,
	}
	insertAt := len(scriptLit.Elts)
	for i, elt := range scriptLit.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := kv.Key.(*ast.Ident)
		if ok && key.Name == "Assertions" {
			insertAt = i
			break
		}
	}
	scriptLit.Elts = append(scriptLit.Elts, nil)
	copy(scriptLit.Elts[insertAt+1:], scriptLit.Elts[insertAt:])
	scriptLit.Elts[insertAt] = setupField
	return lit, nil
}

func sourceForExpr(src []byte, fset *token.FileSet, expr ast.Expr) string {
	start := fset.Position(expr.Pos()).Offset
	end := fset.Position(expr.End()).Offset
	return string(src[start:end])
}

func ensureTrailingComma(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasSuffix(raw, ",") {
		return raw
	}
	return raw + ","
}

func lineStart(src []byte, offset int) int {
	for offset > 0 && src[offset-1] != '\n' {
		offset--
	}
	return offset
}

func includeTrailingCommaLine(src []byte, offset int) int {
	for offset < len(src) && (src[offset] == ' ' || src[offset] == '\t' || src[offset] == '\r' || src[offset] == '\n') {
		offset++
	}
	if offset < len(src) && src[offset] == ',' {
		offset++
	}
	for offset < len(src) && (src[offset] == ' ' || src[offset] == '\t' || src[offset] == '\r') {
		offset++
	}
	if offset < len(src) && src[offset] == '\n' {
		offset++
	}
	return offset
}

func indentBlock(raw, prefix string) string {
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n")
}

func functionName(source string) (string, bool) {
	idx := strings.LastIndex(source, ":")
	if idx < 0 || idx == len(source)-1 {
		return "", false
	}
	return source[idx+1:], true
}

func splitSourceFile(sourcePath, targetDir string, moveFuncs map[string]bool) error {
	src, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, sourcePath, src, parser.ParseComments)
	if err != nil {
		return err
	}

	var moveSpans []span
	var movedDecls []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !moveFuncs[fn.Name.Name] {
			continue
		}
		startPos := fn.Pos()
		if fn.Doc != nil {
			startPos = fn.Doc.Pos()
		}
		start := fset.Position(startPos).Offset
		end := fset.Position(fn.End()).Offset
		for end < len(src) && (src[end] == '\n' || src[end] == '\r') {
			end++
			if end < len(src) && src[end] == '\n' {
				end++
				break
			}
		}
		moveSpans = append(moveSpans, span{start: start, end: end, name: fn.Name.Name})
		movedDecls = append(movedDecls, strings.TrimSpace(string(src[start:end])))
	}
	if len(moveSpans) != len(moveFuncs) {
		found := make(map[string]bool)
		for _, s := range moveSpans {
			found[s.name] = true
		}
		var missing []string
		for name := range moveFuncs {
			if !found[name] {
				missing = append(missing, name)
			}
		}
		sort.Strings(missing)
		return fmt.Errorf("%s: missing function declarations: %s", sourcePath, strings.Join(missing, ", "))
	}

	sort.Slice(moveSpans, func(i, j int) bool { return moveSpans[i].start > moveSpans[j].start })
	topSrc := append([]byte(nil), src...)
	for _, s := range moveSpans {
		topSrc = append(topSrc[:s.start], topSrc[s.end:]...)
	}
	topSrc, err = pruneAndFormat(topSrc)
	if err != nil {
		return fmt.Errorf("%s top-level rewrite: %w", sourcePath, err)
	}

	header := fileHeader(src, fset.Position(file.Package).Offset)
	pgSrc := []byte(header + "package postgres16\n\n" + importBlockForMoved(src, fset, file, moveFuncs) + "\n\n" + strings.Join(movedDecls, "\n\n") + "\n")
	pgSrc, err = pruneAndFormat(pgSrc)
	if err != nil {
		return fmt.Errorf("%s pg16 rewrite: %w", sourcePath, err)
	}

	targetPath := filepath.Join(targetDir, filepath.Base(sourcePath))
	if _, err := os.Stat(targetPath); err == nil {
		return fmt.Errorf("%s already exists", targetPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(sourcePath, topSrc, 0o644); err != nil {
		return err
	}
	return os.WriteFile(targetPath, pgSrc, 0o644)
}

func fileHeader(src []byte, packageOffset int) string {
	header := string(src[:packageOffset])
	return strings.TrimRight(header, " \t\r\n") + "\n\n"
}

func importBlockForMoved(src []byte, fset *token.FileSet, file *ast.File, moveFuncs map[string]bool) string {
	var imports []string
	for _, spec := range file.Imports {
		imports = append(imports, importSpecSource(src, fset, spec))
	}
	imports = append(imports, `. "github.com/dolthub/doltgresql/testing/go"`)
	sort.Strings(imports)
	return "import (\n\t" + strings.Join(imports, "\n\t") + "\n)"
}

func importSpecSource(src []byte, fset *token.FileSet, spec *ast.ImportSpec) string {
	start := fset.Position(spec.Pos()).Offset
	end := fset.Position(spec.End()).Offset
	return string(src[start:end])
}

func pruneAndFormat(src []byte) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	used := usedImportNames(file)
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.IMPORT {
			continue
		}
		var specs []ast.Spec
		for _, spec := range gen.Specs {
			importSpec := spec.(*ast.ImportSpec)
			name := importName(importSpec)
			if name == "." || name == "_" || used[name] {
				specs = append(specs, spec)
			}
		}
		gen.Specs = specs
	}

	var decls []ast.Decl
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if ok && gen.Tok == token.IMPORT && len(gen.Specs) == 0 {
			continue
		}
		decls = append(decls, decl)
	}
	file.Decls = decls

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return nil, err
	}
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return out, nil
}

func sourceHasDeclarations(src []byte) bool {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, parser.ParseComments)
	return err == nil && len(file.Decls) > 0
}

func usedImportNames(file *ast.File) map[string]bool {
	used := make(map[string]bool)
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			if ident, ok := n.X.(*ast.Ident); ok {
				used[ident.Name] = true
			}
		}
		return true
	})
	return used
}

func importName(spec *ast.ImportSpec) string {
	if spec.Name != nil {
		return spec.Name.Name
	}
	path, err := strconv.Unquote(spec.Path.Value)
	if err != nil {
		return ""
	}
	parts := strings.Split(path, "/")
	base := parts[len(parts)-1]
	if isMajorVersionPath(base) && len(parts) > 1 {
		base = parts[len(parts)-2]
	}
	if idx := strings.LastIndex(base, "."); idx > 0 {
		base = base[:idx]
	}
	return base
}

func isMajorVersionPath(path string) bool {
	if len(path) < 2 || path[0] != 'v' {
		return false
	}
	for _, r := range path[1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func splitMapFile(plan filePlan, targetMapDir string) error {
	oldSource := filepath.ToSlash(plan.sourcePath)
	newSource := filepath.ToSlash(filepath.Join("testing/go/postgres16", filepath.Base(plan.sourcePath)))
	topMap := plan.mapFile
	pgMap := plan.mapFile
	topMap.Assertions = nil
	pgMap.Assertions = nil
	pgMap.SourceFile = newSource

	for _, assertion := range plan.mapFile.Assertions {
		fn, ok := functionName(assertion.Source)
		if !ok {
			return fmt.Errorf("%s: cannot parse source %q", plan.mapPath, assertion.Source)
		}
		if plan.funcs[fn] {
			assertion.Source = strings.Replace(assertion.Source, oldSource, newSource, 1)
			assertion.Key = strings.Replace(assertion.Key, oldSource, newSource, 1)
			pgMap.Assertions = append(pgMap.Assertions, assertion)
		} else {
			topMap.Assertions = append(topMap.Assertions, assertion)
		}
	}
	if len(pgMap.Assertions) == 0 {
		return fmt.Errorf("%s: no PostgreSQL assertions moved", plan.mapPath)
	}
	if err := os.MkdirAll(targetMapDir, 0o755); err != nil {
		return err
	}
	targetMap := filepath.Join(targetMapDir, filepath.Base(plan.mapPath))
	if _, err := os.Stat(targetMap); err == nil {
		return fmt.Errorf("%s already exists", targetMap)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeJSON(targetMap, pgMap); err != nil {
		return err
	}
	if len(topMap.Assertions) == 0 {
		return os.Remove(plan.mapPath)
	}
	return writeJSON(plan.mapPath, topMap)
}

func splitScriptMapFile(plan filePlan, targetMapDir string) error {
	oldSource := filepath.ToSlash(plan.sourcePath)
	newSource := filepath.ToSlash(filepath.Join("testing/go/postgres16", filepath.Base(plan.sourcePath)))
	topMap := plan.mapFile
	pgMap := plan.mapFile
	topMap.Assertions = nil
	pgMap.Assertions = nil
	pgMap.SourceFile = newSource

	for _, assertion := range plan.mapFile.Assertions {
		fn, ok := functionName(assertion.Source)
		if !ok {
			return fmt.Errorf("%s: cannot parse source %q", plan.mapPath, assertion.Source)
		}
		if plan.scripts[fn][assertion.ScriptName] {
			assertion.Source = strings.Replace(assertion.Source, oldSource, newSource, 1)
			assertion.Key = strings.Replace(assertion.Key, oldSource, newSource, 1)
			pgMap.Assertions = append(pgMap.Assertions, assertion)
		} else {
			topMap.Assertions = append(topMap.Assertions, assertion)
		}
	}
	topMap.Assertions = renumberAssertions(topMap.Assertions)
	pgMap.Assertions = renumberAssertions(pgMap.Assertions)
	if len(pgMap.Assertions) == 0 {
		return fmt.Errorf("%s: no PostgreSQL script assertions moved", plan.mapPath)
	}
	if err := os.MkdirAll(targetMapDir, 0o755); err != nil {
		return err
	}
	targetMap := filepath.Join(targetMapDir, filepath.Base(plan.mapPath))
	if _, err := os.Stat(targetMap); err == nil {
		data, err := os.ReadFile(targetMap)
		if err != nil {
			return err
		}
		var existing migrationFile
		if err := json.Unmarshal(data, &existing); err != nil {
			return fmt.Errorf("%s: %w", targetMap, err)
		}
		if existing.SourceFile != pgMap.SourceFile {
			return fmt.Errorf("%s: sourceFile is %q, expected %q", targetMap, existing.SourceFile, pgMap.SourceFile)
		}
		pgMap.Assertions = renumberAssertions(append(existing.Assertions, pgMap.Assertions...))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeJSON(targetMap, pgMap); err != nil {
		return err
	}
	if len(topMap.Assertions) == 0 {
		return os.Remove(plan.mapPath)
	}
	return writeJSON(plan.mapPath, topMap)
}

func splitAssertionMapFile(plan filePlan, targetMapDir string) error {
	newSourcePath := filepath.ToSlash(filepath.Join("testing/go/postgres16", filepath.Base(plan.sourcePath)))
	topMap := plan.mapFile
	pgMap := plan.mapFile
	topMap.Assertions = nil
	pgMap.Assertions = nil
	pgMap.SourceFile = newSourcePath

	for _, assertion := range plan.mapFile.Assertions {
		fn, ok := functionName(assertion.Source)
		if !ok {
			return fmt.Errorf("%s: cannot parse source %q", plan.mapPath, assertion.Source)
		}
		if _, ok := plan.assertions[fn][assertion.Ordinal]; ok {
			newSource := fmt.Sprintf("%s:%s", newSourcePath, postgresAssertionFunctionName(fn))
			assertion.Source = newSource
			assertion.Key = fmt.Sprintf("%s#%04d", newSource, assertion.Ordinal)
			pgMap.Assertions = append(pgMap.Assertions, assertion)
		} else {
			topMap.Assertions = append(topMap.Assertions, assertion)
		}
	}
	topMap.Assertions = renumberAssertions(topMap.Assertions)
	pgMap.Assertions = renumberAssertions(pgMap.Assertions)
	if len(pgMap.Assertions) == 0 {
		return fmt.Errorf("%s: no PostgreSQL assertions moved", plan.mapPath)
	}
	if err := os.MkdirAll(targetMapDir, 0o755); err != nil {
		return err
	}
	targetMap := filepath.Join(targetMapDir, filepath.Base(plan.mapPath))
	if _, err := os.Stat(targetMap); err == nil {
		data, err := os.ReadFile(targetMap)
		if err != nil {
			return err
		}
		var existing migrationFile
		if err := json.Unmarshal(data, &existing); err != nil {
			return fmt.Errorf("%s: %w", targetMap, err)
		}
		if existing.SourceFile != pgMap.SourceFile {
			return fmt.Errorf("%s: sourceFile is %q, expected %q", targetMap, existing.SourceFile, pgMap.SourceFile)
		}
		pgMap.Assertions = renumberAssertions(append(existing.Assertions, pgMap.Assertions...))
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeJSON(targetMap, pgMap); err != nil {
		return err
	}
	if len(topMap.Assertions) == 0 {
		return os.Remove(plan.mapPath)
	}
	return writeJSON(plan.mapPath, topMap)
}

func renumberAssertions(assertions []migrationAssertion) []migrationAssertion {
	counters := make(map[string]int)
	for i := range assertions {
		counters[assertions[i].Source]++
		ordinal := counters[assertions[i].Source]
		assertions[i].Ordinal = ordinal
		assertions[i].Key = fmt.Sprintf("%s#%04d", assertions[i].Source, ordinal)
	}
	return assertions
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func countScripts(scripts map[string]map[string]bool) int {
	var count int
	for _, names := range scripts {
		count += len(names)
	}
	return count
}

func countAssertions(assertions map[string]map[int]migrationAssertion) int {
	var count int
	for _, ordinals := range assertions {
		count += len(ordinals)
	}
	return count
}

func sortedScriptFunctions(scripts map[string]map[string]bool) []string {
	keys := make([]string, 0, len(scripts))
	for key := range scripts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func describeScripts(scripts map[string]map[string]bool) []string {
	var descriptions []string
	for _, fn := range sortedScriptFunctions(scripts) {
		names := sortedKeys(scripts[fn])
		for _, name := range names {
			descriptions = append(descriptions, fn+"/"+name)
		}
	}
	return descriptions
}

func sortedAssertionFunctions(assertions map[string]map[int]migrationAssertion) []string {
	keys := make([]string, 0, len(assertions))
	for key := range assertions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func describeAssertions(assertions map[string]map[int]migrationAssertion) []string {
	var descriptions []string
	for _, fn := range sortedAssertionFunctions(assertions) {
		var ordinals []int
		for ordinal := range assertions[fn] {
			ordinals = append(ordinals, ordinal)
		}
		sort.Ints(ordinals)
		for _, ordinal := range ordinals {
			descriptions = append(descriptions, fmt.Sprintf("%s#%04d", fn, ordinal))
		}
	}
	return descriptions
}

func postgresAssertionFunctionName(fn string) string {
	return fn + "PostgresOraclePrefix"
}
