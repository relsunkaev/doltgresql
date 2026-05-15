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
}

func main() {
	sourceDir := flag.String("source-dir", "testing/go", "directory containing top-level Go tests")
	targetDir := flag.String("target-dir", "testing/go/postgres16", "directory for PostgreSQL 16 oracle-backed tests")
	mapDir := flag.String("map-dir", "testing/go/testdata/postgres_oracle_migrations", "top-level oracle-map directory")
	targetMapDir := flag.String("target-map-dir", "testing/go/postgres16/testdata/postgres_oracle_migrations", "PostgreSQL 16 oracle-map directory")
	filesFlag := flag.String("files", "", "comma-separated test basenames to consider, for example foo_test,bar_test")
	dryRun := flag.Bool("dry-run", false, "print the planned split without writing files")
	flag.Parse()

	files := parseCSV(*filesFlag)
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "--files is required")
		os.Exit(1)
	}

	plans, err := buildPlans(*sourceDir, *mapDir, files)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(plans) == 0 {
		fmt.Fprintln(os.Stderr, "no pure PostgreSQL function groups found")
		return
	}

	for _, plan := range plans {
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

func buildPlans(sourceDir, mapDir string, files map[string]bool) ([]filePlan, error) {
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
		}

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
	sort.Slice(plans, func(i, j int) bool {
		return plans[i].sourcePath < plans[j].sourcePath
	})
	return plans, nil
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
	if len(topMap.Assertions) == 0 {
		return fmt.Errorf("%s: split would leave no top-level assertions", plan.mapPath)
	}

	if err := writeJSON(plan.mapPath, topMap); err != nil {
		return err
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
	return writeJSON(targetMap, pgMap)
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
