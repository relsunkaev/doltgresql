// Copyright 2023 Dolthub, Inc.
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
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	vitess "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/doltgresql/postgres/parser/sem/tree"
	"github.com/dolthub/doltgresql/server/auth"
	pgnodes "github.com/dolthub/doltgresql/server/node"
)

// nodeCreateDatabase handles *tree.CreateDatabase nodes.
func nodeCreateDatabase(_ *Context, node *tree.CreateDatabase) (vitess.Statement, error) {
	var charsets []*vitess.CharsetAndCollate

	if len(node.Encoding) > 0 {
		if !isPostgresEncodingName(node.Encoding) {
			return nil, errors.Errorf("%s is not a valid encoding name", node.Encoding)
		}
		logrus.Warnf("unsupported clause ENCODING, ignoring")
	}
	if len(node.Strategy) > 0 {
		switch strings.ToUpper(node.Strategy) {
		case "WAL_LOG", "FILE_COPY":
			logrus.Warnf("unsupported clause STRATEGY, ignoring")
		default:
			return nil, errors.Errorf("unrecognized CREATE DATABASE strategy %s", node.Strategy)
		}
	}
	if len(node.Locale) > 0 {
		logrus.Warnf("unsupported clause LC_LOCALE, ignoring")
	}
	if len(node.Collate) > 0 {
		collation, charset, err := parseLocaleString(node.Collate)
		if err != nil {
			return nil, err
		}

		if collation == "" {
			logrus.Warnf("unsupported LC_COLLATE, ignoring")
		} else {
			charsets = append(charsets,
				&vitess.CharsetAndCollate{
					Type:  "CHARACTER SET",
					Value: charset,
				},
				&vitess.CharsetAndCollate{
					Type:  "COLLATE",
					Value: collation,
				},
			)
		}
	}
	if len(node.CType) > 0 {
		logrus.Warnf("CTYPE clause is not yet supported, ignoring")
	}
	if len(node.Tablespace) > 0 && !strings.EqualFold(node.Tablespace, "pg_default") {
		// pg_default is the only tablespace doltgres exposes. Accepting it as
		// a no-op lets dump/restore scripts that spell out the default run
		// unchanged; any other target name does not resolve here.
		return nil, errors.Errorf(`tablespace "%s" does not exist`, node.Tablespace)
	}

	if hasCreateDatabaseMetadataUpdate(node) || hasCreateDatabaseTemplate(node) {
		update, err := createDatabaseMetadataUpdate(node)
		if err != nil {
			return nil, err
		}
		return vitess.InjectedStatement{
			Statement: &pgnodes.CreateDatabase{
				Name:        bareIdentifier(node.Name),
				IfNotExists: node.IfNotExists,
				Template:    node.Template,
				Update:      update,
			},
		}, nil
	}

	return &vitess.DBDDL{
		Action:           vitess.CreateStr,
		SchemaOrDatabase: "database",
		DBName:           bareIdentifier(node.Name),
		IfNotExists:      node.IfNotExists,
		CharsetCollate:   charsets,
	}, nil
}

func hasCreateDatabaseTemplate(node *tree.CreateDatabase) bool {
	return node.Template != "" && !strings.EqualFold(node.Template, "template0")
}

func hasCreateDatabaseMetadataUpdate(node *tree.CreateDatabase) bool {
	return node.Owner != "" ||
		node.AllowConnections != nil ||
		node.ConnectionLimit != nil ||
		node.IsTemplate != nil ||
		node.Oid != nil ||
		node.Locale != "" ||
		node.Collate != "" ||
		node.CType != "" ||
		node.IcuLocale != "" ||
		node.IcuRules != "" ||
		node.LocaleProvider != "" ||
		node.CollationVersion != ""
}

func createDatabaseMetadataUpdate(node *tree.CreateDatabase) (auth.DatabaseMetadataUpdate, error) {
	update := auth.DatabaseMetadataUpdate{}
	if node.Owner != "" {
		owner := node.Owner
		update.Owner = &owner
	}
	if node.AllowConnections != nil {
		value, err := databaseBoolOption(node.AllowConnections)
		if err != nil {
			return update, err
		}
		update.AllowConnections = &value
	}
	if node.ConnectionLimit != nil {
		value, err := databaseIntOption(node.ConnectionLimit)
		if err != nil {
			return update, err
		}
		update.ConnectionLimit = &value
	}
	if node.IsTemplate != nil {
		value, err := databaseBoolOption(node.IsTemplate)
		if err != nil {
			return update, err
		}
		update.IsTemplate = &value
	}
	if node.Oid != nil {
		value, err := databaseIntOption(node.Oid)
		if err != nil {
			return update, err
		}
		if value < 0 {
			return update, errors.Errorf("OID must not be negative")
		}
		oid := uint32(value)
		update.Oid = &oid
	}
	if node.Locale != "" {
		locale := node.Locale
		update.Collate = &locale
		update.CType = &locale
	}
	if node.Collate != "" {
		collate := node.Collate
		update.Collate = &collate
	}
	if node.CType != "" {
		ctype := node.CType
		update.CType = &ctype
	}
	if node.IcuLocale != "" {
		icuLocale := node.IcuLocale
		update.IcuLocale = &icuLocale
	}
	if node.IcuRules != "" {
		icuRules := node.IcuRules
		update.IcuRules = &icuRules
	}
	if node.LocaleProvider != "" {
		provider, err := databaseLocaleProvider(node.LocaleProvider)
		if err != nil {
			return update, err
		}
		update.LocaleProvider = &provider
	}
	if node.CollationVersion != "" {
		collationVersion := node.CollationVersion
		update.CollationVersion = &collationVersion
	}
	return update, nil
}

func databaseLocaleProvider(provider string) (string, error) {
	switch strings.ToLower(provider) {
	case "libc", "c":
		return "c", nil
	case "icu", "i":
		return "i", nil
	default:
		return "", errors.Errorf("unrecognized locale provider %s", provider)
	}
}

var postgresEncodingNames = map[string]struct{}{
	"BIG5":           {},
	"EUC_CN":         {},
	"EUC_JIS_2004":   {},
	"EUC_JP":         {},
	"EUC_KR":         {},
	"EUC_TW":         {},
	"GB18030":        {},
	"GBK":            {},
	"ISO_8859_5":     {},
	"ISO_8859_6":     {},
	"ISO_8859_7":     {},
	"ISO_8859_8":     {},
	"JOHAB":          {},
	"KOI8R":          {},
	"LATIN1":         {},
	"LATIN2":         {},
	"LATIN3":         {},
	"LATIN4":         {},
	"LATIN5":         {},
	"LATIN6":         {},
	"LATIN7":         {},
	"LATIN8":         {},
	"LATIN9":         {},
	"LATIN10":        {},
	"MULE_INTERNAL":  {},
	"SJIS":           {},
	"SQL_ASCII":      {},
	"SHIFT_JIS_2004": {},
	"UHC":            {},
	"UNICODE":        {},
	"UTF8":           {},
	"UTF-8":          {},
	"WIN866":         {},
	"WIN874":         {},
	"WIN1250":        {},
	"WIN1251":        {},
	"WIN1252":        {},
	"WIN1253":        {},
	"WIN1254":        {},
	"WIN1255":        {},
	"WIN1256":        {},
	"WIN1257":        {},
	"WIN1258":        {},
}

func isPostgresEncodingName(encoding string) bool {
	normalized := strings.ToUpper(strings.Trim(encoding, `"'`))
	_, ok := postgresEncodingNames[normalized]
	return ok
}

var collationRegex = regexp.MustCompile(`^(?P<Language>[^_]+)_?(?P<Region>[^.]+)?\.?(?P<CodePage>\d+)?$`)

// parseLocaleString attempts to parse the locale string given to extract a mysql collation we can use
func parseLocaleString(collation string) (string, string, error) {
	// FindStringSubmatchIndex returns the indices of the matched elements
	match := collationRegex.FindStringSubmatch(collation)

	result := make(map[string]string)
	for i, name := range collationRegex.SubexpNames() {
		if i > 0 && i <= len(match) {
			result[name] = match[i]
		}
	}

	if result["Language"] == "" {
		return "", "", errors.Errorf("malformed collation: %s", collation)
	}

	switch strings.ToLower(result["Language"]) {
	case "english", "en":
		return "latin1_general_cs", "latin1", nil
	}

	return "", "", nil
}
