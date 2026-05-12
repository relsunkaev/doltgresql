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

package auth

import (
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/utils"
)

// Language stores catalog metadata for a procedural language.
type Language struct {
	Name         string
	Owner        string
	IsProcedural bool
	Trusted      bool
	Handler      string
	Inline       string
	Validator    string
}

// Languages contains the languages available in the current database.
type Languages struct {
	Data map[string]Language
}

// NewLanguages returns a new *Languages.
func NewLanguages() *Languages {
	return &Languages{Data: make(map[string]Language)}
}

// GetLanguage returns the language with the given name.
func GetLanguage(name string) (Language, bool) {
	lang, ok := globalDatabase.languages.Data[languageKey(name)]
	return lang, ok
}

// GetAllLanguages returns all languages in a deterministic order.
func GetAllLanguages() []Language {
	languages := make([]Language, 0, len(globalDatabase.languages.Data))
	for _, lang := range globalDatabase.languages.Data {
		languages = append(languages, lang)
	}
	sort.Slice(languages, func(i, j int) bool {
		return languages[i].Name < languages[j].Name
	})
	return languages
}

// CreateLanguage creates the language. If replace is true, an existing language is overwritten.
func CreateLanguage(lang Language, replace bool) error {
	lang.Name = languageKey(lang.Name)
	if len(lang.Name) == 0 {
		return errors.New("language name cannot be empty")
	}
	if _, ok := globalDatabase.languages.Data[lang.Name]; ok && !replace {
		return errors.Errorf(`language "%s" already exists`, lang.Name)
	}
	globalDatabase.languages.Data[lang.Name] = lang
	return nil
}

// DropLanguage drops the language. It returns false when the language did not exist.
func DropLanguage(name string) bool {
	name = languageKey(name)
	if _, ok := globalDatabase.languages.Data[name]; !ok {
		return false
	}
	delete(globalDatabase.languages.Data, name)
	RemoveAllLanguagePrivileges(name)
	return true
}

// RenameLanguage renames a language.
func RenameLanguage(oldName string, newName string) error {
	oldName = languageKey(oldName)
	newName = languageKey(newName)
	lang, ok := globalDatabase.languages.Data[oldName]
	if !ok {
		return errors.Errorf(`language "%s" does not exist`, oldName)
	}
	if _, ok = globalDatabase.languages.Data[newName]; ok {
		return errors.Errorf(`language "%s" already exists`, newName)
	}
	delete(globalDatabase.languages.Data, oldName)
	lang.Name = newName
	globalDatabase.languages.Data[newName] = lang
	RenameLanguagePrivileges(oldName, newName)
	return nil
}

// AlterLanguageOwner updates a language owner.
func AlterLanguageOwner(name string, owner string) error {
	name = languageKey(name)
	lang, ok := globalDatabase.languages.Data[name]
	if !ok {
		return errors.Errorf(`language "%s" does not exist`, name)
	}
	if !RoleExists(owner) {
		return errors.Errorf(`role "%s" does not exist`, owner)
	}
	lang.Owner = owner
	globalDatabase.languages.Data[name] = lang
	return nil
}

// CheckLanguageUsage verifies that a role may use the named language.
func CheckLanguageUsage(roleName string, languageName string) error {
	languageName = languageKey(languageName)
	if len(languageName) == 0 {
		return nil
	}
	var err error
	LockRead(func() {
		if _, ok := GetLanguage(languageName); !ok {
			if languageName == "c" {
				return
			}
			err = errors.Errorf(`language "%s" does not exist`, languageName)
			return
		}
		role := GetRole(roleName)
		if !role.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, roleName)
			return
		}
		public := GetRole("public")
		if !public.IsValid() {
			err = errors.Errorf(`role "%s" does not exist`, public.Name)
			return
		}
		if !HasLanguagePrivilege(LanguagePrivilegeKey{Role: role.ID(), Name: languageName}, Privilege_USAGE) &&
			!HasLanguagePrivilege(LanguagePrivilegeKey{Role: public.ID(), Name: languageName}, Privilege_USAGE) {
			err = errors.Errorf("permission denied for language %s", languageName)
		}
	})
	return err
}

func dbInitDefaultLanguages() {
	user, _ := GetSuperUserAndPassword()
	CreateLanguage(Language{Name: "sql", Owner: user, Trusted: true}, true)
	CreateLanguage(Language{Name: "plpgsql", Owner: user, IsProcedural: true, Trusted: true}, true)
	publicRole := GetRole("public")
	superUser := GetRole(user)
	if publicRole.IsValid() && superUser.IsValid() {
		for _, lang := range []string{"sql", "plpgsql"} {
			AddLanguagePrivilege(LanguagePrivilegeKey{Role: publicRole.ID(), Name: lang}, GrantedPrivilege{
				Privilege: Privilege_USAGE,
				GrantedBy: superUser.ID(),
			}, false)
		}
	}
}

func languageKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func (languages *Languages) serialize(writer *utils.Writer) {
	writer.Uint64(uint64(len(languages.Data)))
	for _, lang := range GetAllLanguages() {
		writer.String(lang.Name)
		writer.String(lang.Owner)
		writer.Bool(lang.IsProcedural)
		writer.Bool(lang.Trusted)
		writer.String(lang.Handler)
		writer.String(lang.Inline)
		writer.String(lang.Validator)
	}
}

func (languages *Languages) deserialize(version uint32, reader *utils.Reader) {
	languages.Data = make(map[string]Language)
	switch version {
	case 0:
	case 1:
		count := reader.Uint64()
		for i := uint64(0); i < count; i++ {
			lang := Language{
				Name:         reader.String(),
				Owner:        reader.String(),
				IsProcedural: reader.Bool(),
				Trusted:      reader.Bool(),
				Handler:      reader.String(),
				Inline:       reader.String(),
				Validator:    reader.String(),
			}
			languages.Data[languageKey(lang.Name)] = lang
		}
	default:
		panic("unexpected version in Languages")
	}
}
