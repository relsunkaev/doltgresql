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

package functions

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/functions/framework"
	pgtypes "github.com/dolthub/doltgresql/server/types"
	"github.com/dolthub/doltgresql/utils"
)

// initXml registers the functions for the XML type.
func initXml() {
	framework.RegisterFunction(xml_in)
	framework.RegisterFunction(xml_out)
	framework.RegisterFunction(xml_recv)
	framework.RegisterFunction(xml_send)
	framework.RegisterFunction(xml_text)
	framework.RegisterFunction(xmlparse_text)
	framework.RegisterFunction(xmlparse_xml)
	framework.RegisterFunction(xmlparse_text_text)
	framework.RegisterFunction(xml_is_well_formed_text)
	framework.RegisterFunction(xml_is_well_formed_document_text)
	framework.RegisterFunction(xml_is_well_formed_content_text)
}

var xml_in = framework.Function1{
	Name:       "xml_in",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Cstring},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if err := pgtypes.ValidateXMLContent(input); err != nil {
			return nil, err
		}
		return input, nil
	},
}

var xml_out = framework.Function1{
	Name:       "xml_out",
	Return:     pgtypes.Cstring,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xml},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return val, nil
	},
}

var xml_recv = framework.Function1{
	Name:       "xml_recv",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Internal},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		data := val.([]byte)
		if data == nil {
			return nil, nil
		}
		input := string(data)
		if err := pgtypes.ValidateXMLContent(input); err != nil {
			return nil, err
		}
		return input, nil
	},
}

var xml_send = framework.Function1{
	Name:       "xml_send",
	Return:     pgtypes.Bytea,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xml},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		str, ok, err := sql.Unwrap[string](ctx, val)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		writer := utils.NewWireWriter()
		writer.WriteString(str)
		return writer.BufferData(), nil
	},
}

var xml_text = framework.Function1{
	Name:       "xml",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if err := pgtypes.ValidateXMLContent(input); err != nil {
			return nil, err
		}
		return input, nil
	},
}

var xmlparse_text = framework.Function1{
	Name:       "xmlparse",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if err := pgtypes.ValidateXMLDocument(input); err != nil {
			return nil, err
		}
		return input, nil
	},
}

var xmlparse_xml = framework.Function1{
	Name:       "xmlparse",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Xml},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if err := pgtypes.ValidateXMLDocument(input); err != nil {
			return nil, err
		}
		return input, nil
	},
}

var xmlparse_text_text = framework.Function2{
	Name:       "xmlparse",
	Return:     pgtypes.Xml,
	Parameters: [2]*pgtypes.DoltgresType{pgtypes.Text, pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [3]*pgtypes.DoltgresType, modeVal any, inputVal any) (any, error) {
		mode := strings.ToLower(modeVal.(string))
		input := inputVal.(string)
		if mode == "content" {
			if err := pgtypes.ValidateXMLContent(input); err != nil {
				return nil, err
			}
			return input, nil
		}
		if err := pgtypes.ValidateXMLDocument(input); err != nil {
			return nil, err
		}
		return input, nil
	},
}

var xml_is_well_formed_text = framework.Function1{
	Name:       "xml_is_well_formed",
	Return:     pgtypes.Bool,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ValidateXMLContent(val.(string)) == nil, nil
	},
}

var xml_is_well_formed_document_text = framework.Function1{
	Name:       "xml_is_well_formed_document",
	Return:     pgtypes.Bool,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ValidateXMLDocument(val.(string)) == nil, nil
	},
}

var xml_is_well_formed_content_text = framework.Function1{
	Name:       "xml_is_well_formed_content",
	Return:     pgtypes.Bool,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		return pgtypes.ValidateXMLContent(val.(string)) == nil, nil
	},
}
