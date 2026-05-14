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
	"fmt"
	"strconv"
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
	framework.RegisterFunction(xmlcomment_text)
	framework.RegisterFunction(xmlelement_any)
	framework.RegisterFunction(xmlforest_any)
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

var xmlcomment_text = framework.Function1{
	Name:       "xmlcomment",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Text},
	Strict:     true,
	Callable: func(ctx *sql.Context, _ [2]*pgtypes.DoltgresType, val any) (any, error) {
		input := val.(string)
		if strings.Contains(input, "--") || strings.HasSuffix(input, "-") {
			return nil, fmt.Errorf("invalid XML comment")
		}
		return "<!--" + input + "-->", nil
	},
}

var xmlelement_any = framework.Function1N{
	Name:       "xmlelement",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Strict:     false,
	Callable: func(ctx *sql.Context, types []*pgtypes.DoltgresType, name any, vals []any) (any, error) {
		elementName, err := xmlConstructorOutput(ctx, types[0], name)
		if err != nil {
			return nil, err
		}
		if len(vals) == 0 {
			return "<" + elementName + "/>", nil
		}
		sb := strings.Builder{}
		sb.WriteByte('<')
		sb.WriteString(elementName)
		sb.WriteByte('>')
		for i, val := range vals {
			if val == nil {
				continue
			}
			output, err := xmlConstructorContent(ctx, types[i+1], val)
			if err != nil {
				return nil, err
			}
			sb.WriteString(output)
		}
		sb.WriteString("</")
		sb.WriteString(elementName)
		sb.WriteByte('>')
		return sb.String(), nil
	},
}

var xmlforest_any = framework.Function1N{
	Name:       "xmlforest",
	Return:     pgtypes.Xml,
	Parameters: [1]*pgtypes.DoltgresType{pgtypes.Any},
	Strict:     false,
	Callable: func(ctx *sql.Context, types []*pgtypes.DoltgresType, val1 any, vals []any) (any, error) {
		args := append([]any{val1}, vals...)
		if len(args)%2 != 0 {
			return nil, fmt.Errorf("xmlforest requires name/value pairs")
		}
		sb := strings.Builder{}
		for i := 0; i < len(args); i += 2 {
			if args[i+1] == nil {
				continue
			}
			name, err := xmlConstructorOutput(ctx, types[i], args[i])
			if err != nil {
				return nil, err
			}
			value, err := xmlConstructorContent(ctx, types[i+1], args[i+1])
			if err != nil {
				return nil, err
			}
			sb.WriteByte('<')
			sb.WriteString(name)
			sb.WriteByte('>')
			sb.WriteString(value)
			sb.WriteString("</")
			sb.WriteString(name)
			sb.WriteByte('>')
		}
		return sb.String(), nil
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

func xmlConstructorOutput(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	if val == nil {
		return "", nil
	}
	switch val := val.(type) {
	case string:
		return val, nil
	case int16:
		return strconv.FormatInt(int64(val), 10), nil
	case int32:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case bool:
		return strconv.FormatBool(val), nil
	}
	output, err := typ.IoOutput(ctx, val)
	if err != nil {
		return "", err
	}
	return output, nil
}

func xmlConstructorContent(ctx *sql.Context, typ *pgtypes.DoltgresType, val any) (string, error) {
	output, err := xmlConstructorOutput(ctx, typ, val)
	if err != nil {
		return "", err
	}
	if typ.ID == pgtypes.Xml.ID {
		return output, nil
	}
	return escapeXMLText(output), nil
}

func escapeXMLText(input string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
	)
	return replacer.Replace(input)
}
