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

package types

import (
	"encoding/xml"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/utils"
)

// Xml is the standard XML type.
var Xml = &DoltgresType{
	ID:                  toInternal("xml"),
	TypLength:           int16(-1),
	PassedByVal:         false,
	TypType:             TypeType_Base,
	TypCategory:         TypeCategory_UserDefinedTypes,
	IsPreferred:         false,
	IsDefined:           true,
	Delimiter:           ",",
	RelID:               id.Null,
	SubscriptFunc:       toFuncID("-"),
	Elem:                id.NullType,
	Array:               toInternal("_xml"),
	InputFunc:           toFuncID("xml_in", toInternal("cstring")),
	OutputFunc:          toFuncID("xml_out", toInternal("xml")),
	ReceiveFunc:         toFuncID("xml_recv", toInternal("internal")),
	SendFunc:            toFuncID("xml_send", toInternal("xml")),
	ModInFunc:           toFuncID("-"),
	ModOutFunc:          toFuncID("-"),
	AnalyzeFunc:         toFuncID("-"),
	Align:               TypeAlignment_Int,
	Storage:             TypeStorage_Extended,
	NotNull:             false,
	BaseTypeID:          id.NullType,
	TypMod:              -1,
	NDims:               0,
	TypCollation:        id.NullCollation,
	DefaulBin:           "",
	Default:             "",
	Acl:                 nil,
	Checks:              nil,
	attTypMod:           -1,
	CompareFunc:         toFuncID("-"),
	SerializationFunc:   serializeTypeXml,
	DeserializationFunc: deserializeTypeXml,
}

// ValidateXMLDocument returns an error when input is not a well-formed XML document with one top-level element.
func ValidateXMLDocument(input string) error {
	decoder := xml.NewDecoder(strings.NewReader(input))
	depth := 0
	roots := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Errorf("invalid XML document: %v", err)
		}
		switch tok := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
			}
			depth++
		case xml.EndElement:
			depth--
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(tok)) != "" {
				return errors.New("invalid XML document: character data is not allowed outside the root element")
			}
		}
	}
	if roots != 1 || depth != 0 {
		return errors.New("invalid XML document")
	}
	return nil
}

// ValidateXMLContent returns an error when input is not well-formed XML content.
func ValidateXMLContent(input string) error {
	decoder := xml.NewDecoder(strings.NewReader(input))
	for {
		_, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Errorf("invalid XML content: %v", err)
		}
	}
}

// serializeTypeXml handles serialization from the standard representation to our serialized representation.
func serializeTypeXml(ctx *sql.Context, t *DoltgresType, val any) ([]byte, error) {
	str, ok, err := sql.Unwrap[string](ctx, val)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Errorf(`"xml" serialization requires a string argument, got %T`, val)
	}
	if err := ValidateXMLContent(str); err != nil {
		return nil, err
	}
	writer := utils.NewWriter(uint64(len(str) + 4))
	writer.String(str)
	return writer.Data(), nil
}

// deserializeTypeXml handles deserialization from the Dolt serialized format.
func deserializeTypeXml(ctx *sql.Context, t *DoltgresType, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader := utils.NewReader(data)
	return reader.String(), nil
}
