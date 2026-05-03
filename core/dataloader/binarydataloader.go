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

package dataloader

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/errors"
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/doltgresql/server/types"
)

var binaryCopySignature = []byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0}

// BinaryCopySignature returns the fixed PostgreSQL binary COPY stream signature.
func BinaryCopySignature() []byte {
	return append([]byte(nil), binaryCopySignature...)
}

// BinaryDataLoader tracks the state of a PostgreSQL binary COPY FROM operation.
type BinaryDataLoader struct {
	results    LoadDataResults
	colTypes   []*types.DoltgresType
	sch        sql.Schema
	buf        []byte
	rows       []sql.Row
	headerRead bool
	done       bool
}

var _ DataLoader = (*BinaryDataLoader)(nil)

// NewBinaryDataLoader creates a new DataLoader instance for PostgreSQL binary COPY data.
func NewBinaryDataLoader(colNames []string, tableSch sql.Schema) (*BinaryDataLoader, error) {
	colTypes, reducedSch, err := getColumnTypes(colNames, tableSch)
	if err != nil {
		return nil, err
	}

	return &BinaryDataLoader{
		colTypes: colTypes,
		sch:      reducedSch,
	}, nil
}

func (bdl *BinaryDataLoader) SetNextDataChunk(ctx *sql.Context, data *bufio.Reader) error {
	if bdl.done {
		return errors.Errorf("received binary COPY data after trailer")
	}
	chunk, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	bdl.buf = append(bdl.buf, chunk...)
	return bdl.parseAvailableRows(ctx)
}

func (bdl *BinaryDataLoader) parseAvailableRows(ctx *sql.Context) error {
	if !bdl.headerRead {
		if len(bdl.buf) < len(binaryCopySignature)+8 {
			return nil
		}
		if !bytes.Equal(bdl.buf[:len(binaryCopySignature)], binaryCopySignature) {
			return errors.Errorf("invalid binary COPY signature")
		}
		flags := binary.BigEndian.Uint32(bdl.buf[len(binaryCopySignature):])
		if flags&0xffff0000 != 0 {
			return errors.Errorf("unsupported binary COPY flags: %d", flags)
		}
		extensionLength := int(binary.BigEndian.Uint32(bdl.buf[len(binaryCopySignature)+4:]))
		headerLength := len(binaryCopySignature) + 8 + extensionLength
		if len(bdl.buf) < headerLength {
			return nil
		}
		bdl.buf = bdl.buf[headerLength:]
		bdl.headerRead = true
	}

	for {
		if len(bdl.buf) < 2 {
			return nil
		}
		fieldCount := int16(binary.BigEndian.Uint16(bdl.buf[:2]))
		if fieldCount == -1 {
			bdl.buf = bdl.buf[2:]
			bdl.done = true
			if len(bdl.buf) != 0 {
				return errors.Errorf("unexpected data after binary COPY trailer")
			}
			return nil
		}
		if int(fieldCount) != len(bdl.colTypes) {
			return errors.Errorf("binary COPY row has %d columns, expected %d", fieldCount, len(bdl.colTypes))
		}

		cursor := 2
		row := make(sql.Row, len(bdl.colTypes))
		for i := range bdl.colTypes {
			if len(bdl.buf[cursor:]) < 4 {
				return nil
			}
			fieldLength := int32(binary.BigEndian.Uint32(bdl.buf[cursor:]))
			cursor += 4
			if fieldLength == -1 {
				row[i] = nil
				continue
			}
			if fieldLength < 0 {
				return errors.Errorf("invalid binary COPY field length: %d", fieldLength)
			}
			if len(bdl.buf[cursor:]) < int(fieldLength) {
				return nil
			}
			valBytes := bdl.buf[cursor : cursor+int(fieldLength)]
			val, err := bdl.colTypes[i].CallReceive(ctx, valBytes)
			if err != nil {
				return err
			}
			row[i] = val
			cursor += int(fieldLength)
		}

		bdl.buf = bdl.buf[cursor:]
		bdl.rows = append(bdl.rows, row)
	}
}

// Finish implements the DataLoader interface.
func (bdl *BinaryDataLoader) Finish(ctx *sql.Context) (*LoadDataResults, error) {
	if !bdl.headerRead {
		return nil, errors.Errorf("binary COPY data missing header")
	}
	if !bdl.done {
		return nil, errors.Errorf("binary COPY data missing trailer")
	}
	if len(bdl.buf) != 0 {
		return nil, errors.Errorf("partial binary COPY data found at end of data load")
	}
	return &bdl.results, nil
}

func (bdl *BinaryDataLoader) Resolved() bool {
	return true
}

func (bdl *BinaryDataLoader) String() string {
	return "BinaryDataLoader"
}

func (bdl *BinaryDataLoader) Schema(ctx *sql.Context) sql.Schema {
	return bdl.sch
}

func (bdl *BinaryDataLoader) Children() []sql.Node {
	return nil
}

func (bdl *BinaryDataLoader) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(bdl, len(children), 0)
	}
	return bdl, nil
}

func (bdl *BinaryDataLoader) IsReadOnly() bool {
	return true
}

type binaryRowIter struct {
	bdl *BinaryDataLoader
}

func (b binaryRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if len(b.bdl.rows) == 0 {
		return nil, io.EOF
	}
	row := b.bdl.rows[0]
	b.bdl.rows = b.bdl.rows[1:]
	b.bdl.results.RowsLoaded++
	return row, nil
}

func (b binaryRowIter) Close(context *sql.Context) error {
	return nil
}

var _ sql.RowIter = (*binaryRowIter)(nil)

func (bdl *BinaryDataLoader) RowIter(ctx *sql.Context, r sql.Row) (sql.RowIter, error) {
	return &binaryRowIter{bdl: bdl}, nil
}
