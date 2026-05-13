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

// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/errors"

	"github.com/dolthub/doltgresql/postgres/parser/lex"
)

// CopyFrom represents a COPY FROM statement.
type CopyFrom struct {
	Table   TableName
	File    string
	Program string
	Columns NameList
	Stdin   bool
	Options CopyOptions
}

// CopyTo represents a COPY TO statement.
type CopyTo struct {
	Table   TableName
	Query   *Select
	File    string
	Program string
	Columns NameList
	Stdout  bool
	Options CopyOptions
}

// CopyOptions describes options for COPY execution.
type CopyOptions struct {
	CopyFormat      CopyFormat
	Header          bool
	Freeze          bool
	OnError         CopyOnError
	OnErrorSet      bool
	RejectLimit     int32
	RejectLimitSet  bool
	LogVerbosity    CopyLogVerbosity
	LogVerbositySet bool
	Delimiter       string
	Default         string
	DefaultSet      bool
}

var _ NodeFormatter = &CopyOptions{}

// Format implements the NodeFormatter interface.
func (node *CopyFrom) Format(ctx *FmtCtx) {
	ctx.WriteString("COPY ")
	ctx.FormatNode(&node.Table)
	if len(node.Columns) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(&node.Columns)
		ctx.WriteString(")")
	}
	ctx.WriteString(" FROM ")
	if node.Program != "" {
		ctx.WriteString("PROGRAM ")
		ctx.WriteString(lex.EscapeSQLString(node.Program))
	} else if node.Stdin {
		ctx.WriteString("STDIN")
	} else {
		ctx.WriteString(lex.EscapeSQLString(node.File))
	}
	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
	if node.Options.Delimiter != "" {
		ctx.WriteString(" DELIMITER '" + node.Options.Delimiter + "'")
	}
}

func (node *CopyTo) Format(ctx *FmtCtx) {
	ctx.WriteString("COPY ")
	if node.Query != nil {
		ctx.FormatNode(node.Query)
	} else {
		ctx.FormatNode(&node.Table)
		if len(node.Columns) > 0 {
			ctx.WriteString(" (")
			ctx.FormatNode(&node.Columns)
			ctx.WriteString(")")
		}
	}
	ctx.WriteString(" TO ")
	if node.Program != "" {
		ctx.WriteString("PROGRAM ")
		ctx.WriteString(lex.EscapeSQLString(node.Program))
	} else if node.Stdout {
		ctx.WriteString("STDOUT")
	} else {
		ctx.WriteString(lex.EscapeSQLString(node.File))
	}
	if !node.Options.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
	if node.Options.Delimiter != "" {
		ctx.WriteString(" DELIMITER '" + node.Options.Delimiter + "'")
	}
}

// Format implements the NodeFormatter interface
func (o *CopyOptions) Format(ctx *FmtCtx) {
	var addSep bool
	maybeAddSep := func() {
		if addSep {
			ctx.WriteString(", ")
		}
		addSep = true
	}
	if o.CopyFormat != CopyFormatText {
		maybeAddSep()
		switch o.CopyFormat {
		case CopyFormatCsv:
			ctx.WriteString("FORMAT CSV")
		case CopyFormatBinary:
			ctx.WriteString("FORMAT BINARY")
		}
	}
	if o.Header {
		maybeAddSep()
		ctx.WriteString("HEADER")
	}
	if o.Freeze {
		maybeAddSep()
		ctx.WriteString("FREEZE")
	}
	if o.OnErrorSet {
		maybeAddSep()
		switch o.OnError {
		case CopyOnErrorIgnore:
			ctx.WriteString("ON_ERROR IGNORE")
		}
	}
	if o.RejectLimitSet {
		maybeAddSep()
		ctx.Printf("REJECT_LIMIT %d", o.RejectLimit)
	}
	if o.LogVerbositySet {
		maybeAddSep()
		switch o.LogVerbosity {
		case CopyLogVerbositySilent:
			ctx.WriteString("LOG_VERBOSITY SILENT")
		}
	}
	if o.DefaultSet {
		maybeAddSep()
		ctx.WriteString("DEFAULT '" + o.Default + "'")
	}
}

// IsDefault returns true if this struct has default value.
func (o CopyOptions) IsDefault() bool {
	return o == CopyOptions{}
}

// CombineWith merges other options into this struct. An error is returned if
// the same option merged multiple times.
func (o *CopyOptions) CombineWith(other *CopyOptions) error {
	if other.CopyFormat != CopyFormatText {
		if o.CopyFormat != CopyFormatText {
			return errors.New("format option specified multiple times")
		}
		o.CopyFormat = other.CopyFormat
	}

	if other.Header {
		if o.Header {
			return errors.New("header option specified multiple times")
		}
		o.Header = other.Header
	}

	if other.Freeze {
		if o.Freeze {
			return errors.New("freeze option specified multiple times")
		}
		o.Freeze = other.Freeze
	}

	if other.Delimiter != "" {
		if o.Delimiter != "" {
			return errors.New("delimiter option specified multiple times")
		}
		o.Delimiter = other.Delimiter
	}

	if other.DefaultSet {
		if o.DefaultSet {
			return errors.New("default option specified multiple times")
		}
		o.Default = other.Default
		o.DefaultSet = true
	}

	if other.OnErrorSet {
		if o.OnErrorSet {
			return errors.New("on_error option specified multiple times")
		}
		o.OnError = other.OnError
		o.OnErrorSet = true
	}

	if other.RejectLimitSet {
		if o.RejectLimitSet {
			return errors.New("reject_limit option specified multiple times")
		}
		o.RejectLimit = other.RejectLimit
		o.RejectLimitSet = true
	}

	if other.LogVerbositySet {
		if o.LogVerbositySet {
			return errors.New("log_verbosity option specified multiple times")
		}
		o.LogVerbosity = other.LogVerbosity
		o.LogVerbositySet = true
	}

	return nil
}

// CopyFormat identifies a COPY data format.
type CopyFormat int

// Valid values for CopyFormat.
const (
	CopyFormatText CopyFormat = iota
	CopyFormatBinary
	CopyFormatCsv
)

// CopyOnError identifies COPY FROM row error handling.
type CopyOnError int

// Valid values for CopyOnError.
const (
	CopyOnErrorStop CopyOnError = iota
	CopyOnErrorIgnore
)

// CopyLogVerbosity identifies COPY diagnostics verbosity.
type CopyLogVerbosity int

// Valid values for CopyLogVerbosity.
const (
	CopyLogVerbosityDefault CopyLogVerbosity = iota
	CopyLogVerbositySilent
)
