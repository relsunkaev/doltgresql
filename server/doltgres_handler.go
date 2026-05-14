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

package server

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime/debug"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"

	"github.com/dolthub/doltgresql/core"
	"github.com/dolthub/doltgresql/core/id"
	"github.com/dolthub/doltgresql/postgres/parser/pgcode"
	"github.com/dolthub/doltgresql/postgres/parser/pgerror"
	"github.com/dolthub/doltgresql/server/ast"
	"github.com/dolthub/doltgresql/server/auth"
	pgexprs "github.com/dolthub/doltgresql/server/expression"
	"github.com/dolthub/doltgresql/server/functions"
	"github.com/dolthub/doltgresql/server/node"
	"github.com/dolthub/doltgresql/server/settings"
	pgtransform "github.com/dolthub/doltgresql/server/transform"
	pgtypes "github.com/dolthub/doltgresql/server/types"
)

var (
	printErrorStackTraces = false
	ForceTextWireFormat   = false
)

const (
	PrintErrorStackTracesEnvKey = "DOLTGRES_PRINT_ERROR_STACK_TRACES"
	ForceTextWireFormatEnvKey   = "DOLTGRES_FORCE_TEXT_WIRE_FORMAT"
)

func init() {
	if _, ok := os.LookupEnv(PrintErrorStackTracesEnvKey); ok {
		printErrorStackTraces = true
	}
	if _, ok := os.LookupEnv(ForceTextWireFormatEnvKey); ok {
		ForceTextWireFormat = true
	}
}

// BindVariables represents arrays of types, format codes and parameters
// used to convert given parameters to binding variables map.
type BindVariables struct {
	varTypes    []uint32
	formatCodes []int16
	parameters  [][]byte
}

// Result represents a query result.
type Result struct {
	Fields       []pgproto3.FieldDescription `json:"fields"`
	Rows         []Row                       `json:"rows"`
	RowsAffected uint64                      `json:"rows_affected"`
}

// Row represents a single row value in bytes format.
// |val| represents array of a single row elements,
// which each element value is in byte array format.
type Row struct {
	val [][]byte
}

const rowsBatch = 128

// DoltgresHandler is a handler uses SQLe engine directly
// running Doltgres specific queries.
type DoltgresHandler struct {
	e                 *sqle.Engine
	sm                *server.SessionManager
	readTimeout       time.Duration
	encodeLoggedQuery bool
	pgTypeMap         *pgtype.Map
	sel               server.ServerEventListener
}

var _ Handler = &DoltgresHandler{}

// ComBind implements the Handler interface.
func (h *DoltgresHandler) ComBind(ctx context.Context, c *mysql.Conn, query string, parsedQuery mysql.ParsedQuery, bindVars BindVariables, formatCodes []int16) (mysql.BoundQuery, []pgproto3.FieldDescription, error) {
	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, query)
	if err != nil {
		return nil, nil, err
	}

	stmt, ok := parsedQuery.(sqlparser.Statement)
	if !ok {
		return nil, nil, errors.Errorf("parsedQuery must be a sqlparser.Statement, but got %T", parsedQuery)
	}

	bvs, err := h.convertBindParameters(sqlCtx, bindVars.varTypes, bindVars.formatCodes, bindVars.parameters)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("unable to convert bind params: %+v\n", err)
		}
		return nil, nil, err
	}

	queryPlan, err := h.e.BoundQueryPlan(sqlCtx, query, stmt, bvs)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("unable to bind query plan: %+v\n", err)
		}
		return nil, nil, err
	}
	fields, err := schemaToFieldDescriptionsWithSource(sqlCtx, queryPlan.Schema(sqlCtx), queryPlan, formatCodes)
	return queryPlan, fields, err
}

// ComExecuteBound implements the Handler interface.
func (h *DoltgresHandler) ComExecuteBound(ctx context.Context, conn *mysql.Conn, query string, boundQuery mysql.BoundQuery, formatCodes []int16, callback func(*sql.Context, *Result) error) error {
	return h.ComExecuteBoundWithFields(ctx, conn, query, boundQuery, formatCodes, nil, callback)
}

// ComExecuteBoundWithFields executes a bound query, reusing field descriptions that were already built during Bind.
func (h *DoltgresHandler) ComExecuteBoundWithFields(ctx context.Context, conn *mysql.Conn, query string, boundQuery mysql.BoundQuery, formatCodes []int16, resultFields []pgproto3.FieldDescription, callback func(*sql.Context, *Result) error) error {
	analyzedPlan, ok := boundQuery.(sql.Node)
	if !ok {
		return errors.Errorf("boundQuery must be a sql.Node, but got %T", boundQuery)
	}

	// TODO: This technically isn't query start and underestimates query execution time
	start := time.Now()
	if h.sel != nil {
		h.sel.QueryStarted()
	}

	err := h.doQuery(ctx, conn, query, nil, analyzedPlan, h.executeBoundPlan, callback, formatCodes, resultFields)
	if err != nil {
		err = castSQLError(err)
	}

	if h.sel != nil {
		h.sel.QueryCompleted(err == nil, time.Since(start))
	}

	return err
}

// ComPrepareParsed implements the Handler interface.
func (h *DoltgresHandler) ComPrepareParsed(ctx context.Context, c *mysql.Conn, query string, parsed sqlparser.Statement) (mysql.ParsedQuery, []pgproto3.FieldDescription, error) {
	sqlCtx, ok := ctx.(*sql.Context)
	if !ok {
		var err error
		sqlCtx, err = h.sm.NewContextWithQuery(ctx, c, query)
		if err != nil {
			return nil, nil, err
		}
	}

	node, err := h.e.PrepareParsedQuery(sqlCtx, query, query, parsed)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("unable to prepare query: %+v\n", err)
		}
		logrus.WithField("query", query).Errorf("unable to prepare query: %s", err.Error())
		return nil, nil, castSQLError(err)
	}
	analyzed := node
	// We do not analyze expressions with bind variables, since that step comes later and analysis will return invalid results
	hasBindVars := false
	pgtransform.InspectNodeExprs(sqlCtx, node, func(sqlCtx *sql.Context, expr sql.Expression) bool {
		if _, ok := expr.(*expression.BindVar); ok {
			hasBindVars = true
			return true
		}
		return false
	})
	if !hasBindVars {
		analyzed, err = h.e.Analyzer.Analyze(sqlCtx, node, nil, nil)
		if err != nil {
			if printErrorStackTraces {
				fmt.Printf("unable to prepare query: %+v\n", err)
			}
			logrus.WithField("query", query).Errorf("unable to prepare query: %s", err.Error())
			return nil, nil, castSQLError(err)
		}
	}

	var fields []pgproto3.FieldDescription
	// The query is not a SELECT statement if it corresponds to an OK result.
	if nodeReturnsOkResultSchema(sqlCtx, analyzed) {
		fields = []pgproto3.FieldDescription{
			{
				Name:         []byte("Rows"),
				DataTypeOID:  id.Cache().ToOID(pgtypes.Int32.ID.AsId()),
				DataTypeSize: int16(pgtypes.Int32.MaxTextResponseByteLength(nil)),
			},
		}
	} else {
		fields, err = schemaToFieldDescriptionsWithSource(sqlCtx, analyzed.Schema(sqlCtx), analyzed, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	return analyzed, fields, nil
}

// ComQuery implements the Handler interface.
func (h *DoltgresHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, parsed sqlparser.Statement, callback func(*sql.Context, *Result) error) error {
	// TODO: This technically isn't query start and underestimates query execution time
	start := time.Now()
	if h.sel != nil {
		h.sel.QueryStarted()
	}

	err := h.doQuery(ctx, c, query, parsed, nil, h.executeQuery, callback, nil, nil)
	if err != nil {
		err = castSQLError(err)
	}

	if h.sel != nil {
		h.sel.QueryCompleted(err == nil, time.Since(start))
	}

	return err
}

func castSQLError(err error) error {
	switch pgerror.GetPGCode(err) {
	case pgcode.DeadlockDetected,
		pgcode.CheckViolation,
		pgcode.DatatypeMismatch,
		pgcode.DependentObjectsStillExist,
		pgcode.DuplicatePreparedStatement,
		pgcode.DuplicateObject,
		pgcode.FeatureNotSupported,
		pgcode.Grouping,
		pgcode.InvalidForeignKey,
		pgcode.InvalidColumnReference,
		pgcode.InvalidObjectDefinition,
		pgcode.InvalidTableDefinition,
		pgcode.InvalidParameterValue,
		pgcode.InvalidTextRepresentation,
		pgcode.InsufficientPrivilege,
		pgcode.LockNotAvailable,
		pgcode.ObjectNotInPrerequisiteState,
		pgcode.ProgramLimitExceeded,
		pgcode.RaiseException,
		pgcode.Syntax,
		pgcode.UniqueViolation,
		pgcode.UndefinedColumn,
		pgcode.UndefinedFunction,
		pgcode.UndefinedObject,
		pgcode.UndefinedPreparedStatement,
		pgcode.Windowing,
		pgcode.WrongObjectType:
		return err
	default:
		return sql.CastSQLError(err)
	}
}

// ComResetConnection implements the Handler interface.
func (h *DoltgresHandler) ComResetConnection(c *mysql.Conn) error {
	logrus.WithField("connectionId", c.ConnectionID).Debug("COM_RESET_CONNECTION command received")

	// Grab the currently selected database name
	db := h.sm.GetCurrentDB(c)

	// Dispose of the connection's current session
	h.maybeReleaseAllLocks(c)
	h.e.CloseSession(c.ConnectionID)

	ctx := context.Background()

	// Create a new session and set the current database
	err := h.sm.NewSession(ctx, c)
	if err != nil {
		return err
	}
	return h.sm.SetDB(ctx, c, db)
}

// ConnectionClosed implements the Handler interface.
func (h *DoltgresHandler) ConnectionClosed(c *mysql.Conn) {
	defer func() {
		if h.sel != nil {
			h.sel.ClientDisconnected()
		}
	}()

	defer h.sm.RemoveConn(c)
	defer h.e.CloseSession(c.ConnectionID)

	h.maybeReleaseAllLocks(c)

	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).Infof("ConnectionClosed")
}

// NewConnection implements the Handler interface.
func (h *DoltgresHandler) NewConnection(c *mysql.Conn) {
	if h.sel != nil {
		h.sel.ClientConnected()
	}

	h.sm.AddConn(c)
	sql.StatusVariables.IncrementGlobal("Connections", 1)

	c.DisableClientMultiStatements = true // TODO: h.disableMultiStmts
	logrus.WithField(sql.ConnectionIdLogField, c.ConnectionID).WithField("DisableClientMultiStatements", c.DisableClientMultiStatements).Infof("NewConnection")
}

// NewContext implements the Handler interface.
func (h *DoltgresHandler) NewContext(ctx context.Context, c *mysql.Conn, query string) (*sql.Context, error) {
	return h.sm.NewContextWithQuery(ctx, c, query)
}

// InitSessionParameterDefault sets a default value to specified parameter for a session.
func (h *DoltgresHandler) InitSessionParameterDefault(ctx context.Context, c *mysql.Conn, name, value string) error {
	return h.sm.InitSessionDefaultVariable(ctx, c, name, value)
}

// convertBindParameters handles the conversion from bind parameters to variable values.
func (h *DoltgresHandler) convertBindParameters(ctx *sql.Context, types []uint32, formatCodes []int16, values [][]byte) (map[string]sqlparser.Expr, error) {
	if len(values) == 0 {
		return nil, nil
	}
	bindings := make(map[string]sqlparser.Expr, len(values))
	// It's valid to send just one format code that should be used by all values, so we extend the slice in that case
	formatCodes, err := extendFormatCodes(len(values), formatCodes)
	if err != nil {
		return nil, err
	}
	typeColl, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	for i := range values {
		formatCode := formatCodes[i]
		dgType := pgtypes.Unknown
		if i < len(types) && types[i] != 0 {
			dgType, err = typeColl.GetType(ctx, id.Type(id.Cache().ToInternal(types[i])))
			if err != nil {
				return nil, err
			}
			if dgType == nil {
				dgType = pgtypes.Unknown
			}
		}
		if values[i] != nil {
			if formatCode == 0 {
				v, err := dgType.IoInput(ctx, string(values[i]))
				if err != nil {
					return nil, err
				}
				bindings[bindVariableName(i)] = sqlparser.InjectedExpr{Expression: pgexprs.NewUnsafeLiteral(v, dgType)}
			} else {
				v, err := receiveBindParameter(ctx, dgType, values[i])
				if err != nil {
					return nil, err
				}
				bindings[bindVariableName(i)] = sqlparser.InjectedExpr{Expression: pgexprs.NewUnsafeLiteral(v, dgType)}
			}
		} else {
			bindings[bindVariableName(i)] = sqlparser.InjectedExpr{Expression: pgexprs.NewUnsafeLiteral(nil, dgType)}
		}
	}
	return bindings, nil
}

func receiveBindParameter(ctx *sql.Context, dgType *pgtypes.DoltgresType, value []byte) (any, error) {
	// psycopg3 can send compact binary integer payloads for small Python
	// ints even when the server resolves an untyped placeholder to int4
	// or int8 through an explicit SQL cast. Widen those payloads before
	// constructing the typed literal so direct psycopg parameters match
	// PostgreSQL's app-driver behavior.
	switch dgType.ID.TypeName() {
	case "unknown":
		return unknownBinaryBindLiteral(value), nil
	case "int4":
		if len(value) == 2 {
			return int32(int16(binary.BigEndian.Uint16(value))), nil
		}
	case "int8":
		switch len(value) {
		case 2:
			return int64(int16(binary.BigEndian.Uint16(value))), nil
		case 4:
			return int64(int32(binary.BigEndian.Uint32(value))), nil
		}
	}
	return dgType.CallReceive(ctx, value)
}

func unknownBinaryBindLiteral(value []byte) string {
	// Untyped binary parameters still flow through later SQL context, such as
	// explicit casts or function resolution. Preserve text as-is, and decode the
	// compact scalar payloads common PostgreSQL drivers send for typed Python
	// values so that downstream casts receive a PostgreSQL-readable literal.
	if printableUTF8(value) {
		return string(value)
	}
	switch len(value) {
	case 1:
		switch value[0] {
		case 0:
			return "false"
		case 1:
			return "true"
		default:
			return string(value)
		}
	case 2:
		return strconv.FormatInt(int64(int16(binary.BigEndian.Uint16(value))), 10)
	case 4:
		return strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(value))), 10)
	case 8:
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(value)), 10)
	default:
		return string(value)
	}
}

func printableUTF8(value []byte) bool {
	if !utf8.Valid(value) {
		return false
	}
	for _, r := range string(value) {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func bindVariableName(index int) string {
	return "v" + strconv.Itoa(index+1)
}

var queryLoggingRegex = regexp.MustCompile(`[\r\n\t ]+`)

func (h *DoltgresHandler) doQuery(ctx context.Context, c *mysql.Conn, query string, parsed sqlparser.Statement, analyzedPlan sql.Node, queryExec QueryExecutor, callback func(*sql.Context, *Result) error, formatCodes []int16, suppliedResultFields []pgproto3.FieldDescription) error {
	sqlCtx, err := h.sm.NewContextWithQuery(ctx, c, query)
	if err != nil {
		return err
	}
	sqlCtx.SetPrivilegeSet(auth.NewPrivilegeSetLayer(sqlCtx), 1)

	start := time.Now()
	var queryStrToLog string
	if h.encodeLoggedQuery {
		queryStrToLog = base64.StdEncoding.EncodeToString([]byte(query))
	} else if logrus.IsLevelEnabled(logrus.DebugLevel) {
		// this is expensive, so skip this unless we're logging at DEBUG level
		queryStrToLog = string(queryLoggingRegex.ReplaceAll([]byte(query), []byte(" ")))
	}

	if queryStrToLog != "" {
		sqlCtx.SetLogger(sqlCtx.GetLogger().WithField("query", queryStrToLog))
	}
	sqlCtx.GetLogger().Debugf("Starting query")
	sqlCtx.GetLogger().Tracef("beginning execution")

	// TODO: it would be nice to put this logic in the engine, not the handler, but we don't want the process to be
	//  marked done until we're done spooling rows over the wire
	lgr := sqlCtx.GetLogger()
	sqlCtx, err = sqlCtx.ProcessList.BeginQuery(sqlCtx, query)
	if err != nil {
		lgr.WithError(err).Warn("error running query; could not open process list context")
		return err
	}
	defer sqlCtx.ProcessList.EndQuery(sqlCtx)

	schema, rowIter, qFlags, err := queryExec(sqlCtx, query, parsed, analyzedPlan)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("error running query: %+v\n", err)
		}
		sqlCtx.GetLogger().WithError(err).Warn("error running query")
		return err
	}

	// create result before goroutines to avoid |ctx| racing
	var r *Result
	var processedAtLeastOneBatch bool

	// zero/single return schema use spooling shortcut
	if types.IsOkResultSchema(schema) {
		r, err = resultForOkIter(sqlCtx, rowIter)
		if err != nil {
			return err
		}
	} else if schema == nil {
		r, err = resultForEmptyIter(sqlCtx, rowIter)
		if err != nil {
			return err
		}
	} else if analyzer.FlagIsSet(qFlags, sql.QFlagMax1Row) {
		resultFields, err := executionResultFields(sqlCtx, schema, analyzedPlan, formatCodes, suppliedResultFields)
		if err != nil {
			return err
		}
		r, err = resultForMax1RowIter(sqlCtx, schema, rowIter, resultFields, formatCodes)
		if err != nil {
			return err
		}
	} else {
		resultFields, err := executionResultFields(sqlCtx, schema, analyzedPlan, formatCodes, suppliedResultFields)
		if err != nil {
			return err
		}
		r, processedAtLeastOneBatch, err = h.resultForDefaultIter(sqlCtx, schema, rowIter, callback, resultFields, formatCodes)
		if err != nil {
			return err
		}
	}

	sqlCtx.GetLogger().Debugf("Query finished in %d ms", time.Since(start).Milliseconds())

	// processedAtLeastOneBatch means we already called callback() at least
	// once, so no need to call it if RowsAffected == 0.
	if r != nil && (r.RowsAffected == 0 && processedAtLeastOneBatch) {
		return nil
	}

	return callback(sqlCtx, r)
}

func executionResultFields(ctx *sql.Context, schema sql.Schema, analyzedPlan sql.Node, formatCodes []int16, suppliedFields []pgproto3.FieldDescription) ([]pgproto3.FieldDescription, error) {
	if suppliedFields != nil && len(suppliedFields) == len(schema) {
		return suppliedFields, nil
	}
	return schemaToFieldDescriptionsWithSource(ctx, schema, analyzedPlan, formatCodes)
}

func executionFormatCodes(fieldLength int, formatCodes []int16) ([]int16, error) {
	if ForceTextWireFormat || len(formatCodes) == 0 {
		return nil, nil
	}
	expandedFormatCodes, err := extendFormatCodes(fieldLength, formatCodes)
	if err != nil {
		return nil, err
	}
	for _, formatCode := range expandedFormatCodes {
		if formatCode != 0 {
			return expandedFormatCodes, nil
		}
	}
	return nil, nil
}

// QueryExecutor is a function that executes a query and returns the result as a schema and iterator. Either of
// |parsed| or |analyzed| can be nil depending on the use case
type QueryExecutor func(ctx *sql.Context, query string, parsed sqlparser.Statement, analyzed sql.Node) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

// executeQuery is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *DoltgresHandler) executeQuery(ctx *sql.Context, query string, parsed sqlparser.Statement, _ sql.Node) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return h.e.QueryWithBindings(ctx, query, parsed, nil, nil)
}

// executeBoundPlan is a QueryExecutor that calls QueryWithBindings on the given engine using the given query and parsed
// statement, which may be nil.
func (h *DoltgresHandler) executeBoundPlan(ctx *sql.Context, query string, _ sqlparser.Statement, plan sql.Node) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return h.e.PrepQueryPlanForExecution(ctx, query, plan, nil)
}

// maybeReleaseAllLocks makes a best effort attempt to release all locks on the given connection. If the attempt fails,
// an error is logged but not returned.
func (h *DoltgresHandler) maybeReleaseAllLocks(c *mysql.Conn) {
	if ctx, err := h.sm.NewContextWithQuery(context.Background(), c, ""); err != nil {
		logrus.Errorf("unable to release all locks on session close: %s", err)
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	} else {
		// Drain any outstanding transaction-scope advisory locks before
		// the session-wide ReleaseAll: we want the in-memory tracker to
		// stay in sync with the LockSubsystem so it doesn't try to
		// re-release them later for a reused connection id. Also
		// discard any uncommitted SET LOCAL snapshots tracked on the
		// session, so the next session reusing the connection id
		// doesn't see stale rollback state.
		_ = functions.ReleaseSessionXactLocksWithSubsystem(ctx, h.e.LS)
		node.ReleaseSessionRowLocks(c.ConnectionID)
		node.ReleaseSessionRelationLocks(c.ConnectionID)
		_ = functions.ReleaseSessionXactVars(ctx)
		functions.ReleaseSessionSharedAdvisoryLocks(c.ConnectionID)
		_, err = h.e.LS.ReleaseAll(ctx)
		if err != nil {
			logrus.Errorf("unable to release all locks on session close: %s", err)
		}
		if err = h.e.Analyzer.Catalog.UnlockTables(ctx, c.ConnectionID); err != nil {
			logrus.Errorf("unable to unlock tables on session close: %s", err)
		}
	}
}

// nodeReturnsOkResultSchema returns whether the node returns OK result or the schema is OK result schema.
// These nodes will eventually return an OK result, but their intermediate forms here return a different schema
// than they will at execution time.
func nodeReturnsOkResultSchema(ctx *sql.Context, node sql.Node) bool {
	switch n := node.(type) {
	case *plan.InsertInto:
		return len(n.Returning) == 0
	case *plan.Update:
		return len(n.Returning) == 0
	case *plan.DeleteFrom, *plan.UpdateJoin:
		return true
	}
	return types.IsOkResultSchema(node.Schema(ctx))
}

// extendFormatCodes ensures that the given format codes match the expected field length by extending the short-form
// variant (or returning the full-form as-is).
func extendFormatCodes(fieldLength int, formatCodes []int16) ([]int16, error) {
	if !ForceTextWireFormat && len(formatCodes) > 0 {
		// It's valid to send just one format code that should be used by all values, so we extend the slice in that case
		if len(formatCodes) < fieldLength {
			if len(formatCodes) > 1 {
				return nil, errors.Errorf(`format codes have length "%d" but fields have length "%d"`, len(formatCodes), fieldLength)
			}
			newFormatCodes := make([]int16, fieldLength)
			for i := range newFormatCodes {
				newFormatCodes[i] = formatCodes[0]
			}
			return newFormatCodes, nil
		} else {
			return formatCodes, nil
		}
	} else {
		// This defaults to zero, which means all format codes represent the "text" option
		return make([]int16, fieldLength), nil
	}
}

func schemaToFieldDescriptions(ctx *sql.Context, s sql.Schema, formatCodes []int16) ([]pgproto3.FieldDescription, error) {
	return schemaToFieldDescriptionsWithSource(ctx, s, nil, formatCodes)
}

// schemaToFieldDescriptionsWithSource is the primary entry point for
// building wire-protocol field descriptions. The optional sourceNode
// parameter lets the caller pass the resolved query plan so result
// columns whose schema entry has lost its Source through a project
// alias (`SELECT col AS x FROM t`) can still be traced back to the
// base table for editor-friendly RowDescription metadata.
func schemaToFieldDescriptionsWithSource(ctx *sql.Context, s sql.Schema, sourceNode sql.Node, formatCodes []int16) ([]pgproto3.FieldDescription, error) {
	textFormatOnly := ForceTextWireFormat || len(formatCodes) == 0
	if !textFormatOnly {
		var err error
		formatCodes, err = extendFormatCodes(len(s), formatCodes)
		if err != nil {
			return nil, err
		}
	}

	// sourceSchemaCache memoizes the (table-name -> resolved metadata)
	// lookup across the columns of a single result so SELECT * never
	// walks search_path more than once per source table. Each entry
	// carries the table OID and the attnum-by-column-name map so a
	// reordered SELECT (`SELECT col2, col1 FROM t`) can report each
	// column's true source-table attnum instead of its result-set
	// position.
	sourceSchemaCache := make(map[string]*sourceTableMeta)
	// aliasHints pulls back source attribution that AliasedExpr would
	// otherwise strip. nil entries fall through to c.Source (the
	// unaliased path).
	aliasHints := extractAliasSourceHints(sourceNode, len(s))
	projectionTypes := extractProjectionTypes(ctx, sourceNode, len(s))

	fields := make([]pgproto3.FieldDescription, len(s))
	for i, c := range s {
		var oid uint32
		var typmod = int32(-1)

		var err error
		colName := c.Name
		// Remap the synthetic alias the AST layer mints for unaliased
		// PostgreSQL expressions (`?column?`, `case`) back to the
		// user-visible name. The unique alias is only an internal
		// identifier required so GMS's analyzer keeps each anonymous
		// projection distinct.
		if display, ok := ast.AnonColumnAliasDisplayName(colName); ok {
			colName = display
		}
		colName = core.DecodePhysicalColumnName(colName)
		columnType := c.Type
		if i < len(projectionTypes) && projectionTypes[i] != nil {
			columnType = projectionTypes[i]
		}
		dataTypeSize := int16(columnType.MaxTextResponseByteLength(ctx))
		tableAttributeNumber := uint16(i + 1) // TODO: this should be based on the actual table field index, not the return schema
		if doltgresType, ok := columnType.(*pgtypes.DoltgresType); ok {
			if doltgresType.ID == pgtypes.Unknown.ID {
				// It appears that the `unknown` type is always converted to `text` on output since they're binary
				// coercible. There are other assumptions that we can make as well, as no function or column will return
				// the `unknown` type, so we can infer that this is a raw value being returned as-is from the query,
				// such as `SELECT 'foo';`
				doltgresType = pgtypes.Text
				dataTypeSize = int16(doltgresType.MaxTextResponseByteLength(ctx))
				// PostgreSQL labels an unaliased unknown-typed column as
				// "?column?" but honors any explicit alias the user
				// provided (e.g. `SELECT 'foo' AS x` produces column
				// name "x", and `SELECT CASE WHEN ... END AS type`
				// produces "type"). The previous unconditional override
				// here was breaking client tooling — drizzle-kit reads
				// the alias by name when introspecting pg_class.
				if colName == "" {
					colName = "?column?"
				}
				tableAttributeNumber = 0
			}
			if doltgresType.TypType == pgtypes.TypeType_Domain {
				doltgresType, err = doltgresType.DomainUnderlyingBaseTypeWithContext(ctx)
				if err != nil {
					return nil, err
				}
			}
			oid = id.Cache().ToOID(doltgresType.ID.AsId())
			typmod = doltgresType.GetAttTypMod() // pg_attribute.atttypmod
		} else {
			doltgresType, convErr := pgtypes.FromGmsTypeToDoltgresType(columnType)
			if convErr == nil && doltgresType.ID == pgtypes.Unknown.ID {
				// GMS represents an untyped NULL projection as its own null
				// type rather than Doltgres Unknown. PostgreSQL reports such
				// result columns as text, so keep Parse and Bind RowDescription
				// metadata stable for prepared client reflection queries.
				doltgresType = pgtypes.Text
				oid = id.Cache().ToOID(doltgresType.ID.AsId())
				typmod = doltgresType.GetAttTypMod()
				dataTypeSize = int16(doltgresType.MaxTextResponseByteLength(ctx))
				if colName == "" {
					colName = "?column?"
				}
				tableAttributeNumber = 0
			} else {
				oid, err = VitessTypeToObjectID(columnType)
			}
			if err != nil {
				panic(err)
			}
		}

		// Prefer the column's declared Source; if the projection
		// stripped it (aliased base column), fall back to a hint
		// recovered from the plan.
		sourceTable := c.Source
		sourceColumn := c.Name
		if sourceTable == "" && i < len(aliasHints) && aliasHints[i] != nil {
			sourceTable = aliasHints[i].table
			sourceColumn = aliasHints[i].column
		}
		var tableOID uint32
		if sourceTable != "" {
			meta := lookupSourceTableMeta(ctx, sourceTable, c.DatabaseSource, sourceSchemaCache)
			if meta != nil {
				tableOID = meta.tableOID
				if attnum, ok := meta.attnumOf(sourceColumn); ok {
					tableAttributeNumber = attnum
				}
			}
		}
		formatCode := int16(0)
		if !textFormatOnly {
			formatCode = formatCodes[i]
		}

		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(colName),
			TableOID:             tableOID,
			TableAttributeNumber: tableAttributeNumber,
			DataTypeOID:          oid,
			DataTypeSize:         dataTypeSize,
			TypeModifier:         typmod,
			Format:               formatCode,
		}
	}

	return fields, nil
}

func extractProjectionTypes(ctx *sql.Context, node sql.Node, columnCount int) []sql.Type {
	if node == nil || columnCount == 0 {
		return nil
	}
	project := findFirstProject(node)
	if project == nil || len(project.Projections) != columnCount {
		return nil
	}
	types := make([]sql.Type, columnCount)
	for i, expr := range project.Projections {
		types[i] = expr.Type(ctx)
	}
	return types
}

// aliasSourceHint records the (sourceTable, sourceColumn) pair we can
// recover for a result column whose schema entry has lost Source
// through plan.Project's AliasedExpr unwrap.
type aliasSourceHint struct {
	table  string
	column string
}

// extractAliasSourceHints walks the result-producing plan node and
// returns one hint per output column. A nil entry means we could not
// confidently identify a single source column (the projection is a
// computed expression, a function call, etc.) so the caller should
// fall through to c.Source.
//
// PG GUI editors and migration tools want to map every result column
// back to a base column whenever possible. The most common case where
// GMS strips the schema's Source is a project alias
// (`SELECT col AS x FROM t`); plan.Project's expression list still
// holds the original GetField, so unwrapping the AliasedExpr recovers
// the source attribution the schema lost. A second case is a
// table-aliased FROM (`SELECT a.col FROM t a`) whose GetField names
// the alias instead of the real table — for that we walk the FROM
// tree and resolve the alias back to its underlying ResolvedTable.
func extractAliasSourceHints(node sql.Node, columnCount int) []*aliasSourceHint {
	if node == nil || columnCount == 0 {
		return nil
	}
	project := findFirstProject(node)
	if project == nil {
		return nil
	}
	exprs := project.Projections
	if len(exprs) != columnCount {
		return nil
	}
	aliasMap := buildTableAliasMap(project.Child)
	hints := make([]*aliasSourceHint, columnCount)
	for i, expr := range exprs {
		hints[i] = aliasHintFromExpr(expr, aliasMap)
	}
	return hints
}

// buildTableAliasMap walks the FROM-side of the plan and returns a
// case-insensitive map of (alias name -> underlying table name) for
// every plan.TableAlias whose child resolves to a real table. The
// map lets aliasHintFromExpr translate `a.col` (where `a` is a FROM
// alias) into the catalog table name needed by the source-meta
// lookup.
func buildTableAliasMap(node sql.Node) map[string]string {
	if node == nil {
		return nil
	}
	aliases := map[string]string{}
	transform.Inspect(node, func(n sql.Node) bool {
		ta, ok := n.(*plan.TableAlias)
		if !ok {
			return true
		}
		if name := underlyingTableName(ta.Child); name != "" {
			aliases[strings.ToLower(ta.Name())] = name
		}
		return true
	})
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

// underlyingTableName returns the catalog table name backing a
// FROM-side plan node, or "" when the source is not a single base
// table (subqueries, set ops, etc.).
func underlyingTableName(node sql.Node) string {
	for node != nil {
		switch n := node.(type) {
		case *plan.ResolvedTable:
			if n.Table != nil {
				return n.Table.Name()
			}
			return ""
		case *plan.UnresolvedTable:
			return n.Name()
		}
		children := node.Children()
		if len(children) != 1 {
			return ""
		}
		node = children[0]
	}
	return ""
}

// findFirstProject returns the closest plan.Project under node along
// the result-producing axis (a sequence of single-child wrappers).
// Returns nil if no qualifying Project is found.
func findFirstProject(node sql.Node) *plan.Project {
	current := node
	for current != nil {
		if project, ok := current.(*plan.Project); ok {
			return project
		}
		children := current.Children()
		if len(children) != 1 {
			return nil
		}
		current = children[0]
	}
	return nil
}

// aliasHintFromExpr unwraps the projection expression and, when it
// resolves to a single GetField, returns the source attribution.
// aliasMap is consulted to translate FROM-side table aliases (the
// ones GetField records) into the catalog table names the editor
// needs to look up pg_class / pg_attribute.
func aliasHintFromExpr(expr sql.Expression, aliasMap map[string]string) *aliasSourceHint {
	for expr != nil {
		switch e := expr.(type) {
		case *expression.Alias:
			expr = e.Child
		case *expression.GetField:
			if e.Table() == "" {
				return nil
			}
			tableName := e.Table()
			if real, ok := aliasMap[strings.ToLower(tableName)]; ok {
				tableName = real
			}
			return &aliasSourceHint{table: tableName, column: e.Name()}
		default:
			return nil
		}
	}
	return nil
}

// sourceTableMeta caches the wire-protocol metadata Doltgres has to
// emit per result column whose schema entry advertises a Source: the
// table OID (so GUI editors can resolve the column back to a base
// table) and the attnum-by-column-name map (so reordered SELECT
// projections still report each column's true source-table attnum
// instead of its position in the result set).
type sourceTableMeta struct {
	tableOID uint32
	attnums  map[string]uint16
}

func (m *sourceTableMeta) attnumOf(columnName string) (uint16, bool) {
	if m == nil || m.attnums == nil {
		return 0, false
	}
	if attnum, ok := m.attnums[strings.ToLower(columnName)]; ok {
		return attnum, true
	}
	return 0, false
}

// lookupSourceTableMeta returns the cached (or freshly resolved)
// table-OID + attnum metadata for source. Resolution walks the
// session search_path (settings.GetCurrentSchemas), probes each
// schema for the named table via the GMS provider, and on first
// match builds an attnum map from the resolved table's schema.
//
// GUI editors (TablePlus, DataGrip, DBeaver, pgAdmin) and ORM
// migration tools (Drizzle Kit, Prisma db pull) inspect both
// RowDescription.TableOID and TableAttributeNumber to map result
// columns back to pg_class / pg_attribute. Without these populated
// they refuse to offer cell edits with errors like "could not
// resolve table name." Cached per-call so SELECT * pays the
// search-path walk once per distinct source table.
func lookupSourceTableMeta(ctx *sql.Context, source, databaseSource string, cache map[string]*sourceTableMeta) *sourceTableMeta {
	if cached, ok := cache[source]; ok {
		return cached
	}
	meta := resolveSourceTableMeta(ctx, source, databaseSource)
	cache[source] = meta
	return meta
}

func resolveSourceTableMeta(ctx *sql.Context, source, databaseSource string) *sourceTableMeta {
	if source == "" {
		return nil
	}
	doltSession, ok := ctx.Session.(*dsess.DoltSession)
	if !ok {
		return nil
	}
	dbName := databaseSource
	if dbName == "" {
		dbName = ctx.GetCurrentDatabase()
	}
	database, err := doltSession.Provider().Database(ctx, dbName)
	if err != nil {
		return nil
	}
	schemaDB, ok := database.(sql.SchemaDatabase)
	if !ok {
		return nil
	}
	searchPath, err := settings.GetCurrentSchemas(ctx)
	if err != nil || len(searchPath) == 0 {
		searchPath = []string{"public"}
	}
	for _, schemaName := range searchPath {
		schema, ok, err := schemaDB.GetSchema(ctx, schemaName)
		if err != nil || !ok {
			continue
		}
		table, found, err := schema.GetTableInsensitive(ctx, source)
		if err != nil || !found {
			continue
		}
		tableSchema := table.Schema(ctx)
		attnums := make(map[string]uint16, len(tableSchema))
		for i, col := range tableSchema {
			// PostgreSQL attnums start at 1; Doltgres has no
			// system columns to skip so the offset matches the
			// positional index directly.
			attnums[strings.ToLower(col.Name)] = uint16(i + 1)
		}
		return &sourceTableMeta{
			tableOID: id.Cache().ToOID(id.NewTable(schema.SchemaName(), source).AsId()),
			attnums:  attnums,
		}
	}
	return nil
}

// resultForOkIter reads a maximum of one result row from a result iterator.
func resultForOkIter(ctx *sql.Context, iter sql.RowIter) (result *Result, err error) {
	defer trace.StartRegion(ctx, "DoltgresHandler.resultForOkIter").End()
	defer func() {
		err = closeResultIter(ctx, iter, err)
	}()

	row, err := iter.Next(ctx)
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("row: %+v\n", err)
		}
		return nil, err
	}
	_, err = iter.Next(ctx)
	if err == nil {
		return nil, errors.Errorf("result schema iterator returned more than one row")
	} else if err != io.EOF {
		return nil, err
	}

	return &Result{
		RowsAffected: row[0].(types.OkResult).RowsAffected,
	}, nil
}

// resultForEmptyIter ensures that an expected empty iterator returns no rows.
func resultForEmptyIter(ctx *sql.Context, iter sql.RowIter) (result *Result, err error) {
	defer trace.StartRegion(ctx, "DoltgresHandler.resultForEmptyIter").End()
	defer func() {
		err = closeResultIter(ctx, iter, err)
	}()
	if _, err := iter.Next(ctx); err == nil {
		return nil, errors.Errorf("result schema iterator returned more than zero rows")
	} else if err != io.EOF {
		return nil, err
	}
	return &Result{Fields: nil}, nil
}

// resultForMax1RowIter ensures that an empty iterator returns at most one row
func resultForMax1RowIter(ctx *sql.Context, schema sql.Schema, iter sql.RowIter, resultFields []pgproto3.FieldDescription, formatCodes []int16) (result *Result, err error) {
	defer trace.StartRegion(ctx, "DoltgresHandler.resultForMax1RowIter").End()
	defer func() {
		err = closeResultIter(ctx, iter, err)
	}()
	row, err := iter.Next(ctx)
	if err == io.EOF {
		return &Result{Fields: resultFields}, nil
	} else if err != nil {
		return nil, err
	}

	if _, err = iter.Next(ctx); err != io.EOF {
		if err == nil {
			return nil, errors.Errorf("result max1Row iterator returned more than one row")
		}
		return nil, err
	}

	outputRow, err := rowToBytes(ctx, schema, row, formatCodes, resultFields)
	if err != nil {
		return nil, err
	}

	ctx.GetLogger().Tracef("spooling result row %s", outputRow)

	return &Result{Fields: resultFields, Rows: []Row{{outputRow}}, RowsAffected: 1}, nil
}

func closeResultIter(ctx *sql.Context, iter sql.RowIter, err error) error {
	closeErr := iter.Close(ctx)
	if closeErr == nil {
		return err
	}
	if err == nil {
		return closeErr
	}
	if closeErr.Error() == err.Error() || strings.Contains(closeErr.Error(), err.Error()) {
		return err
	}
	return fmt.Errorf("%w; close result iterator failed: %v", err, closeErr)
}

// resultForDefaultIter reads batches of rows from the iterator
// and writes results into the callback function.
func (h *DoltgresHandler) resultForDefaultIter(ctx *sql.Context, schema sql.Schema, iter sql.RowIter, callback func(*sql.Context, *Result) error, resultFields []pgproto3.FieldDescription, formatCodes []int16) (*Result, bool, error) {
	defer trace.StartRegion(ctx, "DoltgresHandler.resultForDefaultIter").End()

	var r *Result
	var processedAtLeastOneBatch bool

	eg, ctx := ctx.NewErrgroup()

	var rowChan = make(chan sql.Row, 512)

	pan2err := func(err *error) {
		if HandlePanics {
			if recoveredPanic := recover(); recoveredPanic != nil {
				if err == nil {
					*err = errors.Errorf("DoltgresHandler caught panic with nil error: %v: %s", recoveredPanic, debug.Stack())
				} else {
					// debug.Stack() here prints the stack trace of the original panic, not the lexical stack of this defer function
					*err = goerrors.Join(*err, errors.Errorf("DoltgresHandler caught panic: %v: %s", recoveredPanic, debug.Stack()))
				}
			}
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	// Read rows off the row iterator and send them to the row channel.
	eg.Go(func() (err error) {
		defer pan2err(&err)
		defer wg.Done()
		defer close(rowChan)
		for {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			default:
				row, err := iter.Next(ctx)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				select {
				case rowChan <- row:
				case <-ctx.Done():
					return nil
				}
			}
		}
	})

	// Default waitTime is one minute if there is no timeout configured, in which case
	// it will loop to iterate again unless the socket died by the OS timeout or other problems.
	// If there is a timeout, it will be enforced to ensure that Vitess has a chance to
	// call DoltgresHandler.CloseConnection()
	waitTime := 1 * time.Minute
	if h.readTimeout > 0 {
		waitTime = h.readTimeout
	}
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	// reads rows from the channel, converts them to wire format,
	// and calls |callback| to give them to vitess.
	eg.Go(func() (err error) {
		defer pan2err(&err)
		defer wg.Done()
		for {
			if r == nil {
				r = &Result{Fields: resultFields}
			}
			if r.RowsAffected == rowsBatch {
				if err := callback(ctx, r); err != nil {
					return err
				}
				r = nil
				processedAtLeastOneBatch = true
				continue
			}

			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case row, ok := <-rowChan:
				if !ok {
					return nil
				}
				if types.IsOkResult(row) {
					if len(r.Rows) > 0 {
						panic("Got OkResult mixed with RowResult")
					}
					result := row[0].(types.OkResult)
					r = &Result{
						RowsAffected: result.RowsAffected,
					}
					continue
				}

				outputRow, err := rowToBytes(ctx, schema, row, formatCodes, resultFields)
				if err != nil {
					return err
				}

				ctx.GetLogger().Tracef("spooling result row %s", outputRow)
				r.Rows = append(r.Rows, Row{outputRow})
				r.RowsAffected++
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				if h.readTimeout != 0 {
					// Cancel and return so Vitess can call the CloseConnection callback
					ctx.GetLogger().Tracef("connection timeout")
					return errors.Errorf("row read wait bigger than connection timeout")
				}
			}
			timer.Reset(waitTime)
		}
	})

	// Close() kills this PID in the process list,
	// wait until all rows have be sent over the wire
	eg.Go(func() (err error) {
		defer pan2err(&err)
		wg.Wait()
		return iter.Close(ctx)
	})

	err := eg.Wait()
	if err != nil {
		if printErrorStackTraces {
			fmt.Printf("error running query: %+v\n", err)
		}
		ctx.GetLogger().WithError(err).Warn("error running query")
		return nil, false, err
	}

	return r, processedAtLeastOneBatch, nil
}

func rowToBytes(ctx *sql.Context, s sql.Schema, row sql.Row, formatCodes []int16, resultFields []pgproto3.FieldDescription) ([][]byte, error) {
	if len(row) == 0 {
		return nil, nil
	}
	if len(s) == 0 {
		// should not happen
		return nil, errors.Errorf("received empty schema")
	}
	textFormatOnly := ForceTextWireFormat || len(formatCodes) == 0
	var err error
	if !textFormatOnly {
		formatCodes, err = extendFormatCodes(len(row), formatCodes)
		if err != nil {
			return nil, err
		}
	}
	o := make([][]byte, len(row))
	for i, v := range row {
		if v == nil {
			o[i] = nil
		} else if !textFormatOnly && formatCodes[i] == 1 {
			typ := rowOutputType(s[i].Type, resultFields, i)
			switch d := typ.(type) {
			case *pgtypes.DoltgresType:
				d, err = resolveDoltgresWireType(ctx, d)
				if err != nil {
					return nil, err
				}
				o[i], err = d.CallSend(ctx, v)
				if err != nil {
					return nil, err
				}
			default:
				cast := pgexprs.NewGMSCast(expression.NewLiteral(v, d))
				v, err = cast.Eval(ctx, nil)
				if err != nil {
					return nil, err
				}
				o[i], err = cast.DoltgresType(ctx).CallSend(ctx, v)
				if err != nil {
					return nil, err
				}
			}
		} else {
			typ := rowOutputType(s[i].Type, resultFields, i)
			if d, ok := typ.(*pgtypes.DoltgresType); ok {
				typ, err = resolveDoltgresWireType(ctx, d)
				if err != nil {
					return nil, err
				}
			}
			val, err := typ.SQL(ctx, []byte{}, v) // We use []byte{} as there's a distinction between nil and empty
			if err != nil {
				return nil, err
			}
			o[i] = val.ToBytes()
		}
	}
	return o, nil
}

func rowOutputType(schemaType sql.Type, resultFields []pgproto3.FieldDescription, index int) sql.Type {
	if index >= len(resultFields) {
		return schemaType
	}
	internalID := id.Type(id.Cache().ToInternal(resultFields[index].DataTypeOID))
	doltgresType, ok := pgtypes.IDToBuiltInDoltgresType[internalID]
	if !ok || doltgresType == nil || doltgresType.ID == pgtypes.Unknown.ID {
		return schemaType
	}
	if typmod := resultFields[index].TypeModifier; typmod != -1 {
		doltgresType = doltgresType.WithAttTypMod(typmod)
	}
	return doltgresType
}

func resolveDoltgresWireType(ctx *sql.Context, typ *pgtypes.DoltgresType) (*pgtypes.DoltgresType, error) {
	if typ.IsResolvedType() {
		return typ, nil
	}
	typesCollection, err := core.GetTypesCollectionFromContext(ctx)
	if err != nil {
		return nil, err
	}
	resolvedTyp, err := typesCollection.GetType(ctx, typ.ID)
	if err != nil {
		return nil, err
	}
	if resolvedTyp == nil && typ.ID.SchemaName() == "" {
		schemaName, err := core.GetSchemaName(ctx, nil, "")
		if err != nil {
			return nil, err
		}
		resolvedTyp, err = typesCollection.GetType(ctx, id.NewType(schemaName, typ.ID.TypeName()))
		if err != nil {
			return nil, err
		}
		if resolvedTyp == nil {
			resolvedTyp, err = typesCollection.GetType(ctx, id.NewType("pg_catalog", typ.ID.TypeName()))
			if err != nil {
				return nil, err
			}
		}
	}
	if resolvedTyp == nil {
		return nil, pgtypes.ErrTypeDoesNotExist.New(typ.Name())
	}
	if typmod := typ.GetAttTypMod(); typmod != -1 {
		return resolvedTyp.WithAttTypMod(typmod), nil
	}
	return resolvedTyp, nil
}
